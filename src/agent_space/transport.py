"""Transports: WebSocket, HTTP with server-sent events, and standard input and output.

The base needs only the standard library (`_stdlib`). With the `aiohttp` extra installed, WebSocket
and HTTP use aiohttp instead (`_aiohttp`), for spaces that serve many connections; both speak the
same wire format. `AGENT_SPACE_TRANSPORT=stdlib` keeps the standard-library ones.

A connection carries whole JSON messages. `send` never blocks: messages go out in order through a
writer task, so a space can answer and push events from synchronous code without reordering.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
from dataclasses import dataclass
from typing import Any, Awaitable, Callable



class Connection:
    """One connection between a participant and a space."""

    peer: str = ""

    def __init__(self) -> None:
        self.closed = False
        self._on_message: Callable[[str], None] | None = None
        self._on_close: Callable[[], None] | None = None
        self._pending: list[str] = []
        self._close_fired = False
        self._queue: asyncio.Queue[str | None] = asyncio.Queue()
        self._writer = asyncio.get_running_loop().create_task(self._write_loop())

    # Handlers: messages received before a handler is set are kept for it.
    @property
    def on_message(self) -> Callable[[str], None] | None:
        return self._on_message

    @on_message.setter
    def on_message(self, fn: Callable[[str], None] | None) -> None:
        self._on_message = fn
        if fn:
            pending, self._pending = self._pending, []
            for text in pending:
                fn(text)

    @property
    def on_close(self) -> Callable[[], None] | None:
        return self._on_close

    @on_close.setter
    def on_close(self, fn: Callable[[], None] | None) -> None:
        self._on_close = fn
        if fn and self._close_fired:
            asyncio.get_running_loop().call_soon(fn)

    def send(self, message: dict[str, Any]) -> None:
        """Sends a message. Messages sent after closing are dropped."""
        self.send_raw(json.dumps(message, ensure_ascii=False, separators=(",", ":")))

    def send_raw(self, text: str) -> None:
        if not self.closed:
            self._queue.put_nowait(text)

    def close(self) -> None:
        """Closes the connection after the messages already sent."""
        if self.closed:
            return
        self.closed = True
        self._queue.put_nowait(None)

    async def aclose(self) -> None:
        """Closes the connection and waits until it is shut down."""
        self.close()
        try:
            await asyncio.wait_for(asyncio.shield(self._writer), 5)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass

    def drop(self) -> None:
        """Drops the connection at once, as a network failure would."""
        self.closed = True
        self._writer.cancel()
        asyncio.get_running_loop().create_task(self._shutdown(abrupt=True))
        self._fire_close()

    async def _write_loop(self) -> None:
        try:
            while True:
                text = await self._queue.get()
                if text is None:
                    break
                await self._write(text)
        except asyncio.CancelledError:
            return
        except Exception:
            pass
        await self._shutdown(abrupt=False)
        self._fire_close()

    def _receive(self, text: str) -> None:
        if self.closed and not self._close_fired:
            pass
        if self._on_message:
            try:
                self._on_message(text)
            except Exception as e:  # a handler error must not end the connection
                print(f"asp: error handling a message: {e!r}", file=sys.stderr)
        else:
            self._pending.append(text)

    def _fire_close(self) -> None:
        self.closed = True
        if self._close_fired:
            return
        self._close_fired = True
        if not self._writer.done():
            self._writer.cancel()
        if self._on_close:
            self._on_close()

    async def _write(self, text: str) -> None:
        raise NotImplementedError

    async def _shutdown(self, abrupt: bool) -> None:
        raise NotImplementedError


class _StreamConnection(Connection):
    """Standard input and output of this process, read on a thread (portable to Windows)."""

    def __init__(self) -> None:
        super().__init__()
        self.peer = "stdio"
        loop = asyncio.get_running_loop()
        self._loop = loop

        def read() -> None:
            for raw in sys.stdin.buffer:
                line = raw.decode("utf-8", errors="replace").strip()
                if line:
                    loop.call_soon_threadsafe(self._receive, line)
            loop.call_soon_threadsafe(self._fire_close)

        threading.Thread(target=read, daemon=True).start()

    async def _write(self, text: str) -> None:
        sys.stdout.buffer.write((text.replace("\n", "\\n") + "\n").encode("utf-8"))
        sys.stdout.buffer.flush()

    async def _shutdown(self, abrupt: bool) -> None:
        try:
            sys.stdout.flush()
        except Exception:
            pass


class ProcessConnection(Connection):
    """A connection to a space running as a child process, over its standard input and output."""

    def __init__(self, process: asyncio.subprocess.Process, command: str):
        super().__init__()
        self.process = process
        self.peer = command
        self._reader = asyncio.get_running_loop().create_task(self._read_loop())

    async def _read_loop(self) -> None:
        assert self.process.stdout
        try:
            while True:
                line = await self.process.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8").strip()
                if text:
                    self._receive(text)
        except Exception:
            pass
        self._fire_close()

    async def _write(self, text: str) -> None:
        assert self.process.stdin
        self.process.stdin.write((text.replace("\n", "\\n") + "\n").encode("utf-8"))
        await self.process.stdin.drain()

    async def _shutdown(self, abrupt: bool) -> None:
        try:
            if self.process.stdin:
                self.process.stdin.close()
        except Exception:
            pass
        if abrupt and self.process.returncode is None:
            self.process.kill()
        try:
            await asyncio.wait_for(self.process.wait(), 10)
        except asyncio.TimeoutError:
            self.process.kill()
            await self.process.wait()


@dataclass
class Listener:
    """A running listener: the address participants connect to, with the real port."""

    address: str
    _close: Callable[[], Awaitable[None]]

    async def close(self) -> None:
        await self._close()


def implementation() -> str:
    """Which WebSocket and HTTP implementation is used: "aiohttp" or "stdlib"."""
    if os.environ.get("AGENT_SPACE_TRANSPORT", "").lower() == "stdlib":
        return "stdlib"
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        return "stdlib"
    return "aiohttp"


def _net():  # type: ignore[no-untyped-def]
    if implementation() == "aiohttp":
        from . import _aiohttp

        return _aiohttp
    from . import _stdlib

    return _stdlib


async def listen(address: str, accept: Callable[[Connection], None]) -> Listener:
    """Listens at an address (`ws://host:port`, `http://host:port`, or `stdio`); hands every new connection to `accept`."""
    if address == "stdio":
        conn = _StreamConnection()
        accept(conn)

        async def close_stdio() -> None:
            conn.close()

        return Listener("stdio", close_stdio)
    if not address.startswith(("ws://", "http://")):
        raise ValueError(f"cannot listen at {address}")
    return await _net().listen(address, accept)


async def connect(address: str) -> Connection:
    """Connects to a `ws://`/`wss://` URL, an `http://`/`https://` URL, or a command that starts a space on standard input and output."""
    if address.startswith(("ws://", "wss://", "http://", "https://")):
        return await _net().connect(address)
    process = await asyncio.create_subprocess_shell(address, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
    return ProcessConnection(process, address)
