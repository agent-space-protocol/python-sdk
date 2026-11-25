"""Transports: WebSocket, HTTP with server-sent events, and standard input and output.

A connection carries whole JSON messages. `send` never blocks: messages go out in order through a
writer task, so a space can answer and push events from synchronous code without reordering.
"""

from __future__ import annotations

import asyncio
import json
import sys
import threading
from dataclasses import dataclass
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

import aiohttp
from aiohttp import web

from .util import random_token


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


class _WebSocketConnection(Connection):
    def __init__(self, ws: web.WebSocketResponse | aiohttp.ClientWebSocketResponse, peer: str, session: aiohttp.ClientSession | None = None):
        super().__init__()
        self._ws = ws
        self.peer = peer
        self._session = session
        self._reader = asyncio.get_running_loop().create_task(self._read_loop())

    async def _read_loop(self) -> None:
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self._receive(msg.data)
                elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                    break
        except Exception:
            pass
        self._fire_close()
        if self._session:
            await self._session.close()

    async def _write(self, text: str) -> None:
        await self._ws.send_str(text)

    async def _shutdown(self, abrupt: bool) -> None:
        try:
            if abrupt:
                transport = getattr(getattr(self._ws, "_writer", None), "transport", None) or getattr(getattr(self._ws, "_conn", None), "transport", None)
                if transport is not None:
                    transport.abort()
                else:
                    await self._ws.close()
            else:
                await self._ws.close()
        except Exception:
            pass
        if self._session:
            await self._session.close()

    async def wait_closed(self) -> None:
        await self._reader


class _SseServerConnection(Connection):
    def __init__(self, response: web.StreamResponse, peer: str):
        super().__init__()
        self._response = response
        self.peer = peer
        self.done = asyncio.get_running_loop().create_future()

    async def _write(self, text: str) -> None:
        await self._response.write(f"event: message\ndata: {text}\n\n".encode("utf-8"))

    async def _shutdown(self, abrupt: bool) -> None:
        if not self.done.done():
            self.done.set_result(None)

    def deliver(self, text: str) -> None:
        self._receive(text)


class _SseClientConnection(Connection):
    def __init__(self, url: str, session: aiohttp.ClientSession, response: aiohttp.ClientResponse, session_id: str):
        super().__init__()
        self.peer = url
        self._url = url
        self._session = session
        self._response = response
        self._id = session_id
        self._reader = asyncio.get_running_loop().create_task(self._read_loop())

    async def _read_loop(self) -> None:
        try:
            event, data = "message", []
            async for raw in self._response.content:
                line = raw.decode("utf-8").rstrip("\r\n")
                if not line:
                    if data and event == "message":
                        self._receive("\n".join(data))
                    event, data = "message", []
                elif line.startswith("event:"):
                    event = line[6:].strip()
                elif line.startswith("data:"):
                    data.append(line[5:].lstrip(" ") if line[5:6] == " " else line[5:])
        except Exception:
            pass
        self._fire_close()
        await self._session.close()

    async def _write(self, text: str) -> None:
        async with self._session.post(self._url, data=text.encode("utf-8"), headers={"Content-Type": "application/json", "ASP-Session": self._id}) as res:
            if res.status == 404:
                self.drop()

    async def _shutdown(self, abrupt: bool) -> None:
        self._response.close()
        await self._session.close()


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


async def listen(address: str, accept: Callable[[Connection], None]) -> Listener:
    """Listens at an address (`ws://host:port`, `http://host:port`, or `stdio`); hands every new connection to `accept`."""
    if address == "stdio":
        conn = _StreamConnection()
        accept(conn)

        async def close_stdio() -> None:
            conn.close()

        return Listener("stdio", close_stdio)
    url = urlparse(address)
    host = url.hostname or "127.0.0.1"
    path = url.path or "/"
    app = web.Application()
    connections: set[Connection] = set()
    sessions: dict[str, _SseServerConnection] = {}

    if url.scheme == "ws":

        async def ws_handler(request: web.Request) -> web.StreamResponse:
            ws = web.WebSocketResponse(max_msg_size=64 * 1024 * 1024, autoping=True)
            await ws.prepare(request)
            conn = _WebSocketConnection(ws, request.remote or "")
            connections.add(conn)
            accept(conn)
            await conn.wait_closed()
            connections.discard(conn)
            return ws

        app.router.add_get(path, ws_handler)
    elif url.scheme == "http":

        async def get_handler(request: web.Request) -> web.StreamResponse:
            response = web.StreamResponse(headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"})
            await response.prepare(request)
            session_id = random_token(16)
            conn = _SseServerConnection(response, request.remote or "")
            sessions[session_id] = conn
            connections.add(conn)
            await response.write(f"event: session\ndata: {json.dumps({'session': session_id})}\n\n".encode())
            accept(conn)
            try:
                while not conn.done.done():
                    try:
                        await asyncio.wait_for(asyncio.shield(conn.done), 15)
                    except asyncio.TimeoutError:
                        await response.write(b": keep-alive\n\n")
            except (ConnectionResetError, asyncio.CancelledError, aiohttp.ClientConnectionResetError):
                pass
            finally:
                sessions.pop(session_id, None)
                connections.discard(conn)
                conn._fire_close()
            return response

        async def post_handler(request: web.Request) -> web.StreamResponse:
            conn = sessions.get(request.headers.get("ASP-Session", ""))
            if conn is None:
                return web.Response(status=404)
            text = await request.text()
            conn.deliver(text)
            return web.Response(status=202)

        app.router.add_get(path, get_handler)
        app.router.add_post(path, post_handler)
    else:
        raise ValueError(f"cannot listen at {address}")

    runner = web.AppRunner(app, handle_signals=False, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host, url.port or 0)
    await site.start()
    real = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    suffix = "" if path == "/" else path

    async def close() -> None:
        for c in list(connections):
            c.drop()
        await runner.cleanup()

    return Listener(f"{url.scheme}://{url.hostname}:{real}{suffix}", close)


async def connect(address: str) -> Connection:
    """Connects to a `ws://`/`wss://` URL, an `http://`/`https://` URL, or a command that starts a space on standard input and output."""
    if address.startswith(("ws://", "wss://")):
        session = aiohttp.ClientSession()
        try:
            ws = await session.ws_connect(address, max_msg_size=64 * 1024 * 1024)
        except Exception:
            await session.close()
            raise
        return _WebSocketConnection(ws, address, session)
    if address.startswith(("http://", "https://")):
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_read=None))
        try:
            response = await session.get(address, headers={"Accept": "text/event-stream"})
            if response.status != 200:
                raise ConnectionError(f"HTTP {response.status} opening {address}")
            session_id = ""
            event = ""
            async for raw in response.content:
                line = raw.decode("utf-8").strip()
                if line.startswith("event:"):
                    event = line[6:].strip()
                elif line.startswith("data:") and event == "session":
                    session_id = json.loads(line[5:].strip())["session"]
                elif not line and session_id:
                    break
        except Exception:
            await session.close()
            raise
        return _SseClientConnection(address, session, response, session_id)
    process = await asyncio.create_subprocess_shell(address, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
    return ProcessConnection(process, address)
