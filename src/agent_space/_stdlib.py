"""WebSocket (RFC 6455) and HTTP with server-sent events, client and server, in the standard
library on asyncio streams. The same wire format as `_aiohttp` and the TypeScript SDK:

- WebSocket: one text frame per message.
- HTTP: `GET` opens an event stream whose first event, `session`, carries `{"session": id}`; each
  message from the space is an event `message`. The participant `POST`s each message with the
  header `ASP-Session: id`; the space answers 202, or 404 for a session it does not know.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import ssl
import struct
from typing import Any, Callable
from urllib.parse import urlparse

from .transport import Connection, Listener
from .util import random_token

MAX_MESSAGE = 64 * 1024 * 1024


def _accept_key(key: str) -> str:
    return base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()


async def _read_head(reader: asyncio.StreamReader) -> tuple[str, dict[str, str]]:
    """The first line and the headers (names in lower case) of an HTTP message."""
    raw = await reader.readuntil(b"\r\n\r\n")
    lines = raw.decode("latin-1").split("\r\n")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" in line:
            name, value = line.split(":", 1)
            headers[name.strip().lower()] = value.strip()
    return lines[0], headers


def _open(url: Any) -> Any:
    secure = url.scheme in ("wss", "https")
    port = url.port or (443 if secure else 80)
    context = ssl.create_default_context() if secure else None
    return asyncio.open_connection(url.hostname, port, ssl=context, server_hostname=url.hostname if secure else None, limit=MAX_MESSAGE)


def _target(url: Any) -> str:
    return (url.path or "/") + (f"?{url.query}" if url.query else "")


def _host_header(url: Any) -> str:
    return url.hostname + (f":{url.port}" if url.port else "")


# ── WebSocket ──


class _WebSocketConnection(Connection):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, peer: str, mask: bool):
        super().__init__()
        self._r = reader
        self._w = writer
        self._mask = mask
        self.peer = peer
        self._lock = asyncio.Lock()
        self._reader_task = asyncio.get_running_loop().create_task(self._read_loop())

    async def _frame(self, opcode: int, payload: bytes) -> None:
        head = bytes([0x80 | opcode])
        n = len(payload)
        mask_bit = 0x80 if self._mask else 0
        if n < 126:
            head += bytes([mask_bit | n])
        elif n < 65536:
            head += bytes([mask_bit | 126]) + struct.pack("!H", n)
        else:
            head += bytes([mask_bit | 127]) + struct.pack("!Q", n)
        if self._mask:
            key = os.urandom(4)
            payload = bytes(b ^ key[i % 4] for i, b in enumerate(payload)) if n < 4096 else _xor(payload, key)
            head += key
        async with self._lock:
            self._w.write(head + payload)
            await self._w.drain()

    async def _read_loop(self) -> None:
        parts: list[bytes] = []
        try:
            while True:
                b1, b2 = await self._r.readexactly(2)
                fin, opcode = b1 & 0x80, b1 & 0x0F
                n = b2 & 0x7F
                if n == 126:
                    n = struct.unpack("!H", await self._r.readexactly(2))[0]
                elif n == 127:
                    n = struct.unpack("!Q", await self._r.readexactly(8))[0]
                if n > MAX_MESSAGE:
                    break
                key = await self._r.readexactly(4) if b2 & 0x80 else None
                payload = await self._r.readexactly(n)
                if key:
                    payload = _xor(payload, key)
                if opcode == 0x8:  # close: answer it and stop
                    try:
                        await self._frame(0x8, payload[:2])
                    except Exception:
                        pass
                    break
                if opcode == 0x9:
                    await self._frame(0xA, payload)
                    continue
                if opcode == 0xA:
                    continue
                if opcode in (0x1, 0x2, 0x0):
                    parts.append(payload)
                    if fin:
                        data, parts = b"".join(parts), []
                        self._receive(data.decode("utf-8", errors="replace"))
        except (asyncio.IncompleteReadError, ConnectionError, OSError, asyncio.CancelledError):
            pass
        except Exception:
            pass
        self._fire_close()
        try:
            self._w.close()
        except Exception:
            pass

    async def _write(self, text: str) -> None:
        await self._frame(0x1, text.encode("utf-8"))

    async def _shutdown(self, abrupt: bool) -> None:
        try:
            if abrupt:
                self._w.transport.abort()
            else:
                await asyncio.wait_for(self._frame(0x8, struct.pack("!H", 1000)), 2)
                self._w.close()
        except Exception:
            pass

    async def wait_closed(self) -> None:
        await self._reader_task


def _xor(payload: bytes, key: bytes) -> bytes:
    n = len(payload)
    k = (key * (n // 4 + 1))[:n]
    return (int.from_bytes(payload, "big") ^ int.from_bytes(k, "big")).to_bytes(n, "big") if n else b""


# ── HTTP with server-sent events ──


class _SseServerConnection(Connection):
    def __init__(self, writer: asyncio.StreamWriter, peer: str):
        super().__init__()
        self._w = writer
        self.peer = peer
        self.done = asyncio.get_running_loop().create_future()

    async def chunk(self, data: bytes) -> None:
        self._w.write(f"{len(data):x}\r\n".encode() + data + b"\r\n")
        await self._w.drain()

    async def _write(self, text: str) -> None:
        await self.chunk(f"event: message\ndata: {text}\n\n".encode("utf-8"))

    async def _shutdown(self, abrupt: bool) -> None:
        if not self.done.done():
            self.done.set_result(None)

    def deliver(self, text: str) -> None:
        self._receive(text)


class _SseClientConnection(Connection):
    def __init__(self, url: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, chunked: bool, session_id: str, rest: list[str]):
        super().__init__()
        self.peer = url
        self._url = urlparse(url)
        self._r = reader
        self._w = writer
        self._chunked = chunked
        self._id = session_id
        self._lines = rest
        self._reader_task = asyncio.get_running_loop().create_task(self._read_loop())

    async def _read_loop(self) -> None:
        try:
            event, data = "message", []
            async for line in _lines(self._r, self._chunked, self._lines):
                if not line:
                    if data and event == "message":
                        self._receive("\n".join(data))
                    event, data = "message", []
                elif line.startswith("event:"):
                    event = line[6:].strip()
                elif line.startswith("data:"):
                    data.append(line[6:] if line[5:6] == " " else line[5:])
        except Exception:
            pass
        self._fire_close()
        try:
            self._w.close()
        except Exception:
            pass

    async def _write(self, text: str) -> None:
        body = text.encode("utf-8")
        reader, writer = await _open(self._url)
        try:
            writer.write(
                (f"POST {_target(self._url)} HTTP/1.1\r\nHost: {_host_header(self._url)}\r\nContent-Type: application/json\r\n"
                 f"ASP-Session: {self._id}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode("latin-1") + body
            )
            await writer.drain()
            status, _ = await _read_head(reader)
        finally:
            writer.close()
        if " 404" in status:
            self.drop()

    async def _shutdown(self, abrupt: bool) -> None:
        try:
            self._w.transport.abort() if abrupt else self._w.close()
        except Exception:
            pass


async def _lines(reader: asyncio.StreamReader, chunked: bool, pending: list[str]) -> Any:
    """The lines of a streamed response body, chunked or not."""
    for line in pending:
        yield line
    buffer = b""
    while True:
        if chunked:
            size_line = await reader.readuntil(b"\r\n")
            size = int(size_line.split(b";")[0].strip() or b"0", 16)
            if size == 0:
                return
            data = await reader.readexactly(size)
            await reader.readexactly(2)
        else:
            data = await reader.read(65536)
            if not data:
                return
        buffer += data
        while True:
            i = buffer.find(b"\n")
            if i < 0:
                break
            line, buffer = buffer[:i], buffer[i + 1:]
            yield line.rstrip(b"\r").decode("utf-8")


# ── Servers and clients ──


async def listen(address: str, accept: Callable[[Connection], None]) -> Listener:
    url = urlparse(address)
    host = url.hostname or "127.0.0.1"
    path = url.path or "/"
    websocket = url.scheme == "ws"
    connections: set[Connection] = set()
    sessions: dict[str, _SseServerConnection] = {}

    def respond(writer: asyncio.StreamWriter, status: str, body: bytes = b"") -> None:
        writer.write(f"HTTP/1.1 {status}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode() + body)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = (writer.get_extra_info("peername") or ("",))[0]
        try:
            start, headers = await _read_head(reader)
            method, target = start.split(" ")[:2]
            if urlparse(target).path != path:
                respond(writer, "404 Not Found")
            elif websocket:
                if method != "GET" or headers.get("upgrade", "").lower() != "websocket" or "sec-websocket-key" not in headers:
                    respond(writer, "400 Bad Request")
                else:
                    writer.write(
                        ("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                         f"Sec-WebSocket-Accept: {_accept_key(headers['sec-websocket-key'])}\r\n\r\n").encode()
                    )
                    await writer.drain()
                    ws = _WebSocketConnection(reader, writer, peer, mask=False)
                    connections.add(ws)
                    accept(ws)
                    await ws.wait_closed()
                    connections.discard(ws)
                    return
            elif method == "GET":
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nTransfer-Encoding: chunked\r\n\r\n")
                conn = _SseServerConnection(writer, peer)
                session_id = random_token(16)
                sessions[session_id] = conn
                connections.add(conn)
                await conn.chunk(f"event: session\ndata: {json.dumps({'session': session_id})}\n\n".encode())
                accept(conn)
                closed = asyncio.get_running_loop().create_task(reader.read())  # the participant hung up
                try:
                    while not conn.done.done() and not closed.done():
                        await asyncio.wait({conn.done, closed}, timeout=15, return_when=asyncio.FIRST_COMPLETED)
                        if not conn.done.done() and not closed.done():
                            await conn.chunk(b": keep-alive\n\n")
                except (ConnectionError, OSError, asyncio.CancelledError):
                    pass
                finally:
                    closed.cancel()
                    sessions.pop(session_id, None)
                    connections.discard(conn)
                    conn._fire_close()
                try:
                    writer.write(b"0\r\n\r\n")
                except Exception:
                    pass
            elif method == "POST":
                size = int(headers.get("content-length", "0") or 0)
                body = await reader.readexactly(size) if size else b""
                conn = sessions.get(headers.get("asp-session", ""))
                if conn is None:
                    respond(writer, "404 Not Found")
                else:
                    conn.deliver(body.decode("utf-8", errors="replace"))
                    respond(writer, "202 Accepted")
            else:
                respond(writer, "405 Method Not Allowed")
            await writer.drain()
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, ConnectionError, OSError, ValueError):
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    server = await asyncio.start_server(handle, host, url.port or 0, limit=MAX_MESSAGE)
    real = server.sockets[0].getsockname()[1]
    suffix = "" if path == "/" else path

    async def close() -> None:
        for c in list(connections):
            c.drop()
        server.close()
        try:
            await asyncio.wait_for(server.wait_closed(), 5)
        except asyncio.TimeoutError:
            pass

    return Listener(f"{url.scheme}://{url.hostname}:{real}{suffix}", close)


async def connect(address: str) -> Connection:
    url = urlparse(address)
    reader, writer = await _open(url)
    try:
        if url.scheme in ("ws", "wss"):
            key = base64.b64encode(os.urandom(16)).decode()
            writer.write(
                (f"GET {_target(url)} HTTP/1.1\r\nHost: {_host_header(url)}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                 f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode()
            )
            await writer.drain()
            status, headers = await _read_head(reader)
            if " 101" not in status or headers.get("sec-websocket-accept") != _accept_key(key):
                raise ConnectionError(f"{status.strip()} opening {address}")
            return _WebSocketConnection(reader, writer, address, mask=True)
        writer.write(f"GET {_target(url)} HTTP/1.1\r\nHost: {_host_header(url)}\r\nAccept: text/event-stream\r\n\r\n".encode())
        await writer.drain()
        status, headers = await _read_head(reader)
        if " 200" not in status:
            raise ConnectionError(f"{status.strip()} opening {address}")
        chunked = "chunked" in headers.get("transfer-encoding", "").lower()
        lines = _lines(reader, chunked, [])
        session_id, event, seen = "", "", []
        async for line in lines:
            if line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:") and event == "session":
                session_id = json.loads(line[5:].strip())["session"]
            elif not line and session_id:
                break
        if not session_id:
            raise ConnectionError(f"no session opening {address}")
        await lines.aclose()
        return _SseClientConnection(address, reader, writer, chunked, session_id, seen)
    except BaseException:
        writer.close()
        raise
