"""The WebSocket and HTTP-with-SSE transports on aiohttp (the `aiohttp` extra): a server and a
client for many connections. The standard-library ones in `_stdlib` speak the same wire format."""

from __future__ import annotations

import asyncio
import json
from urllib.parse import urlparse

import aiohttp
from aiohttp import web

from .transport import Connection, Listener
from .util import random_token


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


async def listen(address: str, accept) -> Listener:
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
    raise ValueError(f"not a WebSocket or HTTP address: {address}")
