"""The participant side: connect, shake hands, enter, observe, subscribe (resuming by sequence
number after a disconnect), act with generated IDs, basis and deadlines, receive intermediate
states and exactly one final outcome, cancel, and fetch the record and the vocabulary.

Asynchronous::

    async with await Participant.connect("ws://127.0.0.1:7777", key=generate_key()) as p:
        await p.join(role="member")
        observation = await p.observe()
        outcome = await p.act("room.toggle", object="lamp:1", basis=observation["seq"])

Synchronous, for code without an event loop::

    with SyncParticipant.connect("ws://127.0.0.1:7777") as p:
        p.join()
        outcome = p.act("room.toggle", object="lamp:1")

Outcomes carry a typed `End` and a `Reason` whose `code` is a `ReasonCode` for the protocol's own
reasons. Record entries are plain dicts with a `type` (the kind), so a reaction can be keyed on it.
"""

from __future__ import annotations

import asyncio
import json
import threading
from concurrent.futures import Future as ThreadFuture
from typing import Any, Awaitable, Callable, Coroutine, TypeVar

from .identity import KeyPair, make_proof
from .protocol import INTERMEDIATE, SDK_PARTS, Outcome, OutcomeError
from .transport import Connection, connect
from .util import random_token

T = TypeVar("T")
Listener = Callable[[Any], Any]

_FINALS = {"observe": "observation", "subscribe": "subscribed", "record": "entries", "vocabulary": "vocabulary", "blob": "blob"}


class Pending(Awaitable[Outcome]):
    """A request under way: its ID, its intermediate states, and its one final answer.

    Awaiting it gives the `Outcome` (or, for queries, the reply message as a dict)."""

    def __init__(self, participant: Participant, request_id: str, future: asyncio.Future[Any]):
        self.id = request_id
        self._participant = participant
        self._future = future
        self._updates: list[Callable[[dict[str, Any]], Any]] = []

    def __await__(self):  # type: ignore[override]
        return self._future.__await__()

    @property
    def done(self) -> asyncio.Future[Any]:
        return self._future

    def on_update(self, fn: Callable[[dict[str, Any]], Any]) -> Pending:
        """Calls `fn` with each intermediate state: accepted, progress, waiting, frame."""
        self._updates.append(fn)
        return self

    def cancel(self) -> None:
        """Asks to cancel it. It still gets its own final answer."""
        self._participant.cancel(self.id)


class Participant:
    """A participant connected to one space."""

    def __init__(self, address: str, *, key: KeyPair | None = None, token: str | None = None, parts: dict[str, list[int]] | None = None, client: dict[str, Any] | None = None, reconnect: bool = True, reconnect_for: float = 60.0):
        self.address = address
        self.key = key
        self.token = token
        self._parts = parts or SDK_PARTS
        self._client = client or {"name": "agent-space-protocol (python)"}
        self._reconnect = reconnect
        self._reconnect_for = reconnect_for
        self.hello: dict[str, Any] = {}
        self.presence: dict[str, Any] | None = None
        self.last_seq = 0
        self._conn: Connection | None = None
        self._requests: dict[str, dict[str, Any]] = {}
        self._outbox: list[dict[str, Any]] = []
        self._listeners: dict[str, list[Listener]] = {}
        self._subscription: dict[str, Any] | None = None
        self._vocabulary: tuple[str, dict[str, Any]] | None = None
        self._connected = False
        self._closing = False
        self._counter = 0
        self._prefix = random_token(4)
        self._hello_waiter: asyncio.Future[dict[str, Any]] | None = None
        self._reconnecting: asyncio.Task[None] | None = None

    @classmethod
    async def connect(cls, address: str, **options: Any) -> Participant:
        """Connects to a space and shakes hands."""
        p = cls(address, **options)
        await p._open()
        return p

    async def __aenter__(self) -> Participant:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    @property
    def parts(self) -> dict[str, int]:
        """The negotiated version of each part."""
        return self.hello.get("parts", {})

    @property
    def connected(self) -> bool:
        return self._connected

    def on(self, name: str, fn: Listener) -> Callable[[], None]:
        """Listens: `event` (a record entry, each once and in order), `outcome`, `message`,
        `disconnect`, `reconnect`, `closing`, `lost` (the presence could not be resumed)."""
        self._listeners.setdefault(name, []).append(fn)
        return lambda: self._listeners[name].remove(fn)

    def _fire(self, name: str, value: Any = None) -> None:
        for fn in list(self._listeners.get(name, [])):
            try:
                result = fn(value) if value is not None or name in ("event", "outcome", "message") else fn()
                if asyncio.iscoroutine(result):
                    asyncio.get_running_loop().create_task(result)
            except Exception as e:
                print(f"asp participant: a {name} listener raised {e!r}")

    def next_id(self, prefix: str = "r") -> str:
        self._counter += 1
        return f"{prefix}{self._counter}-{self._prefix}"

    async def _open(self) -> None:
        conn = await connect(self.address)
        self._conn = conn
        self._hello_waiter = asyncio.get_running_loop().create_future()
        conn.on_message = self._on_message
        conn.on_close = lambda: self._on_close(conn)
        hello: dict[str, Any] = {"type": "hello", "parts": self._parts, "client": self._client}
        if self.key:
            hello["participant"] = self.key.identity
        if self.token:
            hello["token"] = self.token
        conn.send(hello)
        self.hello = await asyncio.wait_for(self._hello_waiter, 10)
        self._connected = True

    def _on_message(self, text: str) -> None:
        try:
            msg = json.loads(text)
        except ValueError:
            return
        self._fire("message", msg)
        mtype = msg.get("type")
        if mtype == "hello":
            if self._hello_waiter and not self._hello_waiter.done():
                self._hello_waiter.set_result(msg)
            return
        if mtype == "error" and self._hello_waiter and not self._hello_waiter.done():
            self._hello_waiter.set_exception(ConnectionError(f"{msg.get('code')}: {msg.get('text', '')}"))
            return
        if mtype == "event":
            entry = msg["entry"]
            if entry.get("type") == "asp.closing":
                self._fire("closing")
            if not self._subscription or entry["seq"] <= self.last_seq:
                return
            self.last_seq = entry["seq"]
            self._fire("event", entry)
            return
        rid = msg.get("id") if mtype != "frame" else msg.get("stream")
        req = self._requests.get(rid) if rid is not None else None
        if not req or req["future"].done():
            return
        if mtype == "outcome" or mtype == req["final"]:
            self._requests.pop(rid, None)
            if mtype == "outcome":
                self._fire("outcome", Outcome.of(msg))
            req["future"].set_result(Outcome.of(msg) if mtype == "outcome" else msg)
        elif mtype in INTERMEDIATE:
            for fn in req["pending"]._updates:
                try:
                    fn(msg)
                except Exception as e:
                    print(f"asp participant: an update listener raised {e!r}")

    def _on_close(self, conn: Connection) -> None:
        if conn is not self._conn:
            return
        was = self._connected
        self._connected = False
        if self._closing:
            return
        if was:
            self._fire("disconnect")
        if not self._reconnect:
            for rid, req in list(self._requests.items()):
                if not req["future"].done():
                    req["future"].set_result(Outcome.of({"type": "outcome", "id": rid, "end": "unknown", "reason": {"code": "closed", "text": "the connection closed"}, "unconfirmed": ["delivered"]}))
            self._requests.clear()
            return
        if not self._reconnecting or self._reconnecting.done():
            self._reconnecting = asyncio.get_running_loop().create_task(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        loop = asyncio.get_running_loop()
        until = loop.time() + self._reconnect_for
        wait = 0.1
        while not self._closing and loop.time() < until:
            try:
                await self._open()
                if self.presence:
                    res = await self._raw({"type": "join", "id": self.next_id("j"), "resume": self.presence["resume"], **self._proof()})
                    if not isinstance(res, Outcome) or not res.done:
                        self.presence = None
                        self._fire("lost")
                if self._subscription is not None:
                    await self._raw({"type": "subscribe", "id": self.next_id("s"), "from": self.last_seq + 1, **({"kinds": self._subscription["kinds"]} if self._subscription.get("kinds") else {})})
                assert self._conn
                for req in list(self._requests.values()):
                    self._conn.send(req["message"])
                outbox, self._outbox = self._outbox, []
                for m in outbox:
                    self._conn.send(m)
                self._fire("reconnect")
                return
            except Exception:
                await asyncio.sleep(wait)
                wait = min(wait * 2, 2.0)

    def _proof(self) -> dict[str, str]:
        return {"proof": make_proof(self.key, self.hello["space"], self.hello["nonce"])} if self.key else {}

    async def _raw(self, message: dict[str, Any]) -> Any:
        return await self.request(message)

    def request(self, message: dict[str, Any]) -> Pending:
        """Sends any request and returns it under way; the ID is generated when absent."""
        rid = message.get("id") or self.next_id(str(message["type"])[0])
        full = {**message, "id": rid}
        req = self._requests.get(rid)
        if req is None:
            future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
            pending = Pending(self, rid, future)
            req = {"message": full, "final": _FINALS.get(str(message["type"]), "outcome"), "future": future, "pending": pending}
            self._requests[rid] = req
        if self._connected and self._conn:
            self._conn.send(full)
        return req["pending"]

    def send(self, message: dict[str, Any]) -> None:
        """Sends a message as it is."""
        if self._connected and self._conn:
            self._conn.send(message)
        else:
            self._outbox.append(message)

    async def join(self, role: str | None = None, presentation: dict[str, Any] | None = None, pass_: str | None = None) -> dict[str, Any]:
        """Enters the space. Raises `OutcomeError` when refused."""
        msg: dict[str, Any] = {"type": "join", **self._proof()}
        if role:
            msg["role"] = role
        if presentation:
            msg["presentation"] = presentation
        if pass_:
            msg["pass"] = pass_
        out = await self._raw(msg)
        if not out.done:
            raise OutcomeError(out)
        self.presence = out.result
        if self.last_seq == 0:
            self.last_seq = out.result["head"]
        return out.result

    async def leave(self, cancel: bool = False) -> Outcome:
        out = await self._raw({"type": "leave", **({"cancel": True} if cancel else {})})
        self.presence = None
        self._subscription = None
        return out

    async def observe(self, forms: list[str] | None = None) -> dict[str, Any]:
        """One observation. Raises `OutcomeError` when refused."""
        out = await self._raw({"type": "observe", **({"forms": forms} if forms else {})})
        if isinstance(out, Outcome):
            raise OutcomeError(out)
        return out

    async def subscribe(self, start: int | None = None, kinds: list[str] | None = None) -> dict[str, Any]:
        """Subscribes to the record from a sequence number on (the next new entry by default).
        Entries arrive through `on("event", ...)` or `events()`, each once and in order."""
        msg: dict[str, Any] = {"type": "subscribe"}
        if start is not None:
            msg["from"] = start
        if kinds:
            msg["kinds"] = kinds
        out = await self._raw(msg)
        if isinstance(out, Outcome):
            raise OutcomeError(out)
        self._subscription = {"kinds": kinds}
        self.last_seq = out["from"] - 1
        return out

    async def events(self) -> Any:
        """Record entries as an async iterator."""
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        off = self.on("event", queue.put_nowait)
        try:
            while True:
                yield await queue.get()
        finally:
            off()

    def act(self, verb: str, object: str | None = None, args: dict[str, Any] | None = None, *, basis: int | None = None, deadline: float | None = None, id: str | None = None, **extra: Any) -> Pending:
        """Takes an action. Await the result for its `Outcome`."""
        msg: dict[str, Any] = {"type": "act", "id": id or self.next_id("a"), "verb": verb, **extra}
        if object is not None:
            msg["object"] = object
        if args is not None:
            msg["args"] = args
        if basis is not None:
            msg["basis"] = basis
        if deadline is not None:
            msg["deadline"] = deadline
        return self.request(msg)

    def edit(self, changes: list[dict[str, Any]] | None = None, sentence: str | None = None, *, basis: int | None = None, deadline: float | None = None, id: str | None = None) -> Pending:
        msg: dict[str, Any] = {"type": "edit", "id": id or self.next_id("e")}
        if changes is not None:
            msg["changes"] = changes
        if sentence is not None:
            msg["sentence"] = sentence
        if basis is not None:
            msg["basis"] = basis
        if deadline is not None:
            msg["deadline"] = deadline
        return self.request(msg)

    def cancel(self, request_id: str) -> None:
        """Asks to cancel a request of this participant."""
        self.send({"type": "cancel", "action": request_id})

    async def record(self, start: int | None = None, to: int | None = None, since: str | None = None, until: str | None = None, limit: int | None = None) -> dict[str, Any]:
        msg: dict[str, Any] = {"type": "record"}
        for k, v in (("from", start), ("to", to), ("since", since), ("until", until), ("limit", limit)):
            if v is not None:
                msg[k] = v
        out = await self._raw(msg)
        if isinstance(out, Outcome):
            raise OutcomeError(out)
        return out

    async def vocabulary(self) -> dict[str, Any]:
        """The vocabulary, cached by hash."""
        if self._vocabulary and self._vocabulary[0] == self.hello.get("vocabulary"):
            return self._vocabulary[1]
        out = await self._raw({"type": "vocabulary"})
        if isinstance(out, Outcome):
            raise OutcomeError(out)
        self._vocabulary = (out["hash"], out["vocabulary"])
        return out["vocabulary"]

    async def blob(self, content: str) -> tuple[str, bytes]:
        import base64

        out = await self._raw({"type": "blob", "hash": content})
        if isinstance(out, Outcome):
            raise OutcomeError(out)
        return out["media"], base64.b64decode(out["data"])

    def drop(self) -> None:
        """Drops the connection as a network failure would; the participant reconnects. For tests."""
        if self._conn:
            self._conn.drop()

    async def close(self) -> None:
        """Closes the connection for good."""
        self._closing = True
        self._connected = False
        if self._reconnecting:
            self._reconnecting.cancel()
        if self._conn:
            await self._conn.aclose()


class SyncParticipant:
    """The same participant for code without an event loop: every call blocks until its answer.

    It runs an event loop on a background thread. `act` returns the `Outcome`; `act_async` returns
    a `concurrent.futures.Future` of it. Listeners registered with `on` are called on that thread."""

    def __init__(self, participant: Participant, loop: asyncio.AbstractEventLoop, thread: threading.Thread):
        self._p = participant
        self._loop = loop
        self._thread = thread

    @classmethod
    def connect(cls, address: str, timeout: float = 30.0, **options: Any) -> SyncParticipant:
        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=loop.run_forever, daemon=True, name="agent-space")
        thread.start()
        p = asyncio.run_coroutine_threadsafe(Participant.connect(address, **options), loop).result(timeout)
        return cls(p, loop, thread)

    def __enter__(self) -> SyncParticipant:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _run(self, coro: Coroutine[Any, Any, T], timeout: float | None = None) -> T:
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    @property
    def hello(self) -> dict[str, Any]:
        return self._p.hello

    @property
    def presence(self) -> dict[str, Any] | None:
        return self._p.presence

    @property
    def participant(self) -> Participant:
        """The asynchronous participant underneath, living on the background loop."""
        return self._p

    def on(self, name: str, fn: Listener) -> Callable[[], None]:
        return self._p.on(name, fn)

    def join(self, role: str | None = None, presentation: dict[str, Any] | None = None, timeout: float | None = 30.0) -> dict[str, Any]:
        return self._run(self._p.join(role, presentation), timeout)

    def observe(self, forms: list[str] | None = None, timeout: float | None = 30.0) -> dict[str, Any]:
        return self._run(self._p.observe(forms), timeout)

    def subscribe(self, start: int | None = None, kinds: list[str] | None = None, timeout: float | None = 30.0) -> dict[str, Any]:
        return self._run(self._p.subscribe(start, kinds), timeout)

    def act_async(self, verb: str, object: str | None = None, args: dict[str, Any] | None = None, **kw: Any) -> ThreadFuture[Outcome]:
        async def go() -> Outcome:
            return await self._p.act(verb, object, args, **kw)

        return asyncio.run_coroutine_threadsafe(go(), self._loop)

    def act(self, verb: str, object: str | None = None, args: dict[str, Any] | None = None, timeout: float | None = None, **kw: Any) -> Outcome:
        return self.act_async(verb, object, args, **kw).result(timeout)

    def edit(self, changes: list[dict[str, Any]] | None = None, sentence: str | None = None, timeout: float | None = None, **kw: Any) -> Outcome:
        async def go() -> Outcome:
            return await self._p.edit(changes, sentence, **kw)

        return self._run(go(), timeout)

    def cancel(self, request_id: str) -> None:
        self._loop.call_soon_threadsafe(self._p.cancel, request_id)

    def record(self, timeout: float | None = 30.0, **range_: Any) -> dict[str, Any]:
        return self._run(self._p.record(**range_), timeout)

    def vocabulary(self, timeout: float | None = 30.0) -> dict[str, Any]:
        return self._run(self._p.vocabulary(), timeout)

    def leave(self, cancel: bool = False, timeout: float | None = 30.0) -> Outcome:
        return self._run(self._p.leave(cancel), timeout)

    def close(self) -> None:
        try:
            self._run(self._p.close(), 5)
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(5)
