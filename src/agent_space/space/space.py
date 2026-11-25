"""The space skeleton. It keeps the protocol's guarantees for the implementer: exactly one final
outcome for every action (deadlines, cancels, closing and restarts included), idempotent request
IDs, `basis` and staleness, `unsupported` for what is not declared, `malformed` for broken
messages with an ID, the record by sequence number with subscriptions, resuming and expiry,
projection by role, presences kept for a grace period, consent, pausing and doors.

The implementer writes only how verbs are carried out and what the state looks like::

    space = Space(capabilities={...}, vocabulary={...})

    @space.observer
    def observe(ctx):
        return {"structure": {"things": [...]}}

    @space.verb("room.toggle")
    def toggle(ctx):
        ...
        ctx.emit("room.switched", {"on": True}, things=[ctx.object])
        return {"result": {"on": True}}
"""

from __future__ import annotations

import asyncio
import inspect
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Awaitable, Callable

from jsonschema import Draft202012Validator

from ..identity import KeyPair, check_proof, generate_key, make_pass, peek_pass, read_pass
from ..protocol import CAPABILITY_PARTS, MESSAGE_CAPABILITY, PARTICIPANT_MESSAGES, validate
from ..transport import Connection, Listener, listen
from ..util import canonical_json, content_hash, iso_after, now, random_token, strip_none
from .errors import Failed, Refused, Stopped, Uncertain
from .store import MemoryStore, Store

NOT_CHANGES = {
    "asp.act", "asp.edit", "asp.cancel", "asp.waiting", "asp.consent", "asp.join", "asp.leave",
    "asp.episode", "asp.score", "asp.snapshot", "asp.closing", "asp.restarted",
}
IDEMPOTENT = {"act", "edit", "leave", "stream", "consent", "snapshot", "restore", "episode", "pause", "resume", "sense", "offer"}
DEFAULT_ROLES = {"owner": {"verbs": ["*"], "edit": True, "consent": True}, "member": {"verbs": ["*"]}, "guest": {"verbs": []}}


@dataclass
class PresenceInfo:
    id: str
    role: str
    participant: str | None = None


@dataclass
class ObserveContext:
    presence: PresenceInfo
    forms: list[str]


class RequestContext:
    """What a law is given while it carries out an action, an edit or a stream."""

    def __init__(self, space: Space, running: _Running):
        self._space = space
        self._running = running
        self.id = running.id
        self.presence = _info(running.presence)
        self.act_seq = running.act_seq

    @property
    def stop_reason(self) -> str | None:
        """Why it was asked to stop: `cancel`, `deadline` or `closed`."""
        return self._running.stop_reason

    @property
    def stopping(self) -> bool:
        return self._running.stop_reason is not None

    def progress(self, progress: float | None = None, text: str | None = None) -> None:
        if not self._running.finished:
            self._space._intermediate(self._running, strip_none({"type": "progress", "id": self.id, "progress": progress, "text": text}), False)

    def emit(self, kind: str, data: Any = None, *, things: list[str] | None = None, roles: list[str] | None = None) -> None:
        """Emits an event, appended with the outcome in one commit."""
        if self._running.finished:
            return
        self._running.events.append(strip_none({"source": f"presence/{self._running.presence.id}", "type": kind, "cause": self.act_seq, "data": data, "things": things, "roles": roles}))

    async def checkpoint(self) -> None:
        """Waits while the action is paused; raises `Stopped` when it is asked to stop."""
        r = self._running
        while r.paused and r.stop_reason is None:
            await r.resumed.wait()
        if r.stop_reason is not None:
            raise Stopped()

    async def sleep(self, seconds: float) -> None:
        """Waits, counting only time not paused; raises `Stopped` when asked to stop."""
        left = seconds
        loop = asyncio.get_running_loop()
        while left > 0:
            await self.checkpoint()
            start = loop.time()
            step = min(left, 0.05)
            try:
                await asyncio.wait_for(self._running.stopped.wait(), step)
            except asyncio.TimeoutError:
                pass
            if not self._running.paused:
                left -= loop.time() - start
        await self.checkpoint()


class ActionContext(RequestContext):
    def __init__(self, space: Space, running: _Running, msg: dict[str, Any]):
        super().__init__(space, running)
        self.verb: str = msg["verb"]
        self.object: str | None = msg.get("object")
        self.args: dict[str, Any] = msg.get("args") or {}
        self.basis: int | None = msg.get("basis")


class EditContext(RequestContext):
    def __init__(self, space: Space, running: _Running, msg: dict[str, Any]):
        super().__init__(space, running)
        self.changes: list[dict[str, Any]] | None = msg.get("changes")
        self.sentence: str | None = msg.get("sentence")
        self.basis: int | None = msg.get("basis")


class StreamContext(RequestContext):
    def __init__(self, space: Space, running: _Running, msg: dict[str, Any]):
        super().__init__(space, running)
        self.form: str = msg["form"]
        self.rate: float | None = msg.get("rate")
        self.frames = 0

    def frame(self, content: dict[str, Any]) -> None:
        if self._running.finished:
            return
        sess = self._running.presence.session
        if sess:
            sess.conn.send({"type": "frame", "stream": self.id, "n": self.frames, "time": now(), "content": content})
        self.frames += 1


@dataclass
class Handlers:
    observe: Callable[[ObserveContext], Any] | None = None
    verbs: dict[str, Callable[[ActionContext], Any]] = field(default_factory=dict)
    touches: Callable[[dict[str, Any]], list[str]] | None = None
    edit: Callable[[EditContext], Any] | None = None
    admit: Callable[[dict[str, Any]], str] | None = None
    presentation: Callable[[dict[str, Any]], dict[str, str]] | None = None
    episode: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    score: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    snapshot: Callable[[], Any] | None = None
    restore: Callable[[Any], None] | None = None
    sense: Callable[[str, str, dict[str, Any], PresenceInfo], Any] | None = None
    views: Callable[[], list[dict[str, Any]]] | None = None
    take: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    stream: Callable[[StreamContext], Awaitable[None]] | None = None


@dataclass(eq=False)
class _Session:
    conn: Connection
    nonce: str
    hello: dict[str, Any] | None = None
    parts: dict[str, int] | None = None
    token: str | None = None
    presence: _Presence | None = None


@dataclass(eq=False)
class _Presence:
    id: str
    role: str
    participant: str | None
    key: str
    resume: str
    session: _Session | None
    sub: dict[str, Any] | None = None
    senses: dict[str, dict[str, str]] = field(default_factory=dict)
    grace: asyncio.TimerHandle | None = None
    ended: bool = False


@dataclass(eq=False)
class _Running:
    key: str
    id: str
    type: str
    presence: _Presence
    stored: dict[str, Any]
    act_seq: int | None = None
    stop_reason: str | None = None
    paused: bool = False
    events: list[dict[str, Any]] = field(default_factory=list)
    finished: bool = False
    verb: str | None = None
    consent: dict[str, Any] | None = None
    deadline: asyncio.TimerHandle | None = None
    task: asyncio.Task[None] | None = None
    resumed: asyncio.Event = field(default_factory=asyncio.Event)
    stopped: asyncio.Event = field(default_factory=asyncio.Event)


def _info(p: _Presence) -> PresenceInfo:
    return PresenceInfo(p.id, p.role, p.participant)


def _req_key(p: _Presence, request_id: str) -> str:
    return f"{p.key}\n{request_id}"


def _strip_props(obj: dict[str, Any], hide: list[str]) -> dict[str, Any]:
    if not hide:
        return obj
    return {k: v for k, v in obj.items() if k not in hide or k in ("id", "type")}


def default_parts(capabilities: dict[str, Any]) -> dict[str, list[int]]:
    """The parts a space supports by default: the core, and the part of every declared capability."""
    parts: dict[str, list[int]] = {"core": [0]}
    for p in CAPABILITY_PARTS:
        if p in capabilities:
            parts[p] = [0]
    return parts


class Space:
    """A space: accepts connections and keeps the protocol for its handlers."""

    def __init__(
        self,
        *,
        capabilities: dict[str, Any],
        vocabulary: dict[str, Any],
        id: str | None = None,
        key: KeyPair | None = None,
        parts: dict[str, list[int]] | None = None,
        store: Store | None = None,
        keep_seconds: float | None = None,
        tokens: dict[str, str] | None = None,
        server: dict[str, Any] | None = None,
        hello: dict[str, Any] | None = None,
        stop_grace: float = 2.0,
        handlers: Handlers | None = None,
    ):
        self.key = key or generate_key()
        self.id = id or self.key.identity
        self.capabilities = capabilities
        self.vocabulary = vocabulary
        self.vocabulary_hash = content_hash(vocabulary)
        self.store: Store = store or MemoryStore()
        self.keep_seconds = keep_seconds
        self.parts = parts or default_parts(capabilities)
        self.handlers = handlers or Handlers()
        self._tokens = tokens or {}
        self._server = server
        self._hello_extra = hello or {}
        self._stop_grace = stop_grace
        self._sessions: set[_Session] = set()
        self._presences: dict[str, _Presence] = {}
        self._by_resume: dict[str, _Presence] = {}
        self._running: dict[str, _Running] = {}
        self._last_change: dict[str, int] = {}
        self._last_restore = 0
        self._counter = 0
        self._listeners: list[Listener] = []
        self._blobs: dict[str, tuple[str, bytes]] = {self.vocabulary_hash: ("application/json", canonical_json(vocabulary).encode())}
        self._param_validators: dict[str, Draft202012Validator] = {}
        self._closing = False
        self._closed: asyncio.Future[None] | None = None
        self._pruned_before = self.store.get_meta("pruned_before")
        self._prune_task: asyncio.Task[None] | None = None
        for e in self.store.read():
            self._note_changes(e)
        self._recover()

    # ── Decorators ──

    def observer(self, fn: Callable[[ObserveContext], Any]) -> Callable[[ObserveContext], Any]:
        self.handlers.observe = fn
        return fn

    def verb(self, name: str) -> Callable[[Callable[[ActionContext], Any]], Callable[[ActionContext], Any]]:
        def register(fn: Callable[[ActionContext], Any]) -> Callable[[ActionContext], Any]:
            self.handlers.verbs[name] = fn
            return fn

        return register

    def editor(self, fn: Callable[[EditContext], Any]) -> Callable[[EditContext], Any]:
        self.handlers.edit = fn
        return fn

    # ── Running ──

    @property
    def head(self) -> int:
        return self.store.head()

    async def listen(self, address: str) -> Listener:
        """Listens at `ws://host:port`, `http://host:port` or `stdio`."""
        self._ensure_loop_parts()
        listener = await listen(address, self.accept)
        self._listeners.append(listener)
        return listener

    def _ensure_loop_parts(self) -> None:
        if self._closed is None:
            self._closed = asyncio.get_running_loop().create_future()
        if self.keep_seconds is not None and self._prune_task is None:
            self._prune_task = asyncio.get_running_loop().create_task(self._prune_loop())

    async def wait_closed(self) -> None:
        self._ensure_loop_parts()
        assert self._closed
        await self._closed

    def put_blob(self, data: bytes | str, media: str) -> str:
        """Stores large content and returns its hash."""
        raw = data.encode() if isinstance(data, str) else data
        h = content_hash(raw)
        self._blobs[h] = (media, raw)
        return h

    def emit(self, kind: str, data: Any = None, *, things: list[str] | None = None, roles: list[str] | None = None, source: str = "space", cause: int | None = None) -> dict[str, Any]:
        """Appends an event no participant's action caused: from the space, or from `outside/<who>`."""
        return self._append([strip_none({"source": source, "type": kind, "data": data, "things": things, "roles": roles, "cause": cause})])[0]

    def accept(self, conn: Connection) -> None:
        """Takes a new connection."""
        self._ensure_loop_parts()
        if self._closing:
            conn.close()
            return
        session = _Session(conn, random_token(24))
        self._sessions.add(session)

        def on_message(text: str) -> None:
            try:
                self._on_message(session, text)
            except Exception as e:
                print(f"asp space: error handling a message: {e!r}", file=sys.stderr)

        conn.on_message = on_message
        conn.on_close = lambda: self._on_disconnect(session)

    async def close(self) -> None:
        """Closes: every action under way gets its final outcome, then `asp.closing` is sent, then connections close."""
        self._ensure_loop_parts()
        assert self._closed
        if self._closing:
            await self._closed
            return
        self._closing = True
        running = list(self._running.values())
        for r in running:
            self._stop(r, "closed")
        await asyncio.gather(*(r.task for r in running if r.task), return_exceptions=True)
        closing = self._append([{"source": "space", "type": "asp.closing"}])[0]
        for s in list(self._sessions):
            if not (s.presence and s.presence.sub):
                s.conn.send({"type": "event", "entry": closing})
        for s in list(self._sessions):
            s.conn.close()
        for p in self._presences.values():
            if p.grace:
                p.grace.cancel()
        if self._prune_task:
            self._prune_task.cancel()
        await asyncio.sleep(0.05)
        for listener in self._listeners:
            await listener.close()
        self.store.close()
        if not self._closed.done():
            self._closed.set_result(None)

    # ── Messages ──

    def _on_message(self, session: _Session, text: str) -> None:
        try:
            msg = json.loads(text)
        except ValueError:
            return self._error(session, "malformed", "not JSON")
        if not isinstance(msg, dict):
            return self._error(session, "malformed", "not a JSON object")
        rid = msg.get("id") if isinstance(msg.get("id"), str) and 1 <= len(msg["id"]) <= 128 else None
        mtype = msg.get("type")
        if not isinstance(mtype, str):
            return self._refuse_request(session, msg, rid, "malformed", "no type") if rid else self._error(session, "malformed", "no type")
        if session.parts is None:
            if mtype != "hello":
                return self._error(session, "malformed", "the handshake comes first", mtype)
            return self._on_hello(session, msg)
        if mtype == "hello":
            return self._error(session, "malformed", "the handshake is already done", mtype)
        definition = PARTICIPANT_MESSAGES.get(mtype)
        if not definition:
            why = f"unknown message type {mtype}"
            return self._refuse_request(session, msg, rid, "unsupported", why) if rid else self._error(session, "unsupported", why, mtype)
        capability = MESSAGE_CAPABILITY.get(mtype)
        if capability:
            declared = capability in self.capabilities
            negotiated = capability == "edit" or capability in session.parts
            if not declared or not negotiated:
                why = f"capability {capability} is not declared" if not declared else f"part {capability} was not negotiated"
                return self._refuse_request(session, msg, rid, "unsupported", why) if rid else self._error(session, "unsupported", why, mtype)
        invalid = validate(definition, msg)
        if invalid:
            return self._refuse_request(session, msg, rid, "malformed", invalid) if rid else self._error(session, "malformed", invalid, mtype)
        presence = session.presence
        if presence is None and mtype not in ("join", "vocabulary", "blob"):
            return self._refuse_request(session, msg, rid, "forbidden", "join first") if rid else self._error(session, "forbidden", "join first", mtype)
        if presence and rid and mtype in IDEMPOTENT:
            stored = self.store.get_request(_req_key(presence, rid))
            if stored:
                if stored["state"] == "final":
                    session.conn.send(stored["final"])
                else:
                    r = self._running.get(stored["key"])
                    last = (r.stored.get("last") if r else None) or stored.get("last") or strip_none({"type": "accepted", "id": rid, "seq": stored.get("act")})
                    session.conn.send(last)
                return
        handler = getattr(self, f"_m_{mtype}")
        result = handler(presence, session, msg)
        if inspect.isawaitable(result):
            asyncio.get_running_loop().create_task(result)

    def _error(self, session: _Session, code: str, text: str, about: str | None = None) -> None:
        session.conn.send(strip_none({"type": "error", "code": code, "text": text, "about": about}))

    def _answer_refusal(self, session: _Session | None, rid: str, code: str, text: str, data: Any = None) -> None:
        if session:
            session.conn.send({"type": "outcome", "id": rid, "end": "refused", "reason": strip_none({"code": code, "text": text, "data": data})})

    def _refuse_request(self, session: _Session, msg: dict[str, Any], rid: str, code: str, text: str) -> None:
        presence = session.presence
        if presence and msg.get("type") in ("act", "edit"):
            key = _req_key(presence, rid)
            stored = self.store.get_request(key)
            if stored:
                session.conn.send(stored["final"] if stored["state"] == "final" else stored.get("last") or {"type": "accepted", "id": rid})
                return
            req = {"key": key, "type": msg["type"], "state": "running", "presence": presence.id}
            self._finish_recorded(presence, req, rid, None, [], {"end": "refused", "reason": {"code": code, "text": text}})
            return
        self._answer_refusal(session, rid, code, text)

    # ── Handshake and presences ──

    def _on_hello(self, session: _Session, msg: dict[str, Any]) -> None:
        invalid = validate("ParticipantHello", msg)
        if invalid:
            return self._error(session, "malformed", invalid, "hello")
        parts: dict[str, int] = {}
        for part, versions in self.parts.items():
            theirs = msg["parts"].get(part)
            if not isinstance(theirs, list):
                continue
            common = [v for v in versions if v in theirs]
            if common:
                parts[part] = max(common)
        if "core" not in parts:
            self._error(session, "unsupported", "no common version of the core", "hello")
            session.conn.close()
            return
        if "token" in msg:
            name = self._tokens.get(msg["token"])
            if not name:
                self._error(session, "forbidden", "unknown token", "hello")
                session.conn.close()
                return
            session.token = name
        session.hello = msg
        session.parts = parts
        session.conn.send(
            {
                "type": "hello",
                "space": self.id,
                "parts": parts,
                "supported": self.parts,
                "capabilities": self.capabilities,
                "vocabulary": self.vocabulary_hash,
                "record": {"head": self.store.head(), "earliest": self.store.earliest(), "keep_seconds": self.keep_seconds},
                "nonce": session.nonce,
                "key": self.key.identity,
                **({"server": self._server} if self._server else {}),
                **self._hello_extra,
            }
        )

    def _roles(self) -> dict[str, Any]:
        return self.capabilities.get("participants", {}).get("roles") or DEFAULT_ROLES

    def _m_join(self, presence: _Presence | None, session: _Session, msg: dict[str, Any]) -> None:
        rid = msg["id"]

        def refuse(code: str, text: str) -> None:
            self._answer_refusal(session, rid, code, text)

        participant: str | None = None
        claimed = (session.hello or {}).get("participant")
        if claimed:
            if not msg.get("proof") or not check_proof(claimed, self.id, session.nonce, msg["proof"]):
                return refuse("forbidden", "no valid proof of identity")
            participant = claimed
        elif session.token:
            participant = f"token:{session.token}"

        if "resume" in msg:
            p = self._by_resume.get(msg["resume"])
            if not p or p.ended:
                return refuse("closed", "no presence to resume")
            if p.participant != participant:
                return refuse("forbidden", "the presence belongs to another participant")
            if session.presence and session.presence is not p:
                return refuse("malformed", "this connection already has a presence")
            if p.grace:
                p.grace.cancel()
                p.grace = None
            if p.session and p.session is not session:
                p.session.presence = None
                p.session.conn.close()
            p.session = session
            p.sub = None
            session.presence = p
            entry = self._append([{"source": f"presence/{p.id}", "type": "asp.join", "data": {"presence": p.id, "role": p.role, "participant": p.participant, "resumed": True}}])[0]
            session.conn.send({"type": "outcome", "id": rid, "end": "done", "seq": entry["seq"], "result": strip_none({"presence": p.id, "role": p.role, "participant": p.participant, "resume": p.resume, "resumed": True, "head": entry["seq"]})})
            return
        if session.presence:
            return refuse("malformed", "this connection already has a presence")

        pass_payload = None
        if "pass" in msg:
            issuer = (peek_pass(msg["pass"]) or {}).get("from")
            doors = self.capabilities.get("doors", {}).get("doors") or []
            door = next((d for d in doors if d.get("space") and d["space"] == issuer), None)
            payload = read_pass(msg["pass"], issuer) if door and issuer else None
            if not payload or payload.get("participant") != participant:
                return refuse("forbidden", "the pass is not valid for this participant")
            pass_payload = payload
        roles = self._roles()
        role = msg.get("role") or self.capabilities.get("participants", {}).get("default_role") or next(iter(roles))
        if role not in roles:
            return refuse("forbidden", f"no role {role}")
        if self.handlers.admit:
            try:
                role = self.handlers.admit({"participant": participant, "role": role, "pass": pass_payload})
            except Refused as e:
                return refuse(e.code, e.text or e.code)
        maximum = self.capabilities.get("participants", {}).get("max")
        if maximum is not None and sum(1 for p in self._presences.values() if not p.ended) >= maximum:
            return refuse("forbidden", "the space is full")
        self._counter += 1
        pid = f"p-{self._counter}-{random_token(3)}"
        p = _Presence(id=pid, role=role, participant=participant, key=participant or f"presence:{pid}", resume=random_token(18), session=session)
        self._presences[pid] = p
        self._by_resume[p.resume] = p
        session.presence = p
        presentation = None
        if msg.get("presentation"):
            presentation = self.handlers.presentation(msg["presentation"]) if self.handlers.presentation else {k: "ignored" for k in msg["presentation"]}
        data = {"presence": pid, "role": role, "participant": participant}
        if pass_payload:
            data["door"] = pass_payload.get("door")
        entry = self._append([{"source": f"presence/{pid}", "type": "asp.join", "data": data}])[0]
        session.conn.send({"type": "outcome", "id": rid, "end": "done", "seq": entry["seq"], "result": strip_none({"presence": pid, "role": role, "participant": participant, "presentation": presentation, "resume": p.resume, "head": entry["seq"]})})

    def _m_leave(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        if msg.get("cancel"):
            for r in list(self._running.values()):
                if r.presence is presence:
                    self._stop(r, "cancel")
        entry = self._end_presence(presence, "left")
        final = {"type": "outcome", "id": msg["id"], "end": "done", "seq": entry["seq"]}
        self.store.put_request({"key": _req_key(presence, msg["id"]), "type": "leave", "state": "final", "presence": presence.id, "final": final})
        session.conn.send(final)
        session.presence = None
        presence.session = None

    def _end_presence(self, presence: _Presence, reason: str) -> dict[str, Any]:
        presence.ended = True
        presence.sub = None
        if presence.grace:
            presence.grace.cancel()
        self._by_resume.pop(presence.resume, None)
        return self._append([{"source": f"presence/{presence.id}", "type": "asp.leave", "data": {"presence": presence.id, "reason": reason}}])[0]

    def _on_disconnect(self, session: _Session) -> None:
        self._sessions.discard(session)
        p = session.presence
        if not p or p.session is not session:
            return
        p.session = None
        p.sub = None
        if self._closing or p.ended:
            return
        grace = self.capabilities.get("participants", {}).get("grace_seconds", 300)

        def expire() -> None:
            if not p.session and not p.ended:
                self._end_presence(p, "closed")

        p.grace = asyncio.get_running_loop().call_later(grace, expire)

    # ── Observing and the record ──

    def _hidden(self, role: str) -> dict[str, Any]:
        return self._roles().get(role, {}).get("hide") or {}

    async def _m_observe(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        forms = [f for f in msg.get("forms") or [] if f != "structure"]
        declared = self.capabilities.get("forms") or []
        missing = next((f for f in forms if f not in declared or f not in (self.vocabulary.get("forms") or {})), None)
        if missing:
            return self._answer_refusal(presence.session, msg["id"], "unsupported", f"form {missing} is not declared")
        seq = self.store.head()
        info = _info(presence)
        assert self.handlers.observe, "a space needs an observer"
        try:
            result = self.handlers.observe(ObserveContext(info, forms))
            if inspect.isawaitable(result):
                result = await result
        except Refused as e:
            return self._answer_refusal(presence.session, msg["id"], e.code, e.text or e.code, e.data)
        hide = self._hidden(presence.role).get("properties") or []
        structure = {"things": [_strip_props(t, hide) for t in result["structure"]["things"]]}
        if result["structure"].get("relations") is not None:
            structure["relations"] = result["structure"]["relations"]
        senses = {}
        for name, s in presence.senses.items():
            try:
                senses[name] = self.handlers.sense(s["language"], s["query"], structure, info) if self.handlers.sense else None
            except Exception:
                senses[name] = None
        out = {"type": "observation", "id": msg["id"], "seq": seq}
        if result.get("confirmed"):
            out["confirmed"] = result["confirmed"]
        out["structure"] = structure
        if result.get("forms"):
            out["forms"] = result["forms"]
        if presence.senses:
            out["senses"] = senses
        if presence.session:
            presence.session.conn.send(out)

    def _visible(self, e: dict[str, Any], p: _Presence) -> bool:
        if e.get("roles") and p.role not in e["roles"]:
            return False
        return e["type"] not in (self._hidden(p.role).get("events") or [])

    def _project(self, e: dict[str, Any], p: _Presence) -> dict[str, Any]:
        hide = self._hidden(p.role).get("properties") or []
        if not hide or not isinstance(e.get("data"), dict):
            return e
        return {**e, "data": _strip_props(e["data"], hide)}

    def _m_subscribe(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        head = self.store.head()
        start = msg.get("from", head + 1)
        if start < self.store.earliest():
            return self._answer_refusal(session, msg["id"], "expired", f"the record starts at {self.store.earliest()}", {"earliest": self.store.earliest()})
        presence.sub = {"cursor": max(start, 1), "kinds": msg.get("kinds")}
        session.conn.send({"type": "subscribed", "id": msg["id"], "from": max(start, 1), "head": head})
        self._pump(presence)

    def _pump(self, presence: _Presence) -> None:
        sub, session = presence.sub, presence.session
        if not sub or not session:
            return
        while sub["cursor"] <= self.store.head():
            if sub["cursor"] < self.store.earliest():
                sub["cursor"] = self.store.earliest()
            batch = self.store.read(start=sub["cursor"], limit=500)
            if not batch:
                sub["cursor"] = self.store.head() + 1
                break
            for e in batch:
                sub["cursor"] = e["seq"] + 1
                if not self._visible(e, presence):
                    continue
                if sub["kinds"] and e["type"] not in sub["kinds"]:
                    continue
                session.conn.send({"type": "event", "entry": self._project(e, presence)})

    def _m_record(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        earliest = self.store.earliest()
        expired = (
            ("from" in msg and msg["from"] < earliest)
            or ("since" in msg and self._pruned_before is not None and msg["since"] < self._pruned_before)
            or ("from" not in msg and "since" not in msg and earliest > 1)
        )
        if expired:
            return self._answer_refusal(session, msg["id"], "expired", f"the record starts at {earliest}", {"earliest": earliest})
        limit = min(msg.get("limit", 1000), 1000)
        out: list[dict[str, Any]] = []
        more = False
        start = msg.get("from", 1)
        while True:
            batch = self.store.read(start=start, to=msg.get("to"), since=msg.get("since"), until=msg.get("until"), limit=500)
            if not batch:
                break
            for e in batch:
                start = e["seq"] + 1
                if not self._visible(e, presence):
                    continue
                if len(out) >= limit:
                    more = True
                    break
                out.append(self._project(e, presence))
            if more or len(batch) < 500:
                break
        answer = {"type": "entries", "id": msg["id"], "entries": out, "head": self.store.head(), "earliest": earliest}
        if more:
            answer["more"] = True
        session.conn.send(answer)

    def _m_vocabulary(self, presence: _Presence | None, session: _Session, msg: dict[str, Any]) -> None:
        if msg.get("hash") and msg["hash"] != self.vocabulary_hash:
            return self._answer_refusal(session, msg["id"], "expired", "that vocabulary is not kept")
        session.conn.send({"type": "vocabulary", "id": msg["id"], "hash": self.vocabulary_hash, "vocabulary": self.vocabulary})

    def _m_blob(self, presence: _Presence | None, session: _Session, msg: dict[str, Any]) -> None:
        import base64

        blob = self._blobs.get(msg["hash"])
        if not blob:
            return self._answer_refusal(session, msg["id"], "expired", "no content with that hash is kept")
        session.conn.send({"type": "blob", "id": msg["id"], "hash": msg["hash"], "media": blob[0], "data": base64.b64encode(blob[1]).decode()})

    def _append(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = self.store.append(entries)
        for e in out:
            self._note_changes(e)
        for p in list(self._presences.values()):
            self._pump(p)
        return out

    def _note_changes(self, e: dict[str, Any]) -> None:
        if e["type"] == "asp.restore":
            self._last_restore = e["seq"]
        if not e.get("things") or e["type"] in NOT_CHANGES:
            return
        for t in e["things"]:
            self._last_change[t] = e["seq"]

    async def _prune_loop(self) -> None:
        assert self.keep_seconds is not None
        while True:
            await asyncio.sleep(min(1.0, self.keep_seconds / 4))
            before = (datetime.now(timezone.utc) - timedelta(seconds=self.keep_seconds)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            earliest = self.store.earliest()
            self.store.prune(before)
            if self.store.earliest() != earliest:
                self._pruned_before = before
                self.store.set_meta("pruned_before", before)

    # ── Actions, edits and streams ──

    def _is_stale(self, things: list[str], basis: int | None) -> str | None:
        if basis is None:
            return None
        if self._last_restore > basis:
            return "a save was restored"
        for t in things:
            changed = self._last_change.get(t)
            if changed is not None and changed > basis:
                return f"{t} changed at {changed}, after basis {basis}"
        return None

    def _m_act(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        key = _req_key(presence, msg["id"])
        act_entry = self._append([{"source": f"presence/{presence.id}", "type": "asp.act", "data": strip_none({"presence": presence.id, "id": msg["id"], "verb": msg["verb"], "object": msg.get("object"), "args": msg.get("args"), "basis": msg.get("basis"), "deadline": msg.get("deadline")})}])[0]
        stored = {"key": key, "type": "act", "state": "running", "act": act_entry["seq"], "presence": presence.id, "request": msg}
        self.store.put_request(stored)

        def refuse(code: str, text: str, data: Any = None) -> None:
            self._finish_recorded(presence, stored, msg["id"], act_entry["seq"], [], {"end": "refused", "reason": strip_none({"code": code, "text": text, "data": data})})

        decl = (self.vocabulary.get("verbs") or {}).get(msg["verb"])
        if not decl:
            return refuse("unsupported", f"no verb {msg['verb']} in the vocabulary")
        verbs = self._roles().get(presence.role, {}).get("verbs") or []
        if "*" not in verbs and msg["verb"] not in verbs:
            return refuse("forbidden", f"role {presence.role} may not use {msg['verb']}")
        if decl.get("object") and msg.get("object") is None:
            return refuse("malformed", f"{msg['verb']} needs an object")
        if decl.get("params"):
            v = self._param_validators.get(msg["verb"])
            if v is None:
                v = self._param_validators[msg["verb"]] = Draft202012Validator(decl["params"])
            err = next(iter(v.iter_errors(msg.get("args") or {})), None)
            if err:
                path = "/" + "/".join(str(p) for p in err.absolute_path) if err.absolute_path else ""
                return refuse("malformed", f"args{path} {err.message}")
        if msg.get("basis") is not None and msg["basis"] > self.store.head():
            return refuse("malformed", f"basis {msg['basis']} is beyond the head of the record")
        if decl.get("stale") == "refuse":
            touched = ([msg["object"]] if msg.get("object") else []) + (self.handlers.touches(msg) if self.handlers.touches else [])
            stale = self._is_stale(touched, msg.get("basis"))
            if stale:
                return refuse("stale", stale)
        law = self.handlers.verbs.get(msg["verb"])

        def run(ctx: ActionContext) -> Any:
            if law:
                return law(ctx)
            default = decl["default"]
            if default["end"] == "done":
                return None
            r = default.get("reason") or {"code": "forbidden"}
            raise Refused(r["code"], r.get("text") or "no law applies", r.get("data"))

        running = self._start_running(presence, "act", msg["id"], stored, act_entry["seq"], msg.get("deadline"))
        running.verb = msg["verb"]
        rule = (self.capabilities.get("consent", {}).get("verbs") or {}).get(msg["verb"])
        ctx = ActionContext(self, running, msg)

        async def go() -> None:
            if rule:
                until = iso_after(rule["seconds"])
                self._append([{"source": f"presence/{presence.id}", "type": "asp.waiting", "cause": act_entry["seq"], "data": {"presence": presence.id, "id": msg["id"], "for": {"role": rule["by"]}, "until": until}}])
                self._intermediate(running, {"type": "waiting", "id": msg["id"], "for": {"role": rule["by"]}, "act": act_entry["seq"], "until": until}, True)
                answer: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
                running.consent = {"by": rule["by"], "answer": answer}
                waiter = asyncio.ensure_future(running.stopped.wait())
                done, _ = await asyncio.wait({answer, waiter}, timeout=rule["seconds"], return_when=asyncio.FIRST_COMPLETED)
                waiter.cancel()
                running.consent = None
                if not done:
                    return self._finish_running(running, {"end": "cancelled", "reason": {"code": "deadline", "text": "nobody agreed in time"}})
                if answer not in done:
                    return self._finish_stopped(running, [])
                if not answer.result():
                    return self._finish_running(running, {"end": "refused", "reason": {"code": "forbidden", "text": "consent was refused"}})
            self._intermediate(running, {"type": "accepted", "id": msg["id"], "seq": act_entry["seq"]}, True)
            await self._run_law(running, lambda: run(ctx))

        running.task = asyncio.get_running_loop().create_task(go())

    def _m_edit(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        key = _req_key(presence, msg["id"])
        edit_entry = self._append([{"source": f"presence/{presence.id}", "type": "asp.edit", "data": strip_none({"presence": presence.id, "id": msg["id"], "changes": msg.get("changes"), "sentence": msg.get("sentence"), "basis": msg.get("basis")})}])[0]
        stored = {"key": key, "type": "edit", "state": "running", "act": edit_entry["seq"], "presence": presence.id, "request": msg}
        self.store.put_request(stored)

        def refuse(code: str, text: str) -> None:
            self._finish_recorded(presence, stored, msg["id"], edit_entry["seq"], [], {"end": "refused", "reason": {"code": code, "text": text}})

        cap = self.capabilities.get("edit") or {}
        role = self._roles().get(presence.role, {})
        if not role.get("edit") or (cap.get("roles") and presence.role not in cap["roles"]):
            return refuse("forbidden", f"role {presence.role} may not edit")
        if not msg.get("changes") and msg.get("sentence") is None:
            return refuse("malformed", "an edit needs changes or a sentence")
        if msg.get("sentence") is not None and not cap.get("sentences"):
            return refuse("unsupported", "edits by sentence are not taken")
        kinds = cap.get("changes")
        bad = next((c for c in msg.get("changes") or [] if kinds and c["op"] not in kinds), None)
        if bad:
            return refuse("unsupported", f"changes of kind {bad['op']} are not taken")
        if msg.get("basis") is not None and msg["basis"] > self.store.head():
            return refuse("malformed", f"basis {msg['basis']} is beyond the head of the record")
        touched: list[str] = []
        for c in msg.get("changes") or []:
            if c["op"] == "add":
                touched.append(c["thing"]["id"])
            elif "relation" in c:
                touched += [c["relation"]["from"], c["relation"]["to"]]
            else:
                touched.append(c["thing"])
        stale = self._is_stale(touched, msg.get("basis"))
        if stale:
            return refuse("stale", stale)
        if not self.handlers.edit:
            return refuse("unsupported", "this space takes no edits")
        running = self._start_running(presence, "edit", msg["id"], stored, edit_entry["seq"], msg.get("deadline"))
        ctx = EditContext(self, running, msg)
        editor = self.handlers.edit

        async def go() -> None:
            self._intermediate(running, {"type": "accepted", "id": msg["id"], "seq": edit_entry["seq"]}, True)
            await self._run_law(running, lambda: editor(ctx))

        running.task = asyncio.get_running_loop().create_task(go())

    def _m_stream(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        form = (self.vocabulary.get("forms") or {}).get(msg["form"])
        stored = {"key": _req_key(presence, msg["id"]), "type": "stream", "state": "running", "presence": presence.id}
        if not form or not form.get("continuous") or msg["form"] not in (self.capabilities.get("forms") or []) or not self.handlers.stream:
            return self._finish_simple(presence, msg, {"end": "refused", "reason": {"code": "unsupported", "text": f"{msg['form']} is not a declared continuous form"}})
        self.store.put_request(stored)
        running = self._start_running(presence, "stream", msg["id"], stored, None, None)
        ctx = StreamContext(self, running, msg)
        streamer = self.handlers.stream

        async def go() -> None:
            self._intermediate(running, {"type": "accepted", "id": msg["id"]}, True)

            async def law() -> dict[str, Any]:
                await streamer(ctx)
                return {"result": {"frames": ctx.frames}}

            await self._run_law(running, law)

        running.task = asyncio.get_running_loop().create_task(go())

    def _start_running(self, presence: _Presence, kind: str, rid: str, stored: dict[str, Any], act_seq: int | None, deadline: float | None) -> _Running:
        running = _Running(key=stored["key"], id=rid, type=kind, presence=presence, stored=stored, act_seq=act_seq)
        self._running[running.key] = running
        if deadline is not None:
            running.deadline = asyncio.get_running_loop().call_later(deadline, self._stop, running, "deadline")
        return running

    def _intermediate(self, running: _Running, message: dict[str, Any], persist: bool) -> None:
        if running.finished:
            return
        running.stored["last"] = message
        if persist:
            self.store.put_request(running.stored)
        if running.presence.session:
            running.presence.session.conn.send(message)

    async def _run_law(self, running: _Running, law: Callable[[], Any]) -> None:
        grace: asyncio.TimerHandle | None = None
        loop = asyncio.get_running_loop()

        def give_up() -> None:
            if running.finished:
                return
            reason = running.stop_reason or "closed"
            code = "sdk.not_stopped" if reason == "cancel" else reason
            self._finish_running(running, {"end": "unknown", "reason": {"code": code, "text": f"could not confirm that it stopped ({reason})"}, "known": {"asked_to_stop": True}, "unconfirmed": ["stopped"]})

        async def watch_stop() -> None:
            nonlocal grace
            await running.stopped.wait()
            grace = loop.call_later(self._stop_grace, give_up)

        watcher = loop.create_task(watch_stop())
        try:
            value = law()
            if inspect.isawaitable(value):
                value = await value
            if running.finished:
                return
            value = value or {}
            outcome: dict[str, Any] = {"end": "done"}
            if value.get("result") is not None:
                outcome["result"] = value["result"]
            self._finish_running(running, outcome, value.get("things"))
        except Refused as e:
            if not running.finished:
                running.events = []
                self._finish_running(running, {"end": "refused", "reason": strip_none({"code": e.code, "text": e.text, "data": e.data})})
        except Failed as e:
            if not running.finished:
                self._finish_running(running, {"end": "failed", "reason": strip_none({"code": e.code, "text": e.text, "data": e.data}), "effects": e.effects})
        except Uncertain as e:
            if not running.finished:
                out = {"end": "unknown", "reason": {"code": e.code, "text": e.text}, "known": e.known, "unconfirmed": e.unconfirmed}
                if e.effects:
                    out["effects"] = e.effects
                self._finish_running(running, out)
        except (Stopped, asyncio.CancelledError) as e:
            if not running.finished:
                self._finish_stopped(running, e.effects if isinstance(e, Stopped) else [])
        except Exception as e:
            if not running.finished:
                if running.stop_reason is not None:
                    self._finish_stopped(running, [])
                else:
                    print(f"asp space: a law raised: {e!r}", file=sys.stderr)
                    self._finish_running(running, {"end": "unknown", "reason": {"code": "sdk.exception", "text": str(e)}, "known": {}, "unconfirmed": ["effects"]})
        finally:
            watcher.cancel()
            if grace:
                grace.cancel()

    def _finish_stopped(self, running: _Running, effects: list[dict[str, Any]]) -> None:
        if running.stop_reason == "deadline":
            self._finish_running(running, {"end": "failed", "reason": {"code": "deadline", "text": "stopped at its deadline"}, "effects": effects})
        elif running.stop_reason == "closed":
            self._finish_running(running, {"end": "cancelled", "reason": {"code": "closed", "text": "the space closed"}, "effects": effects})
        else:
            self._finish_running(running, {"end": "cancelled", "effects": effects})

    def _finish_running(self, running: _Running, outcome: dict[str, Any], things: list[str] | None = None) -> None:
        if running.finished:
            return
        running.finished = True
        if running.deadline:
            running.deadline.cancel()
        self._running.pop(running.key, None)
        running.resumed.set()
        events = [] if outcome["end"] == "refused" else running.events
        if running.type == "stream":
            final = {"type": "outcome", "id": running.id, **outcome}
            running.stored.update(state="final", final=final)
            self.store.put_request(running.stored)
            if running.presence.session:
                running.presence.session.conn.send(final)
            return
        self._finish_recorded(running.presence, running.stored, running.id, running.act_seq, events, outcome, things)

    def _finish_recorded(self, presence: _Presence, stored: dict[str, Any], rid: str, act_seq: int | None, events: list[dict[str, Any]], outcome: dict[str, Any], things: list[str] | None = None) -> None:
        with self.store.transaction():
            evs = self.store.append(events)
            seqs = [e["seq"] for e in evs]
            data = strip_none({"presence": presence.id, "id": rid, **outcome, "events": seqs or None})
            entry = self.store.append([strip_none({"source": "space", "type": "asp.outcome", "cause": act_seq, "things": things or None, "data": data})])[0]
            final = strip_none({"type": "outcome", "id": rid, **outcome, "seq": entry["seq"], "events": seqs or None})
            stored.update(state="final", final=final)
            stored.pop("last", None)
            self.store.put_request(stored)
            for e in [*evs, entry]:
                self._note_changes(e)
        if presence.session:
            presence.session.conn.send(final)
        for p in list(self._presences.values()):
            self._pump(p)

    def _finish_simple(self, presence: _Presence, msg: dict[str, Any], outcome: dict[str, Any], remember: bool = True, seq: int | None = None) -> None:
        final = strip_none({"type": "outcome", "id": msg["id"], **outcome, "seq": seq})
        if remember and msg["type"] in IDEMPOTENT:
            self.store.put_request({"key": _req_key(presence, msg["id"]), "type": msg["type"], "state": "final", "presence": presence.id, "final": final})
        if presence.session:
            presence.session.conn.send(final)

    def _stop(self, running: _Running, reason: str) -> None:
        if running.finished or running.stop_reason is not None:
            return
        running.stop_reason = reason
        running.stopped.set()
        running.resumed.set()

    def _m_cancel(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        key = _req_key(presence, msg["action"])
        running = self._running.get(key)
        if not running:
            if not self.store.get_request(key):
                self._error(session, "malformed", f"no request {msg['action']} to cancel", "cancel")
            return
        if running.act_seq is not None:
            self._append([{"source": f"presence/{presence.id}", "type": "asp.cancel", "cause": running.act_seq, "data": {"presence": presence.id, "id": msg["action"]}}])
        self._stop(running, "cancel")

    # ── Capabilities ──

    def _m_consent(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        running = next((r for r in self._running.values() if r.act_seq == msg["act"] and r.consent), None)
        if not running:
            return self._finish_simple(presence, msg, {"end": "refused", "reason": {"code": "malformed", "text": f"no action waits for consent at {msg['act']}"}})
        role = self._roles().get(presence.role, {})
        assert running.consent
        if not role.get("consent") or presence.role != running.consent["by"]:
            return self._finish_simple(presence, msg, {"end": "refused", "reason": {"code": "forbidden", "text": f"role {presence.role} may not consent to this"}})
        entry = self._append([{"source": f"presence/{presence.id}", "type": "asp.consent", "cause": msg["act"], "data": {"presence": presence.id, "act": msg["act"], "agree": msg["agree"]}}])[0]
        answer = running.consent["answer"]
        if not answer.done():
            answer.set_result(msg["agree"])
        self._finish_simple(presence, msg, {"end": "done"}, True, entry["seq"])

    def _m_snapshot(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        try:
            if not self.handlers.snapshot:
                raise Refused("unsupported", "this space cannot save")
            value = self.handlers.snapshot()
            h = self.put_blob(canonical_json(value), "application/json")
            entry = self._append([{"source": f"presence/{presence.id}", "type": "asp.snapshot", "data": {"presence": presence.id, "snapshot": h}}])[0]
            self._finish_simple(presence, msg, {"end": "done", "result": {"snapshot": h}}, True, entry["seq"])
        except Refused as e:
            self._finish_simple(presence, msg, {"end": "refused", "reason": strip_none({"code": e.code, "text": e.text})})

    def _m_restore(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        try:
            if not self.capabilities.get("snapshot", {}).get("restore") or not self.handlers.restore:
                raise Refused("unsupported", "this space cannot restore")
            blob = self._blobs.get(msg["snapshot"])
            if not blob:
                raise Refused("expired", "no save with that hash is kept")
            self.handlers.restore(json.loads(blob[1]))
            entry = self._append([{"source": f"presence/{presence.id}", "type": "asp.restore", "data": {"presence": presence.id, "snapshot": msg["snapshot"]}}])[0]
            self._finish_simple(presence, msg, {"end": "done"}, True, entry["seq"])
        except Refused as e:
            self._finish_simple(presence, msg, {"end": "refused", "reason": strip_none({"code": e.code, "text": e.text})})

    def _m_episode(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        try:
            if not self.handlers.episode:
                raise Refused("unsupported", "this space has no episodes")
            result = self.handlers.episode({"task": msg.get("task"), "seed": msg.get("seed"), "presence": _info(presence)})
            entry = self._append([{"source": f"presence/{presence.id}", "type": "asp.episode", "data": strip_none({"episode": result["episode"], "task": msg.get("task"), "seed": msg.get("seed")})}])[0]
            self._finish_simple(presence, msg, {"end": "done", "result": result}, True, entry["seq"])
        except Refused as e:
            self._finish_simple(presence, msg, {"end": "refused", "reason": strip_none({"code": e.code, "text": e.text})})

    def _m_score(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        evaluator = self.capabilities.get("episode", {}).get("evaluator", "evaluator")
        try:
            if presence.role != evaluator:
                raise Refused("forbidden", f"only the {evaluator} role receives scores")
            if not self.handlers.score:
                raise Refused("unsupported", "this space gives no scores")
            score = self.handlers.score({"episode": msg.get("episode"), "presence": _info(presence)})
            entry = self._append([{"source": "space", "type": "asp.score", "roles": [evaluator], "data": {"episode": score["episode"], "score": score["score"]}}])[0]
            self._finish_simple(presence, msg, {"end": "done", "result": score}, False, entry["seq"])
        except Refused as e:
            self._finish_simple(presence, msg, {"end": "refused", "reason": strip_none({"code": e.code, "text": e.text})}, False)

    def _m_pause(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        running = self._running.get(_req_key(presence, msg["action"]))
        if not running:
            return self._finish_simple(presence, msg, {"end": "refused", "reason": {"code": "malformed", "text": f"no action {msg['action']} under way"}})
        verbs = self.capabilities.get("pause", {}).get("verbs")
        if verbs and running.verb and running.verb not in verbs:
            return self._finish_simple(presence, msg, {"end": "refused", "reason": {"code": "unsupported", "text": f"{running.verb} cannot be paused"}})
        if msg["type"] == "pause":
            running.paused = True
            running.resumed.clear()
        else:
            running.paused = False
            running.resumed.set()
        self._finish_simple(presence, msg, {"end": "done", "result": {"paused": running.paused}})

    _m_resume = _m_pause

    def _m_sense(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        cap = self.capabilities.get("senses") or {}

        def refuse(code: str, text: str) -> None:
            self._finish_simple(presence, msg, {"end": "refused", "reason": {"code": code, "text": text}})

        if msg["op"] == "list":
            return self._finish_simple(presence, msg, {"end": "done", "result": {"senses": [{"name": n, **s} for n, s in presence.senses.items()]}})
        if not msg.get("name"):
            return refuse("malformed", "a sense needs a name")
        if msg["op"] == "remove":
            if presence.senses.pop(msg["name"], None) is None:
                return refuse("malformed", f"no sense named {msg['name']}")
            return self._finish_simple(presence, msg, {"end": "done"})
        if not msg.get("language") or msg.get("query") is None:
            return refuse("malformed", "placing a sense needs a language and a query")
        if cap.get("languages") and msg["language"] not in cap["languages"]:
            return refuse("unsupported", f"language {msg['language']} is not declared")
        if cap.get("max") is not None and len(presence.senses) >= cap["max"] and msg["name"] not in presence.senses:
            return refuse("forbidden", "too many senses")
        presence.senses[msg["name"]] = {"language": msg["language"], "query": msg["query"]}
        self._finish_simple(presence, msg, {"end": "done", "result": {"name": msg["name"]}})

    def _m_views(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        self._finish_simple(presence, msg, {"end": "done", "result": {"views": self.handlers.views() if self.handlers.views else []}}, False)

    def _m_offer(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        doors = self.capabilities.get("doors") or {}

        def refuse(code: str, text: str) -> None:
            self._finish_simple(presence, msg, {"end": "refused", "reason": {"code": code, "text": text}})

        if not msg.get("thing"):
            door = next((d for d in doors.get("doors") or [] if d["thing"] == msg.get("door")), None)
            if not door:
                return refuse("malformed", f"no door {msg.get('door')}")
            if not (presence.participant or "").startswith("ed25519:"):
                return refuse("forbidden", "only a participant with a key can pass through a door")
            token = make_pass(self.key, {"participant": presence.participant, "from": self.key.identity, "to": door["to"], "door": door["thing"], "expires": iso_after(300)})
            self._finish_simple(presence, msg, {"end": "done", "result": {"to": door["to"], "pass": token}})
            self._end_presence(presence, "door")
            if presence.session:
                presence.session.presence = None
            presence.session = None
            return
        if not doors.get("takes") or not self.handlers.take:
            return refuse("unsupported", "this space does not take things")
        thing = msg["thing"]
        known = self.vocabulary.get("types") or {}
        as_type = next((t for t in [thing["type"], *(thing.get("parents") or [])] if t in known), None)
        if not as_type:
            return refuse("unsupported", f"no type of {thing['id']} is known here")
        try:
            taken = self.handlers.take({"thing": thing, "as": as_type, "from": presence.participant, "door": msg.get("door")})
        except Refused as e:
            return refuse(e.code, e.text or e.code)
        entry = self._append([{"source": f"presence/{presence.id}", "type": "asp.take", "things": [taken["thing"]], "data": {"offer": msg["id"], "from": presence.participant, "thing": taken["thing"], "as": as_type}}])[0]
        self._finish_simple(presence, msg, {"end": "done", "result": {"thing": taken["thing"], "as": as_type, "take": entry["seq"]}}, True, entry["seq"])

    def _m_take(self, presence: _Presence, session: _Session, msg: dict[str, Any]) -> None:
        key = _req_key(presence, msg["offer"])
        stored = self.store.get_request(key)
        if not stored:
            self.store.put_request({"key": key, "type": "offer", "state": "final", "presence": presence.id, "final": {"type": "outcome", "id": msg["offer"], "end": "refused", "reason": {"code": "closed", "text": "the offer was asked about before it arrived"}}})
            return self._finish_simple(presence, msg, {"end": "done", "result": {"taken": False}}, False)
        final = stored.get("final") or {}
        if final.get("end") == "done":
            result = {"taken": True, "take": final["result"]["take"], "thing": final["result"]["thing"]}
        else:
            result = {"taken": False}
        self._finish_simple(presence, msg, {"end": "done", "result": result}, False)

    # ── Restarts ──

    def _recover(self) -> None:
        running = self.store.running_requests()
        if self.store.head() == 0 and not running:
            return
        self.store.append([{"source": "space", "type": "asp.restarted"}])
        for r in running:
            rid = r["key"].split("\n", 1)[1]
            outcome = {"end": "unknown", "reason": {"code": "restarted", "text": "the space restarted while it was under way"}, "known": {"taken": True}, "unconfirmed": ["finished"]}
            if r["type"] in ("act", "edit"):
                ghost = _Presence(id=r.get("presence", ""), role="", participant=None, key="", resume="", session=None)
                self._finish_recorded(ghost, r, rid, r.get("act"), [], outcome)
            else:
                r.update(state="final", final={"type": "outcome", "id": rid, **outcome})
                self.store.put_request(r)
