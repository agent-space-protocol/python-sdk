"""The test participant: a minimal participant, written with the Python SDK, that runs the same fixed
scenario as the inspector's TypeScript test participant against any space and reports each step."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Awaitable, Callable

from ..identity import generate_key
from ..participant import Participant
from ..protocol import End, OutcomeError, ReasonCode
from ..util import content_hash


async def run(address: str, show: bool = True) -> bool:
    steps: list[tuple[str, bool, str]] = []

    async def step(name: str, fn: Callable[[], Awaitable[str | None]]) -> None:
        try:
            note = await fn() or ""
            steps.append((name, True, note))
        except (AssertionError, OutcomeError, Exception) as e:
            steps.append((name, False, str(e) or type(e).__name__))
        n, ok, note = steps[-1]
        if show:
            print(f"{'✓ pass' if ok else '✗ fail'} {n:<28} {note}", flush=True)

    key = generate_key()
    state: dict[str, Any] = {"events": [], "profile": {}}
    p: Participant | None = None

    def act(probe: dict[str, Any], **extra: Any):
        assert p
        return p.act(probe["verb"], probe.get("object"), probe.get("args"), **extra)

    async def connect() -> str:
        nonlocal p
        p = await Participant.connect(address, key=key, client={"name": "agent-space-protocol python test participant"})
        assert p.parts["core"] == 0, f"core was negotiated as {p.parts.get('core')}"
        state["profile"] = p.hello.get("inspector.profile") or {}
        return f"space {p.hello['space'][:24]}…, parts {' '.join(p.parts)}"

    await step("connect and shake hands", connect)
    if p is None:
        return _report(steps, show)
    profile = state["profile"]
    events: list[dict[str, Any]] = state["events"]

    async def join() -> str:
        j = await p.join(profile.get("role"), {"name": "test participant"})
        assert j.get("participant") == key.identity, "the space does not know the participant's key"
        return f"presence {j['presence']} as {j['role']}"

    async def vocabulary() -> str:
        v = await p.vocabulary()
        assert content_hash(v) == p.hello["vocabulary"], f"the vocabulary hashes to {content_hash(v)}, hello says {p.hello['vocabulary']}"
        return f"{len(v.get('verbs', {}))} verbs"

    async def observe() -> str:
        o = await p.observe()
        assert isinstance(o["seq"], int), "the observation has no seq"
        return f"{len(o['structure']['things'])} things at {o['seq']}"

    async def subscribe_and_act() -> str:
        p.on("event", events.append)
        await p.subscribe()
        out = await act(profile["done"])
        assert out.end is End.DONE, f"the done probe was {out.end}"
        await asyncio.sleep(0.3)
        assert any(e["seq"] == out.seq for e in events), f"the outcome at {out.seq} was not pushed"
        return f"done at {out.seq}, {len(events)} events pushed"

    async def refusal() -> str:
        out = await p.act("x.test.nothing")
        assert out.end is End.REFUSED and out.reason and out.reason.code is ReasonCode.UNSUPPORTED, f"an unknown verb was {out.end} {out.reason}"
        return "refused unsupported"

    async def cancel() -> str:
        pending = act(profile["slow"])
        updates: list[str] = []
        pending.on_update(lambda u: updates.append(u["type"]))
        await asyncio.sleep(0.4)
        pending.cancel()
        out = await pending
        assert out.end in (End.CANCELLED, End.DONE, End.UNKNOWN), f"a cancelled action was {out.end}"
        return f"{' '.join(dict.fromkeys(updates))} → {out.end}"

    async def resend() -> str:
        rid = p.next_id("same")
        a = await act(profile["counter"], id=rid)
        b = await act(profile["counter"], id=rid)
        assert json.dumps(a.raw, sort_keys=True) == json.dumps(b.raw, sort_keys=True), "a resent ID got a different answer"
        return f"both {a.end} at {a.seq}"

    async def drop_and_resume() -> str:
        reconnected = asyncio.get_running_loop().create_future()
        p.on("reconnect", lambda: reconnected.done() or reconnected.set_result(None))
        start = p.last_seq
        p.drop()
        await asyncio.sleep(0.1)
        meanwhile = act(profile["counter"])
        await asyncio.wait_for(reconnected, 30)
        out = await meanwhile
        assert out.end is End.DONE, f"the action sent meanwhile was {out.end} {out.reason}"
        await asyncio.sleep(0.4)
        record = await p.record(start + 1)
        seqs = [e["seq"] for e in events if e["seq"] > start]
        recorded = [e["seq"] for e in record["entries"] if e["seq"] <= max(seqs)]
        assert seqs == recorded, f"received {seqs} but the record has {recorded}"
        outcomes = [e for e in record["entries"] if e["type"] == "asp.outcome" and (e.get("data") or {}).get("id") == meanwhile.id]
        assert len(outcomes) == 1, f"the action has {len(outcomes)} outcomes in the record"
        return f"resumed; {len(seqs)} entries after {start}, none lost or repeated"

    async def leave() -> str:
        out = await p.leave()
        assert out.end is End.DONE, f"leave was {out.end}"
        return ""

    for name, fn in [
        ("join", join),
        ("vocabulary matches its hash", vocabulary),
        ("observe", observe),
        ("subscribe and act", subscribe_and_act),
        ("a refusal", refusal),
        ("cancel", cancel),
        ("resend an ID", resend),
        ("drop, act meanwhile, resume", drop_and_resume),
        ("leave", leave),
    ]:
        await step(name, fn)
    await p.close()
    return _report(steps, show)


def _report(steps: list[tuple[str, bool, str]], show: bool) -> bool:
    failed = sum(1 for _, ok, _ in steps if not ok)
    if show:
        print(f"\n{len(steps) - failed} passed, {failed} failed")
    return failed == 0
