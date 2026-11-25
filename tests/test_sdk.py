"""Tests of the Python SDK against its own test space, over each transport."""

from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
import sys
import tempfile

import pytest

from agent_space import End, OutcomeError, Participant, ReasonCode, SyncParticipant, canonical_json, content_hash, generate_key
from agent_space.identity import read_pass
from agent_space.space import Space, hand_over
from agent_space.testing.test_space import VOCABULARY, create_test_space


async def start(scheme: str = "ws", **options):
    space = create_test_space(**options)
    listener = await space.listen(f"{scheme}://127.0.0.1:0")
    return space, listener.address


def test_canonical_json_matches_typescript():
    assert canonical_json({"b": 1, "a": [True, None, "x"], "c": {"z": 1.5, "y": 1e21}}) == '{"a":[true,null,"x"],"b":1,"c":{"y":1e+21,"z":1.5}}'
    assert content_hash(VOCABULARY).startswith("sha256:")


@pytest.mark.parametrize("scheme", ["ws", "http"])
async def test_enter_observe_act(scheme):
    space, address = await start(scheme)
    key = generate_key()
    p = await Participant.connect(address, key=key)
    assert p.parts["core"] == 0
    joined = await p.join("member")
    assert joined["participant"] == key.identity
    seen = await p.observe(["text"])
    assert "lamp" in seen["forms"]["text"]["data"]
    updates = []
    out = await p.act("test.toggle", "lamp:1", basis=seen["seq"]).on_update(lambda u: updates.append(u["type"]))
    assert out.end is End.DONE and out.result == {"on": True}
    assert updates == ["accepted"]
    await p.close()
    await space.close()


async def test_stdio():
    command = f'"{sys.executable}" -m agent_space.testing test-space --listen stdio'
    p = await Participant.connect(command, reconnect=False)
    await p.join()
    out = await p.act("test.add", "counter:1", {"n": 2})
    assert out.end is End.DONE and out.result == {"value": 2}
    await p.close()


async def test_refusals_are_typed():
    space, address = await start()
    guest = await Participant.connect(address)
    await guest.join("guest")
    out = await guest.act("test.toggle", "lamp:1")
    assert out.end is End.REFUSED and out.reason.code is ReasonCode.FORBIDDEN
    seen = await guest.observe()
    assert "code" not in next(t for t in seen["structure"]["things"] if t["id"] == "safe:1")
    p = await Participant.connect(address)
    await p.join()
    assert (await p.act("test.add", "counter:1", {"n": "x"})).reason.code is ReasonCode.MALFORMED
    assert (await p.act("x.nothing")).reason.code is ReasonCode.UNSUPPORTED
    before = await p.observe()
    await p.act("test.toggle", "lamp:1")
    stale = await p.act("test.toggle", "lamp:1", basis=before["seq"])
    assert stale.end is End.REFUSED and stale.reason.code is ReasonCode.STALE
    packed = await p.act("test.pack", "box:1", {"balls": ["ball:2", "ball:3"]})
    assert packed.reason.code is ReasonCode.CONSTRAINT and packed.reason.data == {"constraint": "test.capacity"}
    namespaced = await p.act("test.toggle", "lamp:9")
    assert namespaced.reason.code == "test.no_such_thing"
    await guest.close()
    await p.close()
    await space.close()


async def test_idempotent_ids():
    space, address = await start()
    p = await Participant.connect(address, key=generate_key())
    await p.join()
    answers = [await p.act("test.add", "counter:1", {"n": 1}, id="same") for _ in range(3)]
    assert answers[0].raw == answers[1].raw == answers[2].raw
    seen = await p.observe()
    assert next(t for t in seen["structure"]["things"] if t["id"] == "counter:1")["value"] == 1
    await p.close()
    await space.close()


async def test_cancel_deadline_pause():
    space, address = await start()
    p = await Participant.connect(address)
    await p.join()
    slow = p.act("test.wait", args={"seconds": 10})
    await asyncio.sleep(0.2)
    slow.cancel()
    assert (await slow).end is End.CANCELLED
    late = await p.act("test.wait", args={"seconds": 10}, deadline=0.3)
    assert late.end is End.FAILED and late.reason.code is ReasonCode.DEADLINE
    paused = p.act("test.wait", args={"seconds": 0.5})
    await asyncio.sleep(0.1)
    assert (await p.request({"type": "pause", "action": paused.id})).end is End.DONE
    await asyncio.sleep(0.8)
    assert not paused.done.done()
    await p.request({"type": "resume", "action": paused.id})
    assert (await paused).end is End.DONE
    await p.close()
    await space.close()


async def test_resume_after_drop():
    space, address = await start()
    p = await Participant.connect(address, key=generate_key())
    await p.join()
    await p.subscribe()
    seen: list[int] = []
    p.on("event", lambda e: seen.append(e["seq"]))
    other = await Participant.connect(address)
    await other.join()
    await other.act("test.add", "counter:1", {"n": 1})
    await asyncio.sleep(0.1)
    reconnected = asyncio.get_running_loop().create_future()
    p.on("reconnect", lambda: reconnected.done() or reconnected.set_result(None))
    p.drop()
    meanwhile = p.act("test.add", "counter:1", {"n": 10})
    for _ in range(3):
        await other.act("test.add", "counter:1", {"n": 1})
    await asyncio.wait_for(reconnected, 10)
    assert (await meanwhile).end is End.DONE
    await asyncio.sleep(0.3)
    record = await p.record(seen[0])
    assert seen == [e["seq"] for e in record["entries"]]
    assert sum(1 for e in record["entries"] if e["type"] == "asp.outcome" and e["data"]["id"] == meanwhile.id) == 1
    await p.close()
    await other.close()
    await space.close()


async def test_restart_gives_unknown():
    tmp = tempfile.mkdtemp(prefix="asp-py-")
    command = [sys.executable, "-m", "agent_space.testing", "test-space", "--listen", "ws://127.0.0.1:0", "--data", tmp]
    key = generate_key()
    first = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    address = first.stdout.readline().strip()
    p = await Participant.connect(address, key=key, reconnect=False)
    await p.join()
    p.act("test.wait", args={"seconds": 30}, id="long")
    await asyncio.sleep(0.4)
    first.kill()
    first.wait()
    await p.close()
    again = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    try:
        address = again.stdout.readline().strip()
        q = await Participant.connect(address, key=key, reconnect=False)
        await q.join()
        out = await q.act("test.wait", args={"seconds": 30}, id="long")
        assert out.end is End.UNKNOWN and out.reason.code is ReasonCode.RESTARTED
        await q.close()
    finally:
        again.kill()
        again.wait()
        shutil.rmtree(tmp, ignore_errors=True)


async def test_record_expiry():
    space, address = await start(keep_seconds=1)
    p = await Participant.connect(address)
    await p.join()
    await asyncio.sleep(1.6)
    await p.act("test.toggle", "lamp:1")
    with pytest.raises(OutcomeError) as e:
        await p.record(1)
    assert e.value.outcome.reason.code is ReasonCode.EXPIRED
    await p.close()
    await space.close()


async def test_closing_order():
    space, address = await start()
    p = await Participant.connect(address, reconnect=False)
    await p.join()
    order: list[str] = []
    p.on("message", lambda m: order.append(m["entry"]["type"] if m["type"] == "event" else m["type"]))
    slow = p.act("test.wait", args={"seconds": 30})
    await asyncio.sleep(0.2)
    await space.close()
    out = await slow
    assert out.end is End.CANCELLED and out.reason.code is ReasonCode.CLOSED
    assert [t for t in order if t in ("outcome", "asp.closing")] == ["outcome", "asp.closing"]


async def test_doors_hand_over_and_pass():
    b = create_test_space()
    b_address = (await b.listen("ws://127.0.0.1:0")).address
    a = create_test_space(door_to=b_address, door_space=b.key.identity)
    a_address = (await a.listen("ws://127.0.0.1:0")).address
    here = {"mug": True}
    result = await hand_over(a, door="door:east", thing={"id": "mug:1", "type": "x.mug", "parents": ["test.ball"]}, lock=lambda: None, unlock=lambda: None, remove=lambda r: here.update(mug=False))
    assert result["taken"] and result["result"]["as"] == "test.ball" and here["mug"] is False
    traveller = generate_key()
    p = await Participant.connect(a_address, key=traveller)
    await p.join()
    out = await p.request({"type": "offer", "door": "door:east"})
    assert out.done and read_pass(out.result["pass"], a.key.identity)["participant"] == traveller.identity
    await p.close()
    await a.close()
    await b.close()


def test_sync_participant():
    loop = asyncio.new_event_loop()
    space = create_test_space()
    address = loop.run_until_complete(space.listen("ws://127.0.0.1:0")).address
    import threading

    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        with SyncParticipant.connect(address) as p:
            p.join()
            out = p.act("test.toggle", "lamp:1")
            assert out.end is End.DONE
            assert json.dumps(p.observe()["structure"]).count("lamp:1") == 1
    finally:
        asyncio.run_coroutine_threadsafe(space.close(), loop).result(10)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(5)


async def test_an_episode_handler_that_takes_its_time_holds_up_nobody():
    started = asyncio.Event()
    resets = []

    async def episode(request):
        resets.append(request["task"])
        await started.wait()
        return {"episode": f"e-{request['task']}"}

    space = Space(capabilities={"state": "external", "episode": {"tasks": ["a"], "evaluator": "evaluator"}},
                  vocabulary=VOCABULARY)
    space.handlers.observe = lambda ctx: {"structure": {"things": []}}
    space.handlers.episode = episode
    listener = await space.listen("ws://127.0.0.1:0")
    p = await Participant.connect(listener.address, key=generate_key())
    await p.join()
    first = p.request({"type": "episode", "id": "ep-1", "task": "a"})
    p.request({"type": "episode", "id": "ep-1", "task": "a"})  # sent again while it waits
    seen = await asyncio.wait_for(p.observe(), 5)  # answered while the episode is being started
    assert isinstance(seen["seq"], int)
    started.set()
    out = await asyncio.wait_for(first, 5)
    assert out.end is End.DONE and out.result == {"episode": "e-a"}
    assert resets == ["a"]
    await p.close()
    await space.close()


async def test_a_handler_that_fails_still_answers():
    def episode(request):
        raise RuntimeError("the simulator did not start")

    space = Space(capabilities={"state": "external", "episode": {"tasks": ["a"], "evaluator": "evaluator"}},
                  vocabulary=VOCABULARY)
    space.handlers.observe = lambda ctx: {"structure": {"things": []}}
    space.handlers.episode = episode
    listener = await space.listen("ws://127.0.0.1:0")
    p = await Participant.connect(listener.address, key=generate_key())
    await p.join()
    out = await asyncio.wait_for(p.request({"type": "episode", "task": "a"}), 5)
    assert out.end is End.FAILED and out.reason.code == "sdk.handler_error"
    assert "did not start" in out.reason.text
    await p.close()
    await space.close()
