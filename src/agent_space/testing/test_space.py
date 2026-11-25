"""The test space, written with the Python SDK's space skeleton: a minimal space that declares every
capability, with the same vocabulary and profile as the inspector's TypeScript test space, so that
`agent-space check` tests this SDK's space side and every pairing of SDKs can be run."""

from __future__ import annotations

import asyncio
import copy
import json
import os
from pathlib import Path
from typing import Any

from ..identity import KeyPair
from ..space import ActionContext, EditContext, MemoryStore, Refused, Space, SqliteStore, StreamContext

VOCABULARY: dict[str, Any] = {
    "namespaces": {"test": {"version": "0.1.0", "line": "The conformance test space: a lamp, a counter, a box of balls, a safe and a door."}},
    "types": {
        "test.thing": {"line": "Anything in the test space."},
        "test.lamp": {"parent": "test.thing", "line": "A lamp that is on or off.", "properties": {"type": "object", "properties": {"on": {"type": "boolean"}}}},
        "test.counter": {"parent": "test.thing", "line": "A counter that only adds up.", "properties": {"type": "object", "properties": {"value": {"type": "integer"}}}},
        "test.box": {"parent": "test.thing", "line": "A box that holds at most `capacity` balls.", "properties": {"type": "object", "properties": {"capacity": {"type": "integer", "minimum": 0}}}},
        "test.ball": {"parent": "test.thing", "line": "A ball, in a box or loose."},
        "test.safe": {"parent": "test.thing", "line": "A safe whose code guests may not see.", "properties": {"type": "object", "properties": {"code": {"type": "string"}}}},
        "test.door": {"parent": "test.thing", "line": "A door to another space."},
    },
    "relations": {"test.in": {"from": "test.ball", "to": "test.box", "many": "one", "line": "A ball is in a box."}},
    "verbs": {
        "test.toggle": {"line": "Switch a lamp on or off.", "object": "test.lamp", "effect": "change", "outcomes": ["done", "refused"], "reasons": ["stale", "test.no_such_thing"], "default": {"end": "done"}, "stale": "refuse"},
        "test.add": {"line": "Add n to a counter.", "object": "test.counter", "params": {"type": "object", "properties": {"n": {"type": "integer", "minimum": 1}}, "required": ["n"]}, "effect": "change", "default": {"end": "done"}, "stale": "allow"},
        "test.wait": {"line": "Wait a number of seconds, reporting progress; stops when cancelled.", "params": {"type": "object", "properties": {"seconds": {"type": "number", "minimum": 0, "maximum": 3600}}, "required": ["seconds"]}, "effect": "read", "outcomes": ["done", "failed", "cancelled", "unknown"], "default": {"end": "done"}},
        "test.pack": {"line": "Put balls into a box, all or none.", "object": "test.box", "params": {"type": "object", "properties": {"balls": {"type": "array", "items": {"type": "string"}, "minItems": 1}}, "required": ["balls"]}, "effect": "change", "outcomes": ["done", "refused"], "reasons": ["constraint", "test.no_such_thing"], "default": {"end": "done"}},
        "test.launch": {"line": "Launch something that cannot be undone; waits for the owner's consent.", "effect": "irreversible", "default": {"end": "done"}},
        "test.paint": {"line": "Paint a thing; only the owner may.", "object": "test.thing", "params": {"type": "object", "properties": {"color": {"type": "string"}}, "required": ["color"]}, "effect": "change", "default": {"end": "refused", "reason": {"code": "test.nothing_to_paint", "text": "nothing to paint"}}},
    },
    "events": {
        "test.switched": {"line": "A lamp was switched.", "data": {"type": "object", "properties": {"on": {"type": "boolean"}}}, "perceived": "all"},
        "test.added": {"line": "A counter went up.", "data": {"type": "object", "properties": {"value": {"type": "integer"}}}, "perceived": "all"},
        "test.packed": {"line": "Balls were put into a box."},
        "test.launched": {"line": "Something was launched."},
        "test.painted": {"line": "A thing was painted."},
        "test.changed": {"line": "The world was edited."},
        "test.arrived": {"line": "A thing arrived through a door."},
    },
    "constraints": {"test.capacity": {"line": "A box holds at most its capacity of balls."}, "test.one_place": {"line": "A ball is in at most one box."}},
    "forms": {"text": {"media": "text/plain", "line": "The space described in one paragraph."}, "test.ticks": {"media": "text/plain", "continuous": True, "line": "A tick every 1/rate seconds."}},
    "pages": {
        "test": "\n".join(
            [
                "# The test space",
                "",
                "A minimal space for conformance tests. It holds a lamp (`lamp:1`), a counter (`counter:1`),",
                "a box (`box:1`, capacity 2) with `ball:1` in it, two loose balls (`ball:2`, `ball:3`), a safe",
                "(`safe:1`) whose code guests do not see, and a door (`door:east`).",
                "",
                "- `test.toggle lamp:1` switches the lamp; it refuses stale actions.",
                "- `test.add counter:1 {n}` adds to the counter.",
                "- `test.wait {seconds}` waits and reports progress; cancel it to stop it.",
                "- `test.pack box:1 {balls}` puts balls into the box, all or none (`test.capacity`).",
                "- `test.launch` waits for the owner's consent.",
                "- `test.paint <thing> {color}` is for the owner only.",
            ]
        )
    },
}

PROFILE: dict[str, Any] = {
    "role": "member",
    "owner": "owner",
    "done": {"verb": "test.toggle", "object": "lamp:1"},
    "counter": {"verb": "test.add", "object": "counter:1", "args": {"n": 1}, "property": "value"},
    "slow": {"verb": "test.wait", "args": {"seconds": 30}},
    "stale": {"verb": "test.toggle", "object": "lamp:1"},
    "constraint": {"verb": "test.pack", "object": "box:1", "args": {"balls": ["ball:2", "ball:3"]}},
    "consent": {"verb": "test.launch"},
    "offer": {"thing": {"id": "parcel:1", "type": "test.parcel", "parents": ["test.ball", "test.thing"]}, "unknown": {"id": "blob:1", "type": "elsewhere.blob", "parents": ["elsewhere.thing"]}},
}


def capabilities(door_to: str, door_space: str | None, grace: float) -> dict[str, Any]:
    door: dict[str, Any] = {"thing": "door:east", "to": door_to}
    if door_space:
        door["space"] = door_space
    return {
        "state": "held",
        "lifetime": "lasting",
        "participants": {
            "max": 64,
            "outside": True,
            "grace_seconds": grace,
            "default_role": "member",
            "roles": {
                "owner": {"line": "May do and see everything, edit, and consent.", "verbs": ["*"], "edit": True, "consent": True},
                "member": {"line": "May use every verb but test.paint.", "verbs": ["test.toggle", "test.add", "test.wait", "test.pack", "test.launch"]},
                "guest": {"line": "May only look; does not see the safe's code or the counter's events.", "verbs": [], "hide": {"properties": ["code"], "events": ["test.added"]}},
                "evaluator": {"line": "Looks and takes scores.", "verbs": []},
            },
        },
        "forms": ["text", "test.ticks"],
        "inputs": ["text/plain"],
        "edit": {"roles": ["owner"], "changes": ["add", "set", "remove", "relate", "unrelate"], "sentences": False},
        "snapshot": {"restore": True},
        "episode": {"tasks": ["light"], "evaluator": "evaluator"},
        "pace": {"max_hz": 50, "clock": "wall"},
        "consent": {"verbs": {"test.launch": {"by": "owner", "seconds": 1}}},
        "pause": {"verbs": ["test.wait"]},
        "senses": {"languages": ["test.path"], "max": 8},
        "views": {"count": 1},
        "doors": {"takes": True, "doors": [door]},
    }


def initial_state() -> dict[str, Any]:
    return {
        "things": {
            "lamp:1": {"id": "lamp:1", "type": "test.lamp", "on": False},
            "counter:1": {"id": "counter:1", "type": "test.counter", "value": 0},
            "box:1": {"id": "box:1", "type": "test.box", "capacity": 2},
            "ball:1": {"id": "ball:1", "type": "test.ball"},
            "ball:2": {"id": "ball:2", "type": "test.ball"},
            "ball:3": {"id": "ball:3", "type": "test.ball"},
            "safe:1": {"id": "safe:1", "type": "test.safe", "code": "4721"},
            "door:east": {"id": "door:east", "type": "test.door"},
        },
        "relations": [{"rel": "test.in", "from": "ball:1", "to": "box:1"}],
        "episode": 0,
    }


def broken(state: dict[str, Any]) -> tuple[str, str] | None:
    seen: set[str] = set()
    for r in state["relations"]:
        if r["rel"] != "test.in":
            continue
        if r["from"] in seen:
            return "test.one_place", f"{r['from']} would be in two boxes"
        seen.add(r["from"])
    for t in state["things"].values():
        if t["type"] != "test.box":
            continue
        n = sum(1 for r in state["relations"] if r["rel"] == "test.in" and r["to"] == t["id"])
        if n > t["capacity"]:
            return "test.capacity", f"{t['id']} would hold {n} balls, more than its capacity {t['capacity']}"
    return None


def create_test_space(*, data: str | None = None, keep_seconds: float | None = None, grace: float = 30, key: KeyPair | None = None, door_to: str = "ws://127.0.0.1:1/nowhere", door_space: str | None = None) -> Space:
    state_path = Path(data) / "state.json" if data else None
    if data:
        Path(data).mkdir(parents=True, exist_ok=True)
    holder = {"state": json.loads(state_path.read_text()) if state_path and state_path.exists() else initial_state()}

    def state() -> dict[str, Any]:
        return holder["state"]

    def save() -> None:
        if state_path:
            tmp = state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(holder["state"]))
            os.replace(tmp, state_path)

    def thing(tid: str | None) -> dict[str, Any]:
        t = state()["things"].get(tid or "")
        if not t:
            raise Refused("test.no_such_thing", f"there is no {tid}")
        return t

    space = Space(
        capabilities=capabilities(door_to, door_space, grace),
        vocabulary=VOCABULARY,
        key=key,
        store=SqliteStore(Path(data) / "record.db") if data else MemoryStore(),
        keep_seconds=keep_seconds,
        server={"name": "agent-space-protocol python test space", "version": "0.0.0"},
        hello={"inspector.profile": PROFILE},
        stop_grace=1.5,
    )
    view_hash = space.put_blob("<!doctype html><title>test space</title><p>The test space.</p>", "text/html")

    @space.observer
    def observe(ctx: Any) -> dict[str, Any]:
        s = state()
        out: dict[str, Any] = {"structure": {"things": list(s["things"].values()), "relations": s["relations"]}}
        if "text" in ctx.forms:
            lamp = s["things"].get("lamp:1", {})
            in_box = [r["from"] for r in s["relations"] if r["to"] == "box:1"]
            out["forms"] = {"text": {"media": "text/plain", "data": f"A lamp, {'on' if lamp.get('on') else 'off'}. A counter at {s['things'].get('counter:1', {}).get('value')}. A box holding {', '.join(in_box) or 'nothing'}. A safe. A door to the east."}}
        return out

    space.handlers.touches = lambda action: list((action.get("args") or {}).get("balls") or []) if action["verb"] == "test.pack" else []

    @space.verb("test.toggle")
    def toggle(ctx: ActionContext) -> dict[str, Any]:
        lamp = thing(ctx.object)
        if lamp["type"] != "test.lamp":
            raise Refused("malformed", f"{ctx.object} is not a lamp")
        lamp["on"] = not lamp["on"]
        save()
        ctx.emit("test.switched", {"on": lamp["on"]}, things=[lamp["id"]])
        return {"result": {"on": lamp["on"]}}

    @space.verb("test.add")
    def add(ctx: ActionContext) -> dict[str, Any]:
        counter = thing(ctx.object)
        counter["value"] += int(ctx.args["n"])
        save()
        ctx.emit("test.added", {"value": counter["value"]}, things=[counter["id"]])
        return {"result": {"value": counter["value"]}}

    @space.verb("test.wait")
    async def wait(ctx: ActionContext) -> dict[str, Any]:
        total = float(ctx.args["seconds"])
        step, t = 0.25, 0.0
        while t < total:
            await ctx.sleep(min(step, total - t))
            t = min(total, t + step)
            ctx.progress(t / total if total else 1, f"waited {t:g} of {total:g} s")
        return {"result": {"waited": ctx.args["seconds"]}}

    @space.verb("test.pack")
    def pack(ctx: ActionContext) -> dict[str, Any]:
        box = thing(ctx.object)
        balls = ctx.args["balls"]
        for b in balls:
            if thing(b)["type"] != "test.ball":
                raise Refused("malformed", f"{b} is not a ball")
        nxt = copy.deepcopy(state())
        for b in balls:
            nxt["relations"] = [r for r in nxt["relations"] if not (r["rel"] == "test.in" and r["from"] == b)]
            nxt["relations"].append({"rel": "test.in", "from": b, "to": box["id"]})
        bad = broken(nxt)
        if bad:
            raise Refused("constraint", bad[1], {"constraint": bad[0]})
        holder["state"] = nxt
        save()
        ctx.emit("test.packed", {"box": box["id"], "balls": balls}, things=[box["id"], *balls])
        return {"result": {"box": box["id"], "balls": balls}}

    @space.verb("test.launch")
    def launch(ctx: ActionContext) -> dict[str, Any]:
        ctx.emit("test.launched", {"by": ctx.presence.id})
        return {"result": {"launched": True}}

    @space.verb("test.paint")
    def paint(ctx: ActionContext) -> dict[str, Any]:
        t = thing(ctx.object)
        t["color"] = str(ctx.args["color"])
        save()
        ctx.emit("test.painted", {"color": t["color"]}, things=[t["id"]])
        return {"result": {"color": t["color"]}}

    @space.editor
    def edit(ctx: EditContext) -> dict[str, Any]:
        nxt = copy.deepcopy(state())
        changed: list[str] = []
        for c in ctx.changes or []:
            op = c["op"]
            if op == "add":
                if c["thing"]["id"] in nxt["things"]:
                    raise Refused("constraint", f"{c['thing']['id']} already exists", {"constraint": "test.unique"})
                if c["thing"]["type"] not in VOCABULARY["types"]:
                    raise Refused("malformed", f"no type {c['thing']['type']}")
                nxt["things"][c["thing"]["id"]] = c["thing"]
                changed.append(c["thing"]["id"])
            elif op == "set":
                t = nxt["things"].get(c["thing"])
                if not t:
                    raise Refused("test.no_such_thing", f"there is no {c['thing']}")
                if c["property"] in ("id", "type"):
                    raise Refused("malformed", f"{c['property']} cannot be set")
                t[c["property"]] = c["value"]
                changed.append(c["thing"])
            elif op == "remove":
                if c["thing"] not in nxt["things"]:
                    raise Refused("test.no_such_thing", f"there is no {c['thing']}")
                del nxt["things"][c["thing"]]
                nxt["relations"] = [r for r in nxt["relations"] if c["thing"] not in (r["from"], r["to"])]
                changed.append(c["thing"])
            elif op == "relate":
                r = c["relation"]
                if r["from"] not in nxt["things"] or r["to"] not in nxt["things"]:
                    raise Refused("test.no_such_thing", "both ends must exist")
                nxt["relations"].append(r)
                changed += [r["from"], r["to"]]
            elif op == "unrelate":
                r = c["relation"]
                nxt["relations"] = [x for x in nxt["relations"] if not (x["rel"] == r["rel"] and x["from"] == r["from"] and x["to"] == r["to"])]
                changed += [r["from"], r["to"]]
        bad = broken(nxt)
        if bad:
            raise Refused("constraint", bad[1], {"constraint": bad[0]})
        holder["state"] = nxt
        save()
        unique = list(dict.fromkeys(changed))
        ctx.emit("test.changed", {"changes": ctx.changes}, things=unique)
        return {"things": unique, "result": {"changed": unique}}

    space.handlers.presentation = lambda p: {k: ("used" if k == "name" else "ignored") for k in p}

    def episode(req: dict[str, Any]) -> dict[str, Any]:
        n = state()["episode"] + 1
        holder["state"] = {**initial_state(), "episode": n}
        save()
        return {"episode": f"e{n}", "task": "light"}

    space.handlers.episode = episode
    space.handlers.score = lambda req: {"episode": f"e{state()['episode']}", "score": 1 if state()["things"].get("lamp:1", {}).get("on") else 0}
    space.handlers.snapshot = lambda: copy.deepcopy(state())

    def restore(s: Any) -> None:
        holder["state"] = s
        save()

    space.handlers.restore = restore

    def sense(language: str, query: str, structure: dict[str, Any], presence: Any) -> Any:
        if language != "test.path":
            return None
        tid, _, prop = query.partition(".")
        t = next((x for x in structure["things"] if x["id"] == tid), None)
        return (t.get(prop) if prop else t) if t else None

    space.handlers.sense = sense
    space.handlers.views = lambda: [{"name": "test", "line": "A one-page view of the test space.", "media": "text/html", "bundle": view_hash}]

    def take(req: dict[str, Any]) -> dict[str, Any]:
        t = req["thing"]
        tid = f"{t['id']}@here"
        new = {k: v for k, v in t.items() if k != "parents"}
        new.update(id=tid, type=req["as"])
        state()["things"][tid] = new
        save()
        return {"thing": tid}

    space.handlers.take = take

    async def stream(ctx: StreamContext) -> None:
        interval = 1 / min(ctx.rate or 4, 20)
        n = 0
        while not ctx.stopping:
            await ctx.sleep(interval)
            ctx.frame({"media": "text/plain", "data": f"tick {n}"})
            n += 1

    space.handlers.stream = stream
    return space


async def serve(listen: str, **options: Any) -> None:
    """Runs the test space until it is closed (standard input ends, or the process is stopped)."""
    import sys

    from ..identity import load_or_create_key
    from ..transport import listen as listen_at

    close_on_eof = options.pop("close_on_eof", False)
    if options.get("data"):
        options["key"] = load_or_create_key(Path(options["data"]) / "space.key")
    space = create_test_space(**options)
    listener = await space.listen(listen)
    print(f"python test space {space.id} at {listener.address}", file=sys.stderr)
    if listen != "stdio":
        print(listener.address, flush=True)
    if close_on_eof and listen != "stdio":
        loop = asyncio.get_running_loop()
        done = loop.create_future()

        def read() -> None:
            sys.stdin.read()
            loop.call_soon_threadsafe(lambda: done.done() or done.set_result(None))

        import threading

        threading.Thread(target=read, daemon=True).start()
        await done
        await space.close()
        return
    if listen == "stdio":
        # The stdio connection closes when standard input ends; then the space closes.
        while True:
            await asyncio.sleep(0.2)
            if not space._sessions:
                await space.close()
                return
    await space.wait_closed()
