# Agent Space Protocol Python SDK

The Python SDK of the [Agent Space Protocol](https://github.com/agent-space-protocol/agentspaceprotocol):
both ends of the protocol, a participant that enters any space and a skeleton for writing a space
that keeps the protocol's guarantees for you. The same two ends, transports and identity as the
[TypeScript SDK](https://github.com/agent-space-protocol/typescript-sdk).

**Status: draft**, like the protocol. Not yet published to PyPI; install it from git.

- Distribution `agent-space-protocol`, import `agent_space`, Python 3.11 or later, asyncio.
- **No dependencies.** The base is the standard library: both ends, all three transports
  (WebSocket, HTTP with server-sent events, standard input and output), Ed25519 and validation.
- Transports: WebSocket, HTTP with server-sent events, standard input and output.
- Identity: Ed25519 keys (RFC 8032), proofs of identity, passes through doors.
- Types: `agent_space.types` is generated from the specification's `schema.json`; messages are
  validated against it.

```sh
uv add "agent-space-protocol @ git+https://github.com/agent-space-protocol/python-sdk"
```

Three extras do the same work faster or more completely; install them where it matters, such as a
space that serves many connections:

| Extra | Uses | Instead of |
|---|---|---|
| `aiohttp` | aiohttp for WebSocket and HTTP, server and client | the standard-library transports (`agent_space._stdlib`) |
| `crypto` | `cryptography` for Ed25519 | the pure-Python RFC 8032 implementation (`agent_space.ed25519`), milliseconds per signature |
| `validate` | `jsonschema` for every keyword of JSON Schema 2020-12 | `agent_space.schemalite`, the keywords the schema and typical verb parameters use |
| `all` | all three | |

```sh
uv add "agent-space-protocol[all] @ git+https://github.com/agent-space-protocol/python-sdk"
```

Both implementations of each give the same bytes on the wire and the same signatures; the tests
check them against each other (RFC 8032 vectors, a standard-library participant in an aiohttp
space and the other way round). `AGENT_SPACE_TRANSPORT=stdlib` keeps the standard-library
transports even with aiohttp installed. `agent_space.transport.implementation()`,
`agent_space.identity.backend()` and `agent_space.protocol.validator_class()` say which is in use.

## A participant

```python
import asyncio
from agent_space import Participant, generate_key

async def main():
    async with await Participant.connect("ws://127.0.0.1:7777", key=generate_key()) as p:
        await p.join(role="member")
        seen = await p.observe()
        p.on("event", lambda entry: print(entry["seq"], entry["type"]))
        await p.subscribe()
        outcome = await p.act("test.toggle", "lamp:1", basis=seen["seq"])
        print(outcome.end, outcome.reason, outcome.result)

asyncio.run(main())
```

[examples/minimal_participant.py](examples/minimal_participant.py) is a complete participant in
33 lines.

Outcomes are typed: `outcome.end` is an `End` (`DONE`, `REFUSED`, `FAILED`, `CANCELLED`,
`UNKNOWN`) and `outcome.reason.code` is a `ReasonCode` for the protocol's own reasons
(`ReasonCode.STALE`, `ReasonCode.FORBIDDEN`, ...) or the namespaced string a space gives, so code can
decide from it whether to wait, retry or try something else. Record entries are plain dicts keyed by
their kind (`entry["type"]`), ready to trigger reactions. `p.act(...)` returns a `Pending` that can be
awaited, observed with `on_update`, and cancelled.

For code without an event loop, `SyncParticipant` runs one on a background thread:

```python
from agent_space import SyncParticipant

with SyncParticipant.connect("ws://127.0.0.1:7777") as p:
    p.join()
    print(p.act("test.toggle", "lamp:1").end)
```

If the connection drops, the participant reconnects, resumes its presence, continues the
subscription from the last sequence number it received and resends every request still waiting;
request IDs are idempotent, so nothing runs twice and every request gets exactly one final answer.

## A space

```python
from agent_space.space import Space, SqliteStore, Refused

space = Space(
    capabilities={"state": "held", "participants": {"grace_seconds": 60}},
    vocabulary={
        "namespaces": {"room": {"version": "0.1.0"}},
        "types": {"room.lamp": {"line": "A lamp."}},
        "verbs": {"room.toggle": {"object": "room.lamp", "effect": "change", "default": {"end": "done"}, "stale": "refuse"}},
        "events": {"room.switched": {"line": "A lamp was switched."}},
    },
    store=SqliteStore("record.db"),
)
state = {"on": False}

@space.observer
def observe(ctx):
    return {"structure": {"things": [{"id": "lamp:1", "type": "room.lamp", "on": state["on"]}]}}

@space.verb("room.toggle")
def toggle(ctx):
    if ctx.object != "lamp:1":
        raise Refused("room.no_such_lamp", f"there is no {ctx.object}")
    state["on"] = not state["on"]
    ctx.emit("room.switched", {"on": state["on"]}, things=["lamp:1"])
    return {"result": {"on": state["on"]}}

# inside a running event loop:
#     await space.listen("ws://127.0.0.1:7777")
```

The skeleton keeps exactly one final outcome for every action (deadlines, cancels, closing and
restarts included), idempotent IDs, `basis` and staleness, `unsupported` for what is not declared,
`malformed` for broken messages with an ID, the record with subscriptions, resuming and expiry,
projection by role, presences kept for a grace period, consent, pausing and doors. A law returns
for `done`, raises `Refused`, `Failed`, `Uncertain`, or `Stopped` when asked to stop; long laws
`await ctx.sleep(...)` or `await ctx.checkpoint()`.

## The test space and test participant

```sh
python -m agent_space.testing test-space --listen ws://127.0.0.1:7777 --data ./space-data
python -m agent_space.testing participant ws://127.0.0.1:7777
```

The test space has the same vocabulary and profile as the inspector's TypeScript test space, so
`agent-space check` tests this SDK's space side, and the test participant runs the same scenario as
`agent-space participant`. The inspector's `test/pairings.ts` runs all four pairings of the two SDKs.

## Development

```sh
uv sync
uv run pytest                                  # with the extras
AGENT_SPACE_TRANSPORT=stdlib uv run pytest     # the standard-library transports
uv run python scripts/generate_types.py --sync ../agentspaceprotocol --check   # schema copy and types match the specification
```

## Licence

Apache-2.0.
