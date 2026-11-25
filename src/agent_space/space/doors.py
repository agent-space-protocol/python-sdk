"""The giving side of a handover through a door: lock the thing, offer it to the other space, and
remove it once the other side's record shows the take. After a disconnect midway, ask the other
side with `take`: its record decides."""

from __future__ import annotations

import asyncio
from typing import Any, Callable

from ..participant import Participant
from ..protocol import Outcome
from ..util import random_token


async def hand_over(
    space: Any,
    *,
    door: str,
    thing: dict[str, Any],
    lock: Callable[[], None],
    unlock: Callable[[], None],
    remove: Callable[[dict[str, Any]], None],
    role: str | None = None,
    retry_for: float = 60.0,
    offer_id: str | None = None,
) -> dict[str, Any]:
    """Hands a thing over through a door; at every moment exactly one space owns it.

    Returns `{"taken": True, "result": {...}}` or `{"taken": False}`."""
    target = next((d for d in (space.capabilities.get("doors") or {}).get("doors") or [] if d["thing"] == door), None)
    if not target:
        raise ValueError(f"no door {door}")
    oid = offer_id or f"offer-{random_token(8)}"
    lock()
    offered = False
    loop = asyncio.get_running_loop()
    until = loop.time() + retry_for
    while True:
        p: Participant | None = None
        try:
            p = await Participant.connect(target["to"], key=space.key, reconnect=False)
            await p.join(role)
            if not offered:
                offered = True
                res = await p.request({"type": "offer", "id": oid, "door": door, "thing": thing})
                assert isinstance(res, Outcome)
                if res.done:
                    remove(res.result)
                    return {"taken": True, "result": res.result}
                if res.end == "refused":
                    unlock()
                    return {"taken": False, "outcome": res}
            else:
                res = await p.request({"type": "take", "offer": oid})
                if isinstance(res, Outcome) and res.done:
                    if res.result.get("taken"):
                        result = {"thing": res.result["thing"], "as": thing["type"], "take": res.result["take"]}
                        remove(result)
                        return {"taken": True, "result": result}
                    unlock()
                    return {"taken": False}
        except Exception:
            offered = True
        finally:
            if p:
                await p.close()
        if loop.time() > until:
            raise TimeoutError(f"could not learn whether {door} took {thing['id']}; it stays locked")
        await asyncio.sleep(0.2)
