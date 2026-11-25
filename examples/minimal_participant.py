"""A minimal participant: enter a space, observe it, act, and print outcomes and events.

    python examples/minimal_participant.py ws://127.0.0.1:7777 test.toggle lamp:1
"""

import asyncio
import sys

from agent_space import Participant, generate_key


async def main(address: str, verb: str, obj: str | None) -> None:
    async with await Participant.connect(address, key=generate_key()) as p:
        p.on("event", lambda e: print(f"  event {e['seq']:>4} {e['type']} {e.get('things', '')}"))
        joined = await p.join()
        print(f"entered {p.hello['space'][:20]}… as {joined['role']} ({joined['presence']})")

        verbs = (await p.vocabulary()).get("verbs", {})
        print("verbs:", ", ".join(verbs))

        seen = await p.observe()
        for thing in seen["structure"]["things"]:
            print(f"  {thing['id']:<12} {thing['type']}")

        await p.subscribe()
        outcome = await p.act(verb, obj, basis=seen["seq"]).on_update(lambda u: print(f"  {u['type']}"))
        reason = f" ({outcome.reason.code}: {outcome.reason.text})" if outcome.reason else ""
        print(f"{verb} {obj or ''} -> {outcome.end}{reason} result={outcome.result} at {outcome.seq}")
        await asyncio.sleep(0.3)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None))
