"""`python -m agent_space.testing`: the Python test space and test participant.

    python -m agent_space.testing test-space [--listen ws://127.0.0.1:7777] [--data DIR]
                                             [--keep-seconds N] [--grace N] [--close-on-eof]
    python -m agent_space.testing participant <address>
"""

from __future__ import annotations

import argparse
import asyncio
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m agent_space.testing")
    sub = parser.add_subparsers(dest="command", required=True)
    ts = sub.add_parser("test-space", help="run the test space")
    ts.add_argument("--listen", default="ws://127.0.0.1:7777")
    ts.add_argument("--data")
    ts.add_argument("--keep-seconds", type=float)
    ts.add_argument("--grace", type=float, default=30)
    ts.add_argument("--close-on-eof", action="store_true")
    ts.add_argument("--door-to", default="ws://127.0.0.1:1/nowhere")
    ts.add_argument("--door-space")
    tp = sub.add_parser("participant", help="run the test participant against a space")
    tp.add_argument("address")
    args = parser.parse_args(argv)
    if args.command == "participant":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    if args.command == "test-space":
        from .test_space import serve

        asyncio.run(
            serve(
                args.listen,
                data=args.data,
                keep_seconds=args.keep_seconds,
                grace=args.grace,
                close_on_eof=args.close_on_eof,
                door_to=args.door_to,
                door_space=args.door_space,
            )
        )
        return 0
    from .test_participant import run

    return 0 if asyncio.run(run(args.address)) else 1


if __name__ == "__main__":
    sys.exit(main())
