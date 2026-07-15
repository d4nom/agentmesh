"""Peek at a DLQ subject and print whatever's there. Polls up to --wait-seconds
(default 0 = one immediate check) and exits 1 if nothing shows up — a real
pass/fail check for demo scripts, not a log-tail-and-hope.

Pass --correlation-id to require the specific message from a given inject
run rather than accepting whatever else happens to already be on the
subject from an earlier demo run.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time

from platform_core.bus import connect, ensure_streams, pull_subscribe
from platform_core.envelope import Envelope


async def main() -> None:
    arg_parser = argparse.ArgumentParser(description=__doc__)
    arg_parser.add_argument("--subject", required=True, help="e.g. dlq.parse_request")
    arg_parser.add_argument("--wait-seconds", type=float, default=0.0)
    arg_parser.add_argument("--correlation-id", default=None)
    args = arg_parser.parse_args()

    nats_url = os.environ.get("NATS_URL", "nats://localhost:4222")
    nc, js = await connect(nats_url)
    await ensure_streams(js)

    durable = "show-dlq-" + args.subject.replace(".", "-")
    sub = await pull_subscribe(js, args.subject, durable=durable)

    deadline = time.monotonic() + args.wait_seconds
    match: Envelope | None = None
    while True:
        try:
            msgs = await sub.fetch(10, timeout=2)
        except TimeoutError:
            msgs = []
        for msg in msgs:
            envelope = Envelope.model_validate_json(msg.data)
            await msg.ack()
            if args.correlation_id is None or envelope.correlation_id == args.correlation_id:
                if match is None:
                    match = envelope
        if match or time.monotonic() >= deadline:
            break

    # A one-shot peek has nothing left to flush, and draining a pull
    # subscription with an outstanding fetch() window reliably times out
    # (nats-py waits for that window to close); just close instead.
    await nc.close()

    if match is None:
        target = (
            f"correlation_id={args.correlation_id} on {args.subject}"
            if args.correlation_id
            else args.subject
        )
        print(f"nothing found for {target} within {args.wait_seconds}s")
        raise SystemExit(1)

    print(match.model_dump_json(indent=2))


if __name__ == "__main__":
    asyncio.run(main())
