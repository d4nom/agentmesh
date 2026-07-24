"""Wait for a DLQ envelope without sharing persistent consumer state."""

from __future__ import annotations

import argparse
import asyncio
import os

from wait_for_message import (
    DEFAULT_LOOKBACK_SECONDS,
    find_envelope,
    non_negative_float,
    print_result,
)


def main() -> None:
    arg_parser = argparse.ArgumentParser(description=__doc__)
    arg_parser.add_argument("--subject", required=True, help="e.g. dlq.parse_request")
    arg_parser.add_argument("--wait-seconds", type=non_negative_float, default=0.0)
    arg_parser.add_argument(
        "--lookback-seconds",
        type=non_negative_float,
        default=DEFAULT_LOOKBACK_SECONDS,
    )
    arg_parser.add_argument("--correlation-id", default=None)
    args = arg_parser.parse_args()

    nats_url = os.environ.get("NATS_URL", "nats://localhost:4222")
    result = asyncio.run(
        find_envelope(
            nats_url=nats_url,
            subject=args.subject,
            correlation_id=args.correlation_id,
            wait_seconds=args.wait_seconds,
            lookback_seconds=args.lookback_seconds,
            message_type="error",
        )
    )
    print_result(
        result,
        subject=args.subject,
        correlation_id=args.correlation_id,
        wait_seconds=args.wait_seconds,
    )


if __name__ == "__main__":
    main()
