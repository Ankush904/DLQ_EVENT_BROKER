"""Pick the target DLQ from the first CLI argument."""

from __future__ import annotations

import sys

QUEUE_URLS = {
    "event_broker": "https://sqs.ap-south-1.amazonaws.com/927421207401/dlq_event_broker",
    "bulk_sync_broker": "https://sqs.ap-south-1.amazonaws.com/927421207401/dlq_bulk_sync_broker",
}
DEFAULT_QUEUE = "bulk_sync_broker"


def select_queue(args: list[str] | None = None) -> str:
    """Return the queue URL named by the first CLI arg, defaulting to event_broker."""
    argv = sys.argv[1:] if args is None else args
    name = argv[0] if argv else DEFAULT_QUEUE
    if name not in QUEUE_URLS:
        raise SystemExit(f"Unknown queue {name!r}. Choose one of: {', '.join(QUEUE_URLS)}")
    return QUEUE_URLS[name]


if __name__ == "__main__":
    assert select_queue([]) == QUEUE_URLS[DEFAULT_QUEUE]
    assert select_queue(["bulk_sync_broker"]) == QUEUE_URLS["bulk_sync_broker"]
    print("ok")
