"""Keep one message per route in an exported file and delete the duplicates from the DLQ.

Usage:
    python 07_dedupe_route_messages.py <exported_file.json> [queue_name] [--apply]

Without --apply it only reports what would be deleted (dry run).
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import boto3  # pyright: ignore[reportMissingImports]
from botocore.exceptions import BotoCoreError, ClientError  # pyright: ignore[reportMissingImports]
from rich import print

from queues import select_queue

PROJECT_DIR = Path(__file__).resolve().parent
REGION_NAME = "ap-south-1"
PROFILE_NAME = "prod"
OUTPUT_FOLDER_NAME = "messages"
KEEP_PER_ROUTE = 1

JsonDict = dict[str, Any]


def ordinal(day: int) -> str:
    if 11 <= day % 100 <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{day}{suffix}"


def get_today_folder() -> Path:
    today = date.today()
    target_folder = PROJECT_DIR / f"{ordinal(today.day)} {today.strftime('%B %Y')}"
    if not target_folder.is_dir():
        raise FileNotFoundError(f"Today's folder not found: {target_folder}")
    return target_folder


def duplicate_handles(messages: list[JsonDict]) -> list[str]:
    """Receipt handles of every message beyond the first KEEP_PER_ROUTE of each route."""
    by_route: dict[str, list[JsonDict]] = defaultdict(list)
    for message in messages:
        by_route[message.get("route", "")].append(message)

    return [
        handle
        for group in by_route.values()
        for message in group[KEEP_PER_ROUTE:]
        if (handle := message.get("_ReceiptHandle"))
    ]


def delete_messages(queue_url: str, receipt_handles: list[str]) -> int:
    session = boto3.Session(profile_name=PROFILE_NAME, region_name=REGION_NAME)
    sqs = session.client("sqs")
    deleted = 0

    for i in range(0, len(receipt_handles), 10):
        entries = [
            {"Id": str(idx), "ReceiptHandle": handle}
            for idx, handle in enumerate(receipt_handles[i : i + 10])
        ]
        response = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
        deleted += len(response.get("Successful", []))
        for failure in response.get("Failed", []):
            print(f"[red]Failed to delete message: {failure}[/red]")

    return deleted


def main() -> int:
    args = [arg for arg in sys.argv[1:] if arg != "--apply"]
    apply = "--apply" in sys.argv
    if not args:
        print("[red]Usage: python 07_dedupe_route_messages.py <exported_file.json> [queue_name] [--apply][/red]")
        return 1

    queue_url = select_queue(args[1:])
    input_file = get_today_folder() / OUTPUT_FOLDER_NAME / args[0]
    with input_file.open("r", encoding="utf-8") as file:
        messages: list[JsonDict] = json.load(file)

    handles = duplicate_handles(messages)
    kept = len(messages) - len(handles)
    print(f"{len(messages)} message(s); keeping {kept}, deleting {len(handles)}")

    if not handles:
        return 0
    if not apply:
        print("[yellow]Dry run. Re-run with --apply to delete.[/yellow]")
        return 0

    try:
        deleted = delete_messages(queue_url, handles)
    except (BotoCoreError, ClientError) as exc:
        print(f"[red]Failed to delete messages: {exc}[/red]")
        return 1

    print(f"[green]Deleted {deleted}/{len(handles)} message(s) from {queue_url}[/green]")
    return 0


def _self_check() -> None:
    sample = [
        {"route": "a", "_ReceiptHandle": "h1"},
        {"route": "a", "_ReceiptHandle": "h2"},
        {"route": "a", "_ReceiptHandle": "h3"},
        {"route": "b", "_ReceiptHandle": "h4"},
        {"route": "b"},  # no handle -> skipped
    ]
    assert duplicate_handles(sample) == ["h2", "h3"], duplicate_handles(sample)


if __name__ == "__main__":
    _self_check()
    raise SystemExit(main())
