"""Requeue an exported route's messages back to the source queue and purge them from the DLQ.

Usage:
    python 04_requeue_route_messages.py <exported_file.json> [queue_name]

<exported_file.json> is a file under today's messages/ folder (as produced by
02_export_route_messages.py). [queue_name] selects the DLQ (see queues.py),
defaulting to event_broker; the source queue is derived by dropping its
"dlq_" prefix.
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

import boto3  # pyright: ignore[reportMissingImports]
from botocore.exceptions import BotoCoreError, ClientError  # pyright: ignore[reportMissingImports]
from rich import print

from queues import select_queue

PROJECT_DIR = Path(__file__).resolve().parent
DLQ_URL = select_queue(sys.argv[2:])
REGION_NAME = "ap-south-1"
PROFILE_NAME = "prod"
OUTPUT_FOLDER_NAME = "messages"

JsonDict = dict[str, Any]


def ordinal(day: int) -> str:
    if 11 <= day % 100 <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{day}{suffix}"


def get_today_folder() -> Path:
    today = date.today()
    folder_name = f"{ordinal(today.day)} {today.strftime('%B %Y')}"
    target_folder = PROJECT_DIR / folder_name
    if not target_folder.is_dir():
        raise FileNotFoundError(f"Today's folder not found: {target_folder}")
    return target_folder


def load_messages(target_folder: Path, file_name: str) -> list[JsonDict]:
    input_file = target_folder / OUTPUT_FOLDER_NAME / file_name
    if not input_file.exists():
        raise FileNotFoundError(f"Exported route file not found: {input_file}")
    with input_file.open("r", encoding="utf-8") as file:
        return json.load(file)


def source_queue_url(dlq_url: str) -> str:
    """Derive the source (non-DLQ) queue URL by dropping the 'dlq_' prefix."""
    base, name = dlq_url.rsplit("/", 1)
    if not name.startswith("dlq_"):
        raise ValueError(f"{dlq_url!r} is not a dlq_-prefixed queue URL")
    return f"{base}/{name[len('dlq_'):]}"


def create_sqs_client() -> Any:
    session = boto3.Session(profile_name=PROFILE_NAME, region_name=REGION_NAME)
    return session.client("sqs")


def requeue_and_purge(
    sqs: Any, messages: list[JsonDict], source_url: str, dlq_url: str
) -> tuple[int, int]:
    requeued = 0
    purged = 0

    for i in range(0, len(messages), 10):
        batch = messages[i : i + 10]
        send_entries = []
        handles: list[tuple[str, str | None]] = []
        for idx, message in enumerate(batch):
            entry_id = str(idx)
            body = {key: value for key, value in message.items() if key != "_ReceiptHandle"}
            send_entries.append({"Id": entry_id, "MessageBody": json.dumps(body)})
            handles.append((entry_id, message.get("_ReceiptHandle")))

        send_response = sqs.send_message_batch(QueueUrl=source_url, Entries=send_entries)
        sent_ids = {entry["Id"] for entry in send_response.get("Successful", [])}
        requeued += len(sent_ids)
        for failure in send_response.get("Failed", []):
            print(f"[red]Failed to requeue message: {failure}[/red]")

        delete_entries = [
            {"Id": entry_id, "ReceiptHandle": handle}
            for entry_id, handle in handles
            if entry_id in sent_ids and handle
        ]
        if delete_entries:
            delete_response = sqs.delete_message_batch(QueueUrl=dlq_url, Entries=delete_entries)
            purged += len(delete_response.get("Successful", []))
            for failure in delete_response.get("Failed", []):
                print(f"[red]Failed to delete from DLQ: {failure}[/red]")

    return requeued, purged


def main() -> int:
    if len(sys.argv) < 2:
        print("[red]Usage: python 04_requeue_route_messages.py <exported_file.json> [queue_name][/red]")
        return 1

    file_name = "send_analytics.json"
    target_folder = get_today_folder()
    messages = load_messages(target_folder, file_name)

    if not messages:
        print("[yellow]No messages found in the export file. Nothing to requeue.[/yellow]")
        return 0

    source_url = source_queue_url(DLQ_URL)

    try:
        sqs = create_sqs_client()
        requeued, purged = requeue_and_purge(sqs, messages, source_url, DLQ_URL)
    except (BotoCoreError, ClientError) as exc:
        print(f"[red]Failed to requeue messages: {exc}[/red]")
        return 1

    print(f"[green]Requeued {requeued}/{len(messages)} message(s) to {source_url}[/green]")
    print(f"[green]Purged {purged}/{len(messages)} message(s) from {DLQ_URL}[/green]")
    return 0


def _self_check() -> None:
    assert source_queue_url("https://sqs.ap-south-1.amazonaws.com/1/dlq_event_broker") == (
        "https://sqs.ap-south-1.amazonaws.com/1/event_broker"
    )
    try:
        source_queue_url("https://sqs.ap-south-1.amazonaws.com/1/not_a_dlq")
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for non dlq_ queue")


if __name__ == "__main__":
    _self_check()
    raise SystemExit(main())
