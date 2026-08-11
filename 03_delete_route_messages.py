"""Delete already-exported route messages from the SQS DLQ."""

from __future__ import annotations

import json
from datetime import date
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

import boto3  # pyright: ignore[reportMissingImports]
from botocore.exceptions import BotoCoreError, ClientError  # pyright: ignore[reportMissingImports]
from rich import print

from queues import select_queue

PROJECT_DIR = Path(__file__).resolve().parent
QUEUE_URL = select_queue()
REGION_NAME = "ap-south-1"
PROFILE_NAME = "prod"
ROUTE_MESSAGES_FILE = "sqs_routes_messages.json"

# Routes whose messages have been exported locally and should now be purged from SQS.
ROUTES_TO_DELETE = [
    "api/v1/client/organisation/llm-invocation-log"
]

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


def load_route_messages(target_folder: Path) -> dict[str, list[JsonDict]]:
    input_file = target_folder / ROUTE_MESSAGES_FILE
    with input_file.open("r", encoding="utf-8") as file:
        return json.load(file)


def collect_receipt_handles(
    route_messages: dict[str, list[JsonDict]], routes: list[str]
) -> list[str]:
    handles: list[str] = []
    seen_routes: set[str] = set()

    for route in routes:
        if "*" in route or "?" in route:
            matched_routes = [key for key in route_messages if fnmatch(key, route)]
        else:
            matched_routes = [route]

        for matched_route in matched_routes:
            if matched_route in seen_routes:
                continue
            seen_routes.add(matched_route)
            for message in route_messages.get(matched_route, []):
                handle = message.get("_ReceiptHandle")
                if handle:
                    handles.append(handle)

    return handles


def create_sqs_client() -> Any:
    session = boto3.Session(profile_name=PROFILE_NAME, region_name=REGION_NAME)
    return session.client("sqs")


def delete_messages(receipt_handles: list[str]) -> int:
    sqs = create_sqs_client()
    deleted = 0

    for i in range(0, len(receipt_handles), 10):
        batch = receipt_handles[i : i + 10]
        entries = [
            {"Id": str(idx), "ReceiptHandle": handle}
            for idx, handle in enumerate(batch)
        ]
        response = sqs.delete_message_batch(QueueUrl=QUEUE_URL, Entries=entries)
        deleted += len(response.get("Successful", []))
        for failure in response.get("Failed", []):
            print(f"[red]Failed to delete message: {failure}[/red]")

    return deleted


def main() -> int:
    target_folder = get_today_folder()
    route_messages = load_route_messages(target_folder)
    receipt_handles = collect_receipt_handles(route_messages, ROUTES_TO_DELETE)

    if not receipt_handles:
        print("[yellow]No messages found for the given routes. Nothing to delete.[/yellow]")
        return 0

    try:
        deleted = delete_messages(receipt_handles)
    except (BotoCoreError, ClientError) as exc:
        print(f"[red]Failed to delete messages: {exc}[/red]")
        return 1

    print(f"[green]Deleted {deleted}/{len(receipt_handles)} message(s) from SQS.[/green]")
    return 0


def _self_check() -> None:
    sample = {
        ROUTES_TO_DELETE[0]: [
            {"_ReceiptHandle": "h1"},
            {"_ReceiptHandle": "h2"},
        ],
        "other/route": [{"_ReceiptHandle": "h3"}],
    }
    handles = collect_receipt_handles(sample, ROUTES_TO_DELETE)
    assert handles == ["h1", "h2"], handles


if __name__ == "__main__":
    _self_check()
    raise SystemExit(main())
