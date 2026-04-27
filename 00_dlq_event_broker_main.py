"""Fetch messages from the event broker DLQ and save them by run date."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import boto3  # pyright: ignore[reportMissingImports]
from botocore.exceptions import BotoCoreError, ClientError  # pyright: ignore[reportMissingImports]

PROJECT_DIR = Path(__file__).resolve().parent
QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/927421207401/dlq_event_broker"
REGION_NAME = "ap-south-1"
PROFILE_NAME = "prod"
MAX_MESSAGES = 10
WAIT_TIME_SECONDS = 2
VISIBILITY_TIMEOUT_SECONDS = 300

JsonDict = dict[str, Any]


def ordinal(day: int) -> str:
    """Return an ordinal day string, for example 1st, 2nd, 3rd, or 4th."""
    if 11 <= day % 100 <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{day}{suffix}"


def get_run_folder(run_date: date | None = None) -> Path:
    """Return today's output folder in the format '27th April 2026'."""
    selected_date = run_date or date.today()
    folder_name = f"{ordinal(selected_date.day)} {selected_date.strftime('%B %Y')}"
    return PROJECT_DIR / folder_name


def get_output_file(run_date: date | None = None) -> Path:
    """Return the DLQ message output path for the selected date."""
    selected_date = run_date or date.today()
    return get_run_folder(selected_date) / (
        f"sqs_messages_event_broker_{selected_date.isoformat()}.json"
    )


def create_sqs_client() -> Any:
    session = boto3.Session(profile_name=PROFILE_NAME, region_name=REGION_NAME)
    return session.client("sqs")


def fetch_messages() -> list[JsonDict]:
    """Poll SQS until no more messages are returned."""
    sqs = create_sqs_client()
    messages: list[JsonDict] = []

    while True:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=MAX_MESSAGES,
            WaitTimeSeconds=WAIT_TIME_SECONDS,
            VisibilityTimeout=VISIBILITY_TIMEOUT_SECONDS,
        )
        batch = response.get("Messages", [])
        if not batch:
            return messages

        messages.extend(batch)
        print(f"Fetched {len(messages)} message(s) so far...")


def parse_body(body: Any) -> Any:
    if not isinstance(body, str):
        return body

    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return body


def transform_messages(messages: list[JsonDict]) -> list[JsonDict]:
    """Keep the message ID and decode the nested SQS body when possible."""
    return [
        {
            "MessageId": message.get("MessageId"),
            "Body": parse_body(message.get("Body")),
        }
        for message in messages
    ]


def save_json(data: Any, output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)


def main() -> int:
    output_file = get_output_file()

    try:
        raw_messages = fetch_messages()
    except (BotoCoreError, ClientError) as exc:
        print(f"Failed to fetch messages: {exc}")
        return 1

    messages = transform_messages(raw_messages)
    save_json(messages, output_file)

    print(f"Saved {len(messages)} message(s) to {output_file}.")
    if not messages:
        print("No messages available in DLQ.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
