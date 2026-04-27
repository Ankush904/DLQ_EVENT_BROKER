"""Build route-level DLQ artifacts from today's fetched SQS messages."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

PROJECT_DIR = Path(__file__).resolve().parent
INPUT_PATTERN = "sqs_messages_event_broker_*.json"
BODY_OUTPUT_FILE = "sqs_messages_body.json"
ROUTE_OUTPUT_FILE = "sqs_routes_messages.json"

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


def get_latest_input_file(target_folder: Path) -> Path:
    input_files = sorted(target_folder.glob(INPUT_PATTERN))
    if not input_files:
        raise FileNotFoundError(f"No SQS message file found in {target_folder}")
    return input_files[-1]


def load_json(input_file: Path) -> Any:
    with input_file.open("r", encoding="utf-8") as file:
        return json.load(file)


def save_json(data: Any, output_file: Path) -> None:
    with output_file.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)


def extract_message_bodies(messages: list[JsonDict]) -> list[Any]:
    return [message.get("Body") for message in messages]


def group_messages_by_route(message_bodies: list[Any]) -> dict[str, list[JsonDict]]:
    grouped_messages: defaultdict[str, list[JsonDict]] = defaultdict(list)

    for message in message_bodies:
        if not isinstance(message, dict):
            continue

        route = message.get("route")
        if isinstance(route, str) and route:
            grouped_messages[route].append(message)

    return dict(grouped_messages)


def main() -> int:
    target_folder = get_today_folder()
    input_file = get_latest_input_file(target_folder)
    sqs_messages = load_json(input_file)

    if not isinstance(sqs_messages, list):
        raise ValueError(f"Expected a JSON list in {input_file}")

    message_bodies = extract_message_bodies(sqs_messages)
    route_messages = group_messages_by_route(message_bodies)

    save_json(message_bodies, target_folder / BODY_OUTPUT_FILE)
    save_json(route_messages, target_folder / ROUTE_OUTPUT_FILE)

    print(f"Loaded {len(sqs_messages)} messages from {input_file}")
    print(f"Saved {len(message_bodies)} message bodies to {BODY_OUTPUT_FILE}")
    print(f"Saved {len(route_messages)} route group(s) to {ROUTE_OUTPUT_FILE}")
    print("Route message counts:")
    for route, messages in sorted(
        route_messages.items(), key=lambda item: len(item[1]), reverse=True
    ):
        print(f"{route}: {len(messages)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
