"""Export selected route groups into separate JSON files."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

PROJECT_DIR = Path(__file__).resolve().parent
ROUTE_MESSAGES_FILE = "sqs_routes_messages.json"
OUTPUT_FOLDER_NAME = "messages"

ROUTE_EXPORTS = {
    "recorded_conversations_list.json": [
        "/v1/recordings/recorded-conversations",
        "v1/recordings/recorded-conversations",
    ],
    "ml_inference_results_list.json": ["/ml-inference-results"],
    "process_callback_list.json": ["v1/recordings/process-callback"],
}

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
    if not input_file.exists():
        raise FileNotFoundError(f"Route messages file not found: {input_file}")

    with input_file.open("r", encoding="utf-8") as file:
        route_messages = json.load(file)

    if not isinstance(route_messages, dict):
        raise ValueError(f"Expected a JSON object in {input_file}")

    return route_messages


def collect_messages_for_routes(
    route_messages: dict[str, list[JsonDict]], routes: list[str]
) -> list[JsonDict]:
    messages: list[JsonDict] = []
    for route in routes:
        route_items = route_messages.get(route, [])
        if isinstance(route_items, list):
            messages.extend(route_items)
    return messages


def save_json(data: Any, output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)


def export_route_messages(
    route_messages: dict[str, list[JsonDict]], output_folder: Path
) -> dict[str, int]:
    exported_counts: dict[str, int] = {}

    for file_name, routes in ROUTE_EXPORTS.items():
        messages = collect_messages_for_routes(route_messages, routes)
        save_json(messages, output_folder / file_name)
        exported_counts[file_name] = len(messages)

    return exported_counts


def main() -> int:
    target_folder = get_today_folder()
    route_messages = load_route_messages(target_folder)
    output_folder = target_folder / OUTPUT_FOLDER_NAME
    exported_counts = export_route_messages(route_messages, output_folder)

    print(f"Exported route files to {output_folder}")
    for file_name, count in exported_counts.items():
        print(f"{file_name}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
