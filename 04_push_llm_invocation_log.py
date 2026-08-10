"""Push llm-invocation-log DLQ messages to their target API, patching workspace_id."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from datetime import date, datetime
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

from rich import print

PROJECT_DIR = Path(__file__).resolve().parent
ROUTE_MESSAGES_FILE = "sqs_routes_messages.json"
TARGET_ROUTES = [
    "api/v1/client/organisation/llm-invocation-log",
    "/api/v1/client/organisation/llm-invocation-log",
]
SPECIAL_DATE = date(2025, 6, 26)
SPECIAL_WORKSPACE_ID = 3082
DEFAULT_WORKSPACE_ID = 616

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


def collect_target_messages(route_messages: dict[str, list[JsonDict]]) -> list[JsonDict]:
    messages: list[JsonDict] = []
    seen_routes: set[str] = set()

    for route in TARGET_ROUTES:
        matched_routes = (
            [key for key in route_messages if fnmatch(key, route)]
            if "*" in route or "?" in route
            else [route]
        )
        for matched_route in matched_routes:
            if matched_route in seen_routes:
                continue
            seen_routes.add(matched_route)
            messages.extend(route_messages.get(matched_route, []))

    return messages


def workspace_id_for_invoke_time(invoke_time: str) -> int:
    invoke_date = datetime.fromisoformat(invoke_time).date()
    return SPECIAL_WORKSPACE_ID if invoke_date == SPECIAL_DATE else DEFAULT_WORKSPACE_ID


def build_payload(message: JsonDict) -> JsonDict:
    body = dict(message.get("body", {}))
    invoke_time = body.get("meta", {}).get("invoke_time")
    if invoke_time:
        body["workspace_id"] = workspace_id_for_invoke_time(invoke_time)
    return body


def push_message(message: JsonDict, body: JsonDict) -> None:
    url = message["host"].rstrip("/") + "/" + message["route"].lstrip("/")
    headers = message.get("headers", {})
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers=headers,
        method=message.get("method", "POST"),
    )
    with urllib.request.urlopen(request) as response:
        response.read()


def main() -> int:
    target_folder = get_today_folder()
    route_messages = load_route_messages(target_folder)
    messages = collect_target_messages(route_messages)

    if not messages:
        print("[yellow]No llm-invocation-log messages found.[/yellow]")
        return 0

    success = 0
    for index, message in enumerate(messages, start=1):
        body = build_payload(message)
        url = message["host"].rstrip("/") + "/" + message["route"].lstrip("/")
        print(f"[{index}/{len(messages)}] POST {url} workspace_id={body.get('workspace_id')}")
        try:
            push_message(message, body)
            success += 1
            print(f"[{index}/{len(messages)}] [green]OK[/green]")
        except (urllib.error.URLError, urllib.error.HTTPError) as exc:
            print(f"[{index}/{len(messages)}] [red]FAILED (workspace_id={body.get('workspace_id')}): {exc}[/red]")

    print(f"[bold]Pushed {success}/{len(messages)} message(s).[/bold]")
    return 0


def _self_check() -> None:
    assert workspace_id_for_invoke_time("2025-06-26T07:10:54.668864") == SPECIAL_WORKSPACE_ID
    assert workspace_id_for_invoke_time("2026-06-26T07:10:54.668864") == DEFAULT_WORKSPACE_ID
    payload = build_payload(
        {"body": {"workspace_id": None, "meta": {"invoke_time": "2025-06-26T00:00:00"}}}
    )
    assert payload["workspace_id"] == SPECIAL_WORKSPACE_ID, payload


if __name__ == "__main__":
    _self_check()
    raise SystemExit(main())
