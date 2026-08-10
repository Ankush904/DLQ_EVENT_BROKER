"""Push pitch-audit-analysis-summary DLQ messages to their target API."""

from __future__ import annotations

import json
import subprocess
from fnmatch import fnmatch
from datetime import date
from pathlib import Path
from typing import Any
from rich import print

PROJECT_DIR = Path(__file__).resolve().parent
ROUTE_MESSAGES_FILE = "sqs_routes_messages.json"
TARGET_ROUTES = [
    "api/v1/conversation/pitch-audit-analysis/summary",
    "/api/v1/conversation/pitch-audit-analysis/summary",
]
# ponytail: DLQ messages don't carry the ALB session cookie, so the replayed
# request has no auth. Paste a fresh AWSALB/AWSALBCORS cookie here (from a
# working curl/browser session) before running.
AUTH_COOKIE = "AWSALB=TJ8MPufSUAWbRgIjIiosLqJxJhEoj1ZWrgyEWfVZooqaFOdsGDOdzJVXRFvfS+MuU1T6UaYQ3orEhZ3UUVJ4yRSpKnuiGpTppvpwkY0smM9fQCQJlv/Mpezt+ImA; AWSALBCORS=TJ8MPufSUAWbRgIjIiosLqJxJhEoj1ZWrgyEWfVZooqaFOdsGDOdzJVXRFvfS+MuU1T6UaYQ3orEhZ3UUVJ4yRSpKnuiGpTppvpwkY0smM9fQCQJlv/Mpezt+ImA"

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


def build_url(message: JsonDict) -> str:
    protocol = message.get("protocol", "http")
    host = "internal-prod-core-backend-api-service-1403852440.ap-south-1.elb.amazonaws.com"
    return f"{protocol}://{host.rstrip('/')}/{message['route'].lstrip('/')}"


def round_crm_status_confidence(node: Any) -> None:
    """Round 'confidence' in-place on any generated_crm_status/raw_crm_status dict, wherever nested."""
    if isinstance(node, dict):
        if "confidence" in node and "category_id" in node:
            node["confidence"] = round(node["confidence"])
        for value in node.values():
            round_crm_status_confidence(value)
    elif isinstance(node, list):
        for item in node:
            round_crm_status_confidence(item)


def push_message(message: JsonDict) -> None:
    url = build_url(message)
    headers = dict(message.get("headers", {}))
    headers.setdefault("Content-Type", "application/json")
    if AUTH_COOKIE:
        headers["Cookie"] = AUTH_COOKIE
    body = message.get("body", {})
    round_crm_status_confidence(body)

    cmd = ["curl", "--fail", "--silent", "--show-error", "--location",
           "--request", message.get("method", "POST"), url]
    for key, value in headers.items():
        cmd += ["--header", f"{key}: {value}"]
    cmd += ["--data", json.dumps(body)]

    subprocess.run(cmd, check=True, capture_output=True, text=True)


def main() -> int:
    target_folder = get_today_folder()
    route_messages = load_route_messages(target_folder)
    messages = collect_target_messages(route_messages)

    if not messages:
        print("[yellow]No pitch-audit-analysis-summary messages found.[/yellow]")
        return 0

    success = 0
    for index, message in enumerate(messages, start=1):
        url = build_url(message)
        print(f"[{index}/{len(messages)}] POST {url}")
        try:
            push_message(message)
            success += 1
            print(f"[{index}/{len(messages)}] [green]OK[/green]")
        except subprocess.CalledProcessError as exc:
            print(f"[{index}/{len(messages)}] [red]FAILED: {exc.stderr.strip() or exc}[/red]")

    print(f"[bold]Pushed {success}/{len(messages)} message(s).[/bold]")
    return 0


def _self_check() -> None:
    body = {
        "generated_crm_status": {"category_id": "abc", "confidence": 0.6789},
        "nested": {"raw_crm_status": {"category_id": "xyz", "confidence": 0.111}},
    }
    round_crm_status_confidence(body)
    assert body["generated_crm_status"]["confidence"] == 1, body
    assert body["nested"]["raw_crm_status"]["confidence"] == 0, body


if __name__ == "__main__":
    _self_check()
    raise SystemExit(main())
