"""Push exported route messages (today's messages/ folder) to their target APIs.

Usage:
    python 08_push_message_file.py [exported_file.json ...]

With no file argument, every *.json file in today's messages/ folder is pushed.
Each message is a dispatch instruction (protocol/host/route/method/headers/body).
Every request gets a 50 second connect timeout and a 180 second read timeout,
and is retried up to 2 extra times on any failure (non-2xx, timeout, connection
error).
"""

from __future__ import annotations

import json
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

import requests
from rich import print

PROJECT_DIR = Path(__file__).resolve().parent
MESSAGES_FOLDER_NAME = "messages"
CONNECT_TIMEOUT_SECONDS = 50  # max wait to establish the TCP connection
TIMEOUT_SECONDS = 180  # max wait between response bytes once connected
MAX_ATTEMPTS = 3  # 1 try + 2 retries
PAUSE_SECONDS = 5  # pause after every request, successful or not
NEW_HIT_PAUSE_SECONDS = 3  # extra pause before starting each new message

JsonDict = dict[str, Any]

session = requests.Session()  # keeps the ALB connection alive across messages


def get_today_folder() -> Path:
    today = date.today()
    suffix = "th" if 11 <= today.day % 100 <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(today.day % 10, "th")
    folder = PROJECT_DIR / f"{today.day}{suffix} {today:%B %Y}"
    if not folder.is_dir():
        raise FileNotFoundError(f"Today's folder not found: {folder}")
    return folder


def build_url(message: JsonDict) -> str:
    host = message["host"].rstrip("/")
    if "://" not in host:
        host = f"{message.get('protocol', 'http')}://{host}"
    return f"{host}/{message['route'].lstrip('/')}"


def push(message: JsonDict, label: str) -> bool:
    """Send one message, retrying up to MAX_ATTEMPTS times. True if it succeeded."""
    method = message.get("method", "POST")
    headers = {"Content-Type": "application/json", **message.get("headers", {})}

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            session.request(
                method,
                build_url(message),
                headers=headers,
                json=message.get("body", {}),
                timeout=(CONNECT_TIMEOUT_SECONDS, TIMEOUT_SECONDS),
            ).raise_for_status()
            time.sleep(PAUSE_SECONDS)
            print(f"{label} [green]OK[/green]")
            return True
        except requests.RequestException as exc:
            time.sleep(PAUSE_SECONDS)
            response = getattr(exc, "response", None)
            reason = f"{response.status_code} {response.text.strip()[:300]}" if response is not None else exc
            if attempt == MAX_ATTEMPTS:
                print(f"{label} [red]FAILED after {MAX_ATTEMPTS} attempts: {reason}[/red]")
            else:
                print(f"{label} [yellow]attempt {attempt} failed: {reason} — retrying[/yellow]")
    return False


def main(argv: list[str]) -> int:
    folder = get_today_folder() / MESSAGES_FOLDER_NAME
    files = [folder / name for name in argv] if argv else sorted(folder.glob("*.json"))

    total = pushed = 0
    for input_file in files:
        messages = json.loads(input_file.read_text(encoding="utf-8"))
        if not messages:
            continue
        print(f"[bold]== {input_file.name} — {len(messages)} message(s) ==[/bold]")
        total += len(messages)
        for index, message in enumerate(messages, start=1):
            label = f"{input_file.name} {index}/{len(messages)} —"
            time.sleep(NEW_HIT_PAUSE_SECONDS)
            print(f"{label} {message.get('method', 'POST')} {build_url(message)}")
            pushed += push(message, label)

    print(f"[bold]Pushed {pushed}/{total} message(s) from {len(files)} file(s).[/bold]")
    return 0 if pushed == total else 1


def _self_check() -> None:
    assert build_url({"host": "http://h/", "route": "/a/b"}) == "http://h/a/b"
    assert build_url({"protocol": "https", "host": "h", "route": "a/b"}) == "https://h/a/b"


if __name__ == "__main__":
    _self_check()
    raise SystemExit(main(sys.argv[1:]))
