"""Push exported route messages (today's messages/ folder) to their target APIs.

Usage:
    python 08_push_message_file.py [exported_file.json ...]

With no file argument, every *.json file in today's messages/ folder is pushed.
Each message is a dispatch instruction (protocol/host/route/method/headers/body).
Every request gets a 180 second timeout and is retried up to 2 extra times on
any failure (non-2xx, timeout, connection error).
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

from rich import print

PROJECT_DIR = Path(__file__).resolve().parent
MESSAGES_FOLDER_NAME = "messages"
TIMEOUT_SECONDS = 180
MAX_ATTEMPTS = 3  # 1 try + 2 retries
PAUSE_SECONDS = 5  # pause after every request, successful or not
NEW_HIT_PAUSE_SECONDS = 3  # extra pause before starting each new message

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


def load_messages(input_file: Path) -> list[JsonDict]:
    with input_file.open("r", encoding="utf-8") as file:
        return json.load(file)


def build_url(message: JsonDict) -> str:
    host = message["host"].rstrip("/")
    if "://" not in host:
        host = f"{message.get('protocol', 'http')}://{host}"
    return f"{host}/{message['route'].lstrip('/')}"


def push_message(message: JsonDict) -> None:
    headers = dict(message.get("headers", {}))
    headers.setdefault("Content-Type", "application/json")

    cmd = ["curl", "--fail", "--silent", "--show-error", "--location",
           "--max-time", str(TIMEOUT_SECONDS),
           "--request", message.get("method", "POST"), build_url(message)]
    for key, value in headers.items():
        cmd += ["--header", f"{key}: {value}"]
    cmd += ["--data", json.dumps(message.get("body", {}))]

    subprocess.run(cmd, check=True, capture_output=True, text=True)


def push_file(input_file: Path) -> tuple[int, int]:
    messages = load_messages(input_file)
    if not messages:
        print(f"[yellow]No messages in {input_file.name}.[/yellow]")
        return 0, 0

    print(f"[bold]== {input_file.name} — {len(messages)} message(s) ==[/bold]")
    success = 0
    for index, message in enumerate(messages, start=1):
        label = f"[{input_file.name} {index}/{len(messages)}]"
        time.sleep(NEW_HIT_PAUSE_SECONDS)
        print(f"{label} {message.get('method', 'POST')} {build_url(message)}")
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                push_message(message)
                time.sleep(PAUSE_SECONDS)
                success += 1
                print(f"{label} [green]OK[/green]")
                break
            except subprocess.CalledProcessError as exc:
                time.sleep(PAUSE_SECONDS)
                reason = (exc.stderr or "").strip() or f"exit {exc.returncode}"
                if attempt == MAX_ATTEMPTS:
                    print(f"{label} [red]FAILED after {MAX_ATTEMPTS} attempts: {reason}[/red]")
                else:
                    print(f"{label} [yellow]attempt {attempt} failed: {reason} — retrying[/yellow]")

    return success, len(messages)


def main(argv: list[str]) -> int:
    messages_folder = get_today_folder() / MESSAGES_FOLDER_NAME

    if argv:
        input_files = [messages_folder / name for name in argv]
        for input_file in input_files:
            if not input_file.exists():
                raise FileNotFoundError(f"Exported route file not found: {input_file}")
    else:
        input_files = sorted(messages_folder.glob("*.json"))

    total_success = total_count = 0
    for input_file in input_files:
        success, count = push_file(input_file)
        total_success += success
        total_count += count

    print(f"[bold]Pushed {total_success}/{total_count} message(s) across {len(input_files)} file(s).[/bold]")
    return 0 if total_success == total_count else 1


def _self_check() -> None:
    assert build_url({"host": "http://h/", "route": "/a/b"}) == "http://h/a/b"
    assert build_url({"protocol": "https", "host": "h", "route": "a/b"}) == "https://h/a/b"


if __name__ == "__main__":
    _self_check()
    raise SystemExit(main(sys.argv[1:]))
