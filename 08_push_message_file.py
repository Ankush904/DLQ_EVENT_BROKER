"""Push exported route messages (today's messages/ folder) to their target APIs.

Usage:
    python 08_push_message_file.py [exported_file.json ...] [--queue NAME]

With no file argument, every *.json file in today's messages/ folder is pushed.
Each message is a dispatch instruction (protocol/host/route/method/headers/body).
Messages are pushed 10 at a time; after each batch of 10 the messages that
succeeded are deleted from the DLQ (using their _ReceiptHandle) and the script
pauses 2 seconds before the next batch to keep DB load down.
Every request gets a 50 second connect timeout and a 180 second read timeout,
and is retried up to 2 extra times on any failure (non-2xx, timeout, connection
error), waiting 2 then 4 seconds before each retry.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

import boto3  # pyright: ignore[reportMissingImports]
import requests
from botocore.exceptions import BotoCoreError, ClientError  # pyright: ignore[reportMissingImports]
from concurrent.futures import ThreadPoolExecutor
from rich import print

from queues import QUEUE_URLS, DEFAULT_QUEUE

PROJECT_DIR = Path(__file__).resolve().parent
MESSAGES_FOLDER_NAME = "messages"
CONNECT_TIMEOUT_SECONDS = 50  # max wait to establish the TCP connection
TIMEOUT_SECONDS = 180  # max wait between response bytes once connected
MAX_ATTEMPTS = 3  # 1 try + 2 retries
BATCH_SIZE = 10  # messages pushed concurrently; also the SQS delete-batch limit
RETRY_BACKOFF_SECONDS = 2  # gap before a retry, scaled by attempt: 2s then 4s
BATCH_PAUSE_SECONDS = 2  # pause between batches, to keep DB load down
REGION_NAME = "ap-south-1"
PROFILE_NAME = "prod"

JsonDict = dict[str, Any]

session = requests.Session()  # keeps the ALB connection alive across messages
session.mount("https://", requests.adapters.HTTPAdapter(pool_maxsize=BATCH_SIZE))
session.mount("http://", requests.adapters.HTTPAdapter(pool_maxsize=BATCH_SIZE))


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
            print(f"{label} [green]OK[/green]")
            return True
        except requests.RequestException as exc:
            response = getattr(exc, "response", None)
            reason = f"{response.status_code} {response.text.strip()[:300]}" if response is not None else exc
            if attempt == MAX_ATTEMPTS:
                print(f"{label} [red]FAILED after {MAX_ATTEMPTS} attempts: {reason}[/red]")
            else:
                print(f"{label} [yellow]attempt {attempt} failed: {reason} — retrying[/yellow]")
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    return False


def delete_from_dlq(sqs: Any, queue_url: str, messages: list[JsonDict]) -> int:
    """Delete pushed messages from the DLQ by receipt handle. Returns deleted count."""
    entries = [
        {"Id": str(index), "ReceiptHandle": message["_ReceiptHandle"]}
        for index, message in enumerate(messages)
        if message.get("_ReceiptHandle")
    ]
    if not entries:
        return 0
    try:
        response = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
    except (BotoCoreError, ClientError) as exc:
        print(f"[red]Delete batch failed: {exc}[/red]")
        return 0
    for failure in response.get("Failed", []):
        print(f"[red]Failed to delete message: {failure}[/red]")
    return len(response.get("Successful", []))


def main(argv: list[str]) -> int:
    queue_name = DEFAULT_QUEUE
    if "--queue" in argv:  # accepted anywhere in the arguments
        index = argv.index("--queue")
        if index + 1 >= len(argv):
            raise SystemExit("--queue needs a queue name")
        queue_name = argv[index + 1]
        argv = argv[:index] + argv[index + 2 :]
    if queue_name not in QUEUE_URLS:
        raise SystemExit(f"Unknown queue {queue_name!r}. Choose one of: {', '.join(QUEUE_URLS)}")
    queue_url = QUEUE_URLS[queue_name]
    sqs = boto3.Session(profile_name=PROFILE_NAME, region_name=REGION_NAME).client("sqs")

    folder = get_today_folder() / MESSAGES_FOLDER_NAME
    files = [folder / name for name in argv] if argv else sorted(folder.glob("*.json"))

    total = pushed = deleted = 0
    with ThreadPoolExecutor(max_workers=BATCH_SIZE) as pool:
        for input_file in files:
            messages = json.loads(input_file.read_text(encoding="utf-8"))
            if not messages:
                continue
            print(f"[bold]== {input_file.name} — {len(messages)} message(s) ==[/bold]")
            total += len(messages)
            for start in range(0, len(messages), BATCH_SIZE):
                batch = messages[start : start + BATCH_SIZE]
                labels = [
                    f"{input_file.name} {start + offset + 1}/{len(messages)} —"
                    for offset in range(len(batch))
                ]
                results = list(pool.map(push, batch, labels))
                pushed += sum(results)
                succeeded = [m for m, ok in zip(batch, results) if ok]
                deleted += delete_from_dlq(sqs, queue_url, succeeded)
                time.sleep(BATCH_PAUSE_SECONDS)

    print(f"[bold]Pushed {pushed}/{total} message(s) from {len(files)} file(s); deleted {deleted} from {queue_name}.[/bold]")
    return 0 if pushed == total else 1


def _self_check() -> None:
    assert build_url({"host": "http://h/", "route": "/a/b"}) == "http://h/a/b"
    assert build_url({"protocol": "https", "host": "h", "route": "a/b"}) == "https://h/a/b"


if __name__ == "__main__":
    _self_check()
    raise SystemExit(main(sys.argv[1:]))
