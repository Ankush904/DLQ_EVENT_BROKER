"""Export selected route groups into separate JSON files."""

from __future__ import annotations

import json
from fnmatch import fnmatch
from datetime import date
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.table import Table

PROJECT_DIR = Path(__file__).resolve().parent
ROUTE_MESSAGES_FILE = "sqs_routes_messages.json"
OUTPUT_FOLDER_NAME = "messages"

ROUTE_EXPORTS = {
    "recorded_conversations_list.json": [
        "/v1/recordings/recorded-conversations",
        "v1/recordings/recorded-conversations",
    ],
    "ml_inference_results_list.json": [
        "/ml-inference-results",
        "ml-inference-results",
    ],
    "process_callback_list.json": [
        "/v1/recordings/process-callback",
        "v1/recordings/process-callback",
    ],
    "llm-invocation-log.json":[
        "api/v1/client/organisation/llm-invocation-log"
    ],
    "send_analytics.json": [
        "api/v1/client/conversation/send/analytics"
    ],
    "data_capture_sync.json": [
        "api/v1/client/conversation/analysis/data-capture-sync"
    ],
    "pitch_audit_analysis_summary.json": [
        "api/v1/conversation/pitch-audit-analysis/summary"
    ],
    "bulk_upload_recordings.json": ["api/v1/client/conversation/process/bulk-upload-recordings"],
    "conversation_summary.json": [
        "api/v1/client/conversation/summary",
        "/api/v1/client/conversation/summary",
        "/api/v1/client/conversation/summary/*",
    ],
    "crm_fields_list.json": [
        "api/v1/client/conversation/customer-conversations/*/crm-fields",
    ],
    "customer_summary_list.json": [
        "api/v1/client/customer/*/summary",
    ],
    "customer_summary_alt_list.json": [
        "api/v1/customer/*/summary",
    ],
    "customer_insights_list.json": [
        "v1/customers/*/insights",
    ],
    "agent_coaching_list.json": [
        "/api/v1/client/cortex/workspace/*/agent-coaching",
    ],
    "reprocess_conversation_inference_list.json": [
        "v1/conversations/*/reprocess-conversation-inference",
        "api/v1/conversation/*/reprocess-conversation-inference",
    ],
    "partner_ingest_call.json": ["api/v1/client/partner/ingest/call"],
    "bitrix_process_call.json": ["api/v1/client/bitrix/process/call"],
    "bitrix_process_deal_call.json": ["api/v1/client/bitrix/process/deal-call"],
    "outbound_emailer_outreach.json": [
        "api/v1/client/outbound-emailer/initiate/flow/outreach"
    ],
    "agent_orchestration_tick.json": ["api/v1/agent-orchestration/tick"],
    "agent_orchestration_inbound_debounce.json": [
        "api/v1/agent-orchestration/inbound-debounce"
    ],
    "cortex_control_map_reconcile.json": [
        "api/v1/client/cortex/control-map/reconcile"
    ],
    "schedule_sync_to_crm.json": ["/api/v1/client/schedule/sync-to-crm"],
    "whatsapp_process_conversation_list.json": [
        "api/v1/client/messaging/whatsapp/process-conversation/*"
    ],
    "workspace_insert_audit_parameter_suggestion_list.json": [
        "/workspace/*/insert-audit-parameter-suggestion"
    ],
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
    seen_routes: set[str] = set()

    for route in routes:
        matched_routes: list[str]
        if "*" in route or "?" in route:
            matched_routes = [
                route_key for route_key in route_messages if fnmatch(route_key, route)
            ]
        else:
            matched_routes = [route]

        for matched_route in matched_routes:
            if matched_route in seen_routes:
                continue
            seen_routes.add(matched_route)
            route_items = route_messages.get(matched_route, [])
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

    console = Console()
    console.print(f"Exported route files to {output_folder}")
    table = Table()
    table.add_column("File")
    table.add_column("Count", justify="right")
    for file_name, count in sorted(exported_counts.items(), key=lambda item: item[1], reverse=True):
        table.add_row(file_name, str(count))
    console.print(table)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
