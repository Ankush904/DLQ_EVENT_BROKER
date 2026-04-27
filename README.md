# DLQ Event Broker

A 3-stage Python pipeline for fetching, grouping, and exporting failed messages from an AWS SQS Dead Letter Queue (DLQ).

## Overview

```text
AWS SQS DLQ → Fetch raw messages → Group by route → Export selected routes → JSON files
```

Each stage reads the previous stage's output from a date-named folder (e.g. `27th April 2026/`).

## Prerequisites

- Python 3.10+
- AWS `prod` profile configured in `~/.aws/credentials` with access to the `dlq_event_broker` queue

```bash
pip install boto3
```

## Usage

Run the three scripts in order:

```bash
python 00_dlq_event_broker_main.py
python 01_fetch_dlq_route.py
python 02_export_route_messages.py
```

---

### Stage 0 — Fetch messages

**Script:** `00_dlq_event_broker_main.py`

Connects to SQS using the `prod` AWS profile, polls the DLQ until empty, decodes nested JSON bodies, and saves raw messages.

**Output:**

```text
27th April 2026/
└── sqs_messages_event_broker_2026-04-27.json
```

---

### Stage 1 — Group by route

**Script:** `01_fetch_dlq_route.py`

Reads Stage 0 output, extracts message bodies, and groups them by the `route` field.

**Output:**

```text
27th April 2026/
├── sqs_messages_body.json       # flat list of message bodies
└── sqs_routes_messages.json     # messages keyed by route string
```

---

### Stage 2 — Export selected routes

**Script:** `02_export_route_messages.py`

Reads `sqs_routes_messages.json` and writes configured routes to separate files under a `messages/` subfolder.

**Output:**

```text
27th April 2026/
└── messages/
    ├── recorded_conversations_list.json
    ├── ml_inference_results_list.json
    └── process_callback_list.json
```

To export additional routes, add an entry to `ROUTE_EXPORTS` in `02_export_route_messages.py`:

```python
ROUTE_EXPORTS = {
    "my_export.json": ["/my/route", "my/route"],
}
```

Both slash variants are listed because the `route` field in messages is inconsistent.

---

## AWS Configuration

| Setting   | Value                                                                      |
| --------- | -------------------------------------------------------------------------- |
| Profile   | `prod`                                                                     |
| Region    | `ap-south-1`                                                               |
| Queue     | `dlq_event_broker`                                                         |
| Queue URL | `https://sqs.ap-south-1.amazonaws.com/927421207401/dlq_event_broker`       |

## Project Structure

```text
DLQ_EVENT_BROKER/
├── 00_dlq_event_broker_main.py      # Stage 0: fetch from SQS
├── 01_fetch_dlq_route.py            # Stage 1: group by route
├── 02_export_route_messages.py      # Stage 2: export selected routes
├── 01_sqs_routes_messages.ipynb     # interactive route exploration
└── 02_export_route_messages.ipynb   # interactive export testing
```

Run folders are created automatically at runtime and excluded from version control via `.gitignore`.
