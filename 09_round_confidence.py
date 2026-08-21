"""Round every float "confidence" value in a JSON file to a whole number (0 or 1).

Recurses through all nested dicts/lists, so paths like
ml_inference.generated_crm_status.confidence and
raw_generated_insights.intent_analysis.crm_status_analysis.raw_crm_status.confidence
are all covered without listing them.

Integer confidences (0-100 scale) are left alone.

Usage:
    python 09_round_confidence.py "19th August 2026/messages/data_capture_sync.json" [more.json ...]
"""

import json
import sys
from decimal import Decimal, ROUND_HALF_UP

def _round(v):
    """Half-up to nearest int: 0.8 -> 1, 0.5 -> 1, 0.2 -> 0.
    Plain round() would give 0 for 0.5 (banker's rounding)."""
    return int(Decimal(repr(v)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def round_confidence(obj):
    if isinstance(obj, dict):
        return {
            k: _round(v) if k == "confidence" and isinstance(v, float) else round_confidence(v)
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [round_confidence(x) for x in obj]
    return obj


def count_unrounded(obj):
    if isinstance(obj, dict):
        n = sum(count_unrounded(v) for k, v in obj.items())
        c = obj.get("confidence")
        return n + (1 if isinstance(c, float) and _round(c) != c else 0)
    if isinstance(obj, list):
        return sum(count_unrounded(x) for x in obj)
    return 0


def process(path):
    with open(path) as f:
        data = json.load(f)
    before = count_unrounded(data)
    data = round_confidence(data)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    assert count_unrounded(data) == 0, f"{path}: rounding missed values"
    print(f"{path}: rounded {before} value(s)")


def demo():
    sample = {
        "ml_inference": {"generated_crm_status": {"confidence": 0.85}},
        "raw_generated_insights": {
            "intent_analysis": {
                "crm_status_analysis": {
                    "generated_crm_status": {"confidence": 0.6},
                    "raw_crm_status": {"confidence": 0.949},
                }
            }
        },
        "list": [{"confidence": 0.05}],
        "half": {"confidence": 0.5},
        "int_scale": {"confidence": 85},
        "text": {"confidence": "0.85"},
    }
    out = round_confidence(sample)
    assert out["ml_inference"]["generated_crm_status"]["confidence"] == 1
    ca = out["raw_generated_insights"]["intent_analysis"]["crm_status_analysis"]
    assert ca["generated_crm_status"]["confidence"] == 1
    assert ca["raw_crm_status"]["confidence"] == 1
    assert out["list"][0]["confidence"] == 0
    assert out["int_scale"]["confidence"] == 85, "ints untouched"
    assert out["text"]["confidence"] == "0.85", "strings untouched"
    assert out["half"]["confidence"] == 1, "0.5 rounds up"
    assert count_unrounded(out) == 0
    print("self-check passed")


if __name__ == "__main__":
    paths = sys.argv[1:]
    if not paths:
        demo()
    else:
        for p in paths:
            process(p)
