#!/usr/bin/env python
"""Import generated summaries into board items cache.

This script reads summaries from a JSON file and merges them into
the cached board items.

Usage:
    python import_summaries.py [summaries_file]
    python import_summaries.py  # defaults to summaries.json
"""

import json
import sys
from pathlib import Path

CACHE_DIR = Path("cache/board_summary")


def load_board_items() -> list[dict]:
    """Load enriched board items from cache."""
    path = CACHE_DIR / "board_items.json"
    if not path.exists():
        raise FileNotFoundError(f"Board items cache not found: {path}")
    with open(path) as f:
        return json.load(f)


def save_board_items(items: list[dict]):
    """Save board items to cache."""
    path = CACHE_DIR / "board_items.json"
    with open(path, "w") as f:
        json.dump(items, f, indent=2)


def load_summaries(summaries_path: str) -> dict[str, dict | str]:
    """Load summaries from JSON file.

    Summaries can be:
    - Simple strings (old format)
    - Objects with summary, ai_status, action_items (new format)
    """
    with open(summaries_path) as f:
        return json.load(f)


def import_summaries(summaries_path: str = "summaries.json"):
    """Import summaries into board items cache.

    Handles both old format (string) and new format (object with ai_status).
    """
    items = load_board_items()
    summaries = load_summaries(summaries_path)

    matched = 0
    unmatched = []

    for item in items:
        repo = item.get("repo_short", "")
        number = item.get("number", "")
        item_id = f"{repo}#{number}"

        if item_id in summaries:
            summary_data = summaries[item_id]
            if isinstance(summary_data, str):
                # Old format: just a string summary
                item["summary"] = summary_data
            else:
                # New format: object with summary, ai_status, action_items, etc.
                item["summary"] = summary_data.get("summary", "")
                item["ai_status"] = summary_data.get("ai_status", "")
                item["action_items"] = summary_data.get("action_items", [])
                item["action_required_by"] = summary_data.get("action_required_by", [])
                item["action_reason"] = summary_data.get("action_reason", "")
            matched += 1
        else:
            unmatched.append(item_id)

    save_board_items(items)

    print(f"Imported {matched} summaries")
    if unmatched:
        print(f"Items without summaries ({len(unmatched)}):")
        for item_id in unmatched[:10]:
            print(f"  - {item_id}")
        if len(unmatched) > 10:
            print(f"  ... and {len(unmatched) - 10} more")

    # Check for summaries that didn't match any item
    item_ids = {f"{i.get('repo_short', '')}#{i.get('number', '')}" for i in items}
    extra = [s for s in summaries if s not in item_ids]
    if extra:
        print(f"\nSummaries that didn't match any item ({len(extra)}):")
        for s in extra[:5]:
            print(f"  - {s}")

    return matched


if __name__ == "__main__":
    summaries_file = sys.argv[1] if len(sys.argv) > 1 else "summaries.json"
    import_summaries(summaries_file)
