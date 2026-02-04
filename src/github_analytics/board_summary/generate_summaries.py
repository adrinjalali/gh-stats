#!/usr/bin/env python
"""Generate AI summaries for board items.

This script is designed to be called by Claude Code to generate summaries.
It outputs items that need summaries, and accepts summaries as input.

Usage:
    # List items needing summaries (outputs JSON to stdout)
    python generate_summaries.py --list

    # Import summaries from stdin (JSON format)
    python generate_summaries.py --import

    # Show stats
    python generate_summaries.py --stats
"""

import argparse
import json
import sys
from pathlib import Path

CACHE_DIR = Path("cache/board_summary")
SUMMARIES_FILE = Path(__file__).parent / "summaries.json"


def load_board_items() -> list[dict]:
    """Load enriched board items from cache."""
    path = CACHE_DIR / "board_items.json"
    if not path.exists():
        raise FileNotFoundError(f"Board items cache not found: {path}")
    with open(path) as f:
        return json.load(f)


def load_existing_summaries() -> dict:
    """Load existing summaries from file."""
    if SUMMARIES_FILE.exists():
        with open(SUMMARIES_FILE) as f:
            return json.load(f)
    return {}


def save_summaries(summaries: dict):
    """Save summaries to file."""
    with open(SUMMARIES_FILE, "w") as f:
        json.dump(summaries, f, indent=2)


def get_item_id(item: dict) -> str:
    """Get unique ID for an item."""
    return f"{item.get('repo_short', '')}#{item.get('number', '')}"


def list_items_needing_summaries(max_items: int = 50) -> list[dict]:
    """List items that need summaries."""
    items = load_board_items()
    existing = load_existing_summaries()

    needs_summary = []
    for item in items:
        item_id = get_item_id(item)
        if item_id not in existing:
            needs_summary.append(
                {
                    "id": item_id,
                    "title": item.get("title", ""),
                    "type": item.get("type", ""),
                    "url": item.get("url", ""),
                    "author": item.get("author", ""),
                    "state": item.get("state", ""),
                    "board_status": item.get("board_status", ""),
                    "computed_status": item.get("computed_status", ""),
                    "champion": item.get("champion", ""),
                    "reviewer1": item.get("reviewer1", ""),
                    "reviewer2": item.get("reviewer2", ""),
                    "updated_at": item.get("updated_at", ""),
                    "created_at": item.get("created_at", ""),
                    "recent_activity": item.get("recent_activity", [])[:5],
                }
            )

    return needs_summary[:max_items]


def import_summaries_from_stdin():
    """Import summaries from stdin JSON."""
    try:
        new_summaries = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}", file=sys.stderr)
        sys.exit(1)

    existing = load_existing_summaries()
    existing.update(new_summaries)
    save_summaries(existing)

    print(f"Imported {len(new_summaries)} summaries")
    print(f"Total summaries: {len(existing)}")


def show_stats():
    """Show summary statistics."""
    items = load_board_items()
    existing = load_existing_summaries()

    total = len(items)
    with_summary = sum(1 for i in items if get_item_id(i) in existing)
    without_summary = total - with_summary

    print(f"Total items: {total}")
    print(f"With summaries: {with_summary}")
    print(f"Without summaries: {without_summary}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate AI summaries for board items"
    )
    parser.add_argument(
        "--list", action="store_true", help="List items needing summaries (JSON)"
    )
    parser.add_argument(
        "--import",
        dest="import_",
        action="store_true",
        help="Import summaries from stdin",
    )
    parser.add_argument("--stats", action="store_true", help="Show summary statistics")
    parser.add_argument(
        "--max", type=int, default=50, help="Max items to list (default: 50)"
    )

    args = parser.parse_args()

    if args.list:
        items = list_items_needing_summaries(args.max)
        print(json.dumps(items, indent=2))
    elif args.import_:
        import_summaries_from_stdin()
    elif args.stats:
        show_stats()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
