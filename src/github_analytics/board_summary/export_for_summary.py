#!/usr/bin/env python
"""Export board items for summary generation.

This script exports item details (title, comments, reviews) to a text file
that can be read by an LLM to generate summaries.

Usage:
    python export_for_summary.py [output_file]
    python export_for_summary.py  # defaults to items_for_summary.txt
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


def format_item_for_summary(item: dict) -> str:
    """Format a single item for summary generation."""
    lines = []

    # Header
    repo = item.get("repo_short", "")
    number = item.get("number", "")
    item_id = f"{repo}#{number}"

    lines.append(f"{'=' * 60}")
    lines.append(f"ITEM: {item_id}")
    lines.append(f"{'=' * 60}")
    lines.append(f"Title: {item.get('title', '')}")
    lines.append(f"Type: {item.get('type', '')}")
    lines.append(f"URL: {item.get('url', '')}")
    lines.append(f"Author: {item.get('author', '')}")
    lines.append(f"Status: {item.get('computed_status', '')}")
    lines.append(f"Updated: {item.get('updated_at', '')}")
    lines.append(f"Created: {item.get('created_at', '')}")
    lines.append("")

    # Assigned people
    champion = item.get("champion", "")
    reviewer1 = item.get("reviewer1", "")
    reviewer2 = item.get("reviewer2", "")
    lines.append(f"Champion: {champion or 'None'}")
    lines.append(f"Reviewer 1: {reviewer1 or 'None'}")
    lines.append(f"Reviewer 2: {reviewer2 or 'None'}")
    lines.append("")

    # Recent activity
    activity = item.get("recent_activity", [])
    if activity:
        lines.append("Recent Activity:")
        for act in activity[:5]:
            author = act.get("author", "?")
            act_type = act.get("type", "?")
            date = act.get("date", "?")
            summary = act.get("summary", "")[:200]
            lines.append(f"  - [{date}] {author} ({act_type}): {summary}")
    else:
        lines.append("Recent Activity: None")

    lines.append("")
    lines.append(
        "Please write a 1-2 sentence summary of the current state of this item."
    )
    lines.append("")

    return "\n".join(lines)


def export_items(output_path: str = "items_for_summary.txt"):
    """Export all items to a text file for summary generation."""
    items = load_board_items()

    lines = [
        "BOARD ITEMS FOR SUMMARY GENERATION",
        "=" * 60,
        "",
        "Instructions: For each item below, write a brief 1-2 sentence summary",
        "describing the current state and what action (if any) is needed.",
        "",
        "After generating summaries, save them in this JSON format:",
        "{",
        '  "repo#number": "Your summary here",',
        '  "scikit-learn#12345": "Ready to merge after final approval...",',
        "  ...",
        "}",
        "",
        f"Total items: {len(items)}",
        "",
    ]

    for item in items:
        lines.append(format_item_for_summary(item))

    with open(output_path, "w") as f:
        f.write("\n".join(lines))

    print(f"Exported {len(items)} items to {output_path}")
    print("After generating summaries, save them to summaries.json")
    return output_path


if __name__ == "__main__":
    output = sys.argv[1] if len(sys.argv) > 1 else "items_for_summary.txt"
    export_items(output)
