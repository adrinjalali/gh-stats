#!/usr/bin/env python
"""Export board items for specific users for summary generation.

Usage:
    python export_user_items.py ogrisel lesteve
"""

import json
import sys
from pathlib import Path

CACHE_DIR = Path("cache/board_summary")


def load_board_items() -> list[dict]:
    path = CACHE_DIR / "board_items.json"
    with open(path) as f:
        return json.load(f)


def get_user_items(items: list[dict], users: list[str]) -> list[dict]:
    """Get items where any of the users is assigned."""
    users_lower = {u.lower() for u in users}
    result = []
    seen = set()

    for item in items:
        champion = item.get("champion", "").lower()
        reviewer1 = item.get("reviewer1", "").lower()
        reviewer2 = item.get("reviewer2", "").lower()

        if not any(u in users_lower for u in [champion, reviewer1, reviewer2] if u):
            continue

        key = (item.get("repo", ""), item.get("number", ""))
        if key in seen:
            continue
        seen.add(key)
        result.append(item)

    return result


def format_item(item: dict) -> str:
    """Format item for summary generation."""
    repo = item.get("repo_short", "")
    number = item.get("number", "")
    item_id = f"{repo}#{number}"

    champion = item.get("champion", "None")
    r1 = item.get("reviewer1", "None")
    r2 = item.get("reviewer2", "None")
    lines = [
        f"### {item_id}",
        f"**{item.get('title', '')}**",
        f"- Type: {item.get('type', '')} | Status: {item.get('computed_status', '')}",
        f"- Author: {item.get('author', '')} | Updated: {item.get('updated_at', '')}",
        f"- Champion: {champion} | R1: {r1} | R2: {r2}",
    ]

    activity = item.get("recent_activity", [])[:3]
    if activity:
        lines.append("- Recent:")
        for a in activity:
            date = a.get("date", "")
            author = a.get("author", "")
            atype = a.get("type", "")
            summary = a.get("summary", "")[:100]
            lines.append(f"  - {date}: {author} ({atype}): {summary}")

    return "\n".join(lines)


def main(users: list[str]):
    items = load_board_items()
    user_items = get_user_items(items, users)

    print(f"# Items for: {', '.join(users)}")
    print(f"Total: {len(user_items)} items\n")

    for item in user_items:
        print(format_item(item))
        print()

    # Also output JSON template for summaries
    print("\n---")
    print("## Summary Template")
    print("")
    print("For each item, provide:")
    print("- **summary**: 1-2 sentence description of current state")
    print("- **ai_status**: One of: 'Ready to merge', 'Needs minor work',")
    print("  'In progress', 'Blocked', 'Needs review', 'Stale', 'Needs discussion'")
    print("- **action_items**: List of specific next steps (can be empty [])")
    print("- **action_required_by**: List of GitHub handles who need to act")
    print("- **action_reason**: Brief explanation of why those people need to act")
    print("")
    print("```json")
    template = {}
    for item in user_items:
        item_id = f"{item.get('repo_short', '')}#{item.get('number', '')}"
        template[item_id] = {
            "summary": "",
            "ai_status": "",
            "action_items": [],
            "action_required_by": [],
            "action_reason": "",
        }
    print(json.dumps(template, indent=2))
    print("```")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python export_user_items.py user1 [user2 ...]")
        sys.exit(1)
    main(sys.argv[1:])
