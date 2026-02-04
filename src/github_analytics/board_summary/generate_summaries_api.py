#!/usr/bin/env python
"""Generate AI summaries for board items using the Anthropic API.

This script generates summaries for items that don't have them yet,
using the Anthropic Claude API.

Usage:
    python generate_summaries_api.py [--max N] [--model MODEL]

Environment variables:
    ANTHROPIC_API_KEY: Required. Your Anthropic API key.
    Can be set in .env file or as environment variable.
"""

import argparse
import json
import os
import sys

import anthropic
from dotenv import load_dotenv
from generate_summaries import (
    list_items_needing_summaries,
    load_existing_summaries,
    save_summaries,
)

# Load environment variables from .env file if it exists
load_dotenv()

# System prompt for generating summaries
SYSTEM_PROMPT = """\
You are an assistant helping to analyze GitHub pull requests and issues.

For each item, generate a JSON object with these fields:
- summary: A 1-2 sentence description of the item and its current status
- ai_status: One of the status values listed below
- action_items: A list of 0-3 specific next steps needed
- action_required_by: A list of GitHub usernames who need to take action
- action_reason: A brief explanation of why action is needed (or empty if none)

Valid ai_status values:
"Ready to merge", "Needs minor work", "In progress", "Blocked",
"Needs review", "Stale", "Needs discussion", "Waiting for author"

Status definitions:
- Ready to merge: Has approvals, CI passing, no blockers
- Needs minor work: Small changes needed before merge
- In progress: Active development ongoing
- Blocked: Cannot proceed due to external dependency or decision needed
- Needs review: Waiting for reviewer feedback
- Stale: No activity for extended period
- Needs discussion: Design or approach needs team input
- Waiting for author: Reviewer has given feedback, waiting for author

Respond with ONLY valid JSON, no markdown formatting or explanation."""


def generate_summary_for_item(
    client: anthropic.Anthropic, item: dict, model: str
) -> dict:
    """Generate a summary for a single item using the Anthropic API."""
    # Build the prompt with item details
    item_info = f"""
Item: {item["id"]}
Title: {item["title"]}
Type: {item["type"]}
URL: {item["url"]}
Author: {item["author"]}
State: {item["state"]}
Board Status: {item["board_status"]}
Computed Status: {item["computed_status"]}
Champion: {item.get("champion", "None")}
Reviewer 1: {item.get("reviewer1", "None")}
Reviewer 2: {item.get("reviewer2", "None")}
Created: {item["created_at"]}
Updated: {item["updated_at"]}

Recent Activity:
"""
    for activity in item.get("recent_activity", [])[:5]:
        date = activity["date"]
        atype = activity["type"]
        author = activity["author"]
        summary = activity.get("summary", "")[:200]
        item_info += f"- {date} | {atype} by {author}: {summary}\n"

    message = client.messages.create(
        model=model,
        max_tokens=500,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"Generate a summary for this GitHub item:\n{item_info}",
            }
        ],
    )

    # Parse the response
    response_text = message.content[0].text.strip()

    # Try to parse as JSON
    try:
        # Handle potential markdown code blocks
        if response_text.startswith("```"):
            lines = response_text.split("\n")
            response_text = "\n".join(lines[1:-1])

        summary = json.loads(response_text)

        # Validate required fields
        required_fields = [
            "summary",
            "ai_status",
            "action_items",
            "action_required_by",
            "action_reason",
        ]
        for field in required_fields:
            if field not in summary:
                summary[field] = (
                    "" if field in ["summary", "ai_status", "action_reason"] else []
                )

        return summary
    except json.JSONDecodeError as e:
        print(f"Warning: Failed to parse JSON for {item['id']}: {e}", file=sys.stderr)
        return {
            "summary": response_text[:200],
            "ai_status": "In progress",
            "action_items": [],
            "action_required_by": [],
            "action_reason": "Failed to parse AI response",
        }


def generate_summaries(max_items: int = 50, model: str = "claude-sonnet-4-20250514"):
    """Generate summaries for items that need them."""
    # Check for API key
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Error: ANTHROPIC_API_KEY environment variable not set", file=sys.stderr)
        sys.exit(1)

    # Initialize client
    client = anthropic.Anthropic(api_key=api_key)

    # Get items needing summaries
    items = list_items_needing_summaries(max_items)

    if not items:
        print("All items already have summaries!")
        return

    print(f"Generating summaries for {len(items)} items using {model}...")

    # Load existing summaries
    existing = load_existing_summaries()
    new_summaries = {}

    for i, item in enumerate(items):
        item_id = item["id"]
        print(f"  [{i + 1}/{len(items)}] {item_id}: {item['title'][:50]}...")

        try:
            summary = generate_summary_for_item(client, item, model)
            new_summaries[item_id] = summary
            print(f"    -> {summary['ai_status']}")
        except Exception as e:
            print(f"    -> Error: {e}", file=sys.stderr)

    # Save all summaries
    existing.update(new_summaries)
    save_summaries(existing)

    print(f"\nGenerated {len(new_summaries)} new summaries")
    print(f"Total summaries: {len(existing)}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate AI summaries using Anthropic API"
    )
    parser.add_argument(
        "--max",
        type=int,
        default=100,
        help="Maximum number of items to process (default: 100)",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="claude-sonnet-4-20250514",
        help="Anthropic model to use (default: claude-sonnet-4-20250514)",
    )

    args = parser.parse_args()
    generate_summaries(max_items=args.max, model=args.model)


if __name__ == "__main__":
    main()
