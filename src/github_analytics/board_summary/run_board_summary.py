#!/usr/bin/env python
"""Run the complete board summary workflow.

Usage:
    python run_board_summary.py [users...]

Examples:
    python run_board_summary.py                    # All users on the board
    python run_board_summary.py ogrisel lesteve   # Specific users only
"""

import argparse
from pathlib import Path

from fetch_board import (
    enrich_board_items,
    fetch_board_items,
    get_all_users,
    load_from_cache,
    save_to_cache,
)
from generate_report import save_report
from import_summaries import import_summaries
from merge_activity import INCLUDED_REPOS, merge_activity_with_board

# Path to summaries file
SUMMARIES_FILE = Path(__file__).parent / "summaries.json"

CACHE_DIR = Path("cache/board_summary")


def run_board_summary(
    users: list[str] | None = None,
    lookback_days: int = 14,
    output: str = "board_summary.html",
    skip_fetch: bool = False,
):
    """Run the complete board summary workflow.

    Args:
        users: List of GitHub usernames to analyze. If None, uses all users from board.
        lookback_days: Number of days to look back for activity.
        output: Output file path for the HTML report.
        skip_fetch: If True, use cached data instead of fetching fresh data.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Fetch and enrich board items
    if skip_fetch and (CACHE_DIR / "board_items.json").exists():
        print("Using cached board items...")
        enriched_items = load_from_cache()
        # Get board-only items (before merge) for user detection
        board_only_items = [
            i for i in enriched_items if i.get("board_status") != "Not Included"
        ]
    else:
        print("=" * 50)
        print("Step 1: Fetching board items")
        print("=" * 50)
        items = fetch_board_items()
        enriched_items = enrich_board_items(items)
        board_only_items = enriched_items
        save_to_cache(enriched_items)

    # Determine users to analyze
    if users is None:
        users = get_all_users(board_only_items)
        print(f"\nFound {len(users)} users on the board: {', '.join(users)}")

    # Step 2: Fetch user activity and merge with board items
    if skip_fetch and (CACHE_DIR / "user_activity.json").exists():
        print("\nUsing cached user activity and merged items...")
    else:
        print("\n" + "=" * 50)
        print("Step 2: Fetching user activity and merging")
        print("=" * 50)
        print(f"Users: {', '.join(users)}")
        print(f"Lookback: {lookback_days} days")
        print(f"Included repos: {', '.join(INCLUDED_REPOS)}")
        print()

        # This fetches activity, merges with board items, and saves both
        merge_activity_with_board(users, lookback_days=lookback_days)

    # Step 3: Import AI summaries (if summaries.json exists)
    if SUMMARIES_FILE.exists():
        print("\n" + "=" * 50)
        print("Step 3: Importing AI summaries")
        print("=" * 50)
        import_summaries(str(SUMMARIES_FILE))
    else:
        print(f"\nNote: No summaries file found at {SUMMARIES_FILE}")
        print("Run export_user_items.py to generate summaries template")

    # Step 4: Generate report
    print("\n" + "=" * 50)
    print("Step 4: Generating report")
    print("=" * 50)
    save_report(output, users)

    print("\n" + "=" * 50)
    print("Done!")
    print("=" * 50)
    print(f"Report saved to: {output}")

    return output


def main():
    parser = argparse.ArgumentParser(
        description="Generate board summary report for probabl-ai project board"
    )
    parser.add_argument(
        "users",
        nargs="*",
        help="GitHub usernames to analyze (default: all users on board)",
    )
    parser.add_argument(
        "--lookback",
        "-l",
        type=int,
        default=14,
        help="Number of days to look back for activity (default: 14)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="board_summary.html",
        help="Output file path (default: board_summary.html)",
    )
    parser.add_argument(
        "--skip-fetch",
        "-s",
        action="store_true",
        help="Use cached data instead of fetching fresh data",
    )

    args = parser.parse_args()

    users = args.users if args.users else None

    run_board_summary(
        users=users,
        lookback_days=args.lookback,
        output=args.output,
        skip_fetch=args.skip_fetch,
    )


if __name__ == "__main__":
    main()
