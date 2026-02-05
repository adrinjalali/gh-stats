#!/usr/bin/env python
"""Generate board summary reports for all users and per-user.

This script runs the complete workflow:
1. Fetch board data and user activity
2. Generate AI summaries (via Anthropic API)
3. Generate HTML reports (combined + per-user)

Usage:
    python generate_all_reports.py [users...] [options]

Examples:
    python generate_all_reports.py                     # All users, default 14 days
    python generate_all_reports.py ogrisel lesteve    # Specific users
    python generate_all_reports.py -l 7               # Last 7 days
    python generate_all_reports.py -o reports/        # Custom output directory
"""

import argparse
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
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

# Load environment variables
load_dotenv()

# Use GH_PAT for GitHub authentication (same name as GitHub Actions secret)
# Export as GITHUB_TOKEN for gh CLI compatibility
if os.environ.get("GH_PAT"):
    os.environ["GITHUB_TOKEN"] = os.environ["GH_PAT"]

SUMMARIES_FILE = Path(__file__).parent / "summaries.json"
CACHE_DIR = Path("cache/board_summary")


def generate_ai_summaries():
    """Generate AI summaries using Anthropic API."""
    try:
        from generate_summaries_api import generate_summaries

        print("\n" + "=" * 50)
        print("Generating AI summaries")
        print("=" * 50)
        generate_summaries(max_items=500)
    except Exception as e:
        print(f"Warning: Could not generate AI summaries: {e}")
        print("Continuing without new summaries...")


def generate_all_reports(
    users: list[str] | None = None,
    lookback_days: int = 14,
    output_dir: str = "reports",
    skip_fetch: bool = False,
    skip_ai: bool = False,
) -> dict[str, Path]:
    """Generate board summary reports.

    Args:
        users: List of GitHub usernames. If None, uses all users from board.
        lookback_days: Number of days to look back for activity.
        output_dir: Directory for output reports.
        skip_fetch: If True, use cached data instead of fetching.
        skip_ai: If True, skip AI summary generation.

    Returns:
        Dictionary mapping report type to file path.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Step 1: Fetch and enrich board items
    if skip_fetch and (CACHE_DIR / "board_items.json").exists():
        print("Using cached board items...")
        enriched_items = load_from_cache()
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

    # Step 2: Fetch user activity and merge
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
        merge_activity_with_board(users, lookback_days=lookback_days)

    # Step 3: Generate AI summaries
    if not skip_ai:
        generate_ai_summaries()

    # Step 4: Import summaries
    if SUMMARIES_FILE.exists():
        print("\n" + "=" * 50)
        print("Importing AI summaries")
        print("=" * 50)
        import_summaries(str(SUMMARIES_FILE))

    # Step 5: Generate reports
    print("\n" + "=" * 50)
    print("Generating reports")
    print("=" * 50)

    # Build filename components
    date_str = datetime.now().strftime("%Y-%m-%d")
    lookback_str = f"{lookback_days}d"

    reports = {}

    # Generate combined report for all users
    if len(users) > 1:
        users_str = "all" if len(users) > 3 else "_".join(sorted(users))
        combined_name = f"board_summary_{date_str}_{lookback_str}_{users_str}.html"
        combined_path = output_path / combined_name
        save_report(str(combined_path), users)
        reports["combined"] = combined_path
        print(f"  Combined report: {combined_path}")

        # Also create a latest symlink/copy
        latest_combined = output_path / "board_summary_latest.html"
        latest_combined.write_text(combined_path.read_text())
        reports["latest"] = latest_combined

    # Generate per-user reports
    for user in users:
        user_name = f"board_summary_{date_str}_{lookback_str}_{user}.html"
        user_path = output_path / user_name
        save_report(str(user_path), [user])
        reports[user] = user_path
        print(f"  {user}: {user_path}")

    print("\n" + "=" * 50)
    print("Done!")
    print("=" * 50)
    print(f"Reports saved to: {output_path}")

    return reports


def main():
    parser = argparse.ArgumentParser(
        description="Generate board summary reports (combined + per-user)"
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
        default="reports",
        help="Output directory for reports (default: reports)",
    )
    parser.add_argument(
        "--skip-fetch",
        "-s",
        action="store_true",
        help="Use cached data instead of fetching fresh data",
    )
    parser.add_argument(
        "--skip-ai",
        action="store_true",
        help="Skip AI summary generation (use existing summaries only)",
    )

    args = parser.parse_args()

    users = args.users if args.users else None

    generate_all_reports(
        users=users,
        lookback_days=args.lookback,
        output_dir=args.output,
        skip_fetch=args.skip_fetch,
        skip_ai=args.skip_ai,
    )


if __name__ == "__main__":
    main()
