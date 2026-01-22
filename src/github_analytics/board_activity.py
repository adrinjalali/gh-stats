"""Compare GitHub project board assignments with actual user activity.

This script fetches items from a GitHub project board and compares them with
actual user activity (comments, reviews, commits) to identify:
1. Board coverage gaps: Items where assigned users haven't been active recently
2. Per-user summary: Each user's board assignments vs their actual GitHub activity

Usage
-----
$ python -m github_analytics.board_activity --org probabl-ai --project 8

Requires ``GITHUB_TOKEN`` in the environment (``.env`` supported) and
``gh`` CLI with ``read:project`` scope.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from github_analytics.user_activity import collect_user_engagements

load_dotenv()
console = Console()

CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)


def fetch_project_board(org: str, project_number: int) -> list[dict]:
    """Fetch all items from a GitHub project board using gh CLI.

    Note: We clear GITHUB_TOKEN from environment so gh CLI uses its own
    authentication (which has read:project scope) instead of the token
    from .env (which may not have that scope).
    """
    import os

    cmd = [
        "gh",
        "project",
        "item-list",
        str(project_number),
        "--owner",
        org,
        "--format",
        "json",
        "--limit",
        "500",
    ]

    # Use gh CLI's own auth, not the GITHUB_TOKEN from .env
    env = os.environ.copy()
    env.pop("GITHUB_TOKEN", None)
    env.pop("GH_TOKEN", None)

    result = subprocess.run(cmd, capture_output=True, text=True, check=True, env=env)
    data = json.loads(result.stdout)
    return data.get("items", [])


def fetch_project_fields(org: str, project_number: int) -> dict:
    """Fetch project field definitions."""
    cmd = [
        "gh",
        "project",
        "field-list",
        str(project_number),
        "--owner",
        org,
        "--format",
        "json",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    data = json.loads(result.stdout)
    return data


def extract_board_assignments(items: list[dict]) -> pl.DataFrame:
    """Extract user assignments from project board items."""
    rows = []

    for item in items:
        content = item.get("content", {})
        if not content:
            continue

        # Extract basic info
        repo = content.get("repository", "")
        number = content.get("number", 0)
        title = content.get("title", "")
        item_type = content.get("type", "")
        url = content.get("url", "")
        status = item.get("status", "")

        # Extract assignments (case-insensitive field lookup)
        champion = item.get("champion") or item.get("Champion")
        reviewer1 = item.get("reviewer 1") or item.get("Reviewer 1")
        reviewer2 = item.get("reviewer 2") or item.get("Reviewer 2")

        # Create a row for each assigned user
        assigned_users = []
        if champion:
            assigned_users.append(("champion", champion))
        if reviewer1:
            assigned_users.append(("reviewer", reviewer1))
        if reviewer2:
            assigned_users.append(("reviewer", reviewer2))

        for role, user in assigned_users:
            rows.append(
                {
                    "repo": repo,
                    "number": number,
                    "title": title,
                    "type": item_type,
                    "url": url,
                    "status": status,
                    "user": user,
                    "role": role,
                }
            )

    return pl.DataFrame(rows) if rows else pl.DataFrame()


def get_unique_users_from_board(board_df: pl.DataFrame) -> list[str]:
    """Get unique usernames from board assignments."""
    if board_df.is_empty():
        return []
    return board_df["user"].unique().to_list()


def check_pr_activity_for_user(
    repo: str, pr_number: int, user: str, since: datetime
) -> bool:
    """Check if a user has recent activity on a specific PR.

    This catches activity that the general user activity queries miss,
    such as commits on PRs authored by others.
    """
    import os

    owner, name = repo.split("/")
    user_lower = user.lower()

    query = """
    query($owner: String!, $name: String!, $number: Int!) {
      repository(owner: $owner, name: $name) {
        pullRequest(number: $number) {
          commits(last: 50) {
            nodes {
              commit {
                committedDate
                author { user { login } }
              }
            }
          }
          comments(last: 50) {
            nodes {
              createdAt
              author { login }
            }
          }
          reviews(last: 20) {
            nodes {
              submittedAt
              author { login }
            }
          }
        }
      }
    }
    """

    token = os.getenv("GITHUB_TOKEN")
    if not token:
        return False

    import requests

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(
            "https://api.github.com/graphql",
            json={
                "query": query,
                "variables": {"owner": owner, "name": name, "number": pr_number},
            },
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        if "errors" in data:
            return False

        pr_data = data.get("data", {}).get("repository", {}).get("pullRequest")
        if not pr_data:
            return False

        # Check commits
        for node in pr_data.get("commits", {}).get("nodes", []):
            commit = node.get("commit", {})
            author = commit.get("author", {}).get("user", {})
            if author and author.get("login", "").lower() == user_lower:
                committed_date = datetime.fromisoformat(
                    commit["committedDate"].replace("Z", "+00:00")
                )
                if committed_date >= since:
                    return True

        # Check comments
        for node in pr_data.get("comments", {}).get("nodes", []):
            author = node.get("author", {})
            if author and author.get("login", "").lower() == user_lower:
                created = datetime.fromisoformat(
                    node["createdAt"].replace("Z", "+00:00")
                )
                if created >= since:
                    return True

        # Check reviews
        for node in pr_data.get("reviews", {}).get("nodes", []):
            author = node.get("author", {})
            if author and author.get("login", "").lower() == user_lower:
                submitted = datetime.fromisoformat(
                    node["submittedAt"].replace("Z", "+00:00")
                )
                if submitted >= since:
                    return True

    except Exception:
        pass

    return False


def compare_board_with_activity(
    board_df: pl.DataFrame,
    activity_df: pl.DataFrame,
    days: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Compare board assignments with actual activity.

    Returns:
        - gaps_df: Items where assigned users haven't been active
        - extra_df: Activity on items not assigned to the user on the board
    """
    if board_df.is_empty():
        return pl.DataFrame(), pl.DataFrame()

    # Normalize usernames to lowercase for comparison
    board_df = board_df.with_columns(
        pl.col("user").str.to_lowercase().alias("user_lower")
    )

    if not activity_df.is_empty():
        activity_df = activity_df.with_columns(
            pl.col("user").str.to_lowercase().alias("user_lower")
        )

        # Get unique (user, repo, number) pairs from activity
        activity_items = activity_df.select(["user_lower", "repo", "number"]).unique()

        # Find gaps: board assignments where user has no recent activity
        gaps_df = board_df.join(
            activity_items,
            left_on=["user_lower", "repo", "number"],
            right_on=["user_lower", "repo", "number"],
            how="anti",
        ).drop("user_lower")

        # Find extra: activity on items where user is not assigned on board
        board_items = board_df.select(["user_lower", "repo", "number"]).unique()

        extra_df = activity_df.join(
            board_items,
            left_on=["user_lower", "repo", "number"],
            right_on=["user_lower", "repo", "number"],
            how="anti",
        ).drop("user_lower")
    else:
        gaps_df = board_df.drop("user_lower")
        extra_df = pl.DataFrame()

    return gaps_df, extra_df


def print_coverage_gaps(gaps_df: pl.DataFrame, days: int) -> None:
    """Print board items where assigned users haven't been active."""
    if gaps_df.is_empty():
        console.print(
            f"[green]No coverage gaps found - all assigned users have been "
            f"active on their items in the last {days} days![/green]"
        )
        return

    # Filter to only show non-Done items
    active_gaps = gaps_df.filter(~pl.col("status").str.contains("Done"))

    if active_gaps.is_empty():
        console.print(
            f"[green]No coverage gaps on active items (non-Done status) "
            f"in the last {days} days![/green]"
        )
        return

    table = Table(
        title=f"Board Coverage Gaps (no activity in last {days} days)",
        show_lines=True,
    )
    table.add_column("User", style="cyan")
    table.add_column("Role")
    table.add_column("Status")
    table.add_column("Item")
    table.add_column("Title", max_width=50)

    # Sort by user, then status
    sorted_gaps = active_gaps.sort(["user", "status"])

    for row in sorted_gaps.iter_rows(named=True):
        repo_short = row["repo"].split("/")[-1] if "/" in row["repo"] else row["repo"]
        item_ref = f"{repo_short}#{row['number']}"

        table.add_row(
            row["user"],
            row["role"],
            row["status"],
            item_ref,
            row["title"][:50],
        )

    console.print(table)
    console.print(f"\n[dim]Total gaps: {len(active_gaps)} items[/dim]")


def print_user_summary(
    board_df: pl.DataFrame,
    activity_df: pl.DataFrame,
    gaps_df: pl.DataFrame,
    days: int,
) -> None:
    """Print per-user summary of board assignments vs activity."""
    if board_df.is_empty():
        console.print("[yellow]No board assignments found.[/yellow]")
        return

    users = sorted(board_df["user"].unique().to_list())

    for user in users:
        user_lower = user.lower()

        # Board assignments for this user
        user_board = board_df.filter(pl.col("user") == user)
        active_assignments = user_board.filter(~pl.col("status").str.contains("Done"))

        # Activity for this user
        if not activity_df.is_empty():
            user_activity = activity_df.filter(
                pl.col("user").str.to_lowercase() == user_lower
            )
            unique_items_active = (
                user_activity.filter(
                    pl.col("number") != 0
                )  # Exclude commits without PR
                .select(["repo", "number"])
                .unique()
                .height
            )
            total_interactions = user_activity.height
        else:
            unique_items_active = 0
            total_interactions = 0

        # Gaps for this user
        user_gaps = gaps_df.filter(pl.col("user") == user)
        active_gaps = user_gaps.filter(~pl.col("status").str.contains("Done"))

        # Build summary panel
        champion_count = user_board.filter(pl.col("role") == "champion").height
        reviewer_count = user_board.filter(pl.col("role") == "reviewer").height

        summary_lines = [
            "[bold]Board Assignments:[/bold]",
            f"  Champion: {champion_count} items",
            f"  Reviewer: {reviewer_count} items",
            f"  Active (non-Done): {active_assignments.height} items",
            "",
            f"[bold]Activity (last {days} days):[/bold]",
            f"  Items with activity: {unique_items_active}",
            f"  Total interactions: {total_interactions}",
            "",
            "[bold]Coverage:[/bold]",
        ]

        if active_gaps.height > 0:
            gap_msg = f"{active_gaps.height} active items with no recent activity"
            summary_lines.append(f"  [red]Gaps: {gap_msg}[/red]")
        else:
            summary_lines.append("  [green]Full coverage on all active items[/green]")

        panel = Panel(
            "\n".join(summary_lines),
            title=f"[bold cyan]{user}[/bold cyan]",
            border_style="blue",
        )
        console.print(panel)
        console.print()


def print_untracked_activity(
    activity_df: pl.DataFrame,
    board_df: pl.DataFrame,
    days: int,
) -> None:
    """Print activity on items not tracked on the board."""
    if activity_df.is_empty():
        return

    # Get board items
    if not board_df.is_empty():
        board_items = set(
            zip(
                board_df["repo"].to_list(),
                board_df["number"].to_list(),
            )
        )
    else:
        board_items = set()

    # Filter activity to items not on board (excluding commits without PR number)
    untracked = activity_df.filter(pl.col("number") != 0).filter(
        ~pl.struct(["repo", "number"]).is_in(
            [{"repo": r, "number": n} for r, n in board_items]
        )
        if board_items
        else pl.lit(True)
    )

    if untracked.is_empty():
        return

    # Aggregate by user and item
    summary = (
        untracked.group_by(["user", "repo", "number", "type", "title"])
        .agg(pl.count().alias("interactions"))
        .sort(["user", "interactions"], descending=[False, True])
    )

    table = Table(
        title=f"Activity on Items Not on Board (last {days} days)",
        show_lines=True,
    )
    table.add_column("User", style="cyan")
    table.add_column("Item")
    table.add_column("Type")
    table.add_column("Title", max_width=40)
    table.add_column("Interactions", justify="right")

    for row in summary.head(30).iter_rows(named=True):
        repo_short = row["repo"].split("/")[-1] if "/" in row["repo"] else row["repo"]
        item_ref = f"{repo_short}#{row['number']}"

        table.add_row(
            row["user"],
            item_ref,
            row["type"],
            row["title"][:40] if row["title"] else "",
            str(row["interactions"]),
        )

    console.print(table)
    if summary.height > 30:
        console.print(f"[dim]... and {summary.height - 30} more items[/dim]")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare GitHub project board with user activity"
    )
    parser.add_argument(
        "--org",
        default="probabl-ai",
        help="GitHub organization (default: probabl-ai)",
    )
    parser.add_argument(
        "--project",
        type=int,
        default=8,
        help="Project number (default: 8)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Look back N days for activity (default: 14)",
    )
    parser.add_argument(
        "--users",
        help="Comma-separated list of users to analyze (default: all from board)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Output directory for reports (optional)",
    )
    args = parser.parse_args()

    console.print(
        f"[bold]Comparing project board {args.org}/projects/{args.project} "
        f"with activity from last {args.days} days[/bold]\n"
    )

    # Step 1: Fetch project board
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Fetching project board...", total=None)

        try:
            items = fetch_project_board(args.org, args.project)
            progress.update(task, description=f"Fetched {len(items)} board items")
        except subprocess.CalledProcessError as e:
            console.print(f"[red]Error fetching project board: {e.stderr}[/red]")
            console.print(
                "[yellow]Make sure gh CLI has read:project scope: "
                "gh auth refresh -s read:project[/yellow]"
            )
            return

    # Step 2: Extract board assignments
    board_df = extract_board_assignments(items)
    if board_df.is_empty():
        console.print("[yellow]No assignments found on the board.[/yellow]")
        return

    console.print(f"[green]Found {board_df.height} user-item assignments[/green]\n")

    # Step 3: Determine users to analyze
    if args.users:
        users = [u.strip() for u in args.users.split(",")]
    else:
        users = get_unique_users_from_board(board_df)

    console.print(f"[blue]Analyzing {len(users)} users: {', '.join(users)}[/blue]\n")

    # Filter board to only include specified users (case-insensitive)
    users_lower = [u.lower() for u in users]
    board_df = board_df.filter(pl.col("user").str.to_lowercase().is_in(users_lower))

    if board_df.is_empty():
        console.print(
            "[yellow]No board assignments found for specified users.[/yellow]"
        )
        return

    console.print(
        f"[green]Found {board_df.height} assignments for specified users[/green]\n"
    )

    # Step 4: Fetch user activity
    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    activity_df = collect_user_engagements(users, since)

    # Step 5: Compare and generate reports
    console.print("\n" + "=" * 60 + "\n")

    gaps_df, _extra_df = compare_board_with_activity(board_df, activity_df, args.days)

    # Step 6: Verify gaps by checking PRs directly (catches commits on others' PRs)
    if not gaps_df.is_empty():
        console.print("[dim]Verifying gaps by checking PRs directly...[/dim]")
        verified_gaps = []
        for row in gaps_df.iter_rows(named=True):
            # Only check PRs, not issues
            if row.get("type") == "PullRequest" and row.get("number"):
                has_activity = check_pr_activity_for_user(
                    row["repo"], row["number"], row["user"], since
                )
                if not has_activity:
                    verified_gaps.append(row)
            else:
                # Keep issues in gaps (can't check commits on issues)
                verified_gaps.append(row)

        gaps_df = pl.DataFrame(verified_gaps) if verified_gaps else pl.DataFrame()

        console.print(f"[dim]Verified {len(verified_gaps)} actual gaps[/dim]\n")

    # Report 1: Coverage gaps
    console.print("[bold]REPORT 1: Board Coverage Gaps[/bold]\n")
    print_coverage_gaps(gaps_df, args.days)

    console.print("\n" + "=" * 60 + "\n")

    # Report 2: Per-user summary
    console.print("[bold]REPORT 2: Per-User Summary[/bold]\n")
    print_user_summary(board_df, activity_df, gaps_df, args.days)

    console.print("\n" + "=" * 60 + "\n")

    # Report 3: Untracked activity
    console.print("[bold]REPORT 3: Activity Not on Board[/bold]\n")
    print_untracked_activity(activity_df, board_df, args.days)

    # Save reports if output specified
    if args.output:
        out_dir = Path(args.output)
        out_dir.mkdir(parents=True, exist_ok=True)

        if not board_df.is_empty():
            board_df.write_csv(out_dir / "board_assignments.csv")
        if not activity_df.is_empty():
            activity_df.write_csv(out_dir / "user_activity.csv")
        if not gaps_df.is_empty():
            gaps_df.write_csv(out_dir / "coverage_gaps.csv")

        console.print(f"\n[green]Reports saved to {out_dir}[/green]")


if __name__ == "__main__":
    main()
