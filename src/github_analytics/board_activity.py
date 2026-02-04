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
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

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

from github_analytics.user_activity import collect_user_engagements, graphql_request

load_dotenv()
console = Console()

CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

GRAPHQL_URL = "https://api.github.com/graphql"

# GraphQL query for detailed item data (body, comments, reviews, commits)
ITEM_DETAILS_QUERY = """
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    issueOrPullRequest(number: $number) {
      ... on Issue {
        __typename
        body
        author { login }
        createdAt
        updatedAt
        comments(last: 50) {
          nodes {
            author { login }
            body
            createdAt
          }
        }
      }
      ... on PullRequest {
        __typename
        body
        author { login }
        createdAt
        updatedAt
        additions
        deletions
        changedFiles
        comments(last: 50) {
          nodes {
            author { login }
            body
            createdAt
          }
        }
        reviews(last: 20) {
          nodes {
            author { login }
            body
            state
            submittedAt
          }
        }
        reviewRequests(first: 10) {
          nodes {
            requestedReviewer {
              ... on User { login }
            }
          }
        }
        commits(last: 30) {
          nodes {
            commit {
              message
              committedDate
              additions
              deletions
              author {
                user { login }
              }
            }
          }
        }
      }
    }
  }
}
"""


def get_github_token() -> str:
    """Return the GitHub token from environment."""
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN not set in environment or .env file")
    return token


def fetch_item_details(
    repo: str,
    number: int,
    token: str,
) -> dict[str, Any] | None:
    """Fetch detailed data for a single issue or PR.

    Returns dict with: type, body, author, comments, reviews (for PRs),
    review_requests (for PRs), created_at, updated_at.
    """
    if "/" not in repo:
        return None

    owner, name = repo.split("/", 1)

    try:
        data = graphql_request(
            ITEM_DETAILS_QUERY,
            {"owner": owner, "name": name, "number": number},
            token,
        )
    except Exception as e:
        console.print(f"[dim]Warning: Failed to fetch {repo}#{number}: {e}[/dim]")
        return None

    item = data.get("repository", {}).get("issueOrPullRequest")
    if not item:
        return None

    result: dict[str, Any] = {
        "type": item.get("__typename", "Unknown"),
        "body": item.get("body", "") or "",
        "author": item.get("author", {}).get("login", ""),
        "created_at": item.get("createdAt", ""),
        "updated_at": item.get("updatedAt", ""),
        "additions": item.get("additions", 0),
        "deletions": item.get("deletions", 0),
        "changed_files": item.get("changedFiles", 0),
        "comments": [],
        "reviews": [],
        "review_requests": [],
        "commits": [],
    }

    # Extract comments
    for node in item.get("comments", {}).get("nodes", []):
        if node:
            author = node.get("author")
            result["comments"].append(
                {
                    "author": author.get("login", "") if author else "",
                    "body": node.get("body", "") or "",
                    "created_at": node.get("createdAt", ""),
                }
            )

    # Extract reviews (PRs only)
    for node in item.get("reviews", {}).get("nodes", []):
        if node:
            author = node.get("author")
            result["reviews"].append(
                {
                    "author": author.get("login", "") if author else "",
                    "body": node.get("body", "") or "",
                    "state": node.get("state", ""),
                    "submitted_at": node.get("submittedAt", ""),
                }
            )

    # Extract review requests (PRs only)
    for node in item.get("reviewRequests", {}).get("nodes", []):
        if node:
            reviewer = node.get("requestedReviewer", {})
            if reviewer and reviewer.get("login"):
                result["review_requests"].append(reviewer["login"])

    # Extract commits (PRs only)
    for node in item.get("commits", {}).get("nodes", []):
        if node and node.get("commit"):
            commit = node["commit"]
            author_info = commit.get("author", {})
            user = author_info.get("user") if author_info else None
            result["commits"].append(
                {
                    "message": commit.get("message", "") or "",
                    "committed_date": commit.get("committedDate", ""),
                    "author": user.get("login", "") if user else "",
                    "additions": commit.get("additions", 0),
                    "deletions": commit.get("deletions", 0),
                }
            )

    return result


# ---------------------------------------------------------------------------
# LLM Prompt Templates
# ---------------------------------------------------------------------------

INTENT_PROMPT = """\
Summarize the intent of this GitHub {item_type} in 1-2 sentences. Be concise.

Title: {title}
Description:
{body}

Intent:"""

CODE_PROGRESS_PROMPT = """\
Summarize the code changes in this pull request in 2-3 sentences.
Focus on what was implemented or fixed, based on the commit messages.

PR stats: {changed_files} files changed, +{additions}/-{deletions} lines

Recent commits:
{commits_log}

Summary:"""

DISCUSSION_PROMPT = """\
Summarize the review discussion on this GitHub {item_type} in 2-3 sentences.
Focus on feedback given, decisions made, or blockers identified.

Recent comments and reviews:
{discussion_log}

Summary:"""

STATUS_PROMPT = """\
Based on the activity below, determine the current status.
Reply with ONE of these options exactly:
- "Progressing" if actively being worked on
- "Waiting for review from [names]" if awaiting reviews
- "Waiting for author" if reviewer requested changes
- "Stale" if no recent activity
- "Blocked: [reason]" if blocked on something specific

Recent code activity:
{code_log}

Recent discussion:
{discussion_log}

Review requests pending: {review_requests}

Status:"""


def build_discussion_log(
    details: dict[str, Any],
    since: datetime,
    min_entries: int = 10,
) -> str:
    """Build a condensed discussion log from comments and reviews.

    If fewer than min_entries are found in the time window, falls back to
    the last min_entries regardless of date.
    """
    all_entries = []

    # Collect all comments
    for comment in details.get("comments", []):
        if not comment.get("created_at"):
            continue
        created = datetime.fromisoformat(comment["created_at"].replace("Z", "+00:00"))
        author = comment.get("author", "unknown")
        body = comment.get("body", "")[:200]  # Truncate long comments
        date_str = created.strftime("%Y-%m-%d")
        text = f"[{date_str}] {author} commented: {body}"
        all_entries.append({"date": created, "text": text})

    # Collect all reviews (for PRs)
    for review in details.get("reviews", []):
        if not review.get("submitted_at"):
            continue
        ts = review["submitted_at"].replace("Z", "+00:00")
        submitted = datetime.fromisoformat(ts)
        author = review.get("author", "unknown")
        state = review.get("state", "")
        body = review.get("body", "")[:150]
        state_text = {
            "APPROVED": "approved",
            "CHANGES_REQUESTED": "requested changes",
            "COMMENTED": "reviewed",
        }.get(state, state.lower())
        entry = f"[{submitted.strftime('%Y-%m-%d')}] {author} {state_text}"
        if body:
            entry += f": {body}"
        all_entries.append({"date": submitted, "text": entry})

    # Sort by date descending
    all_entries.sort(key=lambda x: x["date"], reverse=True)

    if not all_entries:
        return "No discussion activity."

    # Filter to recent entries
    recent_entries = [e for e in all_entries if e["date"] >= since]

    # If not enough recent entries, fall back to last N entries
    if len(recent_entries) < min_entries:
        entries = all_entries[:min_entries]
    else:
        entries = recent_entries[:min_entries]

    return "\n".join(e["text"] for e in entries)


def build_code_progress_log(
    details: dict[str, Any],
    since: datetime,
    min_entries: int = 10,
) -> str:
    """Build a condensed log of code changes from commits.

    If fewer than min_entries are found in the time window, falls back to
    the last min_entries regardless of date.
    """
    all_entries = []

    for commit in details.get("commits", []):
        if not commit.get("committed_date"):
            continue
        ts = commit["committed_date"].replace("Z", "+00:00")
        committed = datetime.fromisoformat(ts)
        author = commit.get("author", "unknown")
        message = commit.get("message", "").split("\n")[0][:100]  # First line only
        additions = commit.get("additions", 0)
        deletions = commit.get("deletions", 0)
        date_str = committed.strftime("%Y-%m-%d")
        text = f"[{date_str}] {author}: {message} (+{additions}/-{deletions})"
        all_entries.append({"date": committed, "text": text})

    # Sort by date descending
    all_entries.sort(key=lambda x: x["date"], reverse=True)

    if not all_entries:
        return "No commits found."

    # Filter to recent entries
    recent_entries = [e for e in all_entries if e["date"] >= since]

    # If not enough recent entries, fall back to last N entries
    if len(recent_entries) < min_entries:
        entries = all_entries[:min_entries]
    else:
        entries = recent_entries[:min_entries]

    return "\n".join(e["text"] for e in entries)


def generate_llm_summaries(
    item: dict[str, Any],
    details: dict[str, Any],
    days: int,
    model: str,
) -> dict[str, str]:
    """Generate LLM summaries for intent, code progress, discussion, and status.

    Returns dict with keys: intent, code_progress, discussion, status.
    """
    from github_analytics.ollama_client import generate_summary, is_ollama_available

    unavailable = "[LLM unavailable]"
    if not is_ollama_available():
        return {
            "intent": unavailable,
            "code_progress": unavailable,
            "discussion": unavailable,
            "status": unavailable,
        }

    since = datetime.now(timezone.utc) - timedelta(days=days)
    item_type = "pull request" if details.get("type") == "PullRequest" else "issue"
    title = item.get("title", "")
    body = (details.get("body", "") or "")[:1500]  # Truncate very long descriptions
    discussion_log = build_discussion_log(details, since)
    code_log = build_code_progress_log(details, since)
    review_requests = ", ".join(details.get("review_requests", [])) or "None"

    # Generate intent summary
    intent_prompt = INTENT_PROMPT.format(
        item_type=item_type,
        title=title,
        body=body if body else "(No description provided)",
    )
    intent = generate_summary(intent_prompt, model=model)

    # Generate code progress summary (PRs only)
    if details.get("type") == "PullRequest" and details.get("commits"):
        code_prompt = CODE_PROGRESS_PROMPT.format(
            changed_files=details.get("changed_files", 0),
            additions=details.get("additions", 0),
            deletions=details.get("deletions", 0),
            commits_log=code_log,
        )
        code_progress = generate_summary(code_prompt, model=model)
    else:
        if item_type == "issue":
            code_progress = "(No commits - this is an issue)"
        else:
            code_progress = "(No commits yet)"

    # Generate discussion summary
    discussion_prompt = DISCUSSION_PROMPT.format(
        item_type=item_type,
        discussion_log=discussion_log,
    )
    discussion = generate_summary(discussion_prompt, model=model)

    # Generate status
    status_prompt = STATUS_PROMPT.format(
        code_log=code_log,
        discussion_log=discussion_log,
        review_requests=review_requests,
    )
    status = generate_summary(status_prompt, model=model)

    return {
        "intent": intent,
        "code_progress": code_progress,
        "discussion": discussion,
        "status": status,
    }


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


# ---------------------------------------------------------------------------
# HTML Report Generation
# ---------------------------------------------------------------------------


def generate_html_report(
    board_df: pl.DataFrame,
    items: list[dict],
    days: int,
    output_path: Path,
    model: str,
    use_llm: bool = True,
) -> None:
    """Generate an HTML report with LLM-powered summaries.

    Parameters
    ----------
    board_df : pl.DataFrame
        Board assignments dataframe.
    items : list[dict]
        Raw project board items.
    days : int
        Number of days to look back for activity.
    output_path : Path
        Output HTML file path.
    model : str
        Ollama model to use for summaries.
    use_llm : bool
        Whether to generate LLM summaries (default: True).
    """
    from github_analytics.ollama_client import is_ollama_available

    if board_df.is_empty():
        console.print("[yellow]No data to generate HTML report.[/yellow]")
        return

    # Check Ollama availability
    llm_available = use_llm and is_ollama_available()
    if use_llm and not llm_available:
        console.print(
            "[yellow]Warning: Ollama not available. "
            "Summaries will show 'LLM unavailable'.[/yellow]"
        )

    # Get unique items from board
    cols = ["repo", "number", "title", "url", "status"]
    unique_items = board_df.select(cols).unique()

    # Build item -> users mapping
    item_users: dict[tuple[str, int], dict[str, list[str]]] = {}
    for row in board_df.iter_rows(named=True):
        key = (row["repo"], row["number"])
        if key not in item_users:
            item_users[key] = {"champion": [], "reviewer": []}
        if row["role"] == "champion":
            item_users[key]["champion"].append(row["user"])
        else:
            item_users[key]["reviewer"].append(row["user"])

    # Fetch details and generate summaries for each item
    token = get_github_token()
    report_rows = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(
            "Fetching item details...",
            total=unique_items.height,
        )

        for row in unique_items.iter_rows(named=True):
            repo = row["repo"]
            number = row["number"]
            title = row["title"]
            url = row["url"]
            status = row["status"]

            progress.update(
                task,
                description=f"Processing {repo}#{number}...",
                advance=1,
            )

            # Get users for this item
            users = item_users.get((repo, number), {"champion": [], "reviewer": []})
            all_users = users["champion"] + users["reviewer"]

            # Fetch details from GitHub
            details = fetch_item_details(repo, number, token)

            if llm_available and details:
                # Build a minimal item dict for generate_llm_summaries
                item_dict = {"title": title}
                summaries = generate_llm_summaries(item_dict, details, days, model)
            else:
                na = "[LLM unavailable]" if use_llm else "-"
                summaries = {
                    "intent": na,
                    "code_progress": na,
                    "discussion": na,
                    "status": na,
                }

            # Create short repo reference
            repo_short = repo.split("/")[-1] if "/" in repo else repo
            item_ref = f"{repo_short}#{number}"

            report_rows.append(
                {
                    "item_ref": item_ref,
                    "url": url,
                    "title": title,
                    "champion": ", ".join(users["champion"]) or "-",
                    "reviewers": ", ".join(users["reviewer"]) or "-",
                    "intent": summaries["intent"],
                    "code_progress": summaries["code_progress"],
                    "discussion": summaries["discussion"],
                    "llm_status": summaries["status"],
                    "board_status": status,
                    "all_users": all_users,
                }
            )

    # Generate HTML
    html_content = _build_html_report(report_rows, days)

    # Write file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html_content)
    console.print(f"[green]HTML report saved to {output_path}[/green]")


def _build_html_report(report_rows: list[dict], days: int) -> str:
    """Build the HTML content for the report."""
    import html as html_mod

    # Collect all unique users for filtering (case-insensitive deduplication)
    # Keep the first casing encountered for display
    user_display: dict[str, str] = {}  # lowercase -> display name
    for row in report_rows:
        for user in row["all_users"]:
            lower = user.lower()
            if lower not in user_display:
                user_display[lower] = user
    all_users_sorted = sorted(user_display.values(), key=str.lower)

    def get_status_class(status: str) -> str:
        status_lower = status.lower()
        if "progressing" in status_lower:
            return "status-progressing"
        elif "waiting for review" in status_lower:
            return "status-waiting-review"
        elif "waiting for author" in status_lower:
            return "status-waiting-author"
        elif "stale" in status_lower:
            return "status-stale"
        elif "blocked" in status_lower:
            return "status-blocked"
        return "status-unknown"

    # Build table rows
    table_rows = []
    for row in report_rows:
        users_data = ",".join(row["all_users"]).lower()
        status_class = get_status_class(row["llm_status"])

        # Escape HTML in text fields
        title_escaped = html_mod.escape(row["title"][:80])
        title_full = html_mod.escape(row["title"])
        intent_escaped = html_mod.escape(row["intent"])
        code_progress_escaped = html_mod.escape(row["code_progress"])
        discussion_escaped = html_mod.escape(row["discussion"])
        llm_status_escaped = html_mod.escape(row["llm_status"])
        url = row["url"]
        item_ref = row["item_ref"]
        champion = row["champion"]
        reviewers = row["reviewers"]

        status_badge = (
            f'<span class="status-badge {status_class}">{llm_status_escaped}</span>'
        )
        table_rows.append(f"""
        <tr data-users="{users_data}">
            <td><a href="{url}" target="_blank">{item_ref}</a></td>
            <td class="title-cell" title="{title_full}">{title_escaped}</td>
            <td>{champion}</td>
            <td>{reviewers}</td>
            <td class="summary-cell">{intent_escaped}</td>
            <td class="summary-cell">{code_progress_escaped}</td>
            <td class="summary-cell">{discussion_escaped}</td>
            <td>{status_badge}</td>
        </tr>
        """)

    # Build user filter checkboxes
    user_checkboxes = "\n".join(
        f'<label><input type="checkbox" class="user-filter" '
        f'value="{user.lower()}" checked> {user}</label>'
        for user in all_users_sorted
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Board Activity Report</title>
    <style>
        * {{
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI',
                Roboto, Oxygen, Ubuntu, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
            color: #333;
        }}
        h1 {{
            text-align: center;
            color: #2c3e50;
            margin-bottom: 10px;
        }}
        .subtitle {{
            text-align: center;
            color: #7f8c8d;
            margin-bottom: 20px;
        }}
        .filter-bar {{
            background: white;
            padding: 15px 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .filter-bar strong {{
            margin-right: 10px;
            color: #2c3e50;
        }}
        .filter-bar label {{
            margin-right: 15px;
            cursor: pointer;
            white-space: nowrap;
        }}
        .filter-bar input[type="checkbox"] {{
            margin-right: 4px;
        }}
        .filter-bar button {{
            margin-left: 10px;
            padding: 4px 12px;
            border: 1px solid #bdc3c7;
            border-radius: 4px;
            background: #ecf0f1;
            cursor: pointer;
            font-size: 13px;
        }}
        .filter-bar button:hover {{
            background: #d5dbdb;
        }}
        .table-container {{
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow-x: auto;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
            table-layout: fixed;
        }}
        th, td {{
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid #ecf0f1;
            overflow: hidden;
            text-overflow: ellipsis;
        }}
        th {{
            background: #2c3e50;
            color: white;
            font-weight: 600;
            position: sticky;
            top: 0;
            resize: horizontal;
            overflow: auto;
            min-width: 60px;
        }}
        tr:hover {{
            background: #f8f9fa;
        }}
        tr.hidden {{
            display: none;
        }}
        a {{
            color: #3498db;
            text-decoration: none;
        }}
        a:hover {{
            text-decoration: underline;
        }}
        .title-cell {{
            max-width: 250px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        .summary-cell {{
            max-width: 300px;
            font-size: 13px;
            color: #555;
        }}
        .status-badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 500;
            white-space: nowrap;
        }}
        .status-progressing {{
            background: #d5f5e3;
            color: #1e8449;
        }}
        .status-waiting-review {{
            background: #fef9e7;
            color: #b7950b;
        }}
        .status-waiting-author {{
            background: #fdebd0;
            color: #ca6f1e;
        }}
        .status-stale {{
            background: #eaecee;
            color: #5d6d7e;
        }}
        .status-blocked {{
            background: #fadbd8;
            color: #c0392b;
        }}
        .status-unknown {{
            background: #eaecee;
            color: #7f8c8d;
        }}
        .stats {{
            text-align: center;
            padding: 10px;
            color: #7f8c8d;
            font-size: 13px;
        }}
    </style>
</head>
<body>
    <h1>Board Activity Report</h1>
    <p class="subtitle">Activity summary for the last {days} days</p>

    <div class="filter-bar">
        <strong>Filter by user:</strong>
        <button id="select-all">Select all</button>
        <button id="unselect-all">Unselect all</button>
        <br><br>
        {user_checkboxes}
    </div>

    <div class="table-container">
        <table id="report-table">
            <thead>
                <tr>
                    <th>Item</th>
                    <th>Title</th>
                    <th>Champion</th>
                    <th>Reviewers</th>
                    <th>Intent</th>
                    <th>Code Progress</th>
                    <th>Discussion</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                {"".join(table_rows)}
            </tbody>
        </table>
    </div>

    <div class="stats" id="stats">
        Showing <span id="visible-count">{len(report_rows)}</span>
        of {len(report_rows)} items
    </div>

    <script>
        document.addEventListener('DOMContentLoaded', function() {{
            const checkboxes = document.querySelectorAll('.user-filter');
            const rows = document.querySelectorAll('#report-table tbody tr');
            const visibleCount = document.getElementById('visible-count');
            const selectAllBtn = document.getElementById('select-all');
            const unselectAllBtn = document.getElementById('unselect-all');

            function updateFilters() {{
                const selectedUsers = Array.from(checkboxes)
                    .filter(cb => cb.checked)
                    .map(cb => cb.value.toLowerCase());

                let visible = 0;
                rows.forEach(row => {{
                    const rowUsers = row.dataset.users.toLowerCase()
                        .split(',').filter(u => u);
                    const hasMatch = rowUsers.some(
                        u => selectedUsers.includes(u.toLowerCase())
                    );
                    if (hasMatch || rowUsers.length === 0) {{
                        row.classList.remove('hidden');
                        visible++;
                    }} else {{
                        row.classList.add('hidden');
                    }}
                }});
                visibleCount.textContent = visible;
            }}

            selectAllBtn.addEventListener('click', function() {{
                checkboxes.forEach(cb => cb.checked = true);
                updateFilters();
            }});

            unselectAllBtn.addEventListener('click', function() {{
                checkboxes.forEach(cb => cb.checked = false);
                updateFilters();
            }});

            checkboxes.forEach(cb => cb.addEventListener('change', updateFilters));
        }});
    </script>
</body>
</html>"""

    return html


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
    parser.add_argument(
        "--html",
        type=str,
        help="Output HTML report path with LLM summaries (optional)",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="llama3.1:8b",
        help="Ollama model to use for summaries (default: llama3.1:8b)",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Skip LLM summaries in HTML report (show raw data only)",
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

    # Generate HTML report if --html specified
    if args.html:
        console.print("\n" + "=" * 60 + "\n")
        console.print("[bold]Generating HTML Report with LLM Summaries[/bold]\n")
        generate_html_report(
            board_df=board_df,
            items=items,
            days=args.days,
            output_path=Path(args.html),
            model=args.model,
            use_llm=not args.no_llm,
        )


if __name__ == "__main__":
    main()
