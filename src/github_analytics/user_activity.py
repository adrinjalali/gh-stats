"""Summarize a GitHub user's recent engagements.

Given a GitHub *username* the script lists all issues and pull requests where
that user has been **active** (commented, reviewed, opened, etc.) during the
last *N* days (default: 7).  For each item the script shows:

* the type of object (Issue / PR),
* repository and number,
* title,
* the type of the **latest** involvement within the period,
* a link to that latest involvement.

The implementation relies on the REST *events* API exposed via **PyGithub**.
The authenticated user's rate-limit is ~5k requests per hour which is more than
sufficient because we only need to fetch at most the 300 most-recent events -
the maximum returned by the endpoint.  The script stops once events become
older than the requested time window, minimizing API usage.

This script is intended as a quick, self-contained report - no incremental
caching.  Execution time is typically < 2 s.

Usage
-----
$ python -m github_analytics.user_activity --user adrinm

Requires ``GITHUB_TOKEN`` in the environment (``.env`` supported).
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import polars as pl
from dotenv import load_dotenv
from github import Github
from github.Event import Event as GHEvent
from github.GithubException import GithubException
from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

# ---------------------------------------------------------------------------
# Environment & global objects
# ---------------------------------------------------------------------------
load_dotenv()
console = Console()

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def get_github_client() -> Github:
    """Return an authenticated GitHub client using *GITHUB_TOKEN*."""
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN not set in environment or .env file")

    gh = Github(token, per_page=100)
    try:
        console.print(f"[green]Authenticated as {gh.get_user().login}[/green]")
    except Exception as err:
        raise RuntimeError("Invalid GitHub token") from err
    return gh


def end_of_day(dt: datetime) -> datetime:
    """Return *dt* rounded to 23:59:59 of that same day (UTC)."""
    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)


# ---------------------------------------------------------------------------
# Event parsing
# ---------------------------------------------------------------------------

# Mapping of GitHub event types to human-friendly involvement labels and a
# function that extracts title / url / number from the *payload*.
# The lambda receives (*payload*, *repo_full_name*) and must return:
#   title, html_url, number
EventSpec = tuple[str, "callable[[dict[str, Any], str], tuple[str, str, int]]"]
EVENT_SPECS: dict[str, EventSpec] = {
    "IssueCommentEvent": (
        "commented",
        lambda p, _: (
            p["issue"]["title"],
            p["comment"]["html_url"],
            p["issue"]["number"],
        ),
    ),
    "IssuesEvent": (
        # action e.g. opened / closed / reopened / edited
        lambda p: p.get("action", "acted"),  # type: ignore[return-value]
        lambda p, _: (
            p["issue"]["title"],
            p["issue"]["html_url"],
            p["issue"]["number"],
        ),
    ),
    "PullRequestEvent": (
        lambda p: p.get("action", "acted"),  # opened / closed / reopened / etc.
        lambda p, _: (
            p["pull_request"]["title"],
            p["pull_request"]["html_url"],
            p["pull_request"]["number"],
        ),
    ),
    "PullRequestReviewCommentEvent": (
        "review comment",
        lambda p, _: (
            p["pull_request"]["title"],
            p["comment"]["html_url"],
            p["pull_request"]["number"],
        ),
    ),
    "PullRequestReviewEvent": (
        "reviewed",
        lambda p, repo: (
            # PyGithub does not expose review details in *payload*; fall back
            # to PR API URL with review anchor - lacks direct link.
            p["pull_request"]["title"],
            f"https://github.com/{repo}/pull/{p['pull_request']['number']}#pullrequestreview",
            p["pull_request"]["number"],
        ),
    ),
}


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def collect_user_engagements(user_login: str, since: datetime) -> pl.DataFrame:
    """Return a DataFrame of the user's engagements since *since* (UTC)."""
    gh = get_github_client()
    user = gh.get_user(user_login)

    # Data structure: key -> latest event details
    latest: dict[tuple[str, int], dict[str, Any]] = {}

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        evt_task = progress.add_task("Fetching user events...", total=None)

        # Events are returned newest → oldest (max 300). Stop early once older.
        for evt in user.get_events():  # type: ignore[assignment]
            evt: GHEvent
            created: datetime = evt.created_at.replace(tzinfo=timezone.utc)
            if created < since:
                break  # done

            spec = EVENT_SPECS.get(evt.type)
            if not spec:
                continue  # ignore unrelated event types

            involvement_raw, extractor = spec
            involvement = (
                involvement_raw(evt.payload)  # type: ignore[arg-type]
                if callable(involvement_raw)
                else involvement_raw
            )
            repo_full_name = evt.repo.full_name
            title, url, number = extractor(evt.payload, repo_full_name)

            # Determine status character (open ○, closed x, merged ✓) from payload
            status_char = "?"
            if "pull_request" in evt.payload:
                pr_payload = evt.payload["pull_request"]
                if pr_payload.get("merged"):
                    status_char = "✓"
                else:
                    status_char = "x" if pr_payload.get("state") == "closed" else "○"
            elif "issue" in evt.payload:
                issue_payload = evt.payload["issue"]
                status_char = "x" if issue_payload.get("state") == "closed" else "○"

            key = (repo_full_name, number)
            # Keep only the **latest** event per key (events are in descending
            # order so the first one we see is the latest).
            if key not in latest:
                latest[key] = {
                    "repo": repo_full_name,
                    "number": number,
                    "type": ("PR" if "pull" in url else "Issue") + f" {status_char}",
                    "title": title,
                    "involvement": involvement,
                    "date": created,
                    "url": url,
                }

        progress.update(evt_task, description="Finished fetching events")

    # -------------------------------------------------------------------
    # Fallback via Search API - ensure no engagements are missed
    # -------------------------------------------------------------------
    query_since = since.strftime("%Y-%m-%d")
    search_q = f"involves:{user_login} updated:>={query_since}"
    try:
        search_results = gh.search_issues(query=search_q, sort="updated", order="desc")  # type: ignore[arg-type]
        for item in search_results:
            key = (item.repository.full_name, item.number)
            if key in latest:
                continue  # already captured via events
            # Determine status char
            status_char = (
                "✓"
                if getattr(item, "merged", False)
                else ("x" if item.state == "closed" else "○")
            )
            is_pr = bool(item.pull_request)
            involvement = "author" if item.user.login == user_login else "involved"
            latest[key] = {
                "repo": item.repository.full_name,
                "number": item.number,
                "type": ("PR" if is_pr else "Issue") + f" {status_char}",
                "title": item.title,
                "involvement": involvement,
                "date": item.updated_at.replace(tzinfo=timezone.utc),
                "url": item.html_url,
            }
    except GithubException:
        # Search rate-limited or other error - ignore silently
        pass

    if not latest:
        return pl.DataFrame()

    # Convert dict_values to a regular list for Polars compatibility
    df = pl.from_dicts(list(latest.values()))
    df = df.sort("date", descending=True)
    return df


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def print_summary(df: pl.DataFrame, user_login: str, since: datetime) -> None:
    """Pretty-print *df* to the terminal using rich."""
    if df.is_empty():
        console.print(
            f"[yellow]No activity for {user_login} since {since:%Y-%m-%d}.\n[/yellow]"
        )
        return

    table = Table(title=f"GitHub activity for {user_login} since {since:%Y-%m-%d}")
    table.add_column("Type")
    table.add_column("Repo#")
    table.add_column("Title")
    table.add_column("Involvement")
    table.add_column("Date")
    table.add_column("Link")

    for row in df.iter_rows(named=True):
        repo_num = f"{row['repo']}#{row['number']}"
        date_str = row["date"].strftime("%Y-%m-%d")
        table.add_row(
            row["type"],
            repo_num,
            row["title"],
            row["involvement"],
            date_str,
            row["url"],
        )

    console.print(table)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Summarize a user's recent GitHub engagements"
    )
    parser.add_argument("--user", required=True, help="GitHub username (login)")
    parser.add_argument(
        "--days", type=int, default=7, help="Look back N days (default 7)"
    )
    args = parser.parse_args()

    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    df = collect_user_engagements(args.user, since)
    print_summary(df, args.user, since)

    # Optionally export to CSV
    if not df.is_empty():
        out_path = Path(f"cache/user_activity_{args.user}.csv")
        out_path.parent.mkdir(exist_ok=True)
        df.write_csv(out_path)
        console.print(f"[green]Saved to {out_path}[/green]")


if __name__ == "__main__":
    main()
