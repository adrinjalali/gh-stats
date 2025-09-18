"""Find stale pull requests for a GitHub repository.

The script inspects *open* pull requests of a given repository and extracts
activity timestamps in order to highlight PRs whose author has been inactive
while others kept the discussion going (or vice-versa).

Metrics collected per PR
-----------------------
number, url, author,
    author_last  -> most recent activity by the PR author
    others_last  -> most recent activity by anyone else
    last_review  -> most recent review timestamp (if any)
    days_diff    -> (others_last - author_last).days  (positive => others more recent)
    updated_at   -> GitHub's updatedAt field - used for change detection

Key implementation decisions
---------------------------
* **GraphQL API** (single request per 50 PRs) to dramatically cut the number of
  HTTP calls compared to REST-timeline endpoints.  Each page returns the last
  20 reviews, comments and commits for every PR, which is usually enough to
  capture the relevant recent activity for staleness analysis.
* **Incremental CSV cache** under ``cache/stale_prs_<repo>.csv`` - after each
  GraphQL page the file is re-written so the script can be stopped and
  restarted later.  PRs whose ``updated_at`` value hasn't changed are skipped
  (no API cost).
* **Rate-limit guard** - after every GraphQL call we consult the ``rateLimit``
  object and stop early (saving progress) if the remaining points drop below a
  safety threshold (default 100).

Usage
-----
$ python -m github_analytics.stale_prs --repo scikit-learn/scikit-learn

Requires GITHUB_TOKEN in the environment (``.env`` supported).
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import requests
from dotenv import load_dotenv
from github import Github
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
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)
PAGE_SIZE = 50  # PRs per GraphQL query
RECENT_ITEMS = 20  # comments/reviews/commits fetched per PR
RATE_LIMIT_THRESHOLD = 100  # stop if remaining < threshold

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def get_github_client() -> Github:
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN not set in environment or .env file")

    gh = Github(token, per_page=100)
    try:
        console.print(f"[green]Authenticated as {gh.get_user().login}[/green]")
    except Exception as err:
        raise RuntimeError("Invalid GitHub token") from err
    return gh


def split_repo(repo: str) -> tuple[str, str]:
    try:
        owner, name = repo.split("/", 1)
        return owner, name
    except ValueError as err:
        raise ValueError("--repo must be in the form <owner>/<name>") from err


def cache_path(repo: str) -> Path:
    sanitized = repo.replace("/", "_")
    return CACHE_DIR / f"stale_prs_{sanitized}.csv"


def load_cache(path: Path) -> pl.DataFrame:
    if path.exists():
        return pl.read_csv(path, try_parse_dates=True)
    return pl.DataFrame()


def save_cache(df: pl.DataFrame, path: Path) -> None:
    if not df.is_empty():
        df.write_csv(path)


# ---------------------------------------------------------------------------
# GraphQL query helpers
# ---------------------------------------------------------------------------
QUERY_TEMPLATE = """
query ($owner: String!, $name: String!, $pageSize: Int!, $after: String) {
  rateLimit {
    remaining
    resetAt
  }
  repository(owner: $owner, name: $name) {
    pullRequests(states: OPEN, first: $pageSize, after: $after,
                 orderBy: {field: UPDATED_AT, direction: DESC}) {
      nodes {
        number
        url
        updatedAt
        author { login }
        comments(last: %d) {
          nodes { createdAt author { login } }
        }
        reviews(last: %d) {
          nodes { submittedAt author { login } }
        }
        commits(last: %d) {
          nodes {
            commit {
              committedDate
              author { user { login } }
            }
          }
        }
      }
      pageInfo { endCursor hasNextPage }
    }
  }
}
""" % (RECENT_ITEMS, RECENT_ITEMS, RECENT_ITEMS)


def graphql_page(owner: str, name: str, after: str | None) -> dict[str, Any]:
    """Perform a single GraphQL request via raw HTTP.

    We do not rely on :pymeth:`PyGithub.Github.graphql`, which may be missing in
    older PyGithub versions shipped in the pixi environment.
    """
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN not found for GraphQL request")

    variables = {
        "owner": owner,
        "name": name,
        "pageSize": PAGE_SIZE,
        "after": after,
    }
    headers = {"Authorization": f"bearer {token}"}
    response = requests.post(
        "https://api.github.com/graphql",
        json={"query": QUERY_TEMPLATE, "variables": variables},
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    result: dict[str, Any] = response.json()
    if "errors" in result:
        raise RuntimeError(f"GraphQL errors: {result['errors']}")
    return result["data"]


# ---------------------------------------------------------------------------
# Activity extraction logic
# ---------------------------------------------------------------------------


def parse_datetime(ts: str | None) -> datetime | None:
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def collect_activity(
    pr_node: dict[str, Any],
) -> tuple[datetime | None, datetime | None, datetime | None]:
    """Return (author_last, others_last, last_review)."""
    author_login = pr_node.get("author", {}).get("login")

    author_last: datetime | None = None
    others_last: datetime | None = None

    # Comments
    for cm in pr_node["comments"]["nodes"]:
        ts = parse_datetime(cm["createdAt"])
        if not ts:
            continue
        cm_author = cm.get("author", {}) or {}
        cm_login = cm_author.get("login")
        if cm_login == author_login:
            author_last = max(author_last or ts, ts)
        else:
            others_last = max(others_last or ts, ts)

    # Reviews
    last_review: datetime | None = None
    for rv in pr_node["reviews"]["nodes"]:
        ts = parse_datetime(rv["submittedAt"])
        if not ts:
            continue
        last_review = max(last_review or ts, ts)
        rv_author = rv.get("author", {}) or {}
        rv_login = rv_author.get("login")
        if rv_login == author_login:
            author_last = max(author_last or ts, ts)
        else:
            others_last = max(others_last or ts, ts)

    # Commits
    for cm in pr_node["commits"]["nodes"]:
        commit = cm["commit"]
        ts = parse_datetime(commit["committedDate"])
        if not ts:
            continue
        commit_author = (commit.get("author", {}) or {}).get("user", {})
        login = commit_author.get("login") if isinstance(commit_author, dict) else None
        if login == author_login:
            author_last = max(author_last or ts, ts)
        else:
            others_last = max(others_last or ts, ts)

    return author_last, others_last, last_review


# ---------------------------------------------------------------------------
# Main processing routine
# ---------------------------------------------------------------------------


def process_repository(repo: str, out_csv: Path) -> None:
    owner, name = split_repo(repo)

    existing_df = load_cache(out_csv)
    existing_dict = (
        existing_df.select(["number", "updated_at"]).to_dict(as_series=False)
        if not existing_df.is_empty()
        else {"number": [], "updated_at": []}
    )
    cache_updated_at = dict(
        zip(existing_dict.get("number", []), existing_dict.get("updated_at", []))
    )

    new_rows: list[dict[str, Any]] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        page_task = progress.add_task("Querying pull requests...", total=None)
        after: str | None = None
        while True:
            data = graphql_page(owner, name, after)

            rate_remaining = data["rateLimit"]["remaining"]
            if rate_remaining < RATE_LIMIT_THRESHOLD:
                console.print(
                    f"[yellow]Rate-limit low ({rate_remaining}). Saving and exiting "
                    "early.[/yellow]"
                )

            pr_nodes = data["repository"]["pullRequests"]["nodes"]
            if not pr_nodes:
                break

            for pr in pr_nodes:
                number = pr["number"]
                updated_at = pr["updatedAt"]
                # Skip if unchanged since last run
                if cache_updated_at.get(number) == updated_at:
                    continue

                author_last, others_last, last_review = collect_activity(pr)
                row = {
                    "number": number,
                    "url": pr["url"],
                    "author": pr.get("author", {}).get("login"),
                    "author_last": author_last,
                    "others_last": others_last,
                    "last_review": last_review,
                    "days_diff": (
                        (others_last - author_last).days
                        if author_last and others_last
                        else None
                    ),
                    "updated_at": updated_at,
                }
                new_rows.append(row)

            # Merge & save after each page
            if new_rows:
                new_df = pl.DataFrame(new_rows)
                combined = (
                    pl.concat([existing_df, new_df])
                    if not existing_df.is_empty()
                    else new_df
                )
                # keep latest row per PR number (updated_at desc implicit in ordering)
                combined = (
                    combined.sort("updated_at", descending=True)
                    .unique(subset=["number"], keep="first")
                    .sort("number")
                )
                save_cache(combined, out_csv)
                existing_df = combined
                new_rows = []

            page_info = data["repository"]["pullRequests"]["pageInfo"]
            after = page_info["endCursor"] if page_info["hasNextPage"] else None
            if after is None or rate_remaining < RATE_LIMIT_THRESHOLD:
                break

        progress.update(page_task, description="Finished querying pull requests")

    # Pretty print summary table
    if existing_df.is_empty():
        console.print("[yellow]No pull requests processed.[/yellow]")
        return

    table = Table(title=f"Stale Pull Requests - {repo}")
    table.add_column("#", justify="right")
    table.add_column("Author")
    table.add_column("Author last")
    table.add_column("Others last")
    table.add_column("Last review")
    table.add_column("Δ days", justify="right")

    for row in existing_df.sort("days_diff", descending=True).iter_rows(named=True):
        table.add_row(
            str(row["number"]),
            row["author"] or "?",
            row["author_last"].strftime("%Y-%m-%d") if row["author_last"] else "-",
            row["others_last"].strftime("%Y-%m-%d") if row["others_last"] else "-",
            row["last_review"].strftime("%Y-%m-%d") if row["last_review"] else "-",
            str(row["days_diff"]) if row["days_diff"] is not None else "-",
        )

    console.print(table)
    console.print(f"[green]Results saved to {out_csv}[/green]")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Find stale pull requests")
    parser.add_argument("--repo", required=True, help="Repository <owner>/<name>")
    parser.add_argument(
        "--out",
        help="Output CSV path (default: cache/stale_prs_<repo>.csv)",
    )
    args = parser.parse_args()

    out_file = Path(args.out) if args.out else cache_path(args.repo)
    process_repository(args.repo, out_file)


if __name__ == "__main__":
    main()
