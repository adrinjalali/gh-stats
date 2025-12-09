"""Summarize a GitHub user's recent engagements.

Given a GitHub *username* the script lists all issues and pull requests where
that user has been **active** (commented, reviewed, opened, etc.) during the
last *N* days (default: 7).  For each item the script shows:

* the type of object (Issue / PR),
* repository and number,
* title,
* the type of the **latest** involvement within the period,
* a link to that latest involvement.

The implementation uses GitHub's GraphQL API to query:
1. Issue comments by the user
2. Issues created by the user
3. Pull requests created by the user
4. Pull request reviews by the user

Usage
-----
$ python -m github_analytics.user_activity --user StefanieSenger

Requires ``GITHUB_TOKEN`` in the environment (``.env`` supported).
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import polars as pl
import requests
from dotenv import load_dotenv
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

GRAPHQL_URL = "https://api.github.com/graphql"

# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

# Query for issue comments by a user (most recent first)
# Note: issueComments includes comments on PRs too (PRs are issues in GitHub)
# We detect PRs by checking if the URL contains /pull/
ISSUE_COMMENTS_QUERY = """
query($login: String!, $cursor: String) {
  user(login: $login) {
    issueComments(first: 100, after: $cursor, orderBy:
        {field: UPDATED_AT, direction: DESC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        createdAt
        url
        issue {
          number
          title
          url
          state
          repository {
            nameWithOwner
          }
        }
      }
    }
  }
}
"""

# Query for issues created by a user
ISSUES_QUERY = """
query($login: String!, $cursor: String) {
  user(login: $login) {
    issues(first: 100, after: $cursor, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        number
        title
        url
        state
        createdAt
        updatedAt
        repository {
          nameWithOwner
        }
      }
    }
  }
}
"""

# Query for pull requests created by a user
PULL_REQUESTS_QUERY = """
query($login: String!, $cursor: String) {
  user(login: $login) {
    pullRequests(first: 100, after: $cursor, orderBy:
        {field: UPDATED_AT, direction: DESC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        number
        title
        url
        state
        merged
        createdAt
        updatedAt
        repository {
          nameWithOwner
        }
      }
    }
  }
}
"""

# Query for PR reviews by a user (via contributionsCollection)
PR_REVIEWS_QUERY = """
query($login: String!, $from: DateTime!, $to: DateTime!, $cursor: String) {
  user(login: $login) {
    contributionsCollection(from: $from, to: $to) {
      pullRequestReviewContributions(first: 100, after: $cursor) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          occurredAt
          pullRequestReview {
            url
          }
          pullRequest {
            number
            title
            url
            state
            merged
            repository {
              nameWithOwner
            }
          }
        }
      }
    }
  }
}
"""

# Query for commits by a user (via contributionsCollection)
COMMITS_QUERY = """
query($login: String!, $from: DateTime!, $to: DateTime!) {
  user(login: $login) {
    contributionsCollection(from: $from, to: $to) {
      commitContributionsByRepository(maxRepositories: 100) {
        repository {
          nameWithOwner
        }
        contributions(first: 100, orderBy: {field: OCCURRED_AT, direction: DESC}) {
          nodes {
            occurredAt
            commitCount
            repository {
              nameWithOwner
            }
          }
        }
      }
    }
  }
}
"""

# Query for PR comments by a user
PR_COMMENTS_QUERY = """
query($login: String!, $cursor: String) {
  user(login: $login) {
    pullRequests(first: 100, after: $cursor, orderBy:
        {field: UPDATED_AT, direction: DESC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        number
        title
        url
        state
        merged
        updatedAt
        repository {
          nameWithOwner
        }
        comments(first: 100) {
          nodes {
            author { login }
            createdAt
            url
          }
        }
      }
    }
  }
}
"""

# Query for commits on PRs by a user
PR_COMMITS_QUERY = """
query($login: String!, $cursor: String) {
  user(login: $login) {
    pullRequests(first: 100, after: $cursor, orderBy:
        {field: UPDATED_AT, direction: DESC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        number
        title
        url
        state
        merged
        updatedAt
        repository {
          nameWithOwner
        }
        commits(first: 100) {
          nodes {
            commit {
              author {
                user { login }
              }
              committedDate
              url
              message
            }
          }
        }
      }
    }
  }
}
"""


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def get_github_token() -> str:
    """Return the GitHub token from environment."""
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN not set in environment or .env file")
    return token


def graphql_request(query: str, variables: dict[str, Any], token: str) -> dict:
    """Execute a GraphQL request and return the response data."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    response = requests.post(
        GRAPHQL_URL,
        json={"query": query, "variables": variables},
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    result = response.json()
    if "errors" in result:
        raise RuntimeError(f"GraphQL errors: {result['errors']}")
    return result["data"]


def parse_datetime(dt_str: str) -> datetime:
    """Parse ISO datetime string to datetime object."""
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))


def get_status_char(state: str, merged: bool = False) -> str:
    """Return status character based on state."""
    if merged:
        return "✓"
    return "x" if state == "CLOSED" else "○"


# ---------------------------------------------------------------------------
# Data collection functions
# ---------------------------------------------------------------------------


def fetch_issue_comments(
    token: str, login: str, since: datetime
) -> list[dict[str, Any]]:
    """Fetch all issue comments by the user since the given date."""
    results = []
    cursor = None

    while True:
        data = graphql_request(
            ISSUE_COMMENTS_QUERY, {"login": login, "cursor": cursor}, token
        )
        comments = data["user"]["issueComments"]

        for node in comments["nodes"]:
            if not node or not node.get("issue"):
                continue

            created = parse_datetime(node["createdAt"])
            if created < since:
                # Comments are ordered by date desc, so we can stop
                return results

            issue = node["issue"]
            # Check if this is actually a PR by URL pattern (PRs are issues in GitHub)
            is_pr = "/pull/" in issue["url"]
            results.append(
                {
                    "repo": issue["repository"]["nameWithOwner"],
                    "number": issue["number"],
                    "type": "PR" if is_pr else "Issue",
                    "state": issue["state"],
                    "merged": False,  # Can't determine from this query
                    "title": issue["title"],
                    "involvement": "commented",
                    "date": created,
                    "url": node["url"],
                    "item_url": issue["url"],
                }
            )

        if not comments["pageInfo"]["hasNextPage"]:
            break
        cursor = comments["pageInfo"]["endCursor"]

    return results


def fetch_issues(token: str, login: str, since: datetime) -> list[dict[str, Any]]:
    """Fetch all issues created by the user since the given date."""
    results = []
    cursor = None

    while True:
        data = graphql_request(ISSUES_QUERY, {"login": login, "cursor": cursor}, token)
        issues = data["user"]["issues"]

        for node in issues["nodes"]:
            if not node:
                continue

            updated = parse_datetime(node["updatedAt"])
            if updated < since:
                return results

            created = parse_datetime(node["createdAt"])
            results.append(
                {
                    "repo": node["repository"]["nameWithOwner"],
                    "number": node["number"],
                    "type": "Issue",
                    "state": node["state"],
                    "title": node["title"],
                    "involvement": "author",
                    "date": created if created >= since else updated,
                    "url": node["url"],
                    "item_url": node["url"],
                }
            )

        if not issues["pageInfo"]["hasNextPage"]:
            break
        cursor = issues["pageInfo"]["endCursor"]

    return results


def fetch_pull_requests(
    token: str, login: str, since: datetime
) -> list[dict[str, Any]]:
    """Fetch all pull requests created by the user since the given date."""
    results = []
    cursor = None

    while True:
        data = graphql_request(
            PULL_REQUESTS_QUERY, {"login": login, "cursor": cursor}, token
        )
        prs = data["user"]["pullRequests"]

        for node in prs["nodes"]:
            if not node:
                continue

            updated = parse_datetime(node["updatedAt"])
            if updated < since:
                return results

            created = parse_datetime(node["createdAt"])
            results.append(
                {
                    "repo": node["repository"]["nameWithOwner"],
                    "number": node["number"],
                    "type": "PR",
                    "state": node["state"],
                    "merged": node.get("merged", False),
                    "title": node["title"],
                    "involvement": "author",
                    "date": created if created >= since else updated,
                    "url": node["url"],
                    "item_url": node["url"],
                }
            )

        if not prs["pageInfo"]["hasNextPage"]:
            break
        cursor = prs["pageInfo"]["endCursor"]

    return results


def fetch_pr_reviews(token: str, login: str, since: datetime) -> list[dict[str, Any]]:
    """Fetch all PR reviews by the user since the given date."""
    results = []
    cursor = None

    # contributionsCollection requires from/to dates
    from_date = since.isoformat()
    to_date = datetime.now(timezone.utc).isoformat()

    while True:
        data = graphql_request(
            PR_REVIEWS_QUERY,
            {"login": login, "from": from_date, "to": to_date, "cursor": cursor},
            token,
        )
        contributions = data["user"]["contributionsCollection"][
            "pullRequestReviewContributions"
        ]

        for node in contributions["nodes"]:
            if not node or not node.get("pullRequest"):
                continue

            pr = node["pullRequest"]
            occurred = parse_datetime(node["occurredAt"])

            review_url = node.get("pullRequestReview", {}).get("url", pr["url"])

            results.append(
                {
                    "repo": pr["repository"]["nameWithOwner"],
                    "number": pr["number"],
                    "type": "PR",
                    "state": pr["state"],
                    "merged": pr.get("merged", False),
                    "title": pr["title"],
                    "involvement": "reviewed",
                    "date": occurred,
                    "url": review_url,
                    "item_url": pr["url"],
                }
            )

        if not contributions["pageInfo"]["hasNextPage"]:
            break
        cursor = contributions["pageInfo"]["endCursor"]

    return results


def fetch_pr_comments(token: str, login: str, since: datetime) -> list[dict[str, Any]]:
    """Fetch all PR comments by the user since the given date."""
    results = []
    cursor = None
    login_lower = login.lower()

    while True:
        data = graphql_request(
            PR_COMMENTS_QUERY, {"login": login, "cursor": cursor}, token
        )
        prs = data["user"]["pullRequests"]

        for pr_node in prs["nodes"]:
            if not pr_node:
                continue

            updated = parse_datetime(pr_node["updatedAt"])
            if updated < since:
                return results

            # Check comments on this PR
            for comment in pr_node.get("comments", {}).get("nodes", []):
                if not comment:
                    continue
                author = comment.get("author")
                if not author or author.get("login", "").lower() != login_lower:
                    continue

                created = parse_datetime(comment["createdAt"])
                if created < since:
                    continue

                results.append(
                    {
                        "repo": pr_node["repository"]["nameWithOwner"],
                        "number": pr_node["number"],
                        "type": "PR",
                        "state": pr_node["state"],
                        "merged": pr_node.get("merged", False),
                        "title": pr_node["title"],
                        "involvement": "commented",
                        "date": created,
                        "url": comment["url"],
                        "item_url": pr_node["url"],
                    }
                )

        if not prs["pageInfo"]["hasNextPage"]:
            break
        cursor = prs["pageInfo"]["endCursor"]

    return results


def fetch_commits(token: str, login: str, since: datetime) -> list[dict[str, Any]]:
    """Fetch commit contributions by the user since the given date."""
    results = []

    from_date = since.isoformat()
    to_date = datetime.now(timezone.utc).isoformat()

    data = graphql_request(
        COMMITS_QUERY,
        {"login": login, "from": from_date, "to": to_date},
        token,
    )

    repos = data["user"]["contributionsCollection"]["commitContributionsByRepository"]
    for repo_data in repos:
        repo_name = repo_data["repository"]["nameWithOwner"]
        for contrib in repo_data["contributions"]["nodes"]:
            if not contrib:
                continue

            occurred = parse_datetime(contrib["occurredAt"])
            commit_count = contrib.get("commitCount", 1)

            results.append(
                {
                    "repo": repo_name,
                    "number": 0,  # Commits don't have a number
                    "type": "Commit",
                    "state": "OPEN",  # N/A for commits
                    "merged": False,
                    "title": f"{commit_count} commit(s)",
                    "involvement": "committed",
                    "date": occurred,
                    "url": f"https://github.com/{repo_name}/commits?author={login}",
                    "item_url": f"https://github.com/{repo_name}",
                }
            )

    return results


def fetch_pr_commits(token: str, login: str, since: datetime) -> list[dict[str, Any]]:
    """Fetch commits on PRs by the user since the given date."""
    results = []
    cursor = None
    login_lower = login.lower()

    while True:
        data = graphql_request(
            PR_COMMITS_QUERY, {"login": login, "cursor": cursor}, token
        )
        prs = data["user"]["pullRequests"]

        for pr_node in prs["nodes"]:
            if not pr_node:
                continue

            updated = parse_datetime(pr_node["updatedAt"])
            if updated < since:
                return results

            # Check commits on this PR
            for commit_node in pr_node.get("commits", {}).get("nodes", []):
                if not commit_node or not commit_node.get("commit"):
                    continue

                commit = commit_node["commit"]
                author = commit.get("author", {})
                user = author.get("user") if author else None

                if not user or user.get("login", "").lower() != login_lower:
                    continue

                committed = parse_datetime(commit["committedDate"])
                if committed < since:
                    continue

                results.append(
                    {
                        "repo": pr_node["repository"]["nameWithOwner"],
                        "number": pr_node["number"],
                        "type": "PR",
                        "state": pr_node["state"],
                        "merged": pr_node.get("merged", False),
                        "title": pr_node["title"],
                        "involvement": "committed",
                        "date": committed,
                        "url": commit["url"],
                        "item_url": pr_node["url"],
                    }
                )

        if not prs["pageInfo"]["hasNextPage"]:
            break
        cursor = prs["pageInfo"]["endCursor"]

    return results


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def collect_user_engagements(user_login: str, since: datetime) -> pl.DataFrame:
    """Return a DataFrame of the user's engagements since *since* (UTC)."""
    token = get_github_token()

    # Verify authentication
    auth_query = "query { viewer { login } }"
    auth_data = graphql_request(auth_query, {}, token)
    console.print(f"[green]Authenticated as {auth_data['viewer']['login']}[/green]")

    # Collect all activities
    all_activities: list[dict[str, Any]] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Fetching issue comments...", total=None)
        all_activities.extend(fetch_issue_comments(token, user_login, since))

        progress.update(task, description="Fetching issues...")
        all_activities.extend(fetch_issues(token, user_login, since))

        progress.update(task, description="Fetching pull requests...")
        all_activities.extend(fetch_pull_requests(token, user_login, since))

        progress.update(task, description="Fetching PR reviews...")
        all_activities.extend(fetch_pr_reviews(token, user_login, since))

        progress.update(task, description="Fetching PR comments...")
        all_activities.extend(fetch_pr_comments(token, user_login, since))

        progress.update(task, description="Fetching commits...")
        all_activities.extend(fetch_commits(token, user_login, since))

        progress.update(task, description="Fetching PR commits...")
        all_activities.extend(fetch_pr_commits(token, user_login, since))

        progress.update(task, description="Processing results...")

    if not all_activities:
        return pl.DataFrame()

    # Build rows for each activity (no deduplication - show all interactions)
    rows = []
    for activity in all_activities:
        merged = activity.get("merged", False)
        status_char = get_status_char(activity["state"], merged)

        rows.append(
            {
                "repo": activity["repo"],
                "number": activity["number"],
                "type": f"{activity['type']} {status_char}",
                "title": activity["title"],
                "involvement": activity["involvement"],
                "date": activity["date"],
                "url": activity["url"],
            }
        )

    df = pl.from_dicts(rows)
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

    # Sort by repo, then by number, then by date within each item
    df = df.sort(["repo", "number", "date"])

    table = Table(title=f"GitHub activity for {user_login} since {since:%Y-%m-%d}")
    table.add_column("Type")
    table.add_column("Repo#")
    table.add_column("Title")
    table.add_column("Involvement")
    table.add_column("Date")
    table.add_column("Link")

    current_repo = None
    current_item_key = None  # (repo, number) to track repeated items

    for row in df.iter_rows(named=True):
        # Add a separator row when repo changes
        if current_repo is not None and row["repo"] != current_repo:
            table.add_row("", "", "", "", "", "")
        current_repo = row["repo"]

        item_key = (row["repo"], row["number"])
        is_repeat = item_key == current_item_key
        current_item_key = item_key

        # For commits (number=0), just show repo name
        if row["number"] == 0:
            repo_num = row["repo"] if not is_repeat else "↳"
        else:
            repo_num = f"{row['repo']}#{row['number']}" if not is_repeat else "↳"

        title = row["title"] if not is_repeat else "↳"
        date_str = row["date"].strftime("%Y-%m-%d")
        # Shorten URL by removing the common prefix
        short_url = row["url"].replace("https://github.com/", "")

        table.add_row(
            row["type"],
            repo_num,
            title,
            row["involvement"],
            date_str,
            short_url,
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
    parser.add_argument(
        "--output", "-o", type=str, help="Output TSV file path (optional)"
    )
    args = parser.parse_args()

    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    df = collect_user_engagements(args.user, since)
    print_summary(df, args.user, since)

    # Export to TSV if --output specified
    if args.output and not df.is_empty():
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        # Sort by repo, number, date for export
        export_df = df.sort(["repo", "number", "date"])
        export_df.write_csv(out_path, separator="\t")
        console.print(f"[green]Saved to {out_path}[/green]")


if __name__ == "__main__":
    main()
