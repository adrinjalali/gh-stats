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
        labels(first: 10) {
          nodes {
            name
          }
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
        labels(first: 10) {
          nodes {
            name
          }
        }
        reviewDecision
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
            state
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
            labels(first: 10) {
              nodes {
                name
              }
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
# Reduced page sizes to avoid GitHub API timeouts
PR_COMMITS_QUERY = """
query($login: String!, $cursor: String) {
  user(login: $login) {
    pullRequests(first: 25, after: $cursor, orderBy:
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
        commits(first: 50) {
          nodes {
            commit {
              author {
                user { login }
              }
              committedDate
              url
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


def graphql_request(
    query: str,
    variables: dict[str, Any],
    token: str,
    max_retries: int = 3,
) -> dict:
    """Execute a GraphQL request with retry logic for transient errors."""
    import time

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    last_error = None
    for attempt in range(max_retries):
        try:
            response = requests.post(
                GRAPHQL_URL,
                json={"query": query, "variables": variables},
                headers=headers,
                timeout=60,  # Increased timeout
            )
            response.raise_for_status()
            result = response.json()
            if "errors" in result:
                raise RuntimeError(f"GraphQL errors: {result['errors']}")
            return result["data"]
        except requests.exceptions.HTTPError as e:
            last_error = e
            # Retry on 5xx errors (server-side issues)
            if response.status_code >= 500:
                wait_time = 2**attempt  # Exponential backoff: 1, 2, 4 seconds
                console.print(
                    f"[yellow]Server error {response.status_code}, "
                    f"retrying in {wait_time}s...[/yellow]"
                )
                time.sleep(wait_time)
                continue
            raise
        except requests.exceptions.Timeout as e:
            last_error = e
            wait_time = 2**attempt
            console.print(
                f"[yellow]Request timeout, retrying in {wait_time}s...[/yellow]"
            )
            time.sleep(wait_time)
            continue

    raise last_error  # type: ignore[misc]


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
                    "labels": [],
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
            labels = [lbl["name"] for lbl in node.get("labels", {}).get("nodes", [])]
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
                    "labels": labels,
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
            labels = [lbl["name"] for lbl in node.get("labels", {}).get("nodes", [])]
            review_decision = node.get("reviewDecision", "")
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
                    "labels": labels,
                    "review_decision": review_decision,
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
            review_state = node.get("pullRequestReview", {}).get("state", "")
            labels = [lbl["name"] for lbl in pr.get("labels", {}).get("nodes", [])]

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
                    "labels": labels,
                    "review_state": review_state,
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
                        "labels": [],
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
                    "labels": [],
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
                        "labels": [],
                    }
                )

        if not prs["pageInfo"]["hasNextPage"]:
            break
        cursor = prs["pageInfo"]["endCursor"]

    return results


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def collect_single_user_activities(
    token: str, user_login: str, since: datetime, progress: Progress, task_id: int
) -> list[dict[str, Any]]:
    """Collect all activities for a single user."""
    all_activities: list[dict[str, Any]] = []

    progress.update(task_id, description=f"[{user_login}] Fetching issue comments...")
    all_activities.extend(fetch_issue_comments(token, user_login, since))

    progress.update(task_id, description=f"[{user_login}] Fetching issues...")
    all_activities.extend(fetch_issues(token, user_login, since))

    progress.update(task_id, description=f"[{user_login}] Fetching pull requests...")
    all_activities.extend(fetch_pull_requests(token, user_login, since))

    progress.update(task_id, description=f"[{user_login}] Fetching PR reviews...")
    all_activities.extend(fetch_pr_reviews(token, user_login, since))

    progress.update(task_id, description=f"[{user_login}] Fetching PR comments...")
    all_activities.extend(fetch_pr_comments(token, user_login, since))

    progress.update(task_id, description=f"[{user_login}] Fetching commits...")
    all_activities.extend(fetch_commits(token, user_login, since))

    progress.update(task_id, description=f"[{user_login}] Fetching PR commits...")
    all_activities.extend(fetch_pr_commits(token, user_login, since))

    # Add user field to all activities
    for activity in all_activities:
        activity["user"] = user_login

    return all_activities


def collect_user_engagements(user_logins: list[str], since: datetime) -> pl.DataFrame:
    """Return a DataFrame of engagements for one or more users since *since* (UTC)."""
    token = get_github_token()

    # Verify authentication
    auth_query = "query { viewer { login } }"
    auth_data = graphql_request(auth_query, {}, token)
    console.print(f"[green]Authenticated as {auth_data['viewer']['login']}[/green]")

    # Collect all activities for all users
    all_activities: list[dict[str, Any]] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        for user_login in user_logins:
            task = progress.add_task(f"[{user_login}] Starting...", total=None)
            user_activities = collect_single_user_activities(
                token, user_login, since, progress, task
            )
            all_activities.extend(user_activities)
            progress.update(task, description=f"[{user_login}] Done!")

    if not all_activities:
        return pl.DataFrame()

    # Build rows for each activity (no deduplication - show all interactions)
    rows = []
    for activity in all_activities:
        merged = activity.get("merged", False)
        status_char = get_status_char(activity["state"], merged)

        rows.append(
            {
                "user": activity["user"],
                "repo": activity["repo"],
                "number": activity["number"],
                "type": f"{activity['type']} {status_char}",
                "title": activity["title"],
                "involvement": activity["involvement"],
                "date": activity["date"],
                "url": activity["url"],
                "labels": ",".join(activity.get("labels", [])),
                "review_state": activity.get("review_state", ""),
                "review_decision": activity.get("review_decision", ""),
            }
        )

    df = pl.from_dicts(rows)
    df = df.sort("date", descending=True)
    return df


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def print_summary(df: pl.DataFrame, user_logins: list[str], since: datetime) -> None:
    """Pretty-print *df* to the terminal using rich."""
    if df.is_empty():
        users_str = ", ".join(user_logins)
        console.print(
            f"[yellow]No activity for {users_str} since {since:%Y-%m-%d}.\n[/yellow]"
        )
        return

    # Sort by user, repo, number, date
    df = df.sort(["user", "repo", "number", "date"])

    users_str = ", ".join(user_logins)
    table = Table(title=f"GitHub activity for {users_str} since {since:%Y-%m-%d}")
    if len(user_logins) > 1:
        table.add_column("User")
    table.add_column("Type")
    table.add_column("Repo#")
    table.add_column("Title")
    table.add_column("Involvement")
    table.add_column("Date")
    table.add_column("Link")

    current_repo = None
    current_item_key = None  # (user, repo, number) to track repeated items

    for row in df.iter_rows(named=True):
        # Add a separator row when repo changes
        if current_repo is not None and row["repo"] != current_repo:
            if len(user_logins) > 1:
                table.add_row("", "", "", "", "", "", "")
            else:
                table.add_row("", "", "", "", "", "")
        current_repo = row["repo"]

        item_key = (row["user"], row["repo"], row["number"])
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

        if len(user_logins) > 1:
            table.add_row(
                row["user"],
                row["type"],
                repo_num,
                title,
                row["involvement"],
                date_str,
                short_url,
            )
        else:
            table.add_row(
                row["type"],
                repo_num,
                title,
                row["involvement"],
                date_str,
                short_url,
            )

    console.print(table)


def generate_html_report(
    df: pl.DataFrame, user_logins: list[str], since: datetime, output_path: Path
) -> None:
    """Generate an interactive HTML visualization of user activity."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        console.print(
            "[red]Plotly is required for HTML output. "
            "Install with: pixi add plotly[/red]"
        )
        return

    if df.is_empty():
        console.print("[yellow]No data to generate HTML report.[/yellow]")
        return

    # Convert to pandas for plotly
    pdf = df.to_pandas()

    # Extract hour from datetime for color coding (time of day)
    pdf["hour"] = pdf["date"].dt.hour
    pdf["date_only"] = pdf["date"].dt.date

    # Create a y-axis label combining repo and number
    pdf["item"] = pdf.apply(
        lambda r: f"{r['repo']}#{r['number']}" if r["number"] != 0 else r["repo"],
        axis=1,
    )

    # Color mapping for involvement types
    involvement_colors = {
        "author": "#2ecc71",  # Green
        "commented": "#3498db",  # Blue
        "reviewed": "#9b59b6",  # Purple
        "committed": "#e67e22",  # Orange
    }

    # Create the main figure
    fig = go.Figure()

    # Add traces for each involvement type
    for involvement in pdf["involvement"].unique():
        mask = pdf["involvement"] == involvement
        subset = pdf[mask]

        # Time of day affects opacity (darker = later)
        opacities = 0.4 + (subset["hour"] / 24) * 0.6

        hover_text = subset.apply(
            lambda r: (
                f"<b>{r['title']}</b><br>"
                f"User: {r['user']}<br>"
                f"Type: {r['type']}<br>"
                f"Involvement: {r['involvement']}<br>"
                f"Date: {r['date'].strftime('%Y-%m-%d %H:%M')}<br>"
                f"Labels: {r['labels'] if r['labels'] else 'None'}<br>"
                f"<a href='{r['url']}'>Open in GitHub</a>"
            ),
            axis=1,
        )

        fig.add_trace(
            go.Scatter(
                x=subset["date"],
                y=subset["item"],
                mode="markers",
                name=involvement,
                marker={
                    "size": 12,
                    "color": involvement_colors.get(involvement, "#95a5a6"),
                    "opacity": opacities.tolist(),
                    "line": {"width": 1, "color": "white"},
                },
                text=hover_text,
                hoverinfo="text",
                customdata=subset["url"].tolist(),
            )
        )

    # Update layout
    users_str = ", ".join(user_logins)
    title_text = (
        f"GitHub Activity Timeline: {users_str}<br><sub>Since {since:%Y-%m-%d}</sub>"
    )
    fig.update_layout(
        title={
            "text": title_text,
            "x": 0.5,
        },
        xaxis={
            "title": "Date",
            "type": "date",
            "rangeslider": {"visible": True},
            "rangeselector": {
                "buttons": [
                    {
                        "count": 7,
                        "label": "1w",
                        "step": "day",
                        "stepmode": "backward",
                    },
                    {
                        "count": 14,
                        "label": "2w",
                        "step": "day",
                        "stepmode": "backward",
                    },
                    {
                        "count": 1,
                        "label": "1m",
                        "step": "month",
                        "stepmode": "backward",
                    },
                    {"step": "all", "label": "All"},
                ]
            },
        },
        yaxis={
            "title": "Repository / Issue / PR",
            "categoryorder": "category ascending",
        },
        legend={
            "title": "Activity Type",
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "right",
            "x": 1,
        },
        hovermode="closest",
        height=max(600, len(pdf["item"].unique()) * 25),
    )

    # Add JavaScript for click-to-open functionality
    fig.update_traces(
        marker={"symbol": "circle"},
    )

    # Generate HTML with custom JavaScript for clicking
    html_content = fig.to_html(
        include_plotlyjs=True,
        full_html=True,
        config={
            "displayModeBar": True,
            "scrollZoom": True,
        },
    )

    # Add custom CSS and JavaScript for better interactivity
    custom_head = """
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI',
                         Roboto, sans-serif;
        }
        .filter-container {
            padding: 15px;
            background: #f5f5f5;
            border-radius: 8px;
            margin: 10px;
        }
        .filter-container label { margin-right: 15px; cursor: pointer; }
        .filter-container input[type="checkbox"] { margin-right: 5px; }
        h2 { text-align: center; color: #333; }
    </style>
    """

    # Insert filters before the plot
    user_checkboxes = "".join(
        f'<label><input type="checkbox" class="user-filter" '
        f'value="{u}" checked> {u}</label>'
        for u in user_logins
    )
    activity_checkboxes = "".join(
        f'<label><input type="checkbox" class="activity-filter" '
        f'value="{a}" checked> {a}</label>'
        for a in pdf["involvement"].unique()
    )
    filter_html = f"""
    <div class="filter-container">
        <strong>Filter by User:</strong>
        {user_checkboxes}
        &nbsp;&nbsp;|&nbsp;&nbsp;
        <strong>Filter by Activity:</strong>
        {activity_checkboxes}
    </div>
    <script>
        document.addEventListener('DOMContentLoaded', function() {{
            var plot = document.querySelector('.plotly-graph-div');
            if (plot) {{
                plot.on('plotly_click', function(data) {{
                    var url = data.points[0].customdata;
                    if (url) window.open(url, '_blank');
                }});
            }}
        }});
    </script>
    """

    # Insert custom content
    html_content = html_content.replace("</head>", custom_head + "</head>")
    html_content = html_content.replace(
        '<div class="plotly-graph-div"', filter_html + '<div class="plotly-graph-div"'
    )

    # Write the file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html_content)
    console.print(f"[green]HTML report saved to {output_path}[/green]")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Summarize GitHub user activity with optional HTML visualization"
    )
    parser.add_argument(
        "--user",
        required=True,
        help="GitHub username(s), comma-separated (e.g., 'alice,bob,charlie')",
    )
    parser.add_argument(
        "--days", type=int, default=7, help="Look back N days (default 7)"
    )
    parser.add_argument(
        "--output", "-o", type=str, help="Output TSV file path (optional)"
    )
    parser.add_argument("--html", type=str, help="Output HTML report path (optional)")
    parser.add_argument(
        "--no-table",
        action="store_true",
        help="Skip printing the table to console",
    )
    args = parser.parse_args()

    # Parse comma-separated users
    user_logins = [u.strip() for u in args.user.split(",") if u.strip()]

    if not user_logins:
        console.print("[red]No valid usernames provided.[/red]")
        return

    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    df = collect_user_engagements(user_logins, since)

    # Print table unless --no-table is specified
    if not args.no_table:
        print_summary(df, user_logins, since)

    # Export to TSV if --output specified
    if args.output and not df.is_empty():
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        # Sort by user, repo, number, date for export
        export_df = df.sort(["user", "repo", "number", "date"])
        export_df.write_csv(out_path, separator="\t")
        console.print(f"[green]TSV saved to {out_path}[/green]")

    # Generate HTML report if --html specified
    if args.html and not df.is_empty():
        html_path = Path(args.html)
        generate_html_report(df, user_logins, since, html_path)


if __name__ == "__main__":
    main()
