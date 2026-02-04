"""Script to build a comprehensive SQLite database of GitHub project data."""

import argparse
import json
import os
import signal
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from requests.exceptions import ChunkedEncodingError, Timeout
from requests.exceptions import ConnectionError as RequestsConnectionError
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from urllib3.util.retry import Retry

# Load environment variables
load_dotenv()

# Initialize console for rich output
console = Console()

# Database path
DB_PATH = Path("project_database.db")

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle interrupt signals gracefully."""
    global shutdown_requested
    console.print(
        "\n[yellow]Interrupt received. Finishing current batch and saving progress..."
        "[/yellow]"
    )
    shutdown_requested = True


# Register signal handler
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class GitHubGraphQLClient:
    """GraphQL client for GitHub API with robust retry logic."""

    def __init__(self, token: str):
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self.endpoint = "https://api.github.com/graphql"

        # Configure robust HTTP session with retries
        self.session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def query(
        self, query: str, variables: dict[str, Any] | None = None, max_retries: int = 3
    ) -> dict[str, Any]:
        """Execute GraphQL query with retry logic."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        for attempt in range(max_retries + 1):
            try:
                response = self.session.post(
                    self.endpoint,
                    headers=self.headers,
                    json=payload,
                    timeout=60,  # Increased timeout
                )

                if response.status_code == 200:
                    result = response.json()
                    if "errors" in result:
                        # Check if it's a rate limit error
                        for error in result["errors"]:
                            if "rate limit" in error.get("message", "").lower():
                                console.print(
                                    "[yellow]Rate limit hit, waiting 60 seconds..."
                                    "[/yellow]"
                                )
                                time.sleep(60)
                                continue
                        console.print(f"[red]GraphQL errors: {result['errors']}[/red]")
                        raise Exception(f"GraphQL errors: {result['errors']}")
                    return result
                else:
                    console.print(
                        f"[red]HTTP error {response.status_code}: {response.text}[/red]"
                    )
                    response.raise_for_status()

            except (ChunkedEncodingError, RequestsConnectionError, Timeout) as e:
                if attempt < max_retries:
                    wait_time = (2**attempt) + 1  # Exponential backoff
                    console.print(
                        "[yellow]Network error (attempt "
                        f"{attempt + 1}/{max_retries + 1}): {e}[/yellow]"
                    )
                    console.print(
                        f"[yellow]Retrying in {wait_time} seconds...[/yellow]"
                    )
                    time.sleep(wait_time)
                else:
                    console.print(
                        f"[red]Failed after {max_retries + 1} attempts: {e}[/red]"
                    )
                    raise
            except Exception as e:
                console.print(f"[red]Unexpected error: {e}[/red]")
                raise


def init_database() -> sqlite3.Connection:
    """Initialize SQLite database with required tables."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create pull_requests table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pull_requests (
            number INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            body TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            closed_at TEXT,
            merged_at TEXT,
            state TEXT NOT NULL,
            draft BOOLEAN,
            author TEXT,
            assignees TEXT,  -- JSON array
            reviewers TEXT,  -- JSON array
            labels TEXT,     -- JSON array
            milestone TEXT,
            additions INTEGER,
            deletions INTEGER,
            changed_files INTEGER,
            url TEXT NOT NULL,
            last_event_at TEXT,
            fetched_at TEXT NOT NULL,
            UNIQUE(number)
        )
    """)

    # Create issues table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS issues (
            number INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            body TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            closed_at TEXT,
            state TEXT NOT NULL,
            author TEXT,
            assignees TEXT,  -- JSON array
            labels TEXT,     -- JSON array
            milestone TEXT,
            comments_count INTEGER,
            url TEXT NOT NULL,
            last_event_at TEXT,
            fetched_at TEXT NOT NULL,
            UNIQUE(number)
        )
    """)

    # Create progress tracking table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sync_progress (
            id INTEGER PRIMARY KEY,
            repo_name TEXT NOT NULL,
            last_pr_number INTEGER DEFAULT 0,
            last_issue_number INTEGER DEFAULT 0,
            total_prs INTEGER DEFAULT 0,
            total_issues INTEGER DEFAULT 0,
            last_pr_cursor TEXT,
            last_issue_cursor TEXT,
            last_sync_at TEXT,
            UNIQUE(repo_name)
        )
    """)

    # Create indexes for better query performance
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_pr_number ON pull_requests(number);
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_pr_state ON pull_requests(state);
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_pr_created ON pull_requests(created_at);
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_issue_number ON issues(number);
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_issue_state ON issues(state);
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_issue_created ON issues(created_at);
    """)

    conn.commit()
    return conn


def get_sync_progress(conn: sqlite3.Connection, repo_name: str) -> dict[str, Any]:
    """Get current sync progress for a repository."""
    cursor = conn.cursor()

    # First check if columns exist (for backward compatibility)
    cursor.execute("PRAGMA table_info(sync_progress)")
    columns = [row[1] for row in cursor.fetchall()]

    if "last_pr_cursor" in columns and "last_issue_cursor" in columns:
        cursor.execute(
            """
            SELECT last_pr_number, last_issue_number, total_prs, total_issues,
                   last_pr_cursor, last_issue_cursor, last_sync_at
            FROM sync_progress WHERE repo_name = ?
        """,
            (repo_name,),
        )
        result = cursor.fetchone()
        if result:
            return {
                "last_pr_number": result[0],
                "last_issue_number": result[1],
                "total_prs": result[2],
                "total_issues": result[3],
                "last_pr_cursor": result[4],
                "last_issue_cursor": result[5],
                "last_sync_at": result[6],
            }
    else:
        # Old schema, migrate it
        cursor.execute(
            """
            SELECT last_pr_number, last_issue_number, total_prs, total_issues,
            last_sync_at
            FROM sync_progress WHERE repo_name = ?
        """,
            (repo_name,),
        )
        result = cursor.fetchone()
        if result:
            # Add missing columns
            cursor.execute("ALTER TABLE sync_progress ADD COLUMN last_pr_cursor TEXT")
            cursor.execute(
                "ALTER TABLE sync_progress ADD COLUMN last_issue_cursor TEXT"
            )
            conn.commit()
            return {
                "last_pr_number": result[0],
                "last_issue_number": result[1],
                "total_prs": result[2],
                "total_issues": result[3],
                "last_pr_cursor": None,
                "last_issue_cursor": None,
                "last_sync_at": result[4],
            }

    # Initialize progress tracking if not exists
    try:
        cursor.execute(
            """
            INSERT INTO sync_progress (repo_name, last_pr_number, last_issue_number,
            total_prs, total_issues, last_pr_cursor, last_issue_cursor)
            VALUES (?, 0, 0, 0, 0, NULL, NULL)
        """,
            (repo_name,),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        # Record already exists, which is fine
        pass

    return {
        "last_pr_number": 0,
        "last_issue_number": 0,
        "total_prs": 0,
        "total_issues": 0,
        "last_pr_cursor": None,
        "last_issue_cursor": None,
        "last_sync_at": None,
    }


def update_sync_progress(conn: sqlite3.Connection, repo_name: str, **kwargs):
    """Update sync progress."""
    cursor = conn.cursor()

    # Build dynamic UPDATE query
    updates = []
    values = []
    for key, value in kwargs.items():
        if key in [
            "last_pr_number",
            "last_issue_number",
            "total_prs",
            "total_issues",
            "last_pr_cursor",
            "last_issue_cursor",
            "last_sync_at",
        ]:
            updates.append(f"{key} = ?")
            values.append(value)

    if updates:
        values.append(repo_name)
        cursor.execute(
            f"""
            UPDATE sync_progress SET {", ".join(updates)} WHERE repo_name = ?
        """,
            values,
        )
        conn.commit()


def get_repo_counts_query() -> str:
    """GraphQL query to get total PR and issue counts."""
    return """
    query($owner: String!, $name: String!) {
        repository(owner: $owner, name: $name) {
            pullRequests {
                totalCount
            }
            issues {
                totalCount
            }
        }
    }
    """


def get_prs_query(since_date: str | None = None) -> str:
    """GraphQL query to fetch PR details.

    Args:
        since_date: ISO format date string to fetch PRs created  after this date
    """
    # Build the filter string if we have a since date
    if since_date:
        # GitHub doesn't support direct date filtering in pullRequests,
        # so we'll use search query instead for better efficiency
        return """
        query($query: String!, $first: Int!, $after: String) {
            search(query: $query, type: ISSUE, first: $first, after: $after) {
                pageInfo {
                    hasNextPage
                    endCursor
                }
                issueCount
                nodes {
                    ... on PullRequest {
                        number
                        title
                        body
                        createdAt
                        updatedAt
                        closedAt
                        mergedAt
                        state
                        isDraft
                        author {
                            login
                        }
                        assignees(first: 10) {
                            nodes {
                                login
                            }
                        }
                        reviewRequests(first: 10) {
                            nodes {
                                requestedReviewer {
                                    ... on User {
                                        login
                                    }
                                }
                            }
                        }
                        labels(first: 20) {
                            nodes {
                                name
                            }
                        }
                        milestone {
                            title
                        }
                        additions
                        deletions
                        changedFiles
                        url
                        timelineItems(last: 1) {
                            nodes {
                                ... on IssueComment {
                                    createdAt
                                }
                                ... on PullRequestCommit {
                                    commit {
                                        authoredDate
                                    }
                                }
                                ... on PullRequestReview {
                                    createdAt
                                }
                                ... on ClosedEvent {
                                    createdAt
                                }
                                ... on MergedEvent {
                                    createdAt
                                }
                            }
                        }
                    }
                }
            }
        }
        """

    # Original query for full fetch
    return """
    query($owner: String!, $name: String!, $first: Int!, $after: String) {
        repository(owner: $owner, name: $name) {
            pullRequests(first: $first, after: $after, orderBy:
            {field: CREATED_AT, direction: ASC}) {
                pageInfo {
                    hasNextPage
                    endCursor
                }
                nodes {
                    number
                    title
                    body
                    createdAt
                    updatedAt
                    closedAt
                    mergedAt
                    state
                    isDraft
                    author {
                        login
                    }
                    assignees(first: 10) {
                        nodes {
                            login
                        }
                    }
                    reviewRequests(first: 10) {
                        nodes {
                            requestedReviewer {
                                ... on User {
                                    login
                                }
                            }
                        }
                    }
                    labels(first: 20) {
                        nodes {
                            name
                        }
                    }
                    milestone {
                        title
                    }
                    additions
                    deletions
                    changedFiles
                    url
                    timelineItems(last: 1) {
                        nodes {
                            ... on IssueComment {
                                createdAt
                            }
                            ... on PullRequestCommit {
                                commit {
                                    authoredDate
                                }
                            }
                            ... on PullRequestReview {
                                createdAt
                            }
                            ... on ClosedEvent {
                                createdAt
                            }
                            ... on MergedEvent {
                                createdAt
                            }
                        }
                    }
                }
            }
        }
    }
    """


def get_issues_query(since_date: str | None = None) -> str:
    """GraphQL query to fetch issue details.

    Args:
        since_date: ISO format date string to fetch issues created after this date
    """
    if since_date:
        # Use search API for efficient date filtering
        return """
        query($query: String!, $first: Int!, $after: String) {
            search(query: $query, type: ISSUE, first: $first, after: $after) {
                pageInfo {
                    hasNextPage
                    endCursor
                }
                issueCount
                nodes {
                    ... on Issue {
                        number
                        title
                        body
                        createdAt
                        updatedAt
                        closedAt
                        state
                        author {
                            login
                        }
                        assignees(first: 10) {
                            nodes {
                                login
                            }
                        }
                        labels(first: 20) {
                            nodes {
                                name
                            }
                        }
                        milestone {
                            title
                        }
                        comments {
                            totalCount
                        }
                        url
                        timelineItems(last: 1) {
                            nodes {
                                ... on IssueComment {
                                    createdAt
                                }
                                ... on ClosedEvent {
                                    createdAt
                                }
                                ... on ReopenedEvent {
                                    createdAt
                                }
                                ... on LabeledEvent {
                                    createdAt
                                }
                            }
                        }
                    }
                }
            }
        }
        """

    # Original query for full fetch
    return """
    query($owner: String!, $name: String!, $first: Int!, $after: String) {
        repository(owner: $owner, name: $name) {
            issues(first: $first, after: $after, orderBy:
            {field: CREATED_AT, direction: ASC}) {
                pageInfo {
                    hasNextPage
                    endCursor
                }
                nodes {
                    number
                    title
                    body
                    createdAt
                    updatedAt
                    closedAt
                    state
                    author {
                        login
                    }
                    assignees(first: 10) {
                        nodes {
                            login
                        }
                    }
                    labels(first: 20) {
                        nodes {
                            name
                        }
                    }
                    milestone {
                        title
                    }
                    comments {
                        totalCount
                    }
                    url
                    timelineItems(last: 1) {
                        nodes {
                            ... on IssueComment {
                                createdAt
                            }
                            ... on ClosedEvent {
                                createdAt
                            }
                            ... on ReopenedEvent {
                                createdAt
                            }
                            ... on LabeledEvent {
                                createdAt
                            }
                        }
                    }
                }
            }
        }
    }
    """


def extract_last_event_time(timeline_items: list[dict]) -> str | None:
    """Extract the most recent event time from timeline items."""
    if not timeline_items:
        return None

    latest_time = None
    for item in timeline_items:
        event_time = None
        if "createdAt" in item:
            event_time = item["createdAt"]
        elif "commit" in item and "authoredDate" in item["commit"]:
            event_time = item["commit"]["authoredDate"]

        if event_time and (not latest_time or event_time > latest_time):
            latest_time = event_time

    return latest_time


def save_prs_to_db(conn: sqlite3.Connection, prs: list[dict]):
    """Save PRs to database."""
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    for pr in prs:
        assignees = json.dumps(
            [a["login"] for a in pr.get("assignees", {}).get("nodes", [])]
        )
        reviewers = json.dumps(
            [
                r["requestedReviewer"]["login"]
                for r in pr.get("reviewRequests", {}).get("nodes", [])
                if r.get("requestedReviewer") and "login" in r["requestedReviewer"]
            ]
        )
        labels = json.dumps(
            [label["name"] for label in pr.get("labels", {}).get("nodes", [])]
        )

        last_event_at = extract_last_event_time(
            pr.get("timelineItems", {}).get("nodes", [])
        )

        cursor.execute(
            """
            INSERT OR REPLACE INTO pull_requests (
                number, title, body, created_at, updated_at, closed_at, merged_at,
                state, draft, author, assignees, reviewers, labels, milestone,
                additions, deletions, changed_files, url, last_event_at, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                pr["number"],
                pr["title"],
                pr.get("body"),
                pr["createdAt"],
                pr["updatedAt"],
                pr.get("closedAt"),
                pr.get("mergedAt"),
                pr["state"],
                pr.get("isDraft", False),
                pr.get("author", {}).get("login") if pr.get("author") else None,
                assignees,
                reviewers,
                labels,
                pr.get("milestone", {}).get("title") if pr.get("milestone") else None,
                pr.get("additions", 0),
                pr.get("deletions", 0),
                pr.get("changedFiles", 0),
                pr["url"],
                last_event_at,
                now,
            ),
        )

    conn.commit()


def save_issues_to_db(conn: sqlite3.Connection, issues: list[dict]):
    """Save issues to database."""
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    for issue in issues:
        assignees = json.dumps(
            [a["login"] for a in issue.get("assignees", {}).get("nodes", [])]
        )
        labels = json.dumps(
            [label["name"] for label in issue.get("labels", {}).get("nodes", [])]
        )

        last_event_at = extract_last_event_time(
            issue.get("timelineItems", {}).get("nodes", [])
        )

        cursor.execute(
            """
            INSERT OR REPLACE INTO issues (
                number, title, body, created_at, updated_at, closed_at, state,
                author, assignees, labels, milestone, comments_count, url,
                last_event_at, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                issue["number"],
                issue["title"],
                issue.get("body"),
                issue["createdAt"],
                issue["updatedAt"],
                issue.get("closedAt"),
                issue["state"],
                issue.get("author", {}).get("login") if issue.get("author") else None,
                assignees,
                labels,
                issue.get("milestone", {}).get("title")
                if issue.get("milestone")
                else None,
                issue.get("comments", {}).get("totalCount", 0),
                issue["url"],
                last_event_at,
                now,
            ),
        )

    conn.commit()


def fetch_all_prs(
    client: GitHubGraphQLClient, owner: str, repo: str, conn: sqlite3.Connection
):
    """Fetch all PRs using GraphQL pagination with error recovery."""
    batch_size = 25  # Reduced batch size for more reliable requests

    progress = get_sync_progress(conn, f"{owner}/{repo}")
    total_count = progress["total_prs"]
    last_pr_number = progress["last_pr_number"]
    cursor = progress.get("last_pr_cursor")  # Resume from saved cursor

    # Count how many PRs we already have
    cursor_db = conn.cursor()
    cursor_db.execute("SELECT COUNT(*) FROM pull_requests")
    already_processed = cursor_db.fetchone()[0]

    # Get the most recent PR creation date from our database
    cursor_db.execute("""
        SELECT MAX(created_at) FROM pull_requests
    """)
    result = cursor_db.fetchone()
    most_recent_date = result[0] if result and result[0] else None

    remaining_count = max(0, total_count - already_processed)

    # Decide whether to use incremental fetch or full fetch
    use_incremental = (
        most_recent_date and not cursor and already_processed > 100
    )  # Only use incremental if we have substantial data

    if use_incremental:
        console.print(
            f"[blue]Found {already_processed} existing PRs, using incremental fetch "
            "for newer PRs[/blue]"
        )
        console.print(f"[dim]Fetching PRs created after {most_recent_date}[/dim]")
        query = get_prs_query(since_date=most_recent_date)
    elif cursor and last_pr_number > 0 and already_processed > 0:
        console.print(
            f"[blue]Resuming from PR #{last_pr_number}, {remaining_count} PRs "
            "remaining[/blue]"
        )
        console.print(
            "[dim]Using saved pagination cursor to continue from last position[/dim]"
        )
        query = get_prs_query()
    elif already_processed > 0:
        console.print(
            f"[blue]Found {already_processed} existing PRs, fetching remaining "
            f"{remaining_count}[/blue]"
        )
        query = get_prs_query()
    else:
        console.print(f"[blue]Starting fresh fetch of {total_count} PRs[/blue]")
        query = get_prs_query()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress_bar:
        task = progress_bar.add_task(
            "Fetching PRs...", total=remaining_count, completed=0
        )

        while True:
            if use_incremental:
                # Build search query for incremental fetch
                search_query = f"repo:{owner}/{repo} is:pr created:>{most_recent_date}"
                variables = {
                    "query": search_query,
                    "first": batch_size,
                    "after": cursor,
                }
            else:
                variables = {
                    "owner": owner,
                    "name": repo,
                    "first": batch_size,
                    "after": cursor,
                }

            try:
                result = client.query(query, variables)
                if use_incremental:
                    prs_data = result["data"]["search"]
                    # Filter to only get PullRequests (search can return mixed types)
                    prs_data["nodes"] = [
                        node for node in prs_data.get("nodes", []) if node
                    ]
                else:
                    prs_data = result["data"]["repository"]["pullRequests"]

                if not prs_data["nodes"]:
                    console.print(
                        "[green]No more PRs to fetch - all caught up![/green]"
                    )
                    break

                # Check for shutdown request
                if shutdown_requested:
                    console.print("[yellow]Stopping PR fetch as requested...[/yellow]")
                    break

                # Check which PRs are new (not in database)
                pr_numbers = [pr["number"] for pr in prs_data["nodes"]]
                cursor_db = conn.cursor()
                placeholders = ",".join(["?"] * len(pr_numbers))
                cursor_db.execute(
                    "SELECT number FROM pull_requests WHERE number IN "
                    f"({placeholders})",
                    pr_numbers,
                )
                existing_prs = {row[0] for row in cursor_db.fetchall()}

                new_prs = [
                    pr for pr in prs_data["nodes"] if pr["number"] not in existing_prs
                ]

                if new_prs:
                    # Save only new PRs
                    save_prs_to_db(conn, new_prs)
                    batch_prs = len(new_prs)
                else:
                    batch_prs = 0

                # Update progress with current cursor for resumption
                last_pr = max(pr["number"] for pr in prs_data["nodes"])
                next_cursor = prs_data["pageInfo"]["endCursor"]

                update_sync_progress(
                    conn,
                    f"{owner}/{repo}",
                    last_pr_number=last_pr,
                    last_pr_cursor=next_cursor,
                )

                if batch_prs > 0:
                    progress_bar.update(
                        task,
                        advance=batch_prs,
                        description=(
                            f"Fetching PRs (last: #{last_pr}, new: {batch_prs})"
                        ),
                    )
                else:
                    progress_bar.update(
                        task,
                        description=f"Skipping already fetched PRs (last: #{last_pr})",
                    )

                # Check if we have more pages
                if not prs_data["pageInfo"]["hasNextPage"]:
                    break

                cursor = prs_data["pageInfo"]["endCursor"]

                # Rate limiting
                time.sleep(0.2)  # Slightly longer delay

            except Exception as e:
                console.print(
                    f"[red]Error fetching PR batch (cursor: {cursor}): {e}[/red]"
                )
                console.print("[yellow]Waiting 10 seconds before retrying...[/yellow]")
                time.sleep(10)
                # Continue with same cursor to retry


def fetch_all_issues(
    client: GitHubGraphQLClient, owner: str, repo: str, conn: sqlite3.Connection
):
    """Fetch all issues using GraphQL pagination with error recovery."""
    batch_size = 25  # Reduced batch size for more reliable requests

    progress = get_sync_progress(conn, f"{owner}/{repo}")
    total_count = progress["total_issues"]
    last_issue_number = progress["last_issue_number"]
    cursor = progress.get("last_issue_cursor")  # Resume from saved cursor

    # Count how many issues we already have (excluding PRs)
    cursor_db = conn.cursor()
    cursor_db.execute(
        "SELECT COUNT(*) FROM issues WHERE number NOT IN (SELECT number FROM "
        "pull_requests)"
    )
    already_processed = cursor_db.fetchone()[0]

    # Get the most recent issue creation date from our database (excluding PRs)
    cursor_db.execute("""
        SELECT MAX(created_at) FROM issues
        WHERE number NOT IN (SELECT number FROM pull_requests)
    """)
    result = cursor_db.fetchone()
    most_recent_date = result[0] if result and result[0] else None

    remaining_count = max(0, total_count - already_processed)

    # Decide whether to use incremental fetch or full fetch
    use_incremental = (
        most_recent_date and not cursor and already_processed > 100
    )  # Only use incremental if we have substantial data

    if use_incremental:
        console.print(
            f"[blue]Found {already_processed} existing issues, using incremental fetch "
            "for newer issues[/blue]"
        )
        console.print(f"[dim]Fetching issues created after {most_recent_date}[/dim]")
        query = get_issues_query(since_date=most_recent_date)
    elif cursor and last_issue_number > 0 and already_processed > 0:
        console.print(
            f"[blue]Resuming from Issue #{last_issue_number}, {remaining_count} issues "
            "remaining[/blue]"
        )
        console.print(
            "[dim]Using saved pagination cursor to continue from last position[/dim]"
        )
        query = get_issues_query()
    elif already_processed > 0:
        console.print(
            f"[blue]Found {already_processed} existing issues, fetching remaining "
            f"{remaining_count}[/blue]"
        )
        query = get_issues_query()
    else:
        console.print(f"[blue]Starting fresh fetch of {total_count} issues[/blue]")
        query = get_issues_query()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress_bar:
        task = progress_bar.add_task(
            "Fetching Issues...", total=remaining_count, completed=0
        )

        while True:
            if use_incremental:
                # Build search query for incremental fetch (exclude PRs)
                search_query = (
                    f"repo:{owner}/{repo} is:issue created:>{most_recent_date}"
                )
                variables = {
                    "query": search_query,
                    "first": batch_size,
                    "after": cursor,
                }
            else:
                variables = {
                    "owner": owner,
                    "name": repo,
                    "first": batch_size,
                    "after": cursor,
                }

            try:
                result = client.query(query, variables)
                if use_incremental:
                    issues_data = result["data"]["search"]
                    # Filter to only get Issues (search can return mixed types)
                    issues_data["nodes"] = [
                        node for node in issues_data.get("nodes", []) if node
                    ]
                else:
                    issues_data = result["data"]["repository"]["issues"]

                if not issues_data["nodes"]:
                    console.print(
                        "[green]No more issues to fetch - all caught up![/green]"
                    )
                    break

                # Check for shutdown request
                if shutdown_requested:
                    console.print(
                        "[yellow]Stopping issue fetch as requested...[/yellow]"
                    )
                    break

                # Check which issues are new (not in database)
                issue_numbers = [issue["number"] for issue in issues_data["nodes"]]
                cursor_db = conn.cursor()
                placeholders = ",".join(["?"] * len(issue_numbers))
                cursor_db.execute(
                    f"SELECT number FROM issues WHERE number IN ({placeholders})",
                    issue_numbers,
                )
                existing_issues = {row[0] for row in cursor_db.fetchall()}

                new_issues = [
                    issue
                    for issue in issues_data["nodes"]
                    if issue["number"] not in existing_issues
                ]

                if new_issues:
                    # Save only new issues
                    save_issues_to_db(conn, new_issues)
                    batch_issues = len(new_issues)
                else:
                    batch_issues = 0

                # Update progress with current cursor for resumption
                last_issue = max(issue["number"] for issue in issues_data["nodes"])
                next_cursor = issues_data["pageInfo"]["endCursor"]

                update_sync_progress(
                    conn,
                    f"{owner}/{repo}",
                    last_issue_number=last_issue,
                    last_issue_cursor=next_cursor,
                )

                if batch_issues > 0:
                    progress_bar.update(
                        task,
                        advance=batch_issues,
                        description=f"Fetching Issues (last: #{last_issue}, new: "
                        f"{batch_issues})",
                    )
                else:
                    progress_bar.update(
                        task,
                        description=(
                            f"Skipping already fetched issues (last: #{last_issue})"
                        ),
                    )

                # Check if we have more pages
                if not issues_data["pageInfo"]["hasNextPage"]:
                    break

                cursor = issues_data["pageInfo"]["endCursor"]

                # Rate limiting
                time.sleep(0.2)  # Slightly longer delay

            except Exception as e:
                console.print(
                    f"[red]Error fetching issue batch (cursor: {cursor}): {e}[/red]"
                )
                console.print("[yellow]Waiting 10 seconds before retrying...[/yellow]")
                time.sleep(10)
                # Continue with same cursor to retry


def reset_database(conn: sqlite3.Connection, repo_name: str):
    """Reset the database for a fresh start."""
    cursor = conn.cursor()

    console.print(f"[yellow]Resetting database for {repo_name}...[/yellow]")

    # Clear all data
    cursor.execute("DELETE FROM pull_requests")
    cursor.execute("DELETE FROM issues")
    cursor.execute("DELETE FROM sync_progress WHERE repo_name = ?", (repo_name,))

    conn.commit()
    console.print("[green]Database reset complete![/green]")


def main():
    """Main function to build the project database."""
    parser = argparse.ArgumentParser(
        description="Build a comprehensive SQLite database of GitHub project data."
    )
    parser.add_argument(
        "--repo",
        default="scikit-learn/scikit-learn",
        help="Repository in format owner/repo (default: scikit-learn/scikit-learn)",
    )
    parser.add_argument(
        "--reset", action="store_true", help="Reset the database and start fresh"
    )
    parser.add_argument(
        "--skip-prs", action="store_true", help="Skip fetching pull requests"
    )
    parser.add_argument(
        "--skip-issues", action="store_true", help="Skip fetching issues"
    )

    args = parser.parse_args()

    repo_name = args.repo
    owner, repo = repo_name.split("/")

    console.print(f"[bold]Building database for {repo_name}[/bold]")

    # Get GitHub token
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        console.print(
            "[red]GitHub token not found. Please set GITHUB_TOKEN environment "
            "variable.[/red]"
        )
        return

    try:
        # Initialize database
        conn = init_database()
        console.print(f"[green]Database initialized at {DB_PATH}[/green]")

        # Initialize GraphQL client
        client = GitHubGraphQLClient(token)

        # Get repository totals first
        counts_query = get_repo_counts_query()
        result = client.query(counts_query, {"owner": owner, "name": repo})
        repo_data = result["data"]["repository"]

        total_prs = repo_data["pullRequests"]["totalCount"]
        total_issues = repo_data["issues"]["totalCount"]

        console.print(
            f"[blue]Repository has {total_prs} PRs and {total_issues} issues[/blue]"
        )

        # Handle reset if requested
        if args.reset:
            reset_database(conn, repo_name)

        # Update totals in progress tracking
        update_sync_progress(
            conn, repo_name, total_prs=total_prs, total_issues=total_issues
        )

        # Fetch all PRs
        if not args.skip_prs and not shutdown_requested:
            console.print("[yellow]Fetching pull requests...[/yellow]")
            fetch_all_prs(client, owner, repo, conn)
        elif args.skip_prs:
            console.print("[dim]Skipping pull requests as requested[/dim]")

        # Fetch all issues
        if not args.skip_issues and not shutdown_requested:
            console.print("[yellow]Fetching issues...[/yellow]")
            fetch_all_issues(client, owner, repo, conn)
        elif args.skip_issues:
            console.print("[dim]Skipping issues as requested[/dim]")

        # Update final sync time
        update_sync_progress(
            conn, repo_name, last_sync_at=datetime.now(timezone.utc).isoformat()
        )

        if shutdown_requested:
            console.print(
                "[yellow]Database sync interrupted but progress saved! Updated "
                "{DB_PATH}[/yellow]"
            )
            console.print(
                "[yellow]Run the script again to resume from where you left off."
                "[/yellow]"
            )
        else:
            console.print(f"[green]Database sync complete! Updated {DB_PATH}[/green]")

        # Show resumption status
        final_progress = get_sync_progress(conn, repo_name)
        console.print(
            f"[blue]Final state: PR #{final_progress['last_pr_number']}, Issue "
            f"#{final_progress['last_issue_number']}[/blue]"
        )
        console.print(f"[blue]Last sync: {final_progress['last_sync_at']}[/blue]")

        # Show some stats
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM pull_requests")
        pr_count = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COUNT(*) FROM issues WHERE number NOT IN (SELECT number FROM "
            "pull_requests)"
        )
        issue_count = cursor.fetchone()[0]

        console.print(f"[green]Stored {pr_count} PRs and {issue_count} issues[/green]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise
    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    main()
