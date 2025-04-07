"""Script to fetch closed issues and merged pull requests from GitHub repositories."""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import polars as pl
from dotenv import load_dotenv
from github import Github
from github.GithubException import GithubException
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

# Load environment variables
load_dotenv()

# Initialize console for rich output
console = Console()

# Setup cache directory
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)
RAW_CACHE_DIR = CACHE_DIR / "raw"
RAW_CACHE_DIR.mkdir(exist_ok=True)


def get_week_bounds() -> tuple[datetime, datetime]:
    """Get the start and end dates for current and last week."""
    now = datetime.now(timezone.utc)
    current_week_start = now - timedelta(days=now.weekday())
    current_week_start = current_week_start.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    last_week_start = current_week_start - timedelta(weeks=1)
    return last_week_start, now


def get_cache_path(repo_name: str, data_type: str) -> Path:
    """Get the cache file path for a specific repository and data type."""
    return CACHE_DIR / f"{repo_name.replace('/', '_')}_{data_type}.parquet"


def get_raw_cache_path(repo_name: str, data_type: str, item_id: int) -> Path:
    """Get the raw cache file path for a specific item."""
    return RAW_CACHE_DIR / f"{repo_name.replace('/', '_')}_{data_type}_{item_id}.json"


def save_raw_item(item: Any, repo_name: str, data_type: str, item_id: int) -> None:
    """Save raw item data to JSON file."""
    cache_path = get_raw_cache_path(repo_name, data_type, item_id)
    # Convert the GitHub object to a dictionary
    data = dict(item.raw_data)
    with open(cache_path, "w") as f:
        json.dump(data, f)


def load_cached_data(cache_path: Path) -> Optional[pl.DataFrame]:
    """Load cached data if it exists."""
    if cache_path.exists():
        return pl.read_parquet(cache_path)
    return None


def save_to_cache(df: pl.DataFrame, cache_path: Path) -> None:
    """Save data to cache."""
    df.write_parquet(cache_path)


def get_github_client() -> Github:
    """Initialize and return GitHub client using token from environment."""
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        msg = "GitHub token not found. Please set GITHUB_TOKEN environment variable."
        raise ValueError(msg)

    # Test the token by making a simple API call
    client = Github(token)
    try:
        # Just get the authenticated user to test the token
        user = client.get_user().login
        console.print(f"[green]Authenticated as {user}[/green]")
        return client
    except GithubException as e:
        if e.status == 401:
            raise ValueError("Invalid GitHub token. Please check your token.") from e
        raise


def fetch_issues_and_prs(repo_name: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Fetch closed issues and merged PRs from specified repository.

    Uses caching to reduce API calls.

    Args:
        repo_name: Full repository name (e.g., 'scikit-learn/scikit-learn')

    Returns:
        Tuple of (issues_df, prs_df) as Polars DataFrames
    """
    issues_cache = get_cache_path(repo_name, "issues")
    prs_cache = get_cache_path(repo_name, "prs")

    # Get time bounds
    start_date, end_date = get_week_bounds()
    console.print(f"Fetching data from {start_date} to {end_date}")

    # Load cached data
    cached_issues = load_cached_data(issues_cache)
    cached_prs = load_cached_data(prs_cache)

    g = get_github_client()
    try:
        repo = g.get_repo(repo_name)
    except GithubException as e:
        if e.status == 404:
            raise ValueError(f"Repository {repo_name} not found") from e
        raise

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        # Fetch new issues
        new_issues = []
        issue_task = progress.add_task("Fetching closed issues...", total=None)
        try:
            for issue in repo.get_issues(state="closed", since=start_date):
                if not issue.pull_request:  # Skip PRs
                    # Save raw data immediately
                    save_raw_item(issue, repo_name, "issue", issue.number)
                    new_issues.append(
                        {
                            "number": issue.number,
                            "title": issue.title,
                            "created_at": issue.created_at,
                            "closed_at": issue.closed_at,
                            "author": issue.user.login if issue.user else None,
                            "labels": json.dumps(
                                [label.name for label in issue.labels]
                            ),
                            "url": issue.html_url,
                        }
                    )
                    progress.update(
                        issue_task,
                        description=f"Fetched issue #{issue.number}",
                    )

                    # Save to DataFrame periodically (every 10 items)
                    if len(new_issues) % 10 == 0:
                        temp_df = pl.DataFrame(new_issues)
                        if not temp_df.is_empty():
                            save_to_cache(temp_df, issues_cache)

        except GithubException as e:
            console.print(f"[yellow]Warning: Error fetching issues: {e}[/yellow]")
        finally:
            progress.update(issue_task, description="Finished fetching issues")

        # Fetch new PRs
        new_prs = []
        pr_task = progress.add_task("Fetching merged pull requests...", total=None)
        try:
            for pr in repo.get_pulls(
                state="closed",
                sort="updated",
                direction="desc",
            ):
                # Only process PRs that were merged in our time window
                if not (pr.merged and pr.merged_at and pr.merged_at >= start_date):
                    continue

                # Save raw data immediately
                save_raw_item(pr, repo_name, "pr", pr.number)
                new_prs.append(
                    {
                        "number": pr.number,
                        "title": pr.title,
                        "created_at": pr.created_at,
                        "merged_at": pr.merged_at,
                        "author": pr.user.login if pr.user else None,
                        "labels": json.dumps([label.name for label in pr.labels]),
                        "url": pr.html_url,
                        "additions": pr.additions,
                        "deletions": pr.deletions,
                    }
                )
                progress.update(
                    pr_task,
                    description=f"Fetched PR #{pr.number}",
                )

                # Save to DataFrame periodically (every 10 items)
                if len(new_prs) % 10 == 0:
                    temp_df = pl.DataFrame(new_prs)
                    if not temp_df.is_empty():
                        save_to_cache(temp_df, prs_cache)

                # If this PR was merged before our start date, we can stop
                # as PRs are sorted by updated date descending
                if pr.merged_at < start_date:
                    break

        except GithubException as e:
            console.print(
                f"[yellow]Warning: Error fetching pull requests: {e}[/yellow]"
            )
        finally:
            progress.update(pr_task, description="Finished fetching pull requests")

    # Convert to Polars DataFrames
    issues_df = pl.DataFrame(new_issues) if new_issues else pl.DataFrame()
    prs_df = pl.DataFrame(new_prs) if new_prs else pl.DataFrame()

    # Merge with cache if exists
    if cached_issues is not None and not issues_df.is_empty():
        issues_df = pl.concat([cached_issues, issues_df]).unique(subset=["number"])

    if cached_prs is not None and not prs_df.is_empty():
        prs_df = pl.concat([cached_prs, prs_df]).unique(subset=["number"])

    # Save final data to cache
    if not issues_df.is_empty():
        save_to_cache(issues_df, issues_cache)
    if not prs_df.is_empty():
        save_to_cache(prs_df, prs_cache)

    return issues_df, prs_df


def filter_by_date_range(
    df: pl.DataFrame,
    start_date: datetime,
    end_date: datetime,
    date_column: str,
) -> pl.DataFrame:
    """Filter DataFrame by date range."""
    if df.is_empty():
        return df
    return df.filter(
        (pl.col(date_column) >= start_date) & (pl.col(date_column) <= end_date)
    )


def display_summary(
    issues_df: pl.DataFrame,
    prs_df: pl.DataFrame,
    start_date: datetime,
    end_date: datetime,
) -> None:
    """Display summary of issues and PRs using rich tables."""
    # Filter data for the specified time period
    issues_df = filter_by_date_range(issues_df, start_date, end_date, "closed_at")
    prs_df = filter_by_date_range(prs_df, start_date, end_date, "merged_at")

    # Issues table
    date_range = f"{start_date:%Y-%m-%d} to {end_date:%Y-%m-%d}"
    issue_table = Table(title=f"Closed Issues ({date_range})")
    issue_table.add_column("Number", justify="right")
    issue_table.add_column("Title")
    issue_table.add_column("Author")
    issue_table.add_column("Closed At")

    if not issues_df.is_empty():
        for row in issues_df.iter_rows(named=True):
            issue_table.add_row(
                f"{row['number']!s}",  # Explicit string conversion
                row["title"],
                row["author"] or "Unknown",
                row["closed_at"].strftime("%Y-%m-%d") if row["closed_at"] else "N/A",
            )

    # PRs table
    pr_table = Table(title=f"Merged Pull Requests ({date_range})")
    pr_table.add_column("Number", justify="right")
    pr_table.add_column("Title")
    pr_table.add_column("Author")
    pr_table.add_column("Merged At")
    pr_table.add_column("Changes (+/-)")

    if not prs_df.is_empty():
        for row in prs_df.iter_rows(named=True):
            pr_table.add_row(
                f"{row['number']!s}",  # Explicit string conversion
                row["title"],
                row["author"] or "Unknown",
                row["merged_at"].strftime("%Y-%m-%d") if row["merged_at"] else "N/A",
                f"+{row['additions']}/-{row['deletions']}",
            )

    console.print(issue_table)
    console.print("\n")
    console.print(pr_table)


def main() -> None:
    repo_name = "scikit-learn/scikit-learn"
    console.print(f"[bold]Fetching data from {repo_name}...[/bold]")

    try:
        # Get time bounds
        start_date, end_date = get_week_bounds()

        # Fetch all data (will use cache if available)
        issues_df, prs_df = fetch_issues_and_prs(repo_name)

        # Display summaries for last week and current week
        console.print("\n[bold]Last Week's Activity[/bold]")
        display_summary(issues_df, prs_df, start_date, start_date + timedelta(days=7))

        console.print("\n[bold]Current Week's Activity[/bold]")
        display_summary(
            issues_df,
            prs_df,
            start_date + timedelta(days=7),
            end_date,
        )

        # Save to parquet files in the current directory
        if not issues_df.is_empty():
            issues_df.write_parquet("issues.parquet")
        if not prs_df.is_empty():
            prs_df.write_parquet("pull_requests.parquet")
        msg = "Data has been saved to issues.parquet and pull_requests.parquet"
        console.print(f"\n[green]{msg}[/green]")

    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        if isinstance(e, GithubException):
            console.print(
                f"[red]GitHub API Error: Status={e.status}, Data={e.data}[/red]"
            )


if __name__ == "__main__":
    main()
