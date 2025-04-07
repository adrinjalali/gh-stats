"""Script to quickly get PR and issue statistics from GitHub repositories."""

import argparse
import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from github import Github
from rich.console import Console
from rich.table import Table

# Load environment variables
load_dotenv()

# Initialize console for rich output
console = Console()

# List of repositories to analyze
REPOS = [
    "scikit-learn/scikit-learn",
    "joblib/joblib",
    "fairlearn/fairlearn",
    "skops-dev/skops",
    "cloudpipe/cloudpickle",
    "skrub-data/skrub",
    "soda-inria/hazardous",
    "joblib/threadpoolctl",
    "joblib/loky",
    "scikit-learn-contrib/imbalanced-learn",
]


def get_date_bounds(start_date: str | None = None) -> tuple[datetime, datetime]:
    """Get the start and end dates for the analysis period.

    Args:
        start_date: Optional start date in YYYY-MM-DD format.
                   If not provided, defaults to two weeks ago.
    """
    now = datetime.now(timezone.utc)

    if start_date:
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            start = start.replace(tzinfo=timezone.utc)
        except ValueError as e:
            raise ValueError("Start date must be in YYYY-MM-DD format") from e
    else:
        start = now - timedelta(days=14)
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)

    return start, now


def get_github_client() -> Github:
    """Initialize and return GitHub client using token from environment."""
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        msg = "GitHub token not found. Please set GITHUB_TOKEN environment variable."
        raise ValueError(msg)

    client = Github(token)
    try:
        user = client.get_user().login
        console.print(f"[green]Authenticated as {user}[/green]")
        return client
    except Exception as e:
        raise ValueError("Invalid GitHub token. Please check your token.") from e


def get_repo_stats(g: Github, repo_name: str, start_date: datetime) -> dict:
    """Get PR and issue statistics for a repository."""
    stats = {
        "name": repo_name,
        "created_prs": 0,
        "closed_prs": 0,
        "updated_prs": 0,
        "created_issues": 0,
        "closed_issues": 0,
        "updated_issues": 0,
    }

    # Format date for GitHub search
    date_str = start_date.strftime("%Y-%m-%d")

    try:
        # Get PR stats
        created_prs = g.search_issues(f"repo:{repo_name} is:pr created:>={date_str}")
        closed_prs = g.search_issues(f"repo:{repo_name} is:pr closed:>={date_str}")
        updated_prs = g.search_issues(f"repo:{repo_name} is:pr updated:>={date_str}")
        stats["created_prs"] = created_prs.totalCount
        stats["closed_prs"] = closed_prs.totalCount
        stats["updated_prs"] = updated_prs.totalCount

        # Get issue stats (excluding PRs)
        created_issues = g.search_issues(
            f"repo:{repo_name} is:issue -is:pr created:>={date_str}"
        )
        closed_issues = g.search_issues(
            f"repo:{repo_name} is:issue -is:pr closed:>={date_str}"
        )
        updated_issues = g.search_issues(
            f"repo:{repo_name} is:issue -is:pr updated:>={date_str}"
        )
        stats["created_issues"] = created_issues.totalCount
        stats["closed_issues"] = closed_issues.totalCount
        stats["updated_issues"] = updated_issues.totalCount

    except Exception as e:
        console.print(
            f"[yellow]Warning: Error fetching stats for {repo_name}: {e}[/yellow]"
        )

    return stats


def display_stats(stats_list: list[dict]) -> None:
    """Display repository statistics in a table."""
    table = Table(
        title="Repository Activity (Past 2 Weeks)",
        show_lines=True,
    )

    # Add columns
    table.add_column("Repository", style="cyan")
    table.add_column("PRs (Created/Updated/Closed)", justify="right")
    table.add_column("Issues (Created/Updated/Closed)", justify="right")

    # Add rows
    for stats in stats_list:
        table.add_row(
            stats["name"],
            f"{stats['created_prs']}/{stats['updated_prs']}/{stats['closed_prs']}",
            f"{stats['created_issues']}/{stats['updated_issues']}/{stats['closed_issues']}",
        )

    console.print(table)


def main() -> None:
    """Main function."""
    parser = argparse.ArgumentParser(description="Fetch GitHub repository statistics")
    parser.add_argument(
        "--start",
        type=str,
        help="Start date in YYYY-MM-DD format (default: 2 weeks ago)",
    )

    args = parser.parse_args()

    try:
        g = get_github_client()
        start_date, end_date = get_date_bounds(args.start)

        console.print(
            f"[bold]Fetching stats from {start_date:%Y-%m-%d} "
            f"to {end_date:%Y-%m-%d}...[/bold]"
        )

        # Get stats for all repositories
        stats_list = []
        for repo in REPOS:
            console.print(f"Fetching stats for {repo}...")
            stats = get_repo_stats(g, repo, start_date)
            stats_list.append(stats)

        # Display results
        display_stats(stats_list)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")


if __name__ == "__main__":
    main()
