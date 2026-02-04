"""
Script to generate weekly PR statistics plot for scikit-learn over the past 10 years.
"""

import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl
from dotenv import load_dotenv
from github import Github
from github.GithubException import GithubException, RateLimitExceededException
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

# Load environment variables
load_dotenv()

# Initialize console for rich output
console = Console()

# Setup cache directory
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)
WEEKLY_CACHE_DIR = CACHE_DIR / "weekly_stats"
WEEKLY_CACHE_DIR.mkdir(exist_ok=True)


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

        # Check rate limit
        rate_limit = client.get_rate_limit()
        console.print(
            f"[blue]Rate limit remaining: {rate_limit.core.remaining}/"
            f"{rate_limit.core.limit}[/blue]"
        )
        return client
    except GithubException as e:
        if e.status == 401:
            raise ValueError("Invalid GitHub token. Please check your token.") from e
        raise


def handle_rate_limit(g: Github) -> None:
    """Check rate limit and sleep if necessary."""
    rate_limit = g.get_rate_limit()
    if rate_limit.core.remaining < 10:
        reset_time = rate_limit.core.reset
        sleep_time = (reset_time - datetime.now(timezone.utc)).total_seconds() + 60
        console.print(
            f"[yellow]Rate limit low ({rate_limit.core.remaining} remaining). "
            f"Sleeping for {sleep_time / 60:.1f} minutes...[/yellow]"
        )
        time.sleep(sleep_time)


def get_weekly_cache_path(repo_name: str, year: int) -> Path:
    """Get cache file path for weekly stats for a specific year."""
    repo_safe = repo_name.replace("/", "_")
    return WEEKLY_CACHE_DIR / f"{repo_safe}_weekly_{year}.parquet"


def get_last_10_years() -> list[int]:
    """Get list of years for the past 10 years."""
    current_year = datetime.now().year
    return list(range(current_year - 9, current_year + 1))


def get_week_start(date: datetime) -> datetime:
    """Get the start of the week (Monday) for a given date."""
    days_since_monday = date.weekday()
    week_start = date - timedelta(days=days_since_monday)
    return week_start.replace(hour=0, minute=0, second=0, microsecond=0)


def fetch_prs_for_year(g: Github, repo_name: str, year: int) -> pl.DataFrame:
    """Fetch all PRs for a specific year, using cache if available."""
    cache_path = get_weekly_cache_path(repo_name, year)

    # Check if we have cached data for this year
    if cache_path.exists():
        console.print(f"[green]Using cached data for {year}[/green]")
        return pl.read_parquet(cache_path)

    console.print(f"[yellow]Fetching PR data for {year}...[/yellow]")

    # Validate repository exists
    try:
        g.get_repo(repo_name)
    except GithubException as e:
        if e.status == 404:
            raise ValueError(f"Repository {repo_name} not found") from e
        raise

    prs_data = []

    # We'll use search API for efficiency - it's more rate-limit friendly for large
    # queries
    # Search for PRs created in this year
    try:
        created_query = f"repo:{repo_name} is:pr created:{year}-01-01..{year}-12-31"
        created_prs = g.search_issues(created_query)

        console.print(
            f"[blue]Found {created_prs.totalCount} PRs created in {year}[/blue]"
        )

        for pr in created_prs:
            # Check rate limit periodically
            if len(prs_data) % 50 == 0:
                handle_rate_limit(g)

            week_start = get_week_start(pr.created_at)

            pr_data = {
                "number": pr.number,
                "created_at": pr.created_at,
                "week_start": week_start,
                "state": pr.state,
                "merged": getattr(pr, "merged", False),
                "closed_at": pr.closed_at,
                "event_type": "opened",
            }
            prs_data.append(pr_data)

            # If the PR was closed in the same year, add a closed event
            if pr.closed_at and pr.closed_at.year == year:
                closed_week_start = get_week_start(pr.closed_at)
                closed_pr_data = pr_data.copy()
                closed_pr_data.update(
                    {"week_start": closed_week_start, "event_type": "closed"}
                )
                prs_data.append(closed_pr_data)

        # Also search for PRs closed in this year (but created earlier)
        closed_query = f"repo:{repo_name} is:pr closed:{year}-01-01..{year}-12-31"
        closed_prs = g.search_issues(closed_query)

        for pr in closed_prs:
            # Skip if we already processed this PR (created in same year)
            if pr.created_at.year == year:
                continue

            # Check rate limit periodically
            if len(prs_data) % 50 == 0:
                handle_rate_limit(g)

            week_start = get_week_start(pr.closed_at)

            pr_data = {
                "number": pr.number,
                "created_at": pr.created_at,
                "week_start": week_start,
                "state": pr.state,
                "merged": getattr(pr, "merged", False),
                "closed_at": pr.closed_at,
                "event_type": "closed",
            }
            prs_data.append(pr_data)

    except RateLimitExceededException:
        console.print("[red]Rate limit exceeded. Please try again later.[/red]")
        raise
    except GithubException as e:
        console.print(f"[red]GitHub API error: {e}[/red]")
        raise

    # Convert to DataFrame
    if prs_data:
        df = pl.DataFrame(prs_data)
        # Save to cache
        df.write_parquet(cache_path)
        console.print(f"[green]Cached {len(prs_data)} PR events for {year}[/green]")
        return df
    else:
        # Create empty DataFrame with correct schema
        empty_df = pl.DataFrame(
            {
                "number": [],
                "created_at": [],
                "week_start": [],
                "state": [],
                "merged": [],
                "closed_at": [],
                "event_type": [],
            }
        )
        empty_df.write_parquet(cache_path)
        return empty_df


def aggregate_weekly_stats(df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate PR events by week, creating a complete weekly time series."""
    if df.is_empty():
        return pl.DataFrame({"week_start": [], "opened": [], "closed": []})

    # Group by week and event type
    weekly_stats = (
        df.group_by(["week_start", "event_type"])
        .agg(pl.count("number").alias("count"))
        .pivot(index="week_start", on="event_type", values="count")
        .fill_null(0)
    )

    # Ensure we have both opened and closed columns
    if "opened" not in weekly_stats.columns:
        weekly_stats = weekly_stats.with_columns(pl.lit(0).alias("opened"))
    if "closed" not in weekly_stats.columns:
        weekly_stats = weekly_stats.with_columns(pl.lit(0).alias("closed"))

    partial_stats = weekly_stats.select(["week_start", "opened", "closed"]).sort(
        "week_start"
    )

    # Create complete weekly time series from first to last week
    if partial_stats.is_empty():
        return partial_stats

    first_week = partial_stats["week_start"].min()
    last_week = partial_stats["week_start"].max()

    # Generate all weeks in the range
    all_weeks = []
    current_week = first_week
    while current_week <= last_week:
        all_weeks.append(current_week)
        current_week = current_week + timedelta(weeks=1)

    # Create complete time series DataFrame
    complete_weeks_df = pl.DataFrame(
        {"week_start": all_weeks, "opened": 0, "closed": 0}
    )

    # Merge with actual data, keeping zeros for missing weeks
    result = (
        complete_weeks_df.join(
            partial_stats, on="week_start", how="left", coalesce=True
        )
        .with_columns(
            [
                pl.col("opened_right").fill_null(0).alias("opened"),
                pl.col("closed_right").fill_null(0).alias("closed"),
            ]
        )
        .select(["week_start", "opened", "closed"])
        .sort("week_start")
    )

    return result


def create_weekly_plot(weekly_df: pl.DataFrame, repo_name: str) -> None:
    """Create and save the weekly PR statistics plot."""
    if weekly_df.is_empty():
        console.print("[yellow]No data to plot[/yellow]")
        return

    # Extract data directly from polars DataFrame
    week_starts = weekly_df["week_start"].to_list()
    opened_counts = weekly_df["opened"].to_list()
    closed_counts = weekly_df["closed"].to_list()

    # Set up the plot style
    plt.style.use("seaborn-v0_8")
    _fig, ax = plt.subplots(figsize=(16, 10))

    # Create the plot
    ax.plot(
        week_starts,
        opened_counts,
        label="Opened PRs",
        linewidth=1.5,
        alpha=0.8,
        color="green",
    )
    ax.plot(
        week_starts,
        closed_counts,
        label="Closed PRs",
        linewidth=1.5,
        alpha=0.8,
        color="red",
    )

    # Customize the plot
    ax.set_title(
        f"Weekly PR Statistics for {repo_name}\n(Past 10 Years)",
        fontsize=16,
        fontweight="bold",
        pad=20,
    )
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Number of PRs", fontsize=12)
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3)

    # Format x-axis
    ax.tick_params(axis="x", rotation=45)

    # Add some statistics as text using polars aggregations
    total_opened = weekly_df["opened"].sum()
    total_closed = weekly_df["closed"].sum()
    avg_opened_per_week = weekly_df["opened"].mean()
    avg_closed_per_week = weekly_df["closed"].mean()

    stats_text = f"""Total PRs Opened: {total_opened:,}
Total PRs Closed: {total_closed:,}
Avg Opened/Week: {avg_opened_per_week:.1f}
Avg Closed/Week: {avg_closed_per_week:.1f}"""

    ax.text(
        0.02,
        0.98,
        stats_text,
        transform=ax.transAxes,
        fontsize=10,
        verticalalignment="top",
        bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
    )

    plt.tight_layout()

    # Save the plot
    output_path = f"{repo_name.replace('/', '_')}_weekly_pr_stats.png"
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    console.print(f"[green]Plot saved as {output_path}[/green]")

    # Also save as PDF
    pdf_path = f"{repo_name.replace('/', '_')}_weekly_pr_stats.pdf"
    plt.savefig(pdf_path, bbox_inches="tight")
    console.print(f"[green]Plot also saved as {pdf_path}[/green]")

    plt.show()


def main():
    """Main function to generate weekly PR statistics."""
    repo_name = "scikit-learn/scikit-learn"
    console.print(
        f"[bold]Generating weekly PR statistics for {repo_name} (past 10 years)[/bold]"
    )

    try:
        g = get_github_client()
        years = get_last_10_years()

        all_data = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            year_task = progress.add_task("Processing years...", total=len(years))

            for year in years:
                progress.update(year_task, description=f"Processing year {year}")

                # Fetch data for this year
                year_df = fetch_prs_for_year(g, repo_name, year)
                if not year_df.is_empty():
                    all_data.append(year_df)

                progress.advance(year_task)

        # Combine all data
        if all_data:
            combined_df = pl.concat(all_data)
            console.print(
                f"[green]Total PR events collected: {len(combined_df)}[/green]"
            )

            # Aggregate by week
            weekly_stats = aggregate_weekly_stats(combined_df)
            console.print(f"[green]Weeks with data: {len(weekly_stats)}[/green]")

            # Save the aggregated data
            output_file = f"{repo_name.replace('/', '_')}_weekly_stats.parquet"
            weekly_stats.write_parquet(output_file)
            console.print(f"[green]Weekly statistics saved to {output_file}[/green]")

            # Create the plot
            create_weekly_plot(weekly_stats, repo_name)

        else:
            console.print("[yellow]No data collected[/yellow]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise


if __name__ == "__main__":
    main()
