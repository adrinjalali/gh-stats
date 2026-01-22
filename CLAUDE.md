# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GitHub Analytics Tools - a collection of Python scripts for analyzing GitHub repository data (issues, PRs, user activity) with a focus on scikit-learn ecosystem projects.

## Commands

All commands use pixi as the package/environment manager:

```bash
# Install dependencies
pixi install

# Run scripts
pixi run start           # Fetch closed issues and merged PRs (fetch_data.py)
pixi run stats           # Quick PR/issue statistics across multiple repos (quick_stats.py)
pixi run releases        # Get last PyPI releases for tracked projects (pypi_last_releases.py)
pixi run user-activity   # Summarize GitHub user activity (user_activity.py)
pixi run weekly-pr-stats # Generate weekly PR statistics plot (weekly_pr_stats.py)
pixi run build-database  # Build comprehensive SQLite database (build_project_database.py)

# Linting
pixi run lint            # Check code with ruff
pixi run lint-fix        # Auto-fix linting issues
pixi run format          # Format code with ruff
```

## Architecture

### Data Sources
- **GitHub REST API**: Used via PyGithub for basic queries (`fetch_data.py`, `quick_stats.py`, `weekly_pr_stats.py`)
- **GitHub GraphQL API**: Used directly via requests for complex queries requiring pagination and nested data (`build_project_database.py`, `user_activity.py`, `stale_prs.py`)

### Data Storage
- **Parquet files**: Primary cache format for intermediate data (`cache/` directory)
- **SQLite database**: `project_database.db` for comprehensive PR/issue storage with resumable sync
- **CSV files**: For certain caches like stale PR data

### Key Patterns
- All scripts use `python-dotenv` to load `GITHUB_TOKEN` from `.env`
- Rich library used throughout for terminal output (tables, progress bars)
- Polars (not Pandas) is the primary DataFrame library
- Scripts support graceful interruption (SIGINT/SIGTERM) with progress saving
- GraphQL queries include rate-limit checking and automatic retry logic

### Script Purposes
- `fetch_data.py`: Weekly issue/PR data for scikit-learn
- `quick_stats.py`: Activity stats across multiple repos (scikit-learn, joblib, fairlearn, etc.)
- `build_project_database.py`: Full historical database with resumable sync
- `user_activity.py`: Track individual user's GitHub activity with optional HTML visualization
- `weekly_pr_stats.py`: 10-year weekly PR trend visualization
- `stale_prs.py`: Find PRs where author/reviewer activity diverges
- `pypi_last_releases.py`: Track PyPI release dates

## Configuration

- `ruff.toml`: Linting rules - line length 88, Python 3.9+ target
- `.pre-commit-config.yaml`: ruff hooks for linting and formatting
- `pixi.toml`: Dependencies and task definitions
