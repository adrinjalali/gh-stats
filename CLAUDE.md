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
pixi run board-activity  # Compare project board assignments with user activity (board_activity.py)

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
- `board_activity.py`: Compare GitHub project board assignments (champion/reviewer) with actual user activity

## Configuration

- `ruff.toml`: Linting rules - line length 88, Python 3.9+ target
- `.pre-commit-config.yaml`: ruff hooks for linting and formatting
- `pixi.toml`: Dependencies and task definitions

## Claude Tasks

### Weekly Board Summary

Generate an interactive HTML report of the probabl-ai project board activity.

**To run:** Ask Claude: "Run the board summary" or "board analysis for [users]"

**Scripts location:** `src/github_analytics/board_summary/`

**Claude Workflow (IMPORTANT - follow these steps exactly):**

When asked to run the board summary/analysis, Claude MUST execute these steps in order:

### Step 1: Fetch board data and user activity
```bash
pixi run python src/github_analytics/board_summary/run_board_summary.py <users> --lookback <days>
```
Example: `pixi run python src/github_analytics/board_summary/run_board_summary.py ogrisel --lookback 14`

### Step 2: Generate AI summaries for items missing them
```bash
# Get list of items needing summaries
pixi run python src/github_analytics/board_summary/generate_summaries.py --list --max 50
```

For each item returned, Claude generates a summary object with:
- `summary`: 1-2 sentence description of current state
- `ai_status`: One of: Ready to merge | Needs minor work | In progress | Blocked | Needs review | Stale | Needs discussion
- `action_items`: List of specific next steps
- `action_required_by`: GitHub handles of people who need to act
- `action_reason`: Brief explanation of why those people need to act

Then import the generated summaries:
```bash
echo '<JSON summaries>' | pixi run python src/github_analytics/board_summary/generate_summaries.py --import
```

### Step 3: Regenerate the report with new summaries
```bash
pixi run python src/github_analytics/board_summary/run_board_summary.py <users> --skip-fetch
```

### Step 4: Report location
The report is saved to `board_summary.html` in the project root.

---

**Activity items**: Items where users authored, reviewed, or commented but aren't assigned on the board are also shown. These appear with:
- Gray "involved" badges showing interaction type (A=authored, R=reviewed, C=commented)
- "Not Included" in the Board column (styled with dashed border)

**Summary format** (`summaries.json`):
```json
{
  "scikit-learn#12345": {
    "summary": "1-2 sentence description of current state",
    "ai_status": "Ready to merge | Needs minor work | In progress | Blocked | Needs review | Stale | Needs discussion",
    "action_items": ["List of specific next steps"],
    "action_required_by": ["github_handles", "who_need_to_act"],
    "action_reason": "Brief explanation of why those people need to act"
  }
}
```

**AI status guidelines:**
- **Ready to merge**: Approved by all relevant reviewers, CI passing, no outstanding concerns
- **Needs minor work**: Approved but small changes requested (tests, docs, etc.)
- **In progress**: Active development, author working on it
- **Blocked**: Waiting on external dependency or unresolved blocker
- **Needs review**: Waiting for reviewer feedback
- **Stale**: No activity for extended period
- **Needs discussion**: Requires team decision or design discussion

**IMPORTANT - Determining action_required_by:**
When analyzing who needs to act, check the FULL comment/review history, not just the latest approval:
- A PR with one approval may still need another reviewer's sign-off if they raised major concerns earlier
- Look for unresolved concerns in older comments that haven't been formally re-reviewed
- Check if reviewers who requested changes have re-reviewed after fixes
- Example: If reviewer A approved but reviewer B raised major concerns and never re-approved after changes, action_required_by should include B, not A

**Report features:**
- Interactive table with sorting (multi-level, 3-state: unsorted → asc → desc)
- Filters: Person, Repo, Board status, Status, Needs Action, Search
- Expandable rows showing AI summary, action reason, and action items
- Color-coded badges for roles (champion=blue, reviewer=yellow, contributor=gray, needs-action=red)

**Cache location:** `cache/board_summary/`
- `board_items.json` - Merged board items + activity items with summaries
- `board_items_original.json` - Pure board items (before activity merge)
- `user_activity.json` - User activity data (authored PRs, reviewed PRs, comments)

**Configurable repos** (edit in `merge_activity.py`):
Activity items are included from repos in `INCLUDED_REPOS`:
- scikit-learn/scikit-learn, probabl-ai/skore, probabl-ai/probabl-team
- fairlearn/fairlearn, joblib/joblib, scikit-learn-contrib/imbalanced-learn
- dirty-cat/dirty_cat, skrub-data/skrub

**Default parameters:**
- Organization: probabl-ai
- Project: 8
- Lookback: 14 days (configurable with `--lookback`)

**Rate limit handling:**
- User activity fetching uses GraphQL API (5000 points/hour) instead of search REST API (30 req/min)
- Only 3 queries per user (authored, reviewed, commented) instead of 24 (8 repos × 3 types)
- Automatic rate limit checking before queries with wait if needed
- Run `gh api rate_limit` to check current limits

## GitHub Action: Automated Board Summary

A GitHub Action is configured to automatically generate board summary reports.

**Workflow file:** `.github/workflows/board-summary.yml`

**Schedule:** Runs daily at 5am UTC and can be triggered manually.

**Manual trigger options:**
- `users`: Comma-separated list of GitHub usernames (leave empty for all)
- `lookback_days`: Number of days to look back (default: 14)

### Required Secrets

The following secrets must be configured in the repository settings:

| Secret | Description | How to Create |
|--------|-------------|---------------|
| `GH_PAT` | GitHub Personal Access Token for accessing project boards and repos | See below |
| `ANTHROPIC_API_KEY` | Anthropic API key for generating AI summaries | Get from console.anthropic.com |
| `REPORTS_REPO` | Full name of private repo for reports (e.g., `your-org/board-reports`) | Create the repo first |
| `REPORTS_REPO_PAT` | GitHub PAT with write access to the reports repo | See below |

### Creating the GitHub PAT (GH_PAT)

1. Go to GitHub → Settings → Developer settings → Personal access tokens → Fine-grained tokens
2. Click "Generate new token"
3. Set expiration (recommend 90 days, set calendar reminder)
4. Select resource owner: `probabl-ai` (or relevant org)
5. Repository access: "All repositories" or select specific ones
6. Permissions needed:
   - **Repository permissions:**
     - Contents: Read
     - Issues: Read
     - Pull requests: Read
     - Metadata: Read (automatically selected)
   - **Organization permissions:**
     - Projects: Read
7. Generate and copy the token

### Creating the Reports Repo PAT (REPORTS_REPO_PAT)

1. Create a new fine-grained token as above
2. Select resource owner where the reports repo lives
3. Repository access: "Only select repositories" → select your reports repo
4. Permissions needed:
   - **Repository permissions:**
     - Contents: Read and write
     - Metadata: Read
5. Generate and copy the token

### Setting Up the Reports Repository

1. Create a new private repository (e.g., `your-org/board-reports`)
2. Initialize it with a README or empty commit
3. The workflow will push `board_summary.html` and dated copies there

### Workflow Output

The workflow generates:
- `board_summary.html` - Latest report
- `index.html` - Copy of latest (for GitHub Pages if enabled)
- `board_summary_YYYY-MM-DD.html` - Dated archive copies
