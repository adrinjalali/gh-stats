"""Fetch user activity from GitHub using GraphQL for efficiency."""

import json
import subprocess
import time
from datetime import datetime, timedelta

# Repos to search for activity (most relevant to probabl-ai board)
DEFAULT_REPOS = [
    "scikit-learn/scikit-learn",
    "probabl-ai/skore",
    "probabl-ai/probabl-team",
    "fairlearn/fairlearn",
    "joblib/joblib",
    "scikit-learn-contrib/imbalanced-learn",
    "dirty-cat/dirty_cat",
    "skrub-data/skrub",
]


def check_rate_limit() -> dict:
    """Check current GitHub API rate limits."""
    result = subprocess.run(
        [
            "gh",
            "api",
            "rate_limit",
            "--jq",
            "{search: .resources.search, graphql: .resources.graphql}",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode == 0:
        return json.loads(result.stdout)
    return {}


def wait_for_rate_limit(resource: str = "search"):
    """Wait if rate limit is exhausted."""
    limits = check_rate_limit()
    if resource in limits:
        remaining = limits[resource].get("remaining", 1)
        reset_time = limits[resource].get("reset", 0)
        if remaining < 5:
            wait_seconds = max(0, reset_time - time.time()) + 5
            print(f"  Rate limit low ({remaining}), waiting {wait_seconds:.0f}s...")
            time.sleep(wait_seconds)


def run_graphql_query(query: str) -> dict | None:
    """Run a GraphQL query using gh api."""
    result = subprocess.run(
        ["gh", "api", "graphql", "-f", f"query={query}"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode == 0:
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return None
    else:
        # Check if it's a rate limit error
        if "rate limit" in result.stderr.lower():
            print("  Rate limit hit, waiting...")
            wait_for_rate_limit("graphql")
            # Retry once
            result = subprocess.run(
                ["gh", "api", "graphql", "-f", f"query={query}"],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode == 0:
                return json.loads(result.stdout)
    return None


def fetch_user_activity_graphql(
    user: str,
    repos: list[str] | None = None,
    lookback_days: int = 14,
) -> dict:
    """Fetch user activity using GraphQL for efficiency.

    Uses GitHub's GraphQL API which has much higher rate limits than search API.
    """
    if repos is None:
        repos = DEFAULT_REPOS

    since_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    authored_prs = []
    reviewed_prs = []
    issue_comments = []

    # Build repo filter for search
    repo_filter = " ".join(f"repo:{repo}" for repo in repos)

    # Query 1: PRs authored by user
    search_query = f"is:pr author:{user} updated:>={since_date} {repo_filter}"
    query_authored = f"""
    {{
      search(query: "{search_query}", type: ISSUE, first: 100) {{
        nodes {{
          ... on PullRequest {{
            number
            title
            url
            state
            updatedAt
            repository {{
              nameWithOwner
            }}
          }}
        }}
      }}
    }}
    """

    wait_for_rate_limit("graphql")
    result = run_graphql_query(query_authored)
    if result and "data" in result:
        for node in result["data"]["search"]["nodes"]:
            if node:  # Can be null
                authored_prs.append(
                    {
                        "number": node.get("number"),
                        "title": node.get("title"),
                        "url": node.get("url"),
                        "state": node.get("state"),
                        "updatedAt": node.get("updatedAt"),
                        "repository": node.get("repository", {}),
                    }
                )

    # Query 2: PRs reviewed by user
    search_query = f"is:pr reviewed-by:{user} updated:>={since_date} {repo_filter}"
    query_reviewed = f"""
    {{
      search(query: "{search_query}", type: ISSUE, first: 100) {{
        nodes {{
          ... on PullRequest {{
            number
            title
            url
            state
            updatedAt
            repository {{
              nameWithOwner
            }}
          }}
        }}
      }}
    }}
    """

    wait_for_rate_limit("graphql")
    result = run_graphql_query(query_reviewed)
    if result and "data" in result:
        for node in result["data"]["search"]["nodes"]:
            if node:
                reviewed_prs.append(
                    {
                        "number": node.get("number"),
                        "title": node.get("title"),
                        "url": node.get("url"),
                        "state": node.get("state"),
                        "updatedAt": node.get("updatedAt"),
                        "repository": node.get("repository", {}),
                    }
                )

    # Query 3: Issues/PRs commented by user
    search_query = f"commenter:{user} updated:>={since_date} {repo_filter}"
    query_comments = f"""
    {{
      search(query: "{search_query}", type: ISSUE, first: 100) {{
        nodes {{
          ... on Issue {{
            number
            title
            url
            state
            updatedAt
            repository {{
              nameWithOwner
            }}
          }}
          ... on PullRequest {{
            number
            title
            url
            state
            updatedAt
            repository {{
              nameWithOwner
            }}
          }}
        }}
      }}
    }}
    """

    wait_for_rate_limit("graphql")
    result = run_graphql_query(query_comments)
    if result and "data" in result:
        for node in result["data"]["search"]["nodes"]:
            if node:
                issue_comments.append(
                    {
                        "number": node.get("number"),
                        "title": node.get("title"),
                        "url": node.get("url"),
                        "state": node.get("state"),
                        "updatedAt": node.get("updatedAt"),
                        "repository": node.get("repository", {}),
                    }
                )

    # Deduplicate by URL
    def dedupe(items):
        seen = set()
        unique = []
        for item in items:
            url = item.get("url", "")
            if url and url not in seen:
                seen.add(url)
                unique.append(item)
        return unique

    return {
        "user": user,
        "since": since_date,
        "authored_prs": dedupe(authored_prs),
        "reviewed_prs": dedupe(reviewed_prs),
        "issue_comments": dedupe(issue_comments),
    }


# Keep old function name for compatibility
def fetch_user_activity(
    user: str,
    repos: list[str] | None = None,
    lookback_days: int = 14,
    max_workers: int = 6,  # Ignored, kept for compatibility
) -> dict:
    """Fetch user activity (wrapper for GraphQL version)."""
    return fetch_user_activity_graphql(user, repos, lookback_days)


def fetch_all_users_activity(
    users: list[str],
    repos: list[str] | None = None,
    lookback_days: int = 14,
    max_workers_per_user: int = 6,  # Ignored
) -> dict[str, dict]:
    """Fetch activity for multiple users.

    Uses GraphQL for efficiency with proper rate limit handling.
    """
    results = {}

    # Check rate limit before starting
    limits = check_rate_limit()
    graphql_remaining = limits.get("graphql", {}).get("remaining", 5000)
    print(f"GraphQL rate limit: {graphql_remaining} remaining")

    for i, user in enumerate(users):
        print(f"Fetching activity for {user} ({i + 1}/{len(users)})...")
        results[user] = fetch_user_activity_graphql(user, repos, lookback_days)
        authored = len(results[user]["authored_prs"])
        reviewed = len(results[user]["reviewed_prs"])
        comments = len(results[user]["issue_comments"])
        print(
            f"  Found: {authored} authored, {reviewed} reviewed, {comments} commented"
        )

    return results


def format_activity_summary(activity: dict) -> str:
    """Format user activity as a brief summary."""
    user = activity["user"]
    authored = len(activity["authored_prs"])
    reviewed = len(activity["reviewed_prs"])
    issues = len(activity["issue_comments"])

    lines = [f"**{user}** (since {activity['since']}):"]
    lines.append(f"  - {authored} PRs authored")
    lines.append(f"  - {reviewed} PRs reviewed")
    lines.append(f"  - {issues} issues with comments")

    return "\n".join(lines)


if __name__ == "__main__":
    import sys

    users = sys.argv[1:] if len(sys.argv) > 1 else ["ogrisel", "lesteve"]

    print(f"Fetching activity for: {', '.join(users)}")
    print(f"Repos: {', '.join(DEFAULT_REPOS)}")
    print()

    results = fetch_all_users_activity(users, lookback_days=14)

    print("\n=== Summary ===\n")
    for _user, activity in results.items():
        print(format_activity_summary(activity))
        print()
