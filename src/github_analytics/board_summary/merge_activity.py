"""Merge user activity items with board items.

This script takes user activity (authored PRs, reviewed PRs, commented issues)
and adds items not already on the board to the board_items.json cache.
"""

import json

from fetch_board import (
    CACHE_DIR,
    determine_status,
    fetch_issue_details,
    fetch_pr_details,
    get_recent_activity,
)
from fetch_user_activity import DEFAULT_REPOS, fetch_all_users_activity

# Repos to include in the main view (configurable)
# Items from these repos will be shown even if not on board
INCLUDED_REPOS = DEFAULT_REPOS.copy()


def load_board_items() -> list[dict]:
    """Load original board items from cache (not merged)."""
    # First try board_items_original.json (pure board items)
    path = CACHE_DIR / "board_items_original.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    # Fall back to board_items.json for backwards compatibility
    path = CACHE_DIR / "board_items.json"
    if path.exists():
        with open(path) as f:
            items = json.load(f)
            # Filter out activity items if this is a merged file
            return [i for i in items if i.get("board_status") != "Not Included"]
    return []


def load_user_activity() -> dict[str, dict]:
    """Load user activity from cache."""
    path = CACHE_DIR / "user_activity.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def save_board_items(items: list[dict]):
    """Save board items to cache."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / "board_items.json"
    with open(path, "w") as f:
        json.dump(items, f, indent=2)


def get_board_item_keys(items: list[dict]) -> set[tuple[str, int]]:
    """Get set of (repo, number) tuples for board items."""
    return {(item["repo"], item["number"]) for item in items}


def extract_repo_and_number(item: dict) -> tuple[str, int]:
    """Extract repo and number from an activity item."""
    # Activity items have repository.nameWithOwner format
    repo_info = item.get("repository", {})
    if isinstance(repo_info, dict):
        repo = repo_info.get("nameWithOwner", "")
    else:
        repo = str(repo_info)
    number = item.get("number", 0)
    return repo, number


def create_activity_item(
    repo: str,
    number: int,
    item_type: str,
    activity_item: dict,
    involved_users: list[str],
    interaction_types: dict[str, list[str]],
) -> dict:
    """Create a board-compatible item from activity data."""
    repo_short = repo.split("/")[-1] if "/" in repo else repo

    return {
        "repo": repo,
        "repo_short": repo_short,
        "number": number,
        "title": activity_item.get("title", ""),
        "type": item_type,
        "url": activity_item.get("url", ""),
        "board_status": "Not Included",
        "champion": "",
        "reviewer1": "",
        "reviewer2": "",
        # Track which users are involved and how
        "involved_users": involved_users,
        "interaction_types": interaction_types,
    }


def enrich_activity_item(item: dict) -> dict:
    """Enrich an activity item with details from GitHub."""
    repo = item["repo"]
    number = item["number"]

    if item["type"] == "PullRequest":
        details = fetch_pr_details(repo, number)
    else:
        details = fetch_issue_details(repo, number)

    status, status_color, pending_reviewers = determine_status(item, details)

    item["computed_status"] = status
    item["status_color"] = status_color
    item["pending_reviewers"] = pending_reviewers
    item["author"] = details.get("author", {}).get("login", "") if details else ""
    item["updated_at"] = details.get("updatedAt", "")[:10] if details else ""
    item["created_at"] = details.get("createdAt", "")[:10] if details else ""
    item["state"] = details.get("state", "") if details else ""
    item["recent_activity"] = get_recent_activity(details)

    return item


def merge_activity_with_board(
    users: list[str],
    lookback_days: int = 14,
    included_repos: list[str] | None = None,
) -> list[dict]:
    """Merge user activity with board items.

    Args:
        users: List of GitHub usernames to fetch activity for
        lookback_days: Number of days to look back for activity
        included_repos: Repos to include in main view (defaults to INCLUDED_REPOS)

    Returns:
        Updated list of board items including activity items
    """
    if included_repos is None:
        included_repos = INCLUDED_REPOS

    # Normalize repo names for comparison
    included_repos_lower = {r.lower() for r in included_repos}

    # Load existing data
    board_items = load_board_items()
    board_keys = get_board_item_keys(board_items)

    print(f"Loaded {len(board_items)} board items")

    # Fetch fresh user activity
    print(f"\nFetching activity for {len(users)} users (last {lookback_days} days)...")
    user_activity = fetch_all_users_activity(users, included_repos, lookback_days)

    # Save user activity to cache
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_DIR / "user_activity.json", "w") as f:
        json.dump(user_activity, f, indent=2)

    # Collect activity items not on board AND activity on board items
    # Track: (repo, number) -> {users who interacted, how they interacted}
    activity_items: dict[tuple[str, int], dict] = {}  # Non-board items
    board_activity: dict[tuple[str, int], dict] = {}  # Activity on board items

    def add_activity(target_dict, key, item_data, item_type, user, interaction):
        """Helper to add activity to a tracking dict."""
        if key not in target_dict:
            target_dict[key] = {
                "item": item_data,
                "type": item_type,
                "users": set(),
                "interactions": {},
            }
        target_dict[key]["users"].add(user)
        if user not in target_dict[key]["interactions"]:
            target_dict[key]["interactions"][user] = []
        if interaction not in target_dict[key]["interactions"][user]:
            target_dict[key]["interactions"][user].append(interaction)

    for user, activity in user_activity.items():
        # Authored PRs
        for pr in activity.get("authored_prs", []):
            repo, number = extract_repo_and_number(pr)
            if not repo or not number:
                continue
            if repo.lower() not in included_repos_lower:
                continue

            key = (repo, number)
            if key in board_keys:
                add_activity(board_activity, key, pr, "PullRequest", user, "authored")
            else:
                add_activity(activity_items, key, pr, "PullRequest", user, "authored")

        # Reviewed PRs
        for pr in activity.get("reviewed_prs", []):
            repo, number = extract_repo_and_number(pr)
            if not repo or not number:
                continue
            if repo.lower() not in included_repos_lower:
                continue

            key = (repo, number)
            if key in board_keys:
                add_activity(board_activity, key, pr, "PullRequest", user, "reviewed")
            else:
                add_activity(activity_items, key, pr, "PullRequest", user, "reviewed")

        # Issue comments (could be issues or PRs)
        for issue in activity.get("issue_comments", []):
            repo, number = extract_repo_and_number(issue)
            if not repo or not number:
                continue
            if repo.lower() not in included_repos_lower:
                continue

            key = (repo, number)
            if key in board_keys:
                add_activity(board_activity, key, issue, "Issue", user, "commented")
            else:
                add_activity(activity_items, key, issue, "Issue", user, "commented")

    # Update board items with activity information
    for item in board_items:
        key = (item.get("repo", ""), item.get("number", 0))
        if key in board_activity:
            act = board_activity[key]
            item["involved_users"] = list(act["users"])
            item["interaction_types"] = act["interactions"]
        else:
            # Ensure these fields exist even if no activity
            if "involved_users" not in item:
                item["involved_users"] = []
            if "interaction_types" not in item:
                item["interaction_types"] = {}

    print(f"\nFound {len(board_activity)} board items with user activity")
    print(f"Found {len(activity_items)} activity items not on board")

    # Create and enrich activity items
    new_items = []
    for i, ((repo, number), data) in enumerate(activity_items.items()):
        item = create_activity_item(
            repo=repo,
            number=number,
            item_type=data["type"],
            activity_item=data["item"],
            involved_users=list(data["users"]),
            interaction_types=data["interactions"],
        )

        # Enrich with GitHub details
        item = enrich_activity_item(item)
        new_items.append(item)

        if (i + 1) % 10 == 0:
            print(f"  Enriched {i + 1}/{len(activity_items)} activity items")

    print(f"  Enriched {len(new_items)} activity items")

    # Merge with board items
    all_items = board_items + new_items
    board_count = len(board_items)
    activity_count = len(new_items)
    total = len(all_items)
    print(f"\nTotal items: {total} ({board_count} board + {activity_count} activity)")

    # Save merged items
    save_board_items(all_items)

    return all_items


if __name__ == "__main__":
    import sys

    users = sys.argv[1:] if len(sys.argv) > 1 else ["ogrisel", "lesteve"]
    lookback = 14

    print(f"Merging activity for: {', '.join(users)}")
    print(f"Lookback: {lookback} days")
    print(f"Included repos: {', '.join(INCLUDED_REPOS)}")
    print()

    items = merge_activity_with_board(users, lookback_days=lookback)
    print(f"\nDone! Total items: {len(items)}")
