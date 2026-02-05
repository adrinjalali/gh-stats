"""Fetch and enrich board items from GitHub project board."""

import json
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

# Default configuration
DEFAULT_ORG = "probabl-ai"
DEFAULT_PROJECT = 8
CACHE_DIR = Path("cache/board_summary")


def fetch_board_items(
    org: str = DEFAULT_ORG, project: int = DEFAULT_PROJECT
) -> list[dict]:
    """Fetch all items from GitHub project board."""
    print(f"Fetching board items from {org}/projects/{project}...")

    result = subprocess.run(
        [
            "gh",
            "project",
            "item-list",
            str(project),
            "--owner",
            org,
            "--format",
            "json",
            "--limit",
            "500",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Failed to fetch board: {result.stderr}")

    data = json.loads(result.stdout)
    items = []

    for item in data.get("items", []):
        content = item.get("content", {})
        if not content or not content.get("repository"):
            continue

        status = item.get("status", "")
        if "Done" in status:
            continue

        repo = content.get("repository", "")
        items.append(
            {
                "repo": repo,
                "repo_short": repo.split("/")[-1] if "/" in repo else repo,
                "number": content.get("number", 0),
                "title": content.get("title", ""),
                "type": content.get("type", ""),
                "url": content.get("url", ""),
                "board_status": status,
                "champion": item.get("champion") or "",
                "reviewer1": item.get("reviewer 1") or "",
                "reviewer2": item.get("reviewer 2") or "",
            }
        )

    print(f"  Found {len(items)} active items")
    return items


def fetch_pr_details(repo: str, number: int) -> dict | None:
    """Fetch PR details using gh CLI."""
    try:
        result = subprocess.run(
            [
                "gh",
                "pr",
                "view",
                str(number),
                "--repo",
                repo,
                "--json",
                "title,state,author,createdAt,updatedAt,comments,reviews,reviewRequests",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
    except Exception:
        pass
    return None


def fetch_issue_details(repo: str, number: int) -> dict | None:
    """Fetch issue details using gh CLI."""
    try:
        result = subprocess.run(
            [
                "gh",
                "issue",
                "view",
                str(number),
                "--repo",
                repo,
                "--json",
                "title,state,author,createdAt,updatedAt,comments",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
    except Exception:
        pass
    return None


def determine_status(item: dict, details: dict | None) -> tuple[str, str, list[str]]:
    """Determine the status of an item based on activity."""
    if not details:
        return "Unknown", "gray", []

    # Check if PR is already merged
    state = details.get("state", "")
    if state == "MERGED":
        return "Merged", "purple", []

    now = datetime.now()
    updated_str = details.get("updatedAt", "")
    if updated_str:
        updated = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))
        days_since_update = (now - updated.replace(tzinfo=None)).days
    else:
        days_since_update = 999

    reviews = details.get("reviews", [])
    review_requests = details.get("reviewRequests", [])
    pending_reviewers = [r.get("login", "") for r in review_requests if r.get("login")]

    recent_reviews = reviews[-5:] if reviews else []
    approved = any(r.get("state") == "APPROVED" for r in recent_reviews)
    changes_requested = any(
        r.get("state") == "CHANGES_REQUESTED" for r in recent_reviews
    )

    if approved and not changes_requested:
        return "Ready to merge", "green", pending_reviewers
    elif changes_requested:
        return "Waiting for author", "orange", pending_reviewers
    elif pending_reviewers:
        reviewers_str = ", ".join(pending_reviewers)
        return f"Waiting for review from {reviewers_str}", "yellow", pending_reviewers
    elif days_since_update > 30:
        return "Stale (>30 days)", "red", pending_reviewers
    elif days_since_update > 14:
        return "Needs attention", "orange", pending_reviewers
    else:
        if (
            item.get("type") == "PullRequest"
            and not item.get("reviewer1")
            and not item.get("reviewer2")
        ):
            return "In progress (no reviewers)", "yellow", pending_reviewers
        return "In progress", "blue", pending_reviewers


def get_recent_activity(details: dict | None) -> list[dict]:
    """Get recent activity summary."""
    if not details:
        return []

    activities = []

    for comment in details.get("comments", [])[-10:]:
        author = comment.get("author", {}).get("login", "?")
        date_str = comment.get("createdAt", "")[:10]
        body = comment.get("body", "")[:150].replace("\n", " ")
        activities.append(
            {"type": "comment", "author": author, "date": date_str, "summary": body}
        )

    for review in details.get("reviews", [])[-10:]:
        author = review.get("author", {}).get("login", "?")
        date_str = review.get("submittedAt", "")[:10]
        state = review.get("state", "")
        activities.append(
            {"type": "review", "author": author, "date": date_str, "summary": state}
        )

    activities.sort(key=lambda x: x["date"], reverse=True)
    return activities[:5]


def enrich_item(item: dict, all_users: set[str]) -> dict:
    """Enrich a single item with details from GitHub."""
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

    # User roles
    item["user_roles"] = {}
    for user in all_users:
        roles = []
        if item["champion"].lower() == user:
            roles.append("champion")
        if item["reviewer1"].lower() == user:
            roles.append("reviewer")
        if item["reviewer2"].lower() == user:
            roles.append("reviewer")
        if roles:
            item["user_roles"][user] = roles

    return item


def enrich_board_items(items: list[dict], max_workers: int = 8) -> list[dict]:
    """Enrich all board items with PR/issue details using parallel execution."""
    # Get all unique users
    all_users = set()
    for item in items:
        if item["champion"]:
            all_users.add(item["champion"].lower())
        if item["reviewer1"]:
            all_users.add(item["reviewer1"].lower())
        if item["reviewer2"]:
            all_users.add(item["reviewer2"].lower())

    print(f"Enriching {len(items)} items with {max_workers} workers...")
    enriched = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(enrich_item, item.copy(), all_users): i
            for i, item in enumerate(items)
        }

        for i, future in enumerate(as_completed(futures)):
            try:
                result = future.result()
                enriched.append(result)
                if (i + 1) % 20 == 0:
                    print(f"  Processed {i + 1}/{len(items)} items")
            except Exception as e:
                idx = futures[future]
                print(f"  Error processing item {idx}: {e}")

    # Sort by original order
    enriched.sort(key=lambda x: (x["repo"], x["number"]))
    print(f"  Enriched {len(enriched)} items")
    return enriched


def get_all_users(items: list[dict]) -> list[str]:
    """Get sorted list of all unique users from board items."""
    users = set()
    for item in items:
        if item["champion"]:
            users.add(item["champion"])
        if item["reviewer1"]:
            users.add(item["reviewer1"])
        if item["reviewer2"]:
            users.add(item["reviewer2"])
    return sorted(users, key=str.lower)


def save_to_cache(items: list[dict], filename: str = "board_items.json") -> Path:
    """Save items to cache file.

    Also saves a copy to board_items_original.json to preserve
    pure board items before merging with activity.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / filename
    with open(path, "w") as f:
        json.dump(items, f, indent=2)

    # Also save original copy for merge_activity to use
    if filename == "board_items.json":
        original_path = CACHE_DIR / "board_items_original.json"
        with open(original_path, "w") as f:
            json.dump(items, f, indent=2)

    return path


def load_from_cache(filename: str = "board_items.json") -> list[dict] | None:
    """Load items from cache file if it exists."""
    path = CACHE_DIR / filename
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None


if __name__ == "__main__":
    # Test the module
    items = fetch_board_items()
    enriched = enrich_board_items(items)
    path = save_to_cache(enriched)
    print(f"Saved to {path}")
