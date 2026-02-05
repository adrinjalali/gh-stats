"""Generate board summary report from cached data."""

import json
from datetime import datetime
from html import escape
from pathlib import Path

CACHE_DIR = Path("cache/board_summary")


def load_board_items() -> list[dict]:
    """Load enriched board items from cache."""
    path = CACHE_DIR / "board_items.json"
    if not path.exists():
        raise FileNotFoundError(f"Board items cache not found: {path}")
    with open(path) as f:
        return json.load(f)


def load_user_activity() -> dict[str, dict]:
    """Load user activity from cache."""
    path = CACHE_DIR / "user_activity.json"
    if not path.exists():
        return {}  # Return empty dict if no user activity cache
    with open(path) as f:
        return json.load(f)


def get_items_for_user(items: list[dict], user: str) -> list[dict]:
    """Get all items where user is champion or reviewer."""
    user_lower = user.lower()
    user_items = []
    for item in items:
        roles = []
        if item.get("champion", "").lower() == user_lower:
            roles.append("champion")
        if item.get("reviewer1", "").lower() == user_lower:
            roles.append("reviewer")
        if item.get("reviewer2", "").lower() == user_lower:
            roles.append("reviewer")
        if roles:
            item_copy = item.copy()
            item_copy["user_roles_list"] = roles
            user_items.append(item_copy)
    return user_items


STATUS_CONFIG = {
    # AI status values (preferred when available)
    "Merged": {"emoji": "🟣", "color": "#6f42c1", "priority": 0},
    "Ready to merge": {"emoji": "✅", "color": "#22863a", "priority": 1},
    "Needs second review or ready to merge": {
        "emoji": "🔷",
        "color": "#17a2b8",
        "priority": 1.5,
    },
    "Needs minor work": {"emoji": "🟡", "color": "#dbab09", "priority": 2},
    "In progress": {"emoji": "🔵", "color": "#0366d6", "priority": 3},
    "Needs review": {"emoji": "🟠", "color": "#e36209", "priority": 4},
    "Blocked": {"emoji": "🔴", "color": "#cb2431", "priority": 5},
    "Stale": {"emoji": "⚫", "color": "#6a737d", "priority": 6},
    "Needs discussion": {"emoji": "💬", "color": "#6f42c1", "priority": 7},
    # Computed status values (fallback)
    "Waiting for author": {"emoji": "🟠", "color": "#e36209", "priority": 4},
    "Needs attention": {"emoji": "🟠", "color": "#e36209", "priority": 4},
    "Stale (>30 days)": {"emoji": "⚫", "color": "#6a737d", "priority": 6},
    "Waiting for review": {"emoji": "🟠", "color": "#e36209", "priority": 4},
    "In progress (no reviewers)": {"emoji": "🔵", "color": "#0366d6", "priority": 3},
    "Unknown": {"emoji": "⚪", "color": "#6a737d", "priority": 8},
}


def generate_html_report(users: list[str] | None = None) -> str:
    """Generate HTML report with interactive table."""
    items = load_board_items()
    user_activity = load_user_activity()

    if users is None:
        users = list(user_activity.keys())

    # Build table rows for board items and activity items
    # Include items where any specified user is assigned OR has activity
    users_lower = {u.lower() for u in users}
    seen_items = set()  # Track by (repo, number) to avoid duplicates
    table_rows = []

    # Build set of URLs where users have activity
    user_activity_urls = set()
    for user in users:
        if user in user_activity:
            act = user_activity[user]
            for pr in act.get("authored_prs", []):
                if pr.get("url"):
                    user_activity_urls.add(pr["url"])
            for pr in act.get("reviewed_prs", []):
                if pr.get("url"):
                    user_activity_urls.add(pr["url"])
            for issue in act.get("issue_comments", []):
                if issue.get("url"):
                    user_activity_urls.add(issue["url"])

    for item in items:
        champion = item.get("champion", "")
        reviewer1 = item.get("reviewer1", "")
        reviewer2 = item.get("reviewer2", "")
        involved_users = item.get("involved_users", [])
        interaction_types = item.get("interaction_types", {})
        is_board_item = item.get("board_status", "") != "Not Included"
        item_url = item.get("url", "")

        # Check if any of our users is assigned, involved, or has activity
        assigned_users = [champion.lower(), reviewer1.lower(), reviewer2.lower()]
        involved_lower = [u.lower() for u in involved_users]

        user_matches = any(u in users_lower for u in assigned_users if u)
        if not user_matches:
            user_matches = any(u in users_lower for u in involved_lower if u)
        if not user_matches:
            user_matches = item_url in user_activity_urls

        if not user_matches:
            continue

        item_key = (item.get("repo", ""), item.get("number", ""))
        if item_key in seen_items:
            continue
        seen_items.add(item_key)

        # Use AI status when available, fall back to computed status
        # Always prioritize "Merged" computed status over AI status
        ai_status = item.get("ai_status", "")
        computed_status = item.get("computed_status", "Unknown")
        if computed_status == "Merged":
            status_key = "Merged"
        elif ai_status and ai_status in STATUS_CONFIG:
            status_key = ai_status
        else:
            status_key = computed_status.split(" from ")[0]
        config = STATUS_CONFIG.get(status_key, STATUS_CONFIG["Unknown"])

        # Build assigned/involved people list
        if is_board_item:
            # Board items: show champion and reviewers with roles
            assigned = []
            if champion:
                assigned.append(f"{champion} (C)")
            if reviewer1:
                assigned.append(f"{reviewer1} (R)")
            if reviewer2:
                assigned.append(f"{reviewer2} (R)")
        else:
            # Activity items: involved users are shown differently (handled in JS)
            assigned = []

        # Get other contributors from recent activity (not assigned)
        assigned_lower = {champion.lower(), reviewer1.lower(), reviewer2.lower()}
        author = item.get("author", "")
        assigned_lower.add(author.lower())  # Also exclude the PR/issue author

        other_contributors = []
        for activity in item.get("recent_activity", []):
            contributor = activity.get("author", "")
            is_new = contributor.lower() not in assigned_lower
            if contributor and is_new and contributor not in other_contributors:
                other_contributors.append(contributor)
                assigned_lower.add(contributor.lower())  # Avoid duplicates

        table_rows.append(
            {
                "item": f"{item.get('repo_short', '')}#{item.get('number', '')}",
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "type": item.get("type", ""),
                "assigned": ", ".join(assigned),
                "champion": champion,
                "reviewer1": reviewer1,
                "reviewer2": reviewer2,
                "other_contributors": other_contributors,
                "board_status": item.get("board_status", ""),
                "is_board_item": is_board_item,
                "involved_users": involved_users,
                "interaction_types": interaction_types,
                "status": status_key,
                "status_emoji": config["emoji"],
                "status_color": config["color"],
                "status_priority": config["priority"],
                "author": item.get("author", ""),
                "created": item.get("created_at", ""),
                "updated": item.get("updated_at", ""),
                "summary": item.get("summary", ""),
                "ai_status": item.get("ai_status", ""),
                "action_items": item.get("action_items", []),
                "action_required_by": item.get("action_required_by", []),
                "action_reason": item.get("action_reason", ""),
            }
        )

    # Collect URLs already shown in main table
    shown_urls = {r["url"] for r in table_rows if r.get("url")}

    # Build "other activity" rows - only items NOT in main table
    other_activity_rows = []
    for user in users:
        if user in user_activity:
            act = user_activity[user]

            # Filter to only items not already shown
            other_authored = [
                pr
                for pr in act.get("authored_prs", [])
                if pr.get("url") not in shown_urls
            ]
            other_reviewed = [
                pr
                for pr in act.get("reviewed_prs", [])
                if pr.get("url") not in shown_urls
            ]
            other_comments = [
                issue
                for issue in act.get("issue_comments", [])
                if issue.get("url") not in shown_urls
            ]

            # Only add user if they have other activity
            if other_authored or other_reviewed or other_comments:
                other_activity_rows.append(
                    {
                        "user": user,
                        "prs_authored": len(other_authored),
                        "prs_reviewed": len(other_reviewed),
                        "issues_commented": len(other_comments),
                        "authored_prs": other_authored[:10],
                        "reviewed_prs": other_reviewed[:10],
                        "issue_comments": other_comments[:10],
                    }
                )

    # Get unique values for filters
    all_people = set()
    for r in table_rows:
        if r["champion"]:
            all_people.add(r["champion"])
        if r["reviewer1"]:
            all_people.add(r["reviewer1"])
        if r["reviewer2"]:
            all_people.add(r["reviewer2"])
    all_people = sorted(all_people, key=str.lower)

    all_repos = sorted({r["item"].split("#")[0] for r in table_rows})
    all_statuses = sorted(
        {r["status"] for r in table_rows},
        key=lambda s: STATUS_CONFIG.get(s, {}).get("priority", 99),
    )
    all_board_statuses = sorted(
        {r["board_status"] for r in table_rows if r["board_status"]}
    )

    # Collect all people who need to act
    all_action_people = set()
    for r in table_rows:
        for person in r.get("action_required_by", []):
            all_action_people.add(person)
    all_action_people = sorted(all_action_people, key=str.lower)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Board Summary Report</title>
    <style>
        * {{ box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial;
            line-height: 1.5;
            margin: 0;
            padding: 20px;
            color: #24292e;
            background: #f6f8fa;
        }}
        h1 {{ margin: 0 0 5px 0; }}
        .header {{
            background: white;
            padding: 15px 20px;
            border-radius: 6px;
            margin-bottom: 15px;
            border: 1px solid #e1e4e8;
        }}
        .meta {{ color: #586069; font-size: 14px; }}
        .tabs {{
            display: flex;
            gap: 10px;
            margin-bottom: 15px;
        }}
        .tab {{
            padding: 8px 16px;
            background: white;
            border: 1px solid #e1e4e8;
            border-radius: 6px;
            cursor: pointer;
            font-size: 14px;
        }}
        .tab.active {{
            background: #0366d6;
            color: white;
            border-color: #0366d6;
        }}
        .panel {{ display: none; }}
        .panel.active {{ display: block; }}
        .filters {{
            background: white;
            padding: 15px;
            border-radius: 6px;
            margin-bottom: 15px;
            border: 1px solid #e1e4e8;
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
            align-items: center;
        }}
        .filter-group {{
            display: flex;
            align-items: center;
            gap: 5px;
        }}
        .filter-group label {{
            font-size: 13px;
            font-weight: 500;
        }}
        select, input[type="text"] {{
            padding: 5px 10px;
            border: 1px solid #e1e4e8;
            border-radius: 4px;
            font-size: 13px;
        }}
        .table-container {{
            background: white;
            border-radius: 6px;
            border: 1px solid #e1e4e8;
            overflow-x: auto;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }}
        th, td {{
            padding: 10px 12px;
            text-align: left;
            border-bottom: 1px solid #e1e4e8;
        }}
        th {{
            background: #f6f8fa;
            font-weight: 600;
            cursor: pointer;
            user-select: none;
            white-space: nowrap;
        }}
        th:hover {{ background: #e1e4e8; }}
        th .sort-indicator {{
            font-size: 10px;
            margin-left: 4px;
            color: #0366d6;
        }}
        th .sort-order {{
            font-size: 9px;
            color: #586069;
            margin-left: 2px;
        }}
        tr:hover {{ background: #f6f8fa; }}
        .link {{ color: #0366d6; text-decoration: none; }}
        .link:hover {{ text-decoration: underline; }}
        .type-icon {{
            vertical-align: middle;
            margin-right: 4px;
        }}
        .type-icon.pr {{ color: #8250df; }}
        .type-icon.issue {{ color: #1a7f37; }}
        .status {{
            display: inline-flex;
            align-items: center;
            gap: 5px;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 12px;
            white-space: nowrap;
        }}
        .badge {{
            display: inline-block;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 11px;
            margin: 1px;
        }}
        .badge.champion {{
            background: #ddf4ff;
            color: #0969da;
        }}
        .badge.reviewer {{
            background: #fff8c5;
            color: #9a6700;
        }}
        .badge.contributor {{
            background: #f0f0f0;
            color: #586069;
        }}
        .badge.involved {{
            background: #e1e4e8;
            color: #586069;
            font-style: italic;
        }}
        .badge.needs-action {{
            background: #ffeef0;
            color: #cb2431;
            font-weight: 500;
        }}
        .assigned {{
            max-width: 280px;
        }}
        .board-badge {{
            display: inline-block;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 11px;
            background: #e1e4e8;
            color: #24292e;
            white-space: nowrap;
        }}
        .board-badge.high-priority {{
            background: #ffeef0;
            color: #cb2431;
            font-weight: 600;
        }}
        .board-badge.not-included {{
            background: #f6f8fa;
            color: #8b949e;
            font-style: italic;
            border: 1px dashed #d0d7de;
        }}
        .activity-card {{
            background: white;
            border: 1px solid #e1e4e8;
            border-radius: 6px;
            padding: 15px;
            margin-bottom: 15px;
        }}
        .activity-card h3 {{
            margin: 0 0 10px 0;
            padding-bottom: 8px;
            border-bottom: 1px solid #e1e4e8;
        }}
        .activity-stats {{
            display: flex;
            gap: 20px;
            margin-bottom: 10px;
        }}
        .activity-stat {{
            font-size: 13px;
        }}
        .activity-stat strong {{
            font-size: 18px;
            color: #0366d6;
        }}
        .activity-list {{
            font-size: 12px;
            max-height: 150px;
            overflow-y: auto;
        }}
        .activity-list a {{
            color: #0366d6;
            text-decoration: none;
        }}
        .count {{
            background: #e1e4e8;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 12px;
            margin-left: 10px;
        }}
        .count.other {{
            background: #f0f0f0;
            color: #586069;
        }}
        .hidden {{ display: none; }}
        .other-activity-container {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
            gap: 16px;
        }}
        .other-activity-card {{
            background: white;
            border: 1px solid #e1e4e8;
            border-radius: 8px;
            overflow: hidden;
        }}
        .other-activity-card h3 {{
            margin: 0;
            padding: 12px 16px;
            background: #f6f8fa;
            border-bottom: 1px solid #e1e4e8;
            font-size: 16px;
            font-weight: 600;
        }}
        .other-activity-card h3 .user-badge {{
            background: #0366d6;
            color: white;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 14px;
        }}
        .other-activity-stats {{
            display: flex;
            gap: 16px;
            padding: 12px 16px;
            border-bottom: 1px solid #e1e4e8;
            background: #fafbfc;
        }}
        .other-stat {{
            text-align: center;
        }}
        .other-stat-value {{
            font-size: 20px;
            font-weight: 600;
            color: #0366d6;
        }}
        .other-stat-label {{
            font-size: 11px;
            color: #586069;
            text-transform: uppercase;
        }}
        .other-activity-section {{
            padding: 12px 16px;
            border-bottom: 1px solid #e1e4e8;
        }}
        .other-activity-section:last-child {{
            border-bottom: none;
        }}
        .other-activity-section h4 {{
            margin: 0 0 8px 0;
            font-size: 12px;
            font-weight: 600;
            color: #586069;
            text-transform: uppercase;
        }}
        .other-activity-item {{
            display: flex;
            align-items: flex-start;
            gap: 8px;
            padding: 6px 0;
            font-size: 13px;
        }}
        .other-activity-item a {{
            color: #0366d6;
            text-decoration: none;
            font-weight: 500;
        }}
        .other-activity-item a:hover {{
            text-decoration: underline;
        }}
        .other-activity-item .repo {{
            color: #586069;
            font-size: 12px;
        }}
        .other-activity-item .title {{
            color: #24292e;
        }}
        .no-other-activity {{
            text-align: center;
            padding: 40px 20px;
            color: #586069;
        }}
        .no-other-activity p {{
            margin: 0;
            font-size: 14px;
        }}
        .expandable {{
            cursor: pointer;
        }}
        .expand-arrow {{
            display: inline-block;
            width: 16px;
            font-size: 10px;
            color: #586069;
            user-select: none;
        }}
        .summary-row {{
            display: none;
        }}
        .summary-row.visible {{
            display: table-row;
        }}
        .summary-row td {{
            background: #f6f8fa;
            padding: 12px 20px;
            font-size: 13px;
            color: #24292e;
            border-bottom: 2px solid #e1e4e8;
        }}
        .summary-text {{
            max-width: 800px;
            line-height: 1.5;
        }}
        .no-summary {{
            color: #586069;
            font-style: italic;
        }}
        .ai-status {{
            display: inline-block;
            padding: 3px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 500;
            margin-bottom: 8px;
        }}
        .ai-status.merged {{ background: #f5f0ff; color: #6f42c1; }}
        .ai-status.second-review {{ background: #d1ecf1; color: #0c5460; }}
        .ai-status.ready {{ background: #dcffe4; color: #22863a; }}
        .ai-status.minor {{ background: #fff8c5; color: #9a6700; }}
        .ai-status.progress {{ background: #ddf4ff; color: #0366d6; }}
        .ai-status.blocked {{ background: #ffeef0; color: #cb2431; }}
        .ai-status.review {{ background: #f1e05a33; color: #735c0f; }}
        .ai-status.stale {{ background: #f0f0f0; color: #586069; }}
        .ai-status.discussion {{ background: #e8e0ff; color: #5a32a3; }}
        .action-items {{
            margin-top: 10px;
            padding: 8px 12px;
            background: #fffbdd;
            border-left: 3px solid #f9c513;
            border-radius: 0 4px 4px 0;
        }}
        .action-items-title {{
            font-weight: 600;
            font-size: 12px;
            color: #735c0f;
            margin-bottom: 4px;
        }}
        .action-items ul {{
            margin: 0;
            padding-left: 20px;
        }}
        .action-items li {{
            font-size: 13px;
            color: #24292e;
            margin: 4px 0;
        }}
        .action-reason {{
            margin-top: 8px;
            padding: 6px 10px;
            background: #f1f8ff;
            border-left: 3px solid #0366d6;
            border-radius: 0 4px 4px 0;
            font-size: 13px;
            color: #24292e;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Board Summary Report</h1>
        <div class="meta">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")} |
        {len(table_rows)} items | {len(users)} users: {", ".join(users)}</div>
    </div>
"""
    other_count = sum(
        r["prs_authored"] + r["prs_reviewed"] + r["issues_commented"]
        for r in other_activity_rows
    )
    html += f"""
    <div class="tabs">
        <div class="tab active" data-tab="board">
            Tracked Items<span class="count">{len(table_rows)}</span>
        </div>
        <div class="tab" data-tab="activity">
            Other Activity<span class="count other">{other_count}</span>
        </div>
    </div>

    <div id="board" class="panel active">
        <div class="filters">
            <div class="filter-group">
                <label>Person:</label>
                <select id="filter-person">
                    <option value="">All</option>
                    {" ".join(f'<option value="{p}">{p}</option>' for p in all_people)}
                </select>
            </div>
            <div class="filter-group">
                <label>Repo:</label>
                <select id="filter-repo">
                    <option value="">All</option>
                    {" ".join(f'<option value="{r}">{r}</option>' for r in all_repos)}
                </select>
            </div>
            <div class="filter-group">
                <label>Board:</label>
                <select id="filter-board">
                    <option value="">All</option>
                    {" ".join(f'<option value="{b}">{b}</option>' for b in all_board_statuses)}
                </select>
            </div>
            <div class="filter-group">
                <label>Status:</label>
                <select id="filter-status">
                    <option value="">All</option>
                    {" ".join(f'<option value="{s}">{s}</option>' for s in all_statuses)}
                </select>
            </div>
            <div class="filter-group">
                <label>Needs Action:</label>
                <select id="filter-action">
                    <option value="">All</option>
                    {" ".join(f'<option value="{p}">{p}</option>' for p in all_action_people)}
                </select>
            </div>
            <div class="filter-group">
                <label>Search:</label>
                <input type="text" id="filter-search" placeholder="Title or author...">
            </div>
        </div>

        <div class="table-container">
            <table id="board-table">
                <thead>
                    <tr>
                        <th data-col="item">Item</th>
                        <th data-col="title">Title</th>
                        <th data-col="assigned">Assigned</th>
                        <th data-col="needs_action">Needs Action</th>
                        <th data-col="board_status">Board</th>
                        <th data-col="status" data-sort="priority">Status</th>
                        <th data-col="author">Author</th>
                        <th data-col="age">Age</th>
                        <th data-col="updated">Updated</th>
                    </tr>
                </thead>
                <tbody>
                </tbody>
            </table>
        </div>
    </div>

    <div id="activity" class="panel">
        {generate_other_activity_panel(other_activity_rows)}
    </div>

    <script>
        const data = {json.dumps(table_rows)};
        // Multi-level sorting: array of {{col, dir}} objects
        // dir can be 'asc', 'desc', or null (unsorted)
        let sortStack = [];

        function render() {{
            const tbody = document.querySelector('#board-table tbody');
            const filterPerson = document.getElementById('filter-person').value.toLowerCase();
            const filterRepo = document.getElementById('filter-repo').value.toLowerCase();
            const filterBoard = document.getElementById('filter-board').value.toLowerCase();
            const filterStatus = document.getElementById('filter-status').value.toLowerCase();
            const filterAction = document.getElementById('filter-action').value.toLowerCase();
            const filterSearch = document.getElementById('filter-search').value.toLowerCase();

            let filtered = data.filter(r => {{
                if (filterPerson) {{
                    const isAssigned = r.champion.toLowerCase() === filterPerson ||
                                       r.reviewer1.toLowerCase() === filterPerson ||
                                       r.reviewer2.toLowerCase() === filterPerson;
                    if (!isAssigned) return false;
                }}
                if (filterRepo && !r.item.toLowerCase().startsWith(filterRepo.toLowerCase())) return false;
                if (filterBoard && r.board_status.toLowerCase() !== filterBoard) return false;
                if (filterStatus && r.status.toLowerCase() !== filterStatus) return false;
                if (filterAction) {{
                    const needsAction = (r.action_required_by || []).some(p => p.toLowerCase() === filterAction);
                    if (!needsAction) return false;
                }}
                if (filterSearch && !r.title.toLowerCase().includes(filterSearch) && !r.author.toLowerCase().includes(filterSearch)) return false;
                return true;
            }});

            // Multi-level sort
            filtered.sort((a, b) => {{
                for (const sort of sortStack) {{
                    const {{ col, dir }} = sort;
                    let aVal, bVal;

                    if (col === 'status') {{
                        aVal = a.status_priority;
                        bVal = b.status_priority;
                    }} else if (col === 'assigned') {{
                        aVal = (a.champion || '').toLowerCase();
                        bVal = (b.champion || '').toLowerCase();
                    }} else if (col === 'needs_action') {{
                        aVal = (a.action_required_by || []).join(',').toLowerCase();
                        bVal = (b.action_required_by || []).join(',').toLowerCase();
                    }} else if (col === 'board_status') {{
                        // Sort high priority first, then alphabetically
                        const aHigh = a.board_status.toLowerCase().includes('high priority') ? 0 : 1;
                        const bHigh = b.board_status.toLowerCase().includes('high priority') ? 0 : 1;
                        if (aHigh !== bHigh) {{
                            aVal = aHigh;
                            bVal = bHigh;
                        }} else {{
                            aVal = a.board_status.toLowerCase();
                            bVal = b.board_status.toLowerCase();
                        }}
                    }} else if (col === 'age') {{
                        // Sort by created date (older = higher value for ascending)
                        aVal = a.created || '9999';
                        bVal = b.created || '9999';
                    }} else {{
                        aVal = (a[col] || '').toLowerCase();
                        bVal = (b[col] || '').toLowerCase();
                    }}

                    if (aVal < bVal) return dir === 'asc' ? -1 : 1;
                    if (aVal > bVal) return dir === 'asc' ? 1 : -1;
                }}
                return 0;
            }});

            tbody.innerHTML = filtered.map((r, idx) => `
                <tr class="expandable" data-idx="${{idx}}">
                    <td><span class="expand-arrow">▶</span> ${{getTypeIcon(r.type)}} <a class="link" href="${{r.url}}" target="_blank">${{r.item}}</a></td>
                    <td><a class="link" href="${{r.url}}" target="_blank">${{escapeHtml(r.title)}}</a></td>
                    <td class="assigned">${{formatAssigned(r)}}</td>
                    <td class="assigned">${{formatNeedsAction(r)}}</td>
                    <td><span class="board-badge ${{getBoardBadgeClass(r.board_status)}}">${{r.board_status}}</span></td>
                    <td><span class="status" style="background: ${{r.status_color}}20; color: ${{r.status_color}}">${{r.status_emoji}} ${{r.status}}</span></td>
                    <td>${{r.author}}</td>
                    <td>${{formatAge(r.created)}}</td>
                    <td>${{r.updated}}</td>
                </tr>
                <tr class="summary-row" data-idx="${{idx}}">
                    <td colspan="9">
                        ${{formatSummaryContent(r)}}
                    </td>
                </tr>
            `).join('');

            // Re-attach expand handlers
            document.querySelectorAll('.expandable').forEach(row => {{
                row.addEventListener('click', (e) => {{
                    if (e.target.tagName === 'A') return; // Don't toggle when clicking links
                    const idx = row.dataset.idx;
                    const isExpanded = row.classList.toggle('expanded');
                    const arrow = row.querySelector('.expand-arrow');
                    arrow.textContent = isExpanded ? '▼' : '▶';
                    document.querySelector(`.summary-row[data-idx="${{idx}}"]`).classList.toggle('visible');
                }});
            }});

            document.querySelector('.count').textContent = filtered.length;
        }}

        function escapeHtml(text) {{
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }}

        function getTypeIcon(type) {{
            if (type === 'PullRequest') {{
                return '<svg class="type-icon pr" viewBox="0 0 16 16" width="16" height="16"><path fill="currentColor" d="M1.5 3.25a2.25 2.25 0 1 1 3 2.122v5.256a2.251 2.251 0 1 1-1.5 0V5.372A2.25 2.25 0 0 1 1.5 3.25Zm5.677-.177L9.573.677A.25.25 0 0 1 10 .854V2.5h1A2.5 2.5 0 0 1 13.5 5v5.628a2.251 2.251 0 1 1-1.5 0V5a1 1 0 0 0-1-1h-1v1.646a.25.25 0 0 1-.427.177L7.177 3.427a.25.25 0 0 1 0-.354ZM3.75 2.5a.75.75 0 1 0 0 1.5.75.75 0 0 0 0-1.5Zm0 9.5a.75.75 0 1 0 0 1.5.75.75 0 0 0 0-1.5Zm8.25.75a.75.75 0 1 0 1.5 0 .75.75 0 0 0-1.5 0Z"></path></svg>';
            }} else {{
                return '<svg class="type-icon issue" viewBox="0 0 16 16" width="16" height="16"><path fill="currentColor" d="M8 9.5a1.5 1.5 0 1 0 0-3 1.5 1.5 0 0 0 0 3Z"></path><path fill="currentColor" d="M8 0a8 8 0 1 1 0 16A8 8 0 0 1 8 0ZM1.5 8a6.5 6.5 0 1 0 13 0 6.5 6.5 0 0 0-13 0Z"></path></svg>';
            }}
        }}

        function formatAge(createdDate) {{
            if (!createdDate) return '-';
            const created = new Date(createdDate);
            const now = new Date();
            const diffMs = now - created;
            const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

            if (diffDays < 1) return 'today';
            if (diffDays === 1) return '1 day';
            if (diffDays < 7) return `${{diffDays}} days`;
            if (diffDays < 14) return '1 week';
            if (diffDays < 30) return `${{Math.floor(diffDays / 7)}} weeks`;
            if (diffDays < 60) return '1 month';
            if (diffDays < 365) return `${{Math.floor(diffDays / 30)}} months`;
            if (diffDays < 730) return '1 year';
            return `${{Math.floor(diffDays / 365)}} years`;
        }}

        function formatAssigned(r) {{
            const parts = [];

            if (r.is_board_item) {{
                // Board items: show champion and reviewers with roles
                if (r.champion) parts.push(`<span class="badge champion">${{r.champion}}</span>`);
                if (r.reviewer1) parts.push(`<span class="badge reviewer">${{r.reviewer1}}</span>`);
                if (r.reviewer2) parts.push(`<span class="badge reviewer">${{r.reviewer2}}</span>`);
                if (r.other_contributors && r.other_contributors.length > 0) {{
                    r.other_contributors.forEach(c => {{
                        parts.push(`<span class="badge contributor">${{c}}</span>`);
                    }});
                }}
            }} else {{
                // Activity items: show involved users in gray with interaction type
                if (r.involved_users && r.involved_users.length > 0) {{
                    r.involved_users.forEach(user => {{
                        const interactions = r.interaction_types[user] || [];
                        const label = interactions.length > 0 ? interactions[0].charAt(0).toUpperCase() : '?';
                        const title = interactions.join(', ');
                        parts.push(`<span class="badge involved" title="${{title}}">${{user}} (${{label}})</span>`);
                    }});
                }}
            }}
            return parts.join(' ') || '<span style="color: #6a737d; font-style: italic;">-</span>';
        }}

        function formatNeedsAction(r) {{
            if (!r.action_required_by || r.action_required_by.length === 0) {{
                return '<span style="color: #6a737d; font-style: italic;">-</span>';
            }}
            return r.action_required_by.map(p => `<span class="badge needs-action">${{p}}</span>`).join(' ');
        }}

        function getBoardBadgeClass(status) {{
            if (!status) return '';
            const s = status.toLowerCase();
            if (s.includes('high priority')) return 'high-priority';
            if (s === 'not included') return 'not-included';
            return '';
        }}

        function getAiStatusClass(status) {{
            if (!status) return '';
            const s = status.toLowerCase();
            if (s.includes('merged')) return 'merged';
            if (s.includes('second review')) return 'second-review';
            if (s.includes('ready')) return 'ready';
            if (s.includes('minor')) return 'minor';
            if (s.includes('progress')) return 'progress';
            if (s.includes('blocked')) return 'blocked';
            if (s.includes('review')) return 'review';
            if (s.includes('stale')) return 'stale';
            if (s.includes('discussion')) return 'discussion';
            return '';
        }}

        function formatSummaryContent(r) {{
            if (!r.summary && !r.ai_status && (!r.action_items || r.action_items.length === 0)) {{
                return '<div class="summary-text"><span class="no-summary">No summary available. Run export_user_items.py to generate.</span></div>';
            }}

            let html = '<div class="summary-text">';

            // AI Status badge
            if (r.ai_status) {{
                html += `<span class="ai-status ${{getAiStatusClass(r.ai_status)}}">${{escapeHtml(r.ai_status)}}</span><br>`;
            }}

            // Summary text
            if (r.summary) {{
                html += escapeHtml(r.summary);
            }}

            html += '</div>';

            // Why needs action (action_reason)
            if (r.action_reason && r.action_required_by && r.action_required_by.length > 0) {{
                html += '<div class="action-reason">';
                html += '<strong>Why needs action:</strong> ' + escapeHtml(r.action_reason);
                html += '</div>';
            }}

            // Action items
            if (r.action_items && r.action_items.length > 0) {{
                html += '<div class="action-items">';
                html += '<div class="action-items-title">Action Items:</div>';
                html += '<ul>';
                r.action_items.forEach(item => {{
                    html += `<li>${{escapeHtml(item)}}</li>`;
                }});
                html += '</ul></div>';
            }}

            return html;
        }}

        // Sort handlers - 3 states: unsorted -> asc -> desc -> unsorted
        function updateSortIndicators() {{
            document.querySelectorAll('#board-table th').forEach(th => {{
                const col = th.dataset.col;
                const idx = sortStack.findIndex(s => s.col === col);
                const indicator = th.querySelector('.sort-indicator');
                const order = th.querySelector('.sort-order');

                if (indicator) indicator.remove();
                if (order) order.remove();

                if (idx !== -1) {{
                    const sort = sortStack[idx];
                    const arrow = document.createElement('span');
                    arrow.className = 'sort-indicator';
                    arrow.textContent = sort.dir === 'asc' ? '▲' : '▼';
                    th.appendChild(arrow);

                    if (sortStack.length > 1) {{
                        const orderSpan = document.createElement('span');
                        orderSpan.className = 'sort-order';
                        orderSpan.textContent = `(${{idx + 1}})`;
                        th.appendChild(orderSpan);
                    }}
                }}
            }});
        }}

        document.querySelectorAll('#board-table th').forEach(th => {{
            th.addEventListener('click', () => {{
                const col = th.dataset.col;
                const idx = sortStack.findIndex(s => s.col === col);

                if (idx === -1) {{
                    // Not in stack - add as ascending
                    sortStack.push({{ col, dir: 'asc' }});
                }} else {{
                    const current = sortStack[idx];
                    if (current.dir === 'asc') {{
                        // asc -> desc
                        current.dir = 'desc';
                    }} else {{
                        // desc -> remove from stack
                        sortStack.splice(idx, 1);
                    }}
                }}

                updateSortIndicators();
                render();
            }});
        }});

        // Filter handlers
        ['filter-person', 'filter-repo', 'filter-board', 'filter-status', 'filter-action', 'filter-search'].forEach(id => {{
            document.getElementById(id).addEventListener('input', render);
            document.getElementById(id).addEventListener('change', render);
        }});

        // Tab handlers
        document.querySelectorAll('.tab').forEach(tab => {{
            tab.addEventListener('click', () => {{
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
                tab.classList.add('active');
                document.getElementById(tab.dataset.tab).classList.add('active');
            }});
        }});

        // Initial render (no default sort)
        render();
    </script>
</body>
</html>
"""
    return html


def generate_other_activity_panel(activity_rows: list[dict]) -> str:
    """Generate HTML for other activity panel with improved styling."""
    if not activity_rows:
        return """
        <div class="no-other-activity">
            <p>All user activity is already shown in the Tracked Items tab.</p>
        </div>
        """

    def format_item(item: dict) -> str:
        repo = item.get("repository", {})
        repo_name = (
            repo.get("nameWithOwner", "") if isinstance(repo, dict) else str(repo)
        )
        repo_short = repo_name.split("/")[-1] if "/" in repo_name else repo_name
        number = item.get("number", "")
        title = item.get("title", "")[:60]
        url = item.get("url", "")

        return f"""
        <div class="other-activity-item">
            <a href="{escape(url)}" target="_blank">{escape(repo_short)}#{number}</a>
            <span class="title">{escape(title)}</span>
        </div>
        """

    cards = []
    for row in activity_rows:
        authored_html = ""
        if row.get("authored_prs"):
            items = "".join(format_item(pr) for pr in row["authored_prs"])
            authored_html = f"""
            <div class="other-activity-section">
                <h4>PRs Authored</h4>
                {items}
            </div>
            """

        reviewed_html = ""
        if row.get("reviewed_prs"):
            items = "".join(format_item(pr) for pr in row["reviewed_prs"])
            reviewed_html = f"""
            <div class="other-activity-section">
                <h4>PRs Reviewed</h4>
                {items}
            </div>
            """

        comments_html = ""
        if row.get("issue_comments"):
            items = "".join(format_item(issue) for issue in row["issue_comments"])
            comments_html = f"""
            <div class="other-activity-section">
                <h4>Commented On</h4>
                {items}
            </div>
            """

        cards.append(f"""
        <div class="other-activity-card">
            <h3><span class="user-badge">{escape(row["user"])}</span></h3>
            <div class="other-activity-stats">
                <div class="other-stat">
                    <div class="other-stat-value">{row["prs_authored"]}</div>
                    <div class="other-stat-label">Authored</div>
                </div>
                <div class="other-stat">
                    <div class="other-stat-value">{row["prs_reviewed"]}</div>
                    <div class="other-stat-label">Reviewed</div>
                </div>
                <div class="other-stat">
                    <div class="other-stat-value">{row["issues_commented"]}</div>
                    <div class="other-stat-label">Commented</div>
                </div>
            </div>
            {authored_html}
            {reviewed_html}
            {comments_html}
        </div>
        """)

    return f'<div class="other-activity-container">{"".join(cards)}</div>'


def save_report(
    output_path: str = "board_summary.html", users: list[str] | None = None
):
    """Generate and save HTML report."""
    html = generate_html_report(users)
    with open(output_path, "w") as f:
        f.write(html)
    print(f"Report saved to {output_path}")
    return output_path


if __name__ == "__main__":
    import sys

    users = sys.argv[1:] if len(sys.argv) > 1 else None
    output = save_report("board_summary.html", users)
    print(f"Open {output} in a browser to view the report")
