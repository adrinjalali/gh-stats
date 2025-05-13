from datetime import datetime

import requests

PROJECTS = [
    "scikit-learn",
    "joblib",
    "fairlearn",
    "skops",
    "cloudpickle",
    "skrub",
    "hazardous",
    "threadpoolctl",
    "loky",
    "imbalanced-learn",
]

PYPI_URL_TEMPLATE = "https://pypi.org/pypi/{project}/json"


def get_last_releases(project: str, n: int = 3) -> list[tuple[str, str]]:
    """Fetch the last n releases for a PyPI project,
    returning (version, date) tuples."""
    url = PYPI_URL_TEMPLATE.format(project=project)
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    releases = data.get("releases", {})
    version_dates = []
    for version, files in releases.items():
        # Use the latest upload time among files for this version
        if not files:
            continue
        latest_time = max(
            (
                f.get("upload_time_iso_8601")
                for f in files
                if f.get("upload_time_iso_8601")
            ),
            default=None,
        )
        if latest_time:
            version_dates.append((version, latest_time))
    # Sort by upload time descending
    version_dates.sort(key=lambda x: x[1], reverse=True)
    return version_dates[:n]


def main():
    for project in PROJECTS:
        print(f"\nProject: {project}")
        try:
            last_releases = get_last_releases(project)
            for version, date in last_releases:
                dt = datetime.fromisoformat(date.replace("Z", "+00:00"))
                print(f"  Version: {version}  Date: {dt:%Y-%m-%d %H:%M:%S %Z}")
        except Exception as e:
            print(f"  Error fetching data: {e}")


if __name__ == "__main__":
    main()
