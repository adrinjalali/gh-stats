# GitHub Analytics Tools

This project provides tools for analyzing GitHub repository data, including issues, pull requests, and other metrics.

## Setup

1. Install pixi if you haven't already:
```bash
curl -fsSL https://pixi.sh/install.sh | bash
```

2. Set up your GitHub token:
Create a GitHub Personal Access Token with the following permissions:
- `repo` (Full control of private repositories)
- `read:org` (Read organization data)

Then create a `.env` file in the project root with:
```bash
GITHUB_TOKEN=your_token_here
```

3. Install dependencies:
```bash
pixi install
```

## Usage

To fetch closed issues and merged pull requests from scikit-learn:
```bash
pixi run start
```

The script will:
1. Fetch all closed issues and merged pull requests
2. Display a summary in the terminal
3. Save detailed data to CSV files (`issues.csv` and `pull_requests.csv`)
