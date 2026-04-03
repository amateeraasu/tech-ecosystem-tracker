"""
Collect GitHub activity data via REST API and load into Snowflake.

Uses the GitHub Search API to get real repo counts, push activity,
PR/issue volume, and star metrics by language over time (2021-2025).
Then distributes yearly data into monthly rows for raw_github_monthly_activity.
"""

import os
import sys
import time
import math
import snowflake.connector
import requests
from datetime import date
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
}

# Rate limit: GitHub Search API allows 30 requests/minute for authenticated users
SEARCH_RATE_LIMIT = 30
SEARCH_INTERVAL = 60.0 / SEARCH_RATE_LIMIT + 0.1  # ~2.1 seconds between requests

LANGUAGES = [
    "JavaScript", "Python", "TypeScript", "Java", "C#", "C++",
    "PHP", "Ruby", "Go", "Rust", "Swift", "Kotlin", "Dart",
    "Scala", "R", "Shell", "Lua", "Elixir", "Haskell", "Zig",
]

YEARS = [2021, 2022, 2023, 2024, 2025]

# Typical event type distribution ratios (from GH Archive research)
# We'll use search data to anchor PushEvent and CreateEvent, then estimate others
EVENT_RATIOS = {
    "PushEvent": 0.45,
    "PullRequestEvent": 0.18,
    "IssuesEvent": 0.08,
    "CreateEvent": 0.12,
    "ForkEvent": 0.07,
    "WatchEvent": 0.10,
}

request_count = 0
last_request_time = 0


def rate_limited_get(url, params=None):
    """Make a rate-limited GET request to GitHub API."""
    global request_count, last_request_time

    elapsed = time.time() - last_request_time
    if elapsed < SEARCH_INTERVAL:
        time.sleep(SEARCH_INTERVAL - elapsed)

    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    last_request_time = time.time()
    request_count += 1

    # Handle rate limiting
    if resp.status_code == 403:
        remaining = int(resp.headers.get("X-RateLimit-Remaining", 0))
        if remaining == 0:
            reset_time = int(resp.headers.get("X-RateLimit-Reset", 0))
            wait = max(reset_time - time.time(), 10)
            print(f"  Rate limited. Waiting {wait:.0f}s...")
            time.sleep(wait + 1)
            return rate_limited_get(url, params)

    if resp.status_code == 422:
        # Validation error — often means query too broad; return 0
        return {"total_count": 0, "items": []}

    resp.raise_for_status()
    return resp.json()


def search_repos(language, date_start, date_end, sort="updated", per_page=5):
    """Search GitHub repos by language and date range. Returns total_count + top items."""
    q = f"language:{language} pushed:{date_start}..{date_end}"
    data = rate_limited_get(
        "https://api.github.com/search/repositories",
        params={"q": q, "sort": sort, "order": "desc", "per_page": per_page},
    )
    return data


def search_repos_created(language, date_start, date_end, per_page=5):
    """Count repos created in a date range for a language."""
    q = f"language:{language} created:{date_start}..{date_end}"
    data = rate_limited_get(
        "https://api.github.com/search/repositories",
        params={"q": q, "sort": "stars", "order": "desc", "per_page": per_page},
    )
    return data


def search_issues(language, issue_type, date_start, date_end):
    """Count issues or PRs for repos of a given language in a date range."""
    q = f"language:{language} type:{issue_type} created:{date_start}..{date_end}"
    data = rate_limited_get(
        "https://api.github.com/search/issues",
        params={"q": q, "per_page": 1},
    )
    return data


def collect_language_year(language, year):
    """Collect all metrics for a language in a given year."""
    start = f"{year}-01-01"
    end = f"{year}-12-31"

    print(f"  [{language} {year}] Searching...", end=" ", flush=True)

    # 1. Repos with push activity (proxy for overall activity)
    pushed = search_repos(language, start, end, per_page=10)
    repos_pushed = pushed.get("total_count", 0)

    # Extract avg stars from top results
    items = pushed.get("items", [])
    avg_stars = 0
    avg_forks = 0
    if items:
        avg_stars = sum(i.get("stargazers_count", 0) for i in items) / len(items)
        avg_forks = sum(i.get("forks_count", 0) for i in items) / len(items)

    # 2. Repos created (proxy for CreateEvent)
    created = search_repos_created(language, start, end, per_page=1)
    repos_created = created.get("total_count", 0)

    # 3. PRs created (proxy for PullRequestEvent)
    prs = search_issues(language, "pr", start, end)
    pr_count = prs.get("total_count", 0)

    # 4. Issues created (proxy for IssuesEvent)
    issues = search_issues(language, "issue", start, end)
    issue_count = issues.get("total_count", 0)

    print(
        f"pushed={repos_pushed:,} created={repos_created:,} "
        f"PRs={pr_count:,} issues={issue_count:,} "
        f"avg_stars={avg_stars:.0f}",
        flush=True,
    )

    return {
        "repos_pushed": repos_pushed,
        "repos_created": repos_created,
        "pr_count": pr_count,
        "issue_count": issue_count,
        "avg_stars": avg_stars,
        "avg_forks": avg_forks,
    }


def distribute_to_monthly(language, year, metrics):
    """Convert yearly metrics into monthly rows for raw_github_monthly_activity."""
    rows = []
    months_in_year = 12 if year < 2025 else 11  # 2025 data through November

    # Total activity estimate based on real data
    total_push_events = metrics["repos_pushed"] * 8  # ~8 pushes per active repo/year
    total_create_events = metrics["repos_created"]
    total_pr_events = metrics["pr_count"]
    total_issue_events = metrics["issue_count"]
    total_fork_events = int(metrics["avg_forks"] * metrics["repos_created"] * 0.05)
    total_watch_events = int(total_push_events * EVENT_RATIOS["WatchEvent"] / EVENT_RATIOS["PushEvent"])

    event_totals = {
        "PushEvent": total_push_events,
        "CreateEvent": total_create_events,
        "PullRequestEvent": total_pr_events,
        "IssuesEvent": total_issue_events,
        "ForkEvent": total_fork_events,
        "WatchEvent": total_watch_events,
    }

    for month in range(1, months_in_year + 1):
        activity_month = date(year, month, 1).isoformat()
        for event_type, yearly_total in event_totals.items():
            monthly_count = max(1, yearly_total // months_in_year)

            # Estimate unique actors/repos from event counts
            if event_type == "PushEvent":
                unique_actors = max(1, monthly_count // 10)
                unique_repos = max(1, metrics["repos_pushed"] // months_in_year)
                total_commits = monthly_count * 3
            elif event_type == "PullRequestEvent":
                unique_actors = max(1, monthly_count // 5)
                unique_repos = max(1, unique_actors // 2)
                total_commits = 0
            elif event_type == "CreateEvent":
                unique_actors = max(1, monthly_count // 2)
                unique_repos = monthly_count
                total_commits = 0
            else:
                unique_actors = max(1, monthly_count // 8)
                unique_repos = max(1, monthly_count // 12)
                total_commits = 0

            rows.append((
                activity_month,
                language,
                event_type,
                monthly_count,
                unique_actors,
                unique_repos,
                total_commits,
                0,  # total_additions
                0,  # total_deletions
                round(metrics["avg_stars"], 1),
            ))

    return rows


def load_to_snowflake(all_rows):
    """Load collected data into Snowflake raw_github_monthly_activity."""
    print(f"\nConnecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database="TECH_ECOSYSTEM",
        schema="RAW",
        warehouse="TECH_WH",
    )
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE IF EXISTS raw_github_monthly_activity")
    print(f"Truncated raw_github_monthly_activity")

    sql = """INSERT INTO raw_github_monthly_activity
        (activity_month, repo_language, event_type, event_count, unique_actors,
         unique_repos, total_commits, total_additions, total_deletions, avg_repo_stars)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    batch_size = 5000
    for i in range(0, len(all_rows), batch_size):
        cursor.executemany(sql, all_rows[i : i + batch_size])

    conn.commit()

    # Verify
    cursor.execute(
        """SELECT repo_language, SUM(event_count) AS events
           FROM raw_github_monthly_activity
           GROUP BY 1 ORDER BY 2 DESC"""
    )
    print(f"\nLoaded {len(all_rows):,} rows. Events by language:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]:,}")

    cursor.execute(
        """SELECT EXTRACT(YEAR FROM activity_month) AS yr, COUNT(*), SUM(event_count)
           FROM raw_github_monthly_activity GROUP BY 1 ORDER BY 1"""
    )
    print(f"\nBy year:")
    for row in cursor.fetchall():
        print(f"  {int(row[0])}: {row[1]:,} rows, {row[2]:,} events")

    cursor.close()
    conn.close()


def main():
    print("=" * 60)
    print("GitHub API Data Collection")
    print(f"Languages: {len(LANGUAGES)}, Years: {YEARS}")
    total_calls = len(LANGUAGES) * len(YEARS) * 4
    print(f"Estimated API calls: {total_calls} (~{total_calls * SEARCH_INTERVAL / 60:.0f} min)")
    print("=" * 60)

    all_rows = []

    for lang in LANGUAGES:
        print(f"\n--- {lang} ---")
        for year in YEARS:
            try:
                metrics = collect_language_year(lang, year)
                rows = distribute_to_monthly(lang, year, metrics)
                all_rows.extend(rows)
            except Exception as e:
                print(f"  ERROR [{lang} {year}]: {e}")
                continue

    print(f"\n{'='*60}")
    print(f"Collection complete. {len(all_rows):,} monthly rows generated.")
    print(f"Total API calls: {request_count}")

    if all_rows:
        load_to_snowflake(all_rows)

    print("\nDone!")


if __name__ == "__main__":
    main()
