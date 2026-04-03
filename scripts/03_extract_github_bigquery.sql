-- =============================================================================
-- BigQuery SQL to extract GitHub Archive data
-- Run this in BigQuery console, then export results to CSV
-- =============================================================================

-- Monthly aggregated GitHub activity by language and event type
-- This is the practical approach: pre-aggregate in BQ, export summary to Snowflake
-- Covers 2021-01 through 2025-12

CREATE OR REPLACE TABLE `your_project.github_archive.monthly_activity` AS
WITH events AS (
  SELECT
    DATE_TRUNC(created_at, MONTH) AS activity_month,
    type AS event_type,
    -- Extract repo language from the payload or repo metadata
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.base.repo.language') AS pr_language,
    repo.name AS repo_name,
    actor.login AS actor_login,
    -- For PushEvents, count commits
    CASE WHEN type = 'PushEvent'
      THEN ARRAY_LENGTH(JSON_EXTRACT_ARRAY(payload, '$.commits'))
      ELSE 0
    END AS commit_count
  FROM `githubarchive.day.20*`
  WHERE _TABLE_SUFFIX BETWEEN '210101' AND '251231'
    AND type IN (
      'PushEvent', 'PullRequestEvent', 'IssuesEvent',
      'CreateEvent', 'ForkEvent', 'WatchEvent',
      'PullRequestReviewEvent', 'ReleaseEvent'
    )
)
SELECT
  activity_month,
  COALESCE(pr_language, 'Unknown') AS repo_language,
  event_type,
  COUNT(*) AS event_count,
  COUNT(DISTINCT actor_login) AS unique_actors,
  COUNT(DISTINCT repo_name) AS unique_repos,
  SUM(commit_count) AS total_commits
FROM events
GROUP BY 1, 2, 3
HAVING event_count >= 10  -- filter noise
ORDER BY 1, 2, 3;

-- Export: BigQuery Console > Save Results > CSV > google cloud storage or local
-- Then save as: data/github/monthly_activity.csv
