-- one row per repo; every column is a rate, share, median, or slope, never a raw count
with prs as (
    select repo,
           -- responsiveness is scored on recent PRs only (6 complete months before the run month),
           -- so it describes the repo now, not averaged over the whole window;
           -- PRs younger than 7 days haven't had a full week to be reviewed yet
           avg(reviewed_within_7d::integer)
               filter (where in_recent_window and review_window_closed) as pct_reviewed_within_7d,
           median(hours_to_first_review) filter (where in_recent_window) as median_hours_to_first_review,
           -- backlog is already a snapshot at run time: share of open PRs with no activity
           -- in 180+ days; 0 when nothing is open
           coalesce(avg(is_stale::integer) filter (where is_open), 0) as pct_open_stale
    from {{ ref('fct_pull_requests') }}
    group by repo
),

monthly as (
    select repo,
           -- month_index 0 = January 2024; the partial run month is left out of the trend
           -- dividing by the repo's own mean turns the slope into % change per month
           regr_slope(commits, date_diff('month', date '2024-01-01', month))
               filter (where not is_partial_month)
               / nullif(avg(commits) filter (where not is_partial_month), 0) as commit_trend
    from {{ ref('mart_repo_monthly') }}
    group by repo
)

select
    m.repo,
    p.pct_reviewed_within_7d,
    p.median_hours_to_first_review,
    p.pct_open_stale,
    m.commit_trend
from monthly m
left join prs p using (repo)
order by m.repo
