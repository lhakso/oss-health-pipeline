-- one row per PR created in the window; open PRs are kept and flagged, never given a close date
with prs as (
    select * from {{ ref('stg_pull_requests') }}
    where created_at >= '2024-01-01'
),

-- reviews that count: not by the PR author, not by a bot
reviews as (
    select
        r.repo,
        r.pr_number,
        min(r.submitted_at)          as first_review_at,
        bool_or(r.state = 'APPROVED') as has_approving_review
    from {{ ref('stg_pr_reviews') }} r
    join prs p on r.repo = p.repo and r.pr_number = p.number
    where not r.is_bot and r.login is distinct from p.login
    group by r.repo, r.pr_number
),

joined as (
    select
        p.*,
        p.merged_at is not null                          as is_merged,
        p.state = 'closed' and p.merged_at is null       as is_closed_unmerged,
        p.state = 'open'                                 as is_open,
        r.first_review_at,
        date_diff('second', p.created_at, r.first_review_at) / 3600.0 as hours_to_first_review,
        date_diff('second', p.created_at, p.merged_at) / 3600.0       as hours_to_merge,
        date_diff('second', p.created_at, p.closed_at) / 3600.0       as hours_to_close,
        coalesce(r.has_approving_review, false)          as has_approving_review
    from prs p
    left join reviews r on p.repo = r.repo and p.number = r.pr_number
)

select
    *,
    coalesce(hours_to_first_review <= 168, false)               as reviewed_within_7d,
    -- a PR opened in the last 7 days hasn't had its 7 days yet, so it can't count as a miss
    created_at <= {{ run_time() }} - interval 7 days             as review_window_closed,
    -- the 6 complete months before the run month; responsiveness is scored on these PRs
    created_at >= date_trunc('month', {{ run_time() }}) - interval 6 months
        and created_at < date_trunc('month', {{ run_time() }})   as in_recent_window,
    is_open and updated_at < {{ run_time() }} - interval 180 days as is_stale
from joined
