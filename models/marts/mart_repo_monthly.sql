-- repo × month from January 2024 through the run month; months with no activity show 0
with months as (
    select unnest(generate_series(date '2024-01-01',
                                  date_trunc('month', {{ run_time() }})::date,
                                  interval 1 month))::date as month
),

spine as (
    select r.repo, m.month
    from (select distinct repo from {{ ref('stg_pull_requests') }}) r
    cross join months m
),

-- each event counts in the month it happened
opened as (
    select repo, date_trunc('month', created_at)::date as month,
           count(*)                                   as prs_opened,
           avg(reviewed_within_7d::integer) filter (where review_window_closed) as pct_reviewed_within_7d,
           median(hours_to_first_review)              as median_first_review_hours,
           avg(is_open::integer)                      as pct_still_open,
           count(distinct login) filter (where not is_bot) as active_contributors
    from {{ ref('fct_pull_requests') }}
    group by all
),

merged as (
    select repo, date_trunc('month', merged_at)::date as month,
           count(*)               as prs_merged,
           median(hours_to_merge) as median_merge_hours
    from {{ ref('fct_pull_requests') }}
    where is_merged
    group by all
),

closed_unmerged as (
    select repo, date_trunc('month', closed_at)::date as month,
           count(*) as prs_closed_unmerged
    from {{ ref('fct_pull_requests') }}
    where is_closed_unmerged
    group by all
),

commits as (
    select repo, date_trunc('month', committed_at)::date as month,
           count(*) as commits
    from {{ ref('stg_commits') }}
    where committed_at >= '2024-01-01'
    group by all
)

select
    s.repo,
    s.month,
    coalesce(o.prs_opened, 0)           as prs_opened,
    coalesce(m.prs_merged, 0)           as prs_merged,
    coalesce(c.prs_closed_unmerged, 0)  as prs_closed_unmerged,
    coalesce(m.prs_merged, 0)
        / nullif(coalesce(m.prs_merged, 0) + coalesce(c.prs_closed_unmerged, 0), 0) as merge_ratio,
    o.pct_reviewed_within_7d,
    o.median_first_review_hours,
    o.pct_still_open,
    m.median_merge_hours,
    coalesce(k.commits, 0)              as commits,
    coalesce(o.active_contributors, 0)  as active_contributors,
    s.month = date_trunc('month', {{ run_time() }})::date as is_partial_month
from spine s
left join opened o          using (repo, month)
left join merged m          using (repo, month)
left join closed_unmerged c using (repo, month)
left join commits k         using (repo, month)
order by s.repo, s.month
