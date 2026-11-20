-- one row per published release in the window
-- lag runs over all releases first, so the first in-window release still measures its gap
with gaps as (
    select
        repo,
        id,
        tag_name,
        published_at,
        date_diff('day', lag(published_at) over (partition by repo order by published_at),
                  published_at) as days_since_previous,
        prerelease              as is_prerelease
    from {{ ref('stg_releases') }}
)

select * from gaps
where published_at >= '2024-01-01'
