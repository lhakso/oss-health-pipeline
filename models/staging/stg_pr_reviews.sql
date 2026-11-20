-- one row per submitted review; PENDING reviews have no submitted_at and are dropped
select
    repo,
    id,
    regexp_extract(payload->>'pull_request_url', '(\d+)$', 1)::integer as pr_number,
    payload->>'$.user.login'               as login,
    coalesce((payload->>'$.user.type' = 'Bot')
             or (payload->>'$.user.login' like '%[bot]'), false) as is_bot,
    payload->>'state'                       as state,
    (payload->>'submitted_at')::timestamptz as submitted_at
from {{ source('raw', 'raw_pr_reviews') }}
where payload->>'state' <> 'PENDING'
qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
