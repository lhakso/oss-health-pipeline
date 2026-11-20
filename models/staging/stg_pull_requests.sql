-- one row per pull request
select
    repo,
    (payload->>'number')::integer           as number,
    payload->>'$.user.login'               as login,
    coalesce((payload->>'$.user.type' = 'Bot')
             or (payload->>'$.user.login' like '%[bot]'), false) as is_bot,
    payload->>'state'                       as state,
    (payload->>'created_at')::timestamptz   as created_at,
    (payload->>'updated_at')::timestamptz   as updated_at,
    (payload->>'closed_at')::timestamptz    as closed_at,
    (payload->>'merged_at')::timestamptz    as merged_at
from {{ source('raw', 'raw_pull_requests') }}
qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
