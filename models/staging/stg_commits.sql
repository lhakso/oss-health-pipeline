-- one row per commit; author is the linked GitHub account (null when the email isn't linked)
select
    repo,
    payload->>'sha'                                     as sha,
    payload->>'$.author.login'                         as login,
    coalesce((payload->>'$.author.type' = 'Bot')
             or (payload->>'$.author.login' like '%[bot]'), false) as is_bot,
    (payload->>'$.commit.author.date')::timestamptz as committed_at
from {{ source('raw', 'raw_commits') }}
qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
