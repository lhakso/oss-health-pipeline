-- one row per published release; drafts are dropped
select
    repo,
    id,
    payload->>'tag_name'                    as tag_name,
    (payload->>'published_at')::timestamptz as published_at,
    (payload->>'prerelease')::boolean       as prerelease
from {{ source('raw', 'raw_releases') }}
where not (payload->>'draft')::boolean
qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
