-- fails if any review was submitted before its PR was created
select r.repo, r.pr_number, r.id, r.submitted_at, p.created_at
from {{ ref('stg_pr_reviews') }} r
join {{ ref('stg_pull_requests') }} p
  on r.repo = p.repo and r.pr_number = p.number
where r.submitted_at < p.created_at
