import json
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx

from pipeline.settings import RAW_DIR, TOKEN

PER_PAGE = 100               # GitHub max is 100
QUOTA_BUFFER = 20            # sleep until the reset once fewer than this many requests are left
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github+json"}

GRAPHQL_URL = "https://api.github.com/graphql"
GRAPHQL_PAGE_SIZE = 100      # PRs per query; each comes with up to 100 reviews

requests_made = 0            # API odometer: every request this process sends, retries included
graphql_points = 0           # GraphQL budget spent (5,000 points/hour, separate from REST's 5,000 requests)


# seconds to wait if a 403/429 is a rate limit, else None
#   - secondary limit: honor Retry-After, or wait 60s on a "secondary rate limit" message
#   - primary limit exhausted: wait until X-RateLimit-Reset
def rate_limit_wait(r):
    if r.status_code not in (403, 429):
        return None
    if "retry-after" in r.headers:
        return int(r.headers["retry-after"])
    if r.headers.get("x-ratelimit-remaining") == "0":
        return max(int(r.headers["x-ratelimit-reset"]) - time.time(), 0) + 1
    if "secondary rate limit" in r.text.lower():
        return 60                                   # GitHub's advice when no Retry-After is sent
    return None


# from 3022/04_apis/09_github_pr_vals.py
# GET a URL and watch the quota on every response
#   - rate limited: wait (see rate_limit_wait), then retry
#   - primary budget almost gone: sleep until X-RateLimit-Reset before it reaches zero
#   - anything else (5xx, network errors, real 403s) raises, and Prefect's task retries take over
def get_and_check_quota(url, params=None):
    global requests_made
    while True:
        r = httpx.get(url, headers=HEADERS, params=params, timeout=30)
        requests_made += 1
        remaining = int(r.headers.get("x-ratelimit-remaining", QUOTA_BUFFER))
        reset = int(r.headers.get("x-ratelimit-reset", time.time()))

        wait = rate_limit_wait(r)
        if wait is not None:
            print(f"Rate limited ({r.status_code}), sleeping {wait:.0f}s")
            time.sleep(wait)
            continue

        r.raise_for_status()   # 5xx, or a 403 that isn't a rate limit (e.g. bad token)

        if remaining < QUOTA_BUFFER:
            wait = max(reset - time.time(), 0) + 1
            print(f"{remaining} requests left, sleeping {wait:.0f}s until the quota resets")
            time.sleep(wait)
        return r.json(), r.links


# newest updated_at loaded per (repo, endpoint), from the _load_state table written by load
# returns {} on the very first run, when there is no database yet
def read_watermarks(db_path):
    if not Path(db_path).exists():
        return {}
    with duckdb.connect(str(db_path)) as con:
        if not con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = '_load_state'").fetchone()[0]:
            return {}
        con.execute("SET TimeZone = 'UTC'")
        rows = con.execute("""
            SELECT repo, endpoint, strftime(max_updated_at, '%Y-%m-%dT%H:%M:%SZ') FROM _load_state
        """).fetchall()
    return {(repo, endpoint): ts for repo, endpoint, ts in rows}


# from 3022/04_apis/05_pagination.py
# use the "Link" header with the URL of the next page. httpx parses it into
# response.links. The "next" URL already includes per_page and page, so params are only
# needed on the first request. The loop ends when there is no "next" link (last page).
# resume: a page already on disk is read instead of fetched; its saved next_url says where to go next
# stop_before: for pulls (sorted by updated desc), stop once a page ends before this timestamp
def paginate(repo, endpoint, url, params=None, stop_before=None):
    params = {"per_page": PER_PAGE, **(params or {})}
    page = 1
    total = 0
    while url:
        landed = read_page(repo, endpoint, page)
        if landed:
            records, url = landed["data"], landed["next_url"]
            print(f"--- {repo} {endpoint} page {page}: on disk, skipped ({len(records)} records) ---")
        else:
            records, links = get_and_check_quota(url, params)
            # httpx.URL(url, params=None) would drop the query string, so only add params when there are any
            request_url = str(httpx.URL(url, params=params)) if params else url
            url = links.get("next", {}).get("url")   # None on the last page
            write_page(repo, endpoint, page, request_url, url, records)
            print(f"--- {repo} {endpoint} page {page}: {len(records)} records ---")
        total += len(records)

        # ISO 8601 UTC strings compare correctly as plain strings
        if stop_before and records and records[-1]["updated_at"] < stop_before:
            break

        params = None
        page += 1
    return total


# read the landed pulls pages and fetch reviews for every PR created since `since`
# and updated since `updated_since` (the pulls watermark), so an incremental run only
# calls for new or updated PRs; a new review bumps the PR's updated_at
# each PR gets its own folder (reviews/<number>/) so page numbers don't collide
def fetch_reviews(repo, base_url, since, updated_since):
    pulls_dir = RAW_DIR / repo.replace("/", "__") / "pulls"
    numbers = set()
    for path in sorted(pulls_dir.glob("page_*.json")):
        for pr in json.loads(path.read_text())["data"]:
            if pr["created_at"] >= since and pr["updated_at"] >= updated_since:
                numbers.add(pr["number"])

    total = 0
    for number in sorted(numbers):
        total += paginate(repo, f"reviews/{number}", f"{base_url}/{repo}/pulls/{number}/reviews")
    return len(numbers), total


# one GraphQL query = up to 100 PRs, newest-updated first, each with its reviews
# replaces the pulls pages plus one REST reviews call per PR
PULLS_QUERY = """
query($owner: String!, $name: String!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(first: %d, after: $cursor, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number state createdAt updatedAt closedAt mergedAt
        author { login __typename }
        reviews(first: 100) {
          pageInfo { hasNextPage endCursor }
          nodes { databaseId state submittedAt author { login __typename } }
        }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}
""" % GRAPHQL_PAGE_SIZE

# follow-up for the rare PR with more than 100 reviews: the next 100 of one PR's reviews
REVIEWS_QUERY = """
query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviews(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes { databaseId state submittedAt author { login __typename } }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}
"""


# POST one GraphQL query and watch the point budget in the response's rateLimit field
# GraphQL can report errors with HTTP 200, so the body's "errors" list is checked too
def post_graphql(query, variables):
    global requests_made, graphql_points
    while True:
        r = httpx.post(GRAPHQL_URL, headers=HEADERS, json={"query": query, "variables": variables}, timeout=60)
        requests_made += 1

        wait = rate_limit_wait(r)
        if wait is not None:
            print(f"Rate limited ({r.status_code}), sleeping {wait:.0f}s")
            time.sleep(wait)
            continue
        r.raise_for_status()

        body = r.json()
        errors = body.get("errors", [])
        if any(e.get("type") == "RATE_LIMITED" for e in errors):
            print("GraphQL rate limited, sleeping 60s")
            time.sleep(60)
            continue
        if errors:
            raise RuntimeError(f"GraphQL error: {errors}")   # Prefect's task retries take over

        limit = body["data"]["rateLimit"]
        graphql_points += limit["cost"]
        if limit["remaining"] < QUOTA_BUFFER:
            reset = datetime.fromisoformat(limit["resetAt"].replace("Z", "+00:00")).timestamp()
            wait = max(reset - time.time(), 0) + 1
            print(f"{limit['remaining']} GraphQL points left, sleeping {wait:.0f}s until the reset")
            time.sleep(wait)
        return body


# cursor pagination over a repo's PRs (with reviews) into data/raw/<repo>/graphql_pulls/
# same rules as the REST pulls loop: newest-updated first, stop once a page ends before
# stop_before, skip pages already on disk; each file holds the unmodified GraphQL response
def fetch_pulls_graphql(repo, stop_before):
    owner, name = repo.split("/")
    cursor, page, n_prs, n_reviews = None, 1, 0, 0
    while True:
        landed = read_page(repo, "graphql_pulls", page)
        if landed:
            body, has_next, cursor = landed["data"], landed["has_next"], landed["next_cursor"]
            print(f"--- {repo} graphql_pulls page {page}: on disk, skipped ---")
        else:
            body = post_graphql(PULLS_QUERY, {"owner": owner, "name": name, "cursor": cursor})
            info = body["data"]["repository"]["pullRequests"]["pageInfo"]
            write_page(repo, "graphql_pulls", page, GRAPHQL_URL, None, body,
                       extra={"cursor": cursor, "next_cursor": info["endCursor"], "has_next": info["hasNextPage"]})
            has_next, cursor = info["hasNextPage"], info["endCursor"]
            print(f"--- {repo} graphql_pulls page {page} ---")

        prs = body["data"]["repository"]["pullRequests"]["nodes"]
        n_prs += len(prs)
        for pr in prs:
            n_reviews += len(pr["reviews"]["nodes"])
            if pr["reviews"]["pageInfo"]["hasNextPage"]:
                n_reviews += fetch_more_reviews(repo, pr["number"], pr["reviews"]["pageInfo"]["endCursor"])

        if not has_next or (prs and prs[-1]["updatedAt"] < stop_before):
            break
        page += 1
    return n_prs, n_reviews


# reviews 101+ of one PR, paged with the reviews' own cursor into
# data/raw/<repo>/graphql_reviews/<number>/ (like REST's reviews/<number>/), resumable the same way
def fetch_more_reviews(repo, number, cursor):
    owner, name = repo.split("/")
    endpoint = f"graphql_reviews/{number}"
    page, total, has_next = 1, 0, True
    while has_next:
        landed = read_page(repo, endpoint, page)
        if landed:
            body, has_next, cursor = landed["data"], landed["has_next"], landed["next_cursor"]
        else:
            body = post_graphql(REVIEWS_QUERY, {"owner": owner, "name": name, "number": number, "cursor": cursor})
            info = body["data"]["repository"]["pullRequest"]["reviews"]["pageInfo"]
            write_page(repo, endpoint, page, GRAPHQL_URL, None, body,
                       extra={"cursor": cursor, "next_cursor": info["endCursor"], "has_next": info["hasNextPage"]})
            has_next, cursor = info["hasNextPage"], info["endCursor"]
        total += len(body["data"]["repository"]["pullRequest"]["reviews"]["nodes"])
        page += 1
    print(f"--- {repo}#{number}: {total} more reviews past the first 100 ---")
    return total


def page_path(repo, endpoint, page):
    return RAW_DIR / repo.replace("/", "__") / endpoint / f"page_{page:04d}.json"


# a landed page, or None if it is missing, half-written, or from before next_url /
# next_cursor was saved (any of those is simply fetched again)
def read_page(repo, endpoint, page):
    path = page_path(repo, endpoint, page)
    if not path.exists():
        return None
    try:
        landed = json.loads(path.read_text())
    except json.JSONDecodeError:
        return None
    return landed if "next_url" in landed or "next_cursor" in landed else None


# write one response page to data/raw/<owner>__<repo>/<endpoint>/page_NNNN.json
# writes to a .tmp file first, then renames, so a crash never leaves a half-written page
# extra: more request metadata (GraphQL pages save their cursors here)
def write_page(repo, endpoint, page, url, next_url, records, extra=None):
    path = page_path(repo, endpoint, page)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")

    landed = {
        "repo": repo,
        "endpoint": endpoint,
        "url": url,
        "next_url": next_url,   # extra metadata so a resumed run can skip this page
        "page": page,
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data": records,
    }
    if extra:
        landed.update(extra)
    tmp.write_text(json.dumps(landed))
    tmp.rename(path)
    return path
