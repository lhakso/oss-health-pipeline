import json

import duckdb

# GraphQL names a bot by __typename and a merged PR by state MERGED; REST uses user.type
# and state closed + merged_at. These turn a GraphQL node into the REST record shape,
# so the raw tables and every dbt model are the same whichever API fetched the data
def rest_user(author):
    if author is None:            # deleted account ("ghost")
        return None
    return {"login": author["login"], "type": "Bot" if author["__typename"] == "Bot" else "User"}


def graphql_prs(page):
    return [{
        "number": pr["number"],
        "user": rest_user(pr["author"]),
        "state": "open" if pr["state"] == "OPEN" else "closed",
        "created_at": pr["createdAt"],
        "updated_at": pr["updatedAt"],
        "closed_at": pr["closedAt"],
        "merged_at": pr["mergedAt"],
    } for pr in page["data"]["data"]["repository"]["pullRequests"]["nodes"]]


def graphql_reviews(page):
    return [{
        "id": review["databaseId"],
        "pull_request_url": f"https://api.github.com/repos/{page['repo']}/pulls/{pr['number']}",
        "user": rest_user(review["author"]),
        "state": review["state"],
        "submitted_at": review["submittedAt"],
    } for pr in page["data"]["data"]["repository"]["pullRequests"]["nodes"]
      for review in pr["reviews"]["nodes"]]


# follow-up pages for PRs with more than 100 reviews; the PR number is in the folder name
def graphql_more_reviews(page):
    number = page["endpoint"].split("/")[1]
    return [{
        "id": review["databaseId"],
        "pull_request_url": f"https://api.github.com/repos/{page['repo']}/pulls/{number}",
        "user": rest_user(review["author"]),
        "state": review["state"],
        "submitted_at": review["submittedAt"],
    } for review in page["data"]["data"]["repository"]["pullRequest"]["reviews"]["nodes"]]


def rest_records(page):
    return page["data"]


# endpoint folder → raw table, glob under data/raw/, natural key, the record's timestamp,
# and how to get the records out of a page (REST: the page body; GraphQL: mapped nodes)
# commits/reviews/releases have no updated_at, so the closest real date stands in for it
ENDPOINTS = {
    "commits":  {"table": "raw_commits",       "glob": "*/commits/page_*.json",   "id": "sha",
                 "updated": lambda r: r["commit"]["author"]["date"], "records": rest_records},
    "pulls":    {"table": "raw_pull_requests", "glob": "*/pulls/page_*.json",     "id": "number",
                 "updated": lambda r: r["updated_at"], "records": rest_records},
    "reviews":  {"table": "raw_pr_reviews",    "glob": "*/reviews/*/page_*.json", "id": "id",
                 "updated": lambda r: r.get("submitted_at"), "records": rest_records},   # null for PENDING
    "releases": {"table": "raw_releases",      "glob": "*/releases/page_*.json",  "id": "id",
                 "updated": lambda r: r.get("published_at"), "records": rest_records},   # null for drafts
    # one GraphQL page holds both PRs and their reviews
    "graphql_pulls":   {"table": "raw_pull_requests", "glob": "*/graphql_pulls/page_*.json", "id": "number",
                        "updated": lambda r: r["updated_at"], "records": graphql_prs},
    "graphql_reviews": {"table": "raw_pr_reviews",    "glob": "*/graphql_pulls/page_*.json", "id": "id",
                        "updated": lambda r: r.get("submitted_at"), "records": graphql_reviews},
    "graphql_more_reviews": {"table": "raw_pr_reviews", "glob": "*/graphql_reviews/*/page_*.json", "id": "id",
                             "updated": lambda r: r.get("submitted_at"), "records": graphql_more_reviews},
}

# _load_state rows: one per repo and endpoint, whichever API filled the table
STATE_TABLES = {"commits": "raw_commits", "pulls": "raw_pull_requests",
                "reviews": "raw_pr_reviews", "releases": "raw_releases"}


# create the raw tables (one row per record) and the load-state table, if they don't exist
def create_tables(con):
    for table in STATE_TABLES.values():
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                repo        VARCHAR,
                id          VARCHAR,
                updated_at  TIMESTAMPTZ,
                fetched_at  TIMESTAMPTZ,
                source_file VARCHAR,
                payload     JSON,
                PRIMARY KEY (repo, id)
            )
        """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS _load_state (
            repo           VARCHAR,
            endpoint       VARCHAR,
            max_updated_at TIMESTAMPTZ,
            loaded_at      TIMESTAMPTZ,
            PRIMARY KEY (repo, endpoint)
        )
    """)


# unwrap every page file for one endpoint into one row per record, then upsert on (repo, id)
def load_endpoint(con, raw_dir, cfg):
    rows = {}   # keyed by (repo, id), so a record that landed on two pages is kept once
    for path in sorted(raw_dir.glob(cfg["glob"])):
        page = json.loads(path.read_text())
        for rec in cfg["records"](page):
            row = {
                "repo": page["repo"],
                "id": str(rec[cfg["id"]]),
                "updated_at": cfg["updated"](rec),
                "fetched_at": page["fetched_at"],
                "source_file": str(path),
                "payload": json.dumps(rec),
            }
            key = (row["repo"], row["id"])
            # keep the most recently fetched copy
            if key in rows and rows[key]["fetched_at"] > row["fetched_at"]:
                continue
            rows[key] = row

    if rows:
        con.executemany(f"""
            INSERT OR REPLACE INTO {cfg['table']}
            VALUES ($repo, $id, $updated_at, $fetched_at, $source_file, $payload)
        """, list(rows.values()))
    return len(rows)


# record the newest updated_at per repo and endpoint; extract reads this as its watermark
def update_load_state(con):
    for endpoint, table in STATE_TABLES.items():
        con.execute(f"""
            INSERT OR REPLACE INTO _load_state
            SELECT repo, '{endpoint}', max(updated_at), now()
            FROM {table}
            GROUP BY repo
        """)


def row_counts(con):
    return {table: con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in STATE_TABLES.values()}


# load all landed files, then update _load_state, then touch _LOADED (in that order)
def load_all(raw_dir, db_path):
    with duckdb.connect(str(db_path)) as con:
        create_tables(con)
        before = row_counts(con)
        for cfg in ENDPOINTS.values():
            load_endpoint(con, raw_dir, cfg)
        update_load_state(con)
        after = row_counts(con)
    (raw_dir / "_LOADED").touch()
    return before, after
