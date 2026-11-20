import json

import duckdb

from pipeline.load import ENDPOINTS

# raw table → staging model, plus the rows staging drops on purpose
# (PENDING reviews have no submitted_at; draft releases were never published)
STAGING = {
    "raw_commits":       {"model": "stg_commits",       "dropped": "false"},
    "raw_pull_requests": {"model": "stg_pull_requests", "dropped": "false"},
    "raw_pr_reviews":    {"model": "stg_pr_reviews",    "dropped": "payload->>'state' = 'PENDING'"},
    "raw_releases":      {"model": "stg_releases",      "dropped": "(payload->>'draft')::boolean"},
}

# outlier cutoffs
# merge time is so right-skewed that an IQR fence flags ~10% of PRs (just the normal tail),
# so it uses a fixed cutoff instead: merged more than a year after it was opened
MERGE_CUTOFF_DAYS = 365
IQR_MULTIPLIER = 3   # weekly commits: above Q3 + 3·IQR (Tukey's "far out" fence)


# every distinct (repo, id) in the landed JSON must be in the raw table,
# and every raw row (minus the documented drops) must be in its staging model
def check_counts(con, raw_dir):
    failures = []
    rows = []
    for endpoint, cfg in ENDPOINTS.items():
        landed = {}   # repo → set of ids
        for path in raw_dir.glob(cfg["glob"]):
            page = json.loads(path.read_text())
            ids = landed.setdefault(page["repo"], set())
            ids.update(str(rec[cfg["id"]]) for rec in cfg["records"](page))

        stg = STAGING[cfg["table"]]
        for repo, ids in sorted(landed.items()):
            raw_ids = {r[0] for r in con.execute(
                f"SELECT id FROM {cfg['table']} WHERE repo = ?", [repo]).fetchall()}
            expected_stg = con.execute(
                f"SELECT count(*) FROM {cfg['table']} WHERE repo = ? AND NOT ({stg['dropped']})",
                [repo]).fetchone()[0]
            stg_rows = con.execute(
                f"SELECT count(*) FROM {stg['model']} WHERE repo = ?", [repo]).fetchone()[0]

            missing = ids - raw_ids
            if missing:
                failures.append(f"{repo} {endpoint}: {len(missing)} landed records missing from {cfg['table']}")
            if stg_rows != expected_stg:
                failures.append(f"{repo} {endpoint}: {stg['model']} has {stg_rows} rows, expected {expected_stg}")
            rows.append((repo, endpoint, len(ids), len(raw_ids), expected_stg, stg_rows))
    return rows, failures


# every repo needs commits in every complete month since window_start (the run month is exempt)
def check_monthly_commits(con, window_start):
    # commits are fetched newest first, so lost pages show up as empty months at the old end
    # or in the middle; empty months after a repo's latest commit mean the repo went quiet
    # (a real finding, logged as a warning) and can't be caused by a lost page
    empty = con.execute("""
        WITH last_commit AS (
            SELECT repo, date_trunc('month', max(committed_at))::date AS last_month
            FROM stg_commits GROUP BY repo
        )
        SELECT m.repo, m.month, m.month > l.last_month AS after_last_commit
        FROM mart_repo_monthly m JOIN last_commit l USING (repo)
        WHERE NOT m.is_partial_month AND m.month >= date_trunc('month', ?::date) AND m.commits = 0
        ORDER BY m.repo, m.month
    """, [window_start]).fetchall()
    failures = [f"{repo}: no commits in {month:%Y-%m}" for repo, month, quiet in empty if not quiet]
    warnings = [f"{repo}: no commits in {month:%Y-%m}, after its latest commit (repo went quiet)"
                for repo, month, quiet in empty if quiet]
    return failures, warnings


# merge time from the fact table, flagged above MERGE_CUTOFF_DAYS
# weekly commits counted from staging (bots included), flagged above Q3 + 3·IQR per repo
def find_outliers(con, window_start):
    merge = con.execute(f"""
        SELECT repo, number, created_at::date AS created, merged_at::date AS merged,
               round(hours_to_merge / 24) AS days_to_merge
        FROM fct_pull_requests
        WHERE hours_to_merge > {MERGE_CUTOFF_DAYS} * 24
        ORDER BY repo, hours_to_merge DESC
    """).df()

    commits = con.execute(f"""
        WITH weekly AS (
            SELECT repo, date_trunc('week', committed_at) AS week,
                   count(*) AS commits, count(*) FILTER (WHERE is_bot) AS bot_commits
            FROM stg_commits
            WHERE committed_at >= ?::timestamptz
            GROUP BY repo, week
        ),
        fences AS (
            SELECT repo,
                   quantile_cont(commits, 0.75)
                     + {IQR_MULTIPLIER} * (quantile_cont(commits, 0.75) - quantile_cont(commits, 0.25)) AS cutoff
            FROM weekly
            GROUP BY repo
        )
        SELECT w.repo, w.week::date AS week, w.commits, w.bot_commits, round(f.cutoff, 1) AS cutoff
        FROM weekly w JOIN fences f USING (repo)
        WHERE w.commits > f.cutoff
        ORDER BY w.repo, w.week
    """, [window_start]).df()
    return merge, commits


# per repo: median and p90 time to first review and to merge, commits per week, releases per year
# durations come from fct_pull_requests as computed by dbt, never recomputed here
def describe(con, window_start):
    return con.execute("""
        WITH days AS (SELECT date_diff('day', ?::date, (SELECT max(committed_at) FROM stg_commits)::date) AS n),
        prs AS (
            SELECT repo,
                   median(hours_to_first_review)                 AS median_first_review_h,
                   quantile_cont(hours_to_first_review, 0.9)     AS p90_first_review_h,
                   median(hours_to_merge)                        AS median_merge_h,
                   quantile_cont(hours_to_merge, 0.9)            AS p90_merge_h
            FROM fct_pull_requests
            WHERE created_at >= ?::timestamptz
            GROUP BY repo
        ),
        commits AS (
            SELECT repo, sum(commits) AS n FROM mart_repo_monthly
            WHERE month >= date_trunc('month', ?::date) GROUP BY repo
        ),
        releases AS (
            SELECT repo, count(*) AS n FROM fct_releases
            WHERE published_at >= ?::timestamptz GROUP BY repo
        )
        SELECT p.repo,
               round(p.median_first_review_h, 1) AS median_first_review_h,
               round(p.p90_first_review_h, 1)    AS p90_first_review_h,
               round(p.median_merge_h, 1)        AS median_merge_h,
               round(p.p90_merge_h, 1)           AS p90_merge_h,
               round(c.n / (d.n / 7.0), 1)                         AS commits_per_week,
               round(coalesce(r.n, 0) / (d.n / 365.25), 1)         AS releases_per_year
        FROM prs p
        CROSS JOIN days d
        LEFT JOIN commits c  USING (repo)
        LEFT JOIN releases r USING (repo)
        ORDER BY p.repo
    """, [window_start] * 4).df()


def validate_all(raw_dir, db_path, window_start):
    # not read_only: dbt's connection from the dbt step is still open in this process, and DuckDB
    # refuses a second connection to the same file with a different configuration
    with duckdb.connect(str(db_path)) as con:
        counts, failures = check_counts(con, raw_dir)
        month_failures, warnings = check_monthly_commits(con, window_start)
        failures += month_failures
        merge_outliers, commit_outliers = find_outliers(con, window_start)
        stats = describe(con, window_start)
    return counts, failures, warnings, merge_outliers, commit_outliers, stats
