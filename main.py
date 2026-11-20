import functools
import logging
import os
import shutil
import sys
import time

import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository
from prefect.schedules import Cron
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

import pipeline.extract as extract_api   # read extract_api.requests_made live, not a copy at import
from pipeline.extract import fetch_reviews, paginate, read_watermarks
from pipeline.load import load_all
from pipeline.plots import make_plots
from pipeline.scoring import score_repos
from pipeline.settings import DB_PATH, LOG_DIR, OUTPUT_DIR, PAT_BLOCK, PLOTS_DIR, PROJECT_DIR, RAW_DIR, TARGET_DIR
from pipeline.validate import IQR_MULTIPLIER, MERGE_CUTOFF_DAYS, validate_all

LOADED_MARKER = RAW_DIR / "_LOADED"

REPOS = ["scikit-learn/scikit-learn", "dask/dask", "plotly/plotly.py"]
SINCE = "2024-01-01T00:00:00Z"                                           # analysis window start
USE_GRAPHQL = True   # PRs + reviews via one GraphQL query per 100 PRs; False = REST (one reviews call per PR)

# used only by `main.py deploy`; override with env vars to deploy your own fork or work pool
GIT_URL = os.environ.get("GIT_URL", "https://github.com/lhakso/oss-health-pipeline.git")
WORK_POOL = os.environ.get("WORK_POOL", "k8s-pool")

BASE_URL = "https://api.github.com/repos"


# also write a task's Prefect log to logs/<task name>.log (appended, so earlier runs are kept)
# the handler is removed when the task ends so tasks don't write into each other's files
def log_to_file(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        logger = get_run_logger()
        LOG_DIR.mkdir(exist_ok=True)
        handler = logging.FileHandler(LOG_DIR / f"{fn.__name__}.log")
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s"))
        logger.logger.addHandler(handler)
        try:
            return fn(*args, **kwargs)
        except Exception:
            logger.exception(f"{fn.__name__} failed")
            raise
        finally:
            logger.logger.removeHandler(handler)
            handler.close()
    return wrapper


# retries cover 5xx and network errors; a retry finds no _LOADED marker, so it resumes
# and skips every page the failed attempt already landed
@task(retries=3, retry_delay_seconds=[30, 60, 120], log_prints=True)   # backoff: each retry waits longer   # print() in pipeline/extract.py goes to the logger too
@log_to_file
def extract():
    """Extract: fetch all endpoints for all repos into data/raw/."""
    logger = get_run_logger()
    start = time.time()

    # _LOADED present: everything on disk is safe in DuckDB, so start fresh
    # no _LOADED but files on disk: the last run stopped early, so resume
    if LOADED_MARKER.exists():
        logger.info("_LOADED found: deleting data/raw/ and fetching only what's new")
        shutil.rmtree(RAW_DIR)
    elif RAW_DIR.exists():
        logger.info("No _LOADED marker: resuming, pages already on disk are skipped")
    else:
        logger.info("First run: fetching the full window")

    # where each repo/endpoint left off; falls back to the window start on the first run
    watermarks = read_watermarks(DB_PATH)

    for repo in REPOS:
        commits_since = watermarks.get((repo, "commits"), SINCE)
        pulls_since = watermarks.get((repo, "pulls"), SINCE)
        logger.info(f"{repo}: commits since {commits_since}, pulls updated since {pulls_since}")

        n = paginate(repo, "commits", f"{BASE_URL}/{repo}/commits", {"since": commits_since})
        logger.info(f"{repo} commits: {n} records")

        # PRs + reviews: timed and counted on their own for the REST vs. GraphQL comparison
        pr_start, pr_requests = time.time(), extract_api.requests_made
        if USE_GRAPHQL:
            prs, n = extract_api.fetch_pulls_graphql(repo, pulls_since)
            logger.info(f"{repo} pulls: {prs} records, reviews: {n} records (GraphQL)")
        else:
            n = paginate(repo, "pulls", f"{BASE_URL}/{repo}/pulls",
                         {"state": "all", "sort": "updated", "direction": "desc"}, stop_before=pulls_since)
            logger.info(f"{repo} pulls: {n} records")

            # reviews for PRs created in the window and updated since the watermark
            prs, n = fetch_reviews(repo, BASE_URL, SINCE, pulls_since)
            logger.info(f"{repo} reviews: {n} records across {prs} PRs")
        logger.info(f"{repo} PRs + reviews via {'GraphQL' if USE_GRAPHQL else 'REST'}: "
                    f"{extract_api.requests_made - pr_requests} requests in {time.time() - pr_start:.0f}s")

        n = paginate(repo, "releases", f"{BASE_URL}/{repo}/releases")
        logger.info(f"{repo} releases: {n} records")

    logger.info(f"API requests this run: {extract_api.requests_made} in {time.time() - start:.0f}s"
                f" (GraphQL points spent: {extract_api.graphql_points})")
    return extract_api.requests_made


@task
@log_to_file
def load():
    """Load: upsert landed JSON into raw_* tables, update _load_state, touch _LOADED."""
    logger = get_run_logger()
    before, after = load_all(RAW_DIR, DB_PATH)
    for table in after:
        logger.info(f"{table}: {before[table]} rows before, {after[table]} after")
    return after


@task
@log_to_file
def dbt_build():
    """dbt: run dbt build; fail on any model or test failure."""
    logger = get_run_logger()
    runner = PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=str(PROJECT_DIR),
            profiles_dir=str(PROJECT_DIR),
            target_path=TARGET_DIR,
        )
    )
    result = runner.invoke(["build"])
    if not result.success:
        raise RuntimeError("dbt build failed; see the dbt log above")
    logger.info("dbt build passed")
    return result.success


@task
@log_to_file
def validate():
    """Validate: completeness checks, outlier detection, descriptive stats (no dbt)."""
    logger = get_run_logger()
    counts, failures, warnings, merge_outliers, commit_outliers, stats = validate_all(RAW_DIR, DB_PATH, SINCE)

    counts = pd.DataFrame(counts, columns=["repo", "endpoint", "landed", "raw", "stg_expected", "stg"])
    logger.info(f"Completeness (landed → raw → staging):\n{counts.to_string(index=False)}")
    for warning in warnings:
        logger.warning(warning)

    # outliers are kept: they are real PRs and real weeks, and every scored metric is a
    # median or share, so a few extreme values can't move the score much
    logger.info(f"Merge-time outliers (> {MERGE_CUTOFF_DAYS} days, kept): {len(merge_outliers)}\n"
                f"{merge_outliers.to_string(index=False)}")
    logger.info(f"Weekly-commit outliers (> Q3 + {IQR_MULTIPLIER}·IQR commits, kept): {len(commit_outliers)}\n"
                f"{commit_outliers.to_string(index=False)}")
    logger.info(f"Descriptive statistics (hours):\n{stats.to_string(index=False)}")

    if failures:
        raise ValueError("Validation failed:\n" + "\n".join(failures))
    logger.info("All completeness checks passed")
    return stats


@task
@log_to_file
def model_and_rank():
    """Score: scale and weight metrics from mart_health_inputs, rank repos, write results to DB."""
    logger = get_run_logger()
    scores, sensitivity = score_repos(DB_PATH)
    logger.info("Health ranking (written to health_scores; score_low/high = 95% bootstrap interval, "
                f"pct_rank_held = share of resamples with the same rank):\n{scores.to_string(index=False)}")
    logger.info(f"Ranking under other weightings (written to health_weight_sensitivity):\n"
                f"{sensitivity.to_string(index=False)}")
    return scores


@task
@log_to_file
def plot():
    """Plot: save dimension profile, monthly trends, and relationship plots to plots/."""
    logger = get_run_logger()
    for path in make_plots(DB_PATH, PLOTS_DIR):
        logger.info(f"Saved {path.relative_to(OUTPUT_DIR)}")


# each task is timed on its own and the flow total is logged at the end (also to logs/package_health_flow.log)
@flow(name="oss-health-pipeline")
@log_to_file
def package_health_flow():
    logger = get_run_logger()
    start = time.time()
    for step in (extract, load, dbt_build, validate, model_and_rank, plot):
        step_start = time.time()
        step()
        logger.info(f"{step.name} finished in {time.time() - step_start:.0f}s")
    logger.info(f"Flow finished in {time.time() - start:.0f}s")


# register the flow with a Prefect server: a worker clones the repo and runs it at
# midnight Eastern on the 28th of every month. The deployment stores only a reference
# to the Secret block, so the token is never written into its configuration
def deploy():
    package_health_flow.from_source(
        source=GitRepository(
            url=GIT_URL,
            branch="main",
            credentials={"access_token": Secret.load(PAT_BLOCK)},
        ),
        entrypoint="main.py:package_health_flow",
    ).deploy(
        name="monthly",
        work_pool_name=WORK_POOL,
        schedule=Cron("0 0 28 * *", timezone="America/New_York"),
        job_variables={
            "cpu_request": "2",
            "cpu_limit": "2",
            "memory_request": "2Gi",
            "memory_limit": "4Gi",
        },
    )


if __name__ == "__main__":
    if sys.argv[1:] == ["deploy"]:   # uv run --env-file .env main.py deploy
        deploy()
    else:
        package_health_flow()
