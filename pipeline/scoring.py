import duckdb
import numpy as np
import pandas as pd

# metric → dimension, weight within its dimension (weights in a dimension sum to 1), and the
# fixed reference points it is min-max scaled between: `worst` scores 0, `best` scores 1,
# values past either end are clipped. Fixed points instead of the observed min and max, so a
# score means the same thing whichever repos are compared, and a near-tie stays a near-tie
METRICS = {
    "pct_reviewed_within_7d":       {"dimension": "responsiveness", "weight": 0.6, "worst": 0.0, "best": 1.0},
    # log scale, so every doubling of the wait costs the same (1 h → 2 h as much as 1 day → 2 days):
    # 1 hour → 1, 4 hours → 0.79, 1 day → 0.52, 1 week → 0.22, 30 days (effectively ignored) → 0;
    # past a week the 7-day share already counts the PR as missed, so this metric separates slow from ignored
    "median_hours_to_first_review": {"dimension": "responsiveness", "weight": 0.4, "worst": 720, "best": 1, "log": True},
    "pct_open_stale":               {"dimension": "backlog",        "weight": 1.0, "worst": 1.0, "best": 0.0},
    # % change per month: shrinking 5% of its average level a month (~60% a year) or faster → 0;
    # flat or growing → 1 (growth is not healthier than steady)
    "commit_trend":                 {"dimension": "activity_trend", "weight": 1.0, "worst": -0.05, "best": 0.0},
}

# dimension weights (sum to 1)
# responsiveness and backlog show directly whether maintainers keep up with contributions;
# commit trend is weighted lowest because commit habits vary and AI tooling inflates counts
# everywhere, so only a repo lagging the others is meaningful
DIMENSIONS = {"responsiveness": 0.45, "backlog": 0.35, "activity_trend": 0.20}

# other weightings the ranking is re-run under, to see whether the order depends on the weights
WEIGHT_SCENARIOS = {
    "chosen (45/35/20)":           DIMENSIONS,
    "equal (33/33/33)":            {"responsiveness": 1 / 3, "backlog": 1 / 3, "activity_trend": 1 / 3},
    "responsiveness-heavy (60/20/20)": {"responsiveness": 0.6, "backlog": 0.2, "activity_trend": 0.2},
    "backlog-heavy (20/60/20)":    {"responsiveness": 0.2, "backlog": 0.6, "activity_trend": 0.2},
    "trend-heavy (20/20/60)":      {"responsiveness": 0.2, "backlog": 0.2, "activity_trend": 0.6},
}

N_BOOTSTRAP = 1000
SEED = 3022   # fixed, so a rerun on the same data gives the same intervals


def scale(values, worst, best, log=False):
    if log:
        values, worst, best = np.log(values), np.log(worst), np.log(best)
    return ((values - worst) / (best - worst)).clip(0, 1)


# one subscore per dimension: weighted average of its scaled metrics
def subscores(metrics):
    subs = pd.DataFrame(index=metrics.index)
    for dim in DIMENSIONS:
        subs[dim] = sum(cfg["weight"] * scale(metrics[m], cfg["worst"], cfg["best"], cfg.get("log", False))
                        for m, cfg in METRICS.items() if cfg["dimension"] == dim)
    return subs


# weighted total on a 0–100 scale
def total(subs, weights=DIMENSIONS):
    return 100 * sum(w * subs[dim] for dim, w in weights.items())


def short(repo):
    return repo.split("/")[1]


# resample each repo's recent PRs, open PRs, and commits with replacement and recompute every
# metric. Draw 0 is the original sample (identity indices), so it must match mart_health_inputs:
# that checks these formulas are the same ones dbt used
def bootstrap_metrics(con, rng):
    prs = con.execute("""
        SELECT repo, reviewed_within_7d, review_window_closed, hours_to_first_review
        FROM fct_pull_requests WHERE in_recent_window ORDER BY repo, number
    """).df()   # fixed row order: the same seed must resample the same PRs on every run
    open_prs = con.execute("SELECT repo, is_stale FROM fct_pull_requests WHERE is_open ORDER BY repo, number").df()
    monthly = con.execute("""
        SELECT repo, date_diff('month', DATE '2024-01-01', month) AS month_index, commits
        FROM mart_repo_monthly WHERE NOT is_partial_month ORDER BY repo, month
    """).df()

    draws = []
    for repo, months in monthly.groupby("repo"):
        recent = prs[prs["repo"] == repo]
        n = len(recent)
        idx = np.vstack([np.arange(n), rng.integers(0, n, (N_BOOTSTRAP, n))])
        closed = recent["review_window_closed"].to_numpy()[idx]
        reviewed = recent["reviewed_within_7d"].to_numpy()[idx] & closed
        hours = recent["hours_to_first_review"].to_numpy(dtype=float)[idx]   # NaN = never reviewed

        stale = open_prs.loc[open_prs["repo"] == repo, "is_stale"].to_numpy()
        if len(stale):
            pct_stale = stale[np.vstack([np.arange(len(stale)),
                                         rng.integers(0, len(stale), (N_BOOTSTRAP, len(stale)))])].mean(axis=1)
        else:
            pct_stale = np.zeros(N_BOOTSTRAP + 1)   # nothing open, as in the mart

        # resampling every commit = a multinomial draw over the months, total kept the same
        counts = months["commits"].to_numpy()
        commits = np.vstack([counts, rng.multinomial(counts.sum(), counts / counts.sum(), N_BOOTSTRAP)])
        x = months["month_index"].to_numpy() - months["month_index"].mean()
        slope = (x * (commits - commits.mean(axis=1, keepdims=True))).sum(axis=1) / (x ** 2).sum()

        draws.append(pd.DataFrame({
            "draw": np.arange(N_BOOTSTRAP + 1),
            "repo": repo,
            "pct_reviewed_within_7d": reviewed.sum(axis=1) / closed.sum(axis=1),
            "median_hours_to_first_review": np.nanmedian(hours, axis=1),
            "pct_open_stale": pct_stale,
            "commit_trend": slope / commits.mean(axis=1),
        }))
    return pd.concat(draws, ignore_index=True)


def score_repos(db_path):
    with duckdb.connect(str(db_path)) as con:
        inputs = con.execute("SELECT * FROM mart_health_inputs").df().set_index("repo")

        scores = subscores(inputs)
        scores["score"] = total(scores)
        scores["rank"] = scores["score"].rank(ascending=False, method="min").astype(int)

        # uncertainty: score the bootstrap draws the same way
        boot = bootstrap_metrics(con, np.random.default_rng(SEED))
        observed = boot[boot["draw"] == 0].set_index("repo")[list(METRICS)]
        if not np.allclose(observed.loc[inputs.index], inputs[list(METRICS)]):
            raise ValueError(f"bootstrap metrics don't match mart_health_inputs:\n{observed}\n{inputs}")
        boot = boot[boot["draw"] > 0].set_index(["draw", "repo"])
        boot = subscores(boot).assign(score=lambda d: total(d)).reset_index()
        boot["rank"] = boot.groupby("draw")["score"].rank(ascending=False, method="min").astype(int)

        by_repo = boot.groupby("repo")
        scores["score_low"] = by_repo["score"].quantile(0.025)    # 95% interval
        scores["score_high"] = by_repo["score"].quantile(0.975)
        scores["pct_rank_held"] = boot.merge(scores["rank"].rename("point_rank"), on="repo") \
            .assign(held=lambda d: d["rank"] == d["point_rank"]).groupby("repo")["held"].mean()

        # sensitivity: rank again under each weighting
        sensitivity = []
        for name, weights in WEIGHT_SCENARIOS.items():
            totals = total(scores, weights).sort_values(ascending=False)
            sensitivity.append({"weights": name,
                                **{short(repo): round(s, 1) for repo, s in totals.sort_index().items()},
                                "ranking": " > ".join(short(repo) for repo in totals.index)})
        sensitivity = pd.DataFrame(sensitivity)

        scores = scores[["rank", "score", "score_low", "score_high", "pct_rank_held", *DIMENSIONS]]
        scores = scores.sort_values("rank").round(3).reset_index()

        con.execute("CREATE OR REPLACE TABLE health_scores AS SELECT * FROM scores")
        con.execute("CREATE OR REPLACE TABLE health_bootstrap AS SELECT * FROM boot")
        con.execute("CREATE OR REPLACE TABLE health_weight_sensitivity AS SELECT * FROM sensitivity")
    return scores, sensitivity
