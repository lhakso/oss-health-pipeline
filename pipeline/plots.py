import duckdb
import matplotlib

matplotlib.use("Agg")   # write PNGs without opening a window
import matplotlib.pyplot as plt
import matplotlib.ticker
import pandas as pd

# one fixed color per repo in every plot (first three slots of a colorblind-validated palette)
COLORS = {
    "scikit-learn/scikit-learn": "#2a78d6",   # blue
    "dask/dask":                 "#eb6834",   # orange
    "plotly/plotly.py":          "#1baf7a",   # aqua
}
DIMENSION_LABELS = {
    "responsiveness": "Responsiveness (45%)",
    "backlog":        "Backlog (35%)",
    "activity_trend": "Activity trend (20%)",
}
# mart_repo_monthly column → panel title (with units)
MONTHLY_PANELS = {
    "pct_reviewed_within_7d":    "PRs reviewed within 7 days (% of PRs opened)",
    "median_first_review_hours": "Median time to first review (hours, log scale)",
    "pct_still_open":            "PRs still open today (% of PRs opened)",
    "commit_index":              "Commits (index, 2024 monthly average = 100)",
}
RECENT_MONTHS = 6   # responsiveness is scored on the last 6 complete months (in_recent_window)
# time to first review groups for the relationship plot: (upper bound in hours, label)
REVIEW_GROUPS = [(4, "≤ 4 hours"), (24, "4–24 hours"), (168, "1–7 days"), (672, "1–4 weeks"),
                 (None, "> 4 weeks")]
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"

plt.rcParams.update({
    "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb",
    "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "text.color": INK,
    "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.titlesize": 11, "axes.titleweight": "bold", "font.size": 9,
})


def short(repo):
    return repo.split("/")[1]


def plain_numbers(ax):
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda y, _: f"{y:g}"))


# small multiples: total score, then one panel per scored dimension; repos ordered
# healthiest → least healthy and labeled with rank and score; whiskers = 95% bootstrap interval
def plot_dimension_profile(scores, boot, path):
    scores = scores.sort_values("rank", ascending=False)   # barh draws bottom-up, so #1 lands on top
    labels = [f"#{r.rank} {short(r.repo)} ({r.score:.0f})" for r in scores.itertuples()]
    colors = [COLORS[r] for r in scores["repo"]]
    intervals = boot.groupby("repo").quantile([0.025, 0.975]).unstack()

    panels = {"score": "Total score (0–100)", **DIMENSION_LABELS}
    fig, axes = plt.subplots(1, len(panels), figsize=(14, 3.8), sharey=True)
    for ax, (col, title) in zip(axes, panels.items()):
        low = intervals.loc[scores["repo"], (col, 0.025)].to_numpy()
        high = intervals.loc[scores["repo"], (col, 0.975)].to_numpy()
        values = scores[col].to_numpy()
        ax.barh(labels, values, color=colors, height=0.6)
        ax.errorbar(values, labels, xerr=[values - low, high - values], fmt="none",
                    ecolor=INK, elinewidth=1.2, capsize=4)
        top = 100 if col == "score" else 1
        for y, (value, hi) in enumerate(zip(values, high)):
            text = f"{value:.0f}" if col == "score" else f"{value:.2f}"
            ax.text(hi + top * 0.02, y, text, va="center", color=MUTED)
        ax.set_title(title, loc="left")
        ax.set_xlim(0, top * 1.18)
        ax.set_xlabel("Points" if col == "score" else "Subscore (0 = worst reference point, 1 = best)")
        ax.grid(axis="y", visible=False)
    fig.suptitle("Health score and subscores, ranked (label: rank, repo, score; whiskers: 95% bootstrap interval)",
                 x=0.01, ha="left", fontsize=13, fontweight="bold")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


# small multiples: one panel per metric behind a scored dimension, one line per repo,
# January 2024 → last complete month, smoothed with a 3-month rolling average
def plot_monthly_trends(monthly, path):
    monthly = monthly[~monthly["is_partial_month"]].copy()   # the run month isn't over yet
    base = monthly[monthly["month"].dt.year == 2024].groupby("repo")["commits"].mean()
    monthly["commit_index"] = 100 * monthly["commits"] / monthly["repo"].map(base)
    last = monthly["month"].max()
    recent_start = last - pd.DateOffset(months=RECENT_MONTHS - 1)

    fig, axes = plt.subplots(2, 2, figsize=(13, 7.5), sharex=True)
    for ax, (col, title) in zip(axes.flat, MONTHLY_PANELS.items()):
        for repo, color in COLORS.items():
            rows = monthly[monthly["repo"] == repo]
            ax.plot(rows["month"], rows[col].rolling(3, min_periods=1).mean(),
                    color=color, linewidth=2, label=short(repo))
        ax.set_title(title, loc="left")
        ax.tick_params(axis="x", rotation=45)
    for ax in axes[0]:   # the two responsiveness metrics are scored on the recent window only
        ax.axvspan(recent_start - pd.Timedelta(days=15), last + pd.Timedelta(days=15),
                   color=GRID, alpha=0.8, linewidth=0)
        ax.text(recent_start, 1.0, " scored window", transform=ax.get_xaxis_transform(),
                va="top", color=MUTED)
    axes[0, 0].yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    axes[1, 0].yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    axes[0, 1].set_yscale("log")   # review times span hours to weeks
    plain_numbers(axes[0, 1])
    axes[1, 1].axhline(100, color=MUTED, linewidth=1, linestyle="--")
    for ax in axes[1]:
        ax.set_xlabel("Month")
    axes[0, 0].legend(frameon=False, loc="lower left")
    fig.suptitle(f"Monthly trends, January 2024 to {last:%B %Y} (3-month rolling average; "
                 "PRs grouped by the month they were opened)",
                 x=0.01, ha="left", fontsize=13, fontweight="bold")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


# bubble chart, one bubble per repo × review-time group, over closed PRs:
# x = time to first review, y = merge rate, size = closed PRs in the group, color = repo
def plot_relationship(groups, path):
    positions = {label: i for i, (_, label) in enumerate(REVIEW_GROUPS)}
    positions["never reviewed"] = len(REVIEW_GROUPS) + 0.6   # set apart: not a point on the time scale
    offsets = dict(zip(COLORS, (-0.12, 0, 0.12)))             # side by side, so bubbles don't stack

    def size(prs):
        return prs * 0.8 + 15

    fig, ax = plt.subplots(figsize=(11, 6))
    for repo, color in COLORS.items():
        rows = groups[groups["repo"] == repo].copy()
        rows["x"] = rows["review_group"].map(positions) + offsets[repo]
        reviewed = rows[rows["review_group"] != "never reviewed"].sort_values("x")
        ax.plot(reviewed["x"], reviewed["merge_rate"], color=color, linewidth=1.2, alpha=0.6)
        ax.scatter(rows["x"], rows["merge_rate"], s=size(rows["closed_prs"]), color=color, alpha=0.6,
                   edgecolors="#fcfcfb", linewidths=1.5, zorder=3)
    ax.axvline(len(REVIEW_GROUPS) - 0.2, color=MUTED, linewidth=1, linestyle="--")
    ax.set_xticks(list(positions.values()), list(positions))
    ax.set_ylim(-0.05, 1.08)
    ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    ax.grid(axis="x", visible=False)
    ax.set_xlabel("Time from PR opened to first review (non-author, non-bot)")
    ax.set_ylabel("Merge rate (merged ÷ closed)")
    ax.set_title("Merge rate by time to first review, closed PRs opened since January 2024",
                 loc="left", fontsize=12)

    # two legends: color = repo, size = closed PRs
    repo_handles = [plt.scatter([], [], s=80, color=c, alpha=0.6, label=short(r)) for r, c in COLORS.items()]
    size_handles = [plt.scatter([], [], s=size(n), color=MUTED, alpha=0.3, label=f"{n} PRs") for n in (50, 250, 1000)]
    repo_legend = ax.legend(handles=repo_handles, title="Repo", frameon=False,
                            loc="upper left", bbox_to_anchor=(1.01, 1))
    ax.add_artist(repo_legend)
    ax.legend(handles=size_handles, title="Closed PRs", frameon=False,
              loc="lower left", bbox_to_anchor=(1.01, 0), labelspacing=2.6, borderpad=1.4, handletextpad=2)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


# closed PRs per repo and review-time group, with their merge rate
def review_groups(con):
    cases = "\n".join(f"WHEN hours_to_first_review <= {hours} THEN '{label}'"
                      for hours, label in REVIEW_GROUPS if hours is not None)
    return con.execute(f"""
        SELECT repo,
               CASE WHEN hours_to_first_review IS NULL THEN 'never reviewed'
                    {cases}
                    ELSE '{REVIEW_GROUPS[-1][1]}' END AS review_group,
               count(*)                AS closed_prs,
               avg(is_merged::integer) AS merge_rate
        FROM fct_pull_requests
        WHERE NOT is_open
        GROUP BY ALL
    """).df()


def make_plots(db_path, plots_dir):
    plots_dir.mkdir(exist_ok=True)
    with duckdb.connect(str(db_path)) as con:
        scores = con.execute("SELECT * FROM health_scores").df()
        boot = con.execute("SELECT repo, score, responsiveness, backlog, activity_trend FROM health_bootstrap").df()
        monthly = con.execute("SELECT * FROM mart_repo_monthly ORDER BY repo, month").df()
        groups = review_groups(con)

    paths = [plots_dir / "dimension_profile.png", plots_dir / "monthly_trends.png",
             plots_dir / "relationship.png"]
    plot_dimension_profile(scores, boot, paths[0])
    plot_monthly_trends(monthly, paths[1])
    plot_relationship(groups, paths[2])
    return paths
