import os
from pathlib import Path

from prefect.blocks.system import Secret

PROJECT_DIR = Path(__file__).parent.parent
PAT_BLOCK = os.environ.get("PAT_BLOCK", "github-pat")   # Prefect Secret block holding the GitHub token for deployed runs

# a worker sets PREFECT__FLOW_RUN_ID when it starts a deployment's pod; local runs don't have it
# pods have a read-only root filesystem and are thrown away after the run, so outputs go to /tmp
OUTPUT_DIR = Path("/tmp/package-health") if os.environ.get("PREFECT__FLOW_RUN_ID") else PROJECT_DIR
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RAW_DIR = OUTPUT_DIR / "data" / "raw"
LOG_DIR = OUTPUT_DIR / "logs"
PLOTS_DIR = OUTPUT_DIR / "plots"
TARGET_DIR = OUTPUT_DIR / "target"
DB_PATH = OUTPUT_DIR / "packages.duckdb"

# dbt reads these: profiles.yml for the database, dbt itself for its log folder
os.environ["DUCKDB_PATH"] = str(DB_PATH)
os.environ["DBT_LOG_PATH"] = str(LOG_DIR)

# local runs use GH_PAT from .env; a pod has no .env, so it loads the Secret block
# _sync=True because a worker can import this module from inside an event loop
TOKEN = os.environ.get("GH_PAT") or Secret.load(PAT_BLOCK, _sync=True).get()
