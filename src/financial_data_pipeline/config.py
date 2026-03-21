import os
from dotenv import load_dotenv
from pathlib import Path
load_dotenv() # load API Keys from .env file

# Path constants
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BARS_BASE_DIR = PROJECT_ROOT / "data" / "raw" / "polygon" / "bars"
WAREHOUSE_DIR = PROJECT_ROOT / "warehouse"
SYNC_STATE_PATH = PROJECT_ROOT / "data" / "sync_state.json"


def require_env(name: str):
    """
    Generate required env var
    """
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"missing required env var: {name}")
    
    return v

POLYGON_API_KEY = require_env("POLYGON_API_KEY")
BQ_PROJECT_ID = require_env("BQ_PROJECT_ID")
BQ_DATASET_ID = require_env("BQ_DATASET_ID")

