import os
from dotenv import load_dotenv

load_dotenv() # load API Keys from .env file

def require_env(name: str):
    """
    Generate required env var
    """
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"missing required env var: {name}")
    
    return v

POLYGON_API_KEY = require_env("POLYGON_API_KEY")