"""
Utility to automatically load environment variables from .env file.
This makes it easier to manage credentials without modifying shell profiles.
"""
import os
from pathlib import Path


def load_env_file(env_file: str = ".env") -> None:
    """
    Load environment variables from a .env file in the repo root.
    
    The .env file should contain lines like:
        QBO_CLIENT_ID=your_client_id
        EPOS_USERNAME=your_username
    
    Lines starting with # are treated as comments and ignored.
    """
    repo_root = Path(__file__).resolve().parent
    env_path = repo_root / env_file
    
    if not env_path.exists():
        # .env file is optional - if it doesn't exist, use system env vars
        return
    
    try:
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                
                # Skip empty lines and comments
                if not line or line.startswith("#"):
                    continue
                
                # Parse KEY=VALUE format
                if "=" in line:
                    key, value = line.split("=", 1)  # Split on first = only
                    key = key.strip()
                    value = value.strip()
                    
                    # Remove quotes if present
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    
                    # Only set if not already in environment (env vars take precedence)
                    if key and not os.environ.get(key):
                        os.environ[key] = value
    except Exception as e:
        # Silently fail - if .env can't be read, fall back to system env vars
        pass
