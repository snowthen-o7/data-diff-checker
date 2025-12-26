"""
Configuration and constants for Diaz Diff Checker.

All configurable values are centralized here for easy customization.
Users can create a local config file (.diaz-diff.json) to override defaults.
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


# ============================================================================
# DEFAULT VALUES
# ============================================================================

# Maximum example IDs to include in diff output
DEFAULT_MAX_EXAMPLES: int = 10

# HTTP request timeout in seconds (15 minutes)
DEFAULT_TIMEOUT: int = 900

# Concurrency limits
DEFAULT_MAX_CONCURRENT_FETCHES: int = 250
DEFAULT_MAX_CONCURRENT_DIFFS: int = 10

# Default primary key column
DEFAULT_PRIMARY_KEY: str = "id"

# Default output directories
DEFAULT_OUTPUT_DIR: str = "responses"
DEFAULT_SUMMARY_DIR: str = "summaries"

# Columns to exclude from "meaningful change" detection
# Changes to these columns won't be counted in rows_updated
EXCLUDED_COLUMN_PATTERNS: List[str] = [
    "inventory",
    "availability", 
    "_fdx",
]

# Local config file name (should be gitignored)
LOCAL_CONFIG_FILENAME: str = ".diaz-diff.json"


def find_local_config() -> Optional[Path]:
    """
    Search for local config file in current directory and parents.
    
    Returns:
        Path to config file if found, None otherwise
    """
    current = Path.cwd()
    
    # Check current directory and parents up to home or root
    for directory in [current] + list(current.parents):
        config_path = directory / LOCAL_CONFIG_FILENAME
        if config_path.exists():
            return config_path
        # Stop at home directory
        if directory == Path.home():
            break
    
    return None


def load_local_config() -> Dict[str, Any]:
    """
    Load configuration from local .diaz-diff.json file.
    
    Returns:
        Dictionary of configuration values, empty dict if no config found
    """
    config_path = find_local_config()
    if config_path is None:
        return {}
    
    try:
        with open(config_path, 'r') as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
            return {}
    except (json.JSONDecodeError, IOError) as e:
        import logging
        logging.warning(f"Error loading {config_path}: {e}")
        return {}


# Load local config once at module import
_LOCAL_CONFIG: Dict[str, Any] = load_local_config()


def get_config_value(key: str, default: Any = None) -> Any:
    """Get a config value, checking local config first."""
    return _LOCAL_CONFIG.get(key, default)


@dataclass
class EndpointConfig:
    """Configuration for API endpoints (for URL fetch mode)."""
    
    production_url: str = field(default_factory=lambda: get_config_value("prod_url", ""))
    development_url: str = field(default_factory=lambda: get_config_value("dev_url", ""))
    verify_ssl_prod: bool = True
    verify_ssl_dev: bool = False  # Dev environments often use self-signed certs
    
    # Keys used to deduplicate test cases (e.g., same shop shouldn't be tested twice)
    deduplication_keys: List[str] = field(default_factory=lambda: get_config_value(
        "dedup_keys", 
        ["connection_info[store_hash]"]
    ))


@dataclass
class DiffConfig:
    """Configuration for diff operations."""
    
    primary_keys: List[str] = field(default_factory=lambda: [DEFAULT_PRIMARY_KEY])
    max_examples: int = DEFAULT_MAX_EXAMPLES
    max_rows: Optional[int] = None  # None = no limit
    excluded_patterns: List[str] = field(default_factory=lambda: EXCLUDED_COLUMN_PATTERNS.copy())
    
    @classmethod
    def from_primary_key_string(cls, pk_string: str, **kwargs) -> "DiffConfig":
        """Create config from comma-separated primary key string."""
        keys = [k.strip() for k in pk_string.split(",")]
        return cls(primary_keys=keys, **kwargs)


@dataclass 
class OutputConfig:
    """Configuration for output paths and formatting."""
    
    output_dir: str = DEFAULT_OUTPUT_DIR
    summary_dir: str = DEFAULT_SUMMARY_DIR
    save_responses: bool = True  # Whether to save raw response files
    
    def ensure_directories(self) -> None:
        """Create output directories if they don't exist."""
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.summary_dir, exist_ok=True)


@dataclass
class RuntimeConfig:
    """Runtime configuration combining all settings."""
    
    diff: DiffConfig = field(default_factory=DiffConfig)
    endpoints: EndpointConfig = field(default_factory=EndpointConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    
    timeout: int = DEFAULT_TIMEOUT
    max_concurrent_fetches: int = DEFAULT_MAX_CONCURRENT_FETCHES
    max_concurrent_diffs: int = DEFAULT_MAX_CONCURRENT_DIFFS
    verbose: bool = False
    source_limit: Optional[int] = None  # Limit number of test cases