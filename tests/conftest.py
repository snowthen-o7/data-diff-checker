"""
Pytest configuration and shared fixtures.
"""

import pytest
from pathlib import Path


@pytest.fixture
def fixtures_dir():
    """Return the path to the test fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def basic_prod_csv(fixtures_dir):
    """Path to basic production CSV."""
    return fixtures_dir / "basic_prod.csv"


@pytest.fixture
def basic_dev_csv(fixtures_dir):
    """Path to basic development CSV."""
    return fixtures_dir / "basic_dev.csv"


@pytest.fixture
def composite_key_prod_csv(fixtures_dir):
    """Path to composite key production CSV."""
    return fixtures_dir / "composite_key_prod.csv"


@pytest.fixture
def composite_key_dev_csv(fixtures_dir):
    """Path to composite key development CSV."""
    return fixtures_dir / "composite_key_dev.csv"
