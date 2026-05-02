"""Shared pytest fixtures for fon-mcp unit tests."""

from __future__ import annotations

import pytest

from fon_mcp import _db as db
from fon_mcp._settings import Settings

# ---------------------------------------------------------------------------
# Minimal mock for FastMCP — captures registered tools as plain callables
# ---------------------------------------------------------------------------


class MockMCP:
    """Captures @mcp.tool() decorated functions without starting an MCP server."""

    def __init__(self):
        self.tools: dict = {}

    def tool(self):
        def decorator(fn):
            self.tools[fn.__name__] = fn
            return fn

        return decorator

    def prompt(self):
        def decorator(fn):
            return fn

        return decorator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def isolated_db(tmp_path):
    """Each test gets a fresh DuckDB in a temp directory."""
    db_file = str(tmp_path / "test.duckdb")
    db.init(db_file)
    yield
    db.close()


@pytest.fixture
def test_settings(tmp_path, monkeypatch):
    """Provide a Settings instance with fast TTLs pointing to tmp_path."""
    cfg = Settings(
        db_file=str(tmp_path / "test.duckdb"),
        attachments_dir=str(tmp_path / "attachments"),
        risk_free_rate=0.40,
        cache_ttl_price=3600,
        cache_ttl_snapshot=900,
        cache_ttl_fund_list=3600,
        cache_ttl_disclosure=3600,
        cache_ttl_disclosure_detail=3600,
        cache_ttl_allocation=3600,
        cache_ttl_metrics=3600,
    )
    monkeypatch.setattr("fon_mcp._settings._settings", cfg)
    return cfg


@pytest.fixture
def mcp(test_settings):
    """Return a MockMCP that captures registered tools."""
    return MockMCP()
