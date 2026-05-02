"""Tests for analytics MCP tools — calculate_metrics, compare_funds, correlate_funds."""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from fon_mcp import _db as db
from fon_mcp.tools import analytics

# ---------------------------------------------------------------------------
# Module-level fixture: patch Tefas to prevent real HTTP calls.
# Tests that need actual prices should seed data via _seed_prices before calling tools.
# _ensure_prices will find the seeded data in cache and skip the Tefas call.
# For unknown funds (no cache, no seeded data), Tefas returns empty dict → no prices.
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def no_real_tefas():
    mock_instance = MagicMock()
    mock_instance.__enter__ = MagicMock(return_value=mock_instance)
    mock_instance.__exit__ = MagicMock(return_value=False)
    mock_instance.fetch.return_value = {}
    with patch("fon_mcp.tools.analytics.Tefas", return_value=mock_instance):
        yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_prices(fund_code: str, start: str, prices: list[float]) -> None:
    """Insert a synthetic price series into price_cache starting from `start`."""
    start_dt = date.fromisoformat(start)
    rows = [
        {
            "date": (start_dt + timedelta(days=i)).isoformat(),
            "price": p,
            "portfolio_size": 1e9,
            "share_count": 1e8,
            "person_count": 1000,
        }
        for i, p in enumerate(prices)
    ]
    db.price_cache_set(fund_code, rows)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tools(mcp):
    analytics.register(mcp)
    return mcp.tools


# ---------------------------------------------------------------------------
# Test: calculate_metrics happy path (rising prices)
# ---------------------------------------------------------------------------


class TestCalculateMetrics:
    def test_rising_prices_cagr_positive(self, tools):
        # 30 days of monotonically rising prices: 10.0 → ~13.0 (+30%)
        prices = [10.0 + i * 0.1 for i in range(30)]
        _seed_prices("AAK", "2024-01-01", prices)

        result = tools["calculate_metrics"]("AAK", "2024-01-01", "2024-01-30")

        assert "error" not in result
        assert result["cagr_pct"] > 0
        assert result["total_return_pct"] > 0
        assert result["trading_days"] == 30

    def test_falling_prices_max_drawdown_negative(self, tools):
        # 30 days of monotonically falling prices: 20.0 → 11.0
        prices = [20.0 - i * 0.3 for i in range(30)]
        _seed_prices("IPB", "2024-01-01", prices)

        result = tools["calculate_metrics"]("IPB", "2024-01-01", "2024-01-30")

        assert "error" not in result
        assert result["max_drawdown_pct"] < 0
        assert result["cagr_pct"] < 0

    def test_single_price_returns_error(self, tools):
        _seed_prices("TI2", "2024-01-01", [15.0])

        result = tools["calculate_metrics"]("TI2", "2024-01-01", "2024-01-01")

        assert "error" in result
        assert "Yetersiz veri" in result["error"]

    def test_returns_sharpe_ratio(self, tools):
        prices = [10.0 + i * 0.05 for i in range(30)]
        _seed_prices("AAK", "2024-01-01", prices)

        result = tools["calculate_metrics"]("AAK", "2024-01-01", "2024-01-30", risk_free_rate=0.0)

        # With zero risk-free rate and positive CAGR, Sharpe should be positive
        assert result.get("sharpe_ratio") is not None
        assert result["sharpe_ratio"] > 0

    def test_cache_hit_on_second_call(self, tools):
        prices = [10.0 + i * 0.1 for i in range(30)]
        _seed_prices("AAK", "2024-01-01", prices)

        r1 = tools["calculate_metrics"]("AAK", "2024-01-01", "2024-01-30")
        r2 = tools["calculate_metrics"]("AAK", "2024-01-01", "2024-01-30")

        assert r1["cagr_pct"] == r2["cagr_pct"]
        assert r2["source"] == "cache"

    def test_risk_free_rate_from_config(self, tools, test_settings):
        prices = [10.0 + i * 0.05 for i in range(30)]
        _seed_prices("AAK", "2024-01-01", prices)

        result = tools["calculate_metrics"]("AAK", "2024-01-01", "2024-01-30")

        assert result["risk_free_rate_used"] == test_settings.risk_free_rate


# ---------------------------------------------------------------------------
# Test: compare_funds
# ---------------------------------------------------------------------------


class TestCompareFunds:
    def test_two_funds_sorted_by_cagr(self, tools):
        # Fund A grows faster than fund B
        _seed_prices("AFAST", "2024-01-01", [10.0 + i * 0.2 for i in range(30)])
        _seed_prices("BSLOW", "2024-01-01", [10.0 + i * 0.05 for i in range(30)])

        result = tools["compare_funds"](["AFAST", "BSLOW"], "2024-01-01", "2024-01-30")

        comps = result["comparisons"]
        assert len(comps) == 2
        # Sorted descending by cagr_pct
        assert comps[0]["cagr_pct"] >= comps[1]["cagr_pct"]
        assert comps[0]["fund_code"] == "AFAST"

    def test_compare_with_missing_fund_has_error_entry(self, tools):
        _seed_prices("GOOD", "2024-01-01", [10.0 + i * 0.1 for i in range(10)])
        # NONE has no price data seeded

        result = tools["compare_funds"](["GOOD", "NOFUND"], "2024-01-01", "2024-01-10")

        codes = [c["fund_code"] for c in result["comparisons"]]
        assert "GOOD" in codes
        missing = next(c for c in result["comparisons"] if c["fund_code"] == "NOFUND")
        assert "error" in missing


# ---------------------------------------------------------------------------
# Test: correlate_funds
# ---------------------------------------------------------------------------


class TestCorrelateFunds:
    def test_identical_prices_correlation_is_one(self, tools):
        prices = [10.0 + i * 0.1 for i in range(30)]
        _seed_prices("FX", "2024-01-01", prices)
        _seed_prices("FY", "2024-01-01", prices)

        result = tools["correlate_funds"](["FX", "FY"], "2024-01-01", "2024-01-30")

        assert result["matrix"]["FX"]["FY"] == pytest.approx(1.0, abs=0.01)

    def test_opposite_price_series_correlation_is_minus_one(self, tools):
        # Zigzag returns: UP goes +1% on even days, -0.5% on odd; DN does the opposite
        up_prices, dn_prices = [10.0], [10.0]
        for i in range(1, 30):
            factor = 1.01 if i % 2 == 0 else 0.995
            up_prices.append(up_prices[-1] * factor)
            dn_prices.append(dn_prices[-1] * (1 / factor))  # inverse

        _seed_prices("UP", "2024-01-01", up_prices)
        _seed_prices("DN", "2024-01-01", dn_prices)

        result = tools["correlate_funds"](["UP", "DN"], "2024-01-01", "2024-01-30")

        corr = result["matrix"]["UP"]["DN"]
        assert corr is not None
        assert corr < -0.9

    def test_diagonal_is_always_one(self, tools):
        prices = [10.0 + i * 0.1 for i in range(20)]
        _seed_prices("FA", "2024-01-01", prices)
        _seed_prices("FB", "2024-01-01", [p * 1.1 for p in prices])

        result = tools["correlate_funds"](["FA", "FB"], "2024-01-01", "2024-01-20")

        assert result["matrix"]["FA"]["FA"] == 1.0
        assert result["matrix"]["FB"]["FB"] == 1.0

    def test_no_data_returns_empty_matrix(self, tools):
        result = tools["correlate_funds"](["NONE1", "NONE2"], "2024-01-01", "2024-01-30")
        assert result["matrix"] == {}
