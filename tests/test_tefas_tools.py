"""Tests for TEFAS MCP tools — most frequent user questions."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from fon_mcp.tools import tefas

# ---------------------------------------------------------------------------
# Helpers: build fake tefas_client model objects
# ---------------------------------------------------------------------------


def _make_history(
    dt: str, price: float, market_cap: float = 1e9, shares: float = 1e8, investors: int = 500
):
    h = MagicMock()
    h.date = date.fromisoformat(dt)
    h.price = price
    h.market_cap = market_cap
    h.number_of_shares = shares
    h.number_of_investors = investors
    h.allocation = None
    return h


def _make_fund(code: str, history_rows: list):
    f = MagicMock()
    f.code = code
    f.title = f"{code} Test Fonu"
    f.history = history_rows
    return f


def _make_overview(code: str, price: float = 25.0):
    ov = MagicMock()
    ov.code = code
    ov.title = f"{code} Test Fonu"
    ov.price = price
    ov.daily_return = 0.5
    ov.market_cap = 5e9
    ov.shares = 2e8
    ov.number_of_investors = 12_000
    ov.market_share = 0.01
    ov.category = "Para Piyasası Fonları"
    ov.category_rank = 3
    ov.category_fund_count = 45
    return ov


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tools(mcp):
    tefas.register(mcp)
    return mcp.tools


# ---------------------------------------------------------------------------
# Test: "X fonunun güncel fiyatı nedir?" → get_fund_snapshot
# ---------------------------------------------------------------------------


class TestGetFundSnapshot:
    def _make_mocks(self, code: str, price: float = 25.5):
        """fetch_overview için tools.tefas.Tefas, fetch için _tefas_utils.Tefas mock'u."""
        ov = _make_overview(code, price=price)
        mock_ov = MagicMock()
        mock_ov.__enter__ = MagicMock(return_value=mock_ov)
        mock_ov.__exit__ = MagicMock(return_value=False)
        mock_ov.fetch_overview.return_value = ov

        h = _make_history("2025-01-02", price)
        fund = _make_fund(code, [h])
        mock_hist = MagicMock()
        mock_hist.__enter__ = MagicMock(return_value=mock_hist)
        mock_hist.__exit__ = MagicMock(return_value=False)
        mock_hist.fetch.return_value = {code: fund}

        return mock_ov, mock_hist

    def test_returns_price_from_api(self, tools):
        mock_ov, mock_hist = self._make_mocks("IPB", price=25.5)

        with (
            patch("fon_mcp.tools.tefas.Tefas", return_value=mock_ov),
            patch("fon_mcp._tefas_utils.Tefas", return_value=mock_hist),
        ):
            result = tools["get_fund_snapshot"]("IPB")

        assert result["fund_code"] == "IPB"
        assert result["price"] == 25.5
        assert result["source"] == "api"
        assert result["category"] == "Para Piyasası Fonları"

    def test_returns_from_cache_on_second_call(self, tools):
        mock_ov, mock_hist = self._make_mocks("AAK", price=10.0)

        with (
            patch("fon_mcp.tools.tefas.Tefas", return_value=mock_ov),
            patch("fon_mcp._tefas_utils.Tefas", return_value=mock_hist),
        ):
            tools["get_fund_snapshot"]("AAK")
            result = tools["get_fund_snapshot"]("AAK")

        assert result["source"] == "cache"
        assert result["price"] == 10.0

    def test_fund_code_normalized_uppercase(self, tools):
        mock_ov, mock_hist = self._make_mocks("AAK")

        with (
            patch("fon_mcp.tools.tefas.Tefas", return_value=mock_ov),
            patch("fon_mcp._tefas_utils.Tefas", return_value=mock_hist),
        ):
            result = tools["get_fund_snapshot"]("aak")

        assert result["fund_code"] == "AAK"


# ---------------------------------------------------------------------------
# Test: "X fonunun fiyat geçmişi" → get_fund_price_history
# ---------------------------------------------------------------------------


class TestGetFundPriceHistory:
    def test_returns_history_entries(self, tools):
        history = [_make_history("2025-01-02", 10.0), _make_history("2025-01-03", 10.5)]
        fund = _make_fund("AAK", history)
        mock_tefas = MagicMock()
        mock_tefas.__enter__ = MagicMock(return_value=mock_tefas)
        mock_tefas.__exit__ = MagicMock(return_value=False)
        mock_tefas.fetch.return_value = {"AAK": fund}

        with patch("fon_mcp._tefas_utils.Tefas", return_value=mock_tefas):
            result = tools["get_fund_price_history"]("AAK", "2025-01-02", "2025-01-03")

        assert result["fund_code"] == "AAK"
        assert len(result["entries"]) == 2
        assert result["entries"][0]["price"] == 10.0
        assert result["source"] == "api"

    def test_cache_hit_on_repeat_query(self, tools):
        history = [_make_history("2025-01-02", 10.0)]
        fund = _make_fund("AAK", history)
        mock_tefas = MagicMock()
        mock_tefas.__enter__ = MagicMock(return_value=mock_tefas)
        mock_tefas.__exit__ = MagicMock(return_value=False)
        mock_tefas.fetch.return_value = {"AAK": fund}

        with patch("fon_mcp._tefas_utils.Tefas", return_value=mock_tefas):
            tools["get_fund_price_history"]("AAK", "2025-01-02", "2025-01-02")
            result = tools["get_fund_price_history"]("AAK", "2025-01-02", "2025-01-02")

        assert result["source"] == "cache"

    def test_unknown_fund_returns_empty(self, tools):
        mock_tefas = MagicMock()
        mock_tefas.__enter__ = MagicMock(return_value=mock_tefas)
        mock_tefas.__exit__ = MagicMock(return_value=False)
        mock_tefas.fetch.return_value = {}

        with patch("fon_mcp._tefas_utils.Tefas", return_value=mock_tefas):
            result = tools["get_fund_price_history"]("NOPE", "2025-01-01", "2025-01-31")

        assert result["entries"] == []


# ---------------------------------------------------------------------------
# Test: "X fonunun portföy dağılımı nedir?" → get_fund_allocation
# ---------------------------------------------------------------------------


class TestGetFundAllocation:
    def test_returns_allocation(self, tools):
        alloc = MagicMock()
        alloc.assets = {"hs": 0.60, "dt": 0.40}
        alloc.asset_names = {"hs": "Hisse Senedi", "dt": "Devlet Tahvili"}

        h = _make_history("2025-01-02", 10.0)
        h.allocation = alloc

        fund = _make_fund("TI2", [h])
        mock_tefas = MagicMock()
        mock_tefas.__enter__ = MagicMock(return_value=mock_tefas)
        mock_tefas.__exit__ = MagicMock(return_value=False)
        mock_tefas.fetch.return_value = {"TI2": fund}

        with patch("fon_mcp._tefas_utils.Tefas", return_value=mock_tefas):
            result = tools["get_fund_allocation"]("TI2", "2025-01-02")

        assert result["assets"]["hs"] == 0.60
        assert result["source"] == "api"

    def test_allocation_cached_on_repeat(self, tools):
        alloc = MagicMock()
        alloc.assets = {"r": 1.0}
        alloc.asset_names = {"r": "Repo"}

        h = _make_history("2025-01-02", 5.0)
        h.allocation = alloc

        fund = _make_fund("IPB", [h])
        mock_tefas = MagicMock()
        mock_tefas.__enter__ = MagicMock(return_value=mock_tefas)
        mock_tefas.__exit__ = MagicMock(return_value=False)
        mock_tefas.fetch.return_value = {"IPB": fund}

        with patch("fon_mcp._tefas_utils.Tefas", return_value=mock_tefas):
            tools["get_fund_allocation"]("IPB", "2025-01-02")
            result = tools["get_fund_allocation"]("IPB", "2025-01-02")

        assert result["source"] == "cache"


# ---------------------------------------------------------------------------
# Test: "Altın fonları listele" → search_funds + list_fund_types
# ---------------------------------------------------------------------------


class TestSearchFunds:
    def _make_fund_mock(self, code, title, price=50.0, market_cap=2e9):
        history_entry = MagicMock()
        history_entry.price = price
        history_entry.market_cap = market_cap
        history_entry.date = date(2025, 1, 2)

        fund_mock = MagicMock()
        fund_mock.title = title
        fund_mock.history = [history_entry]
        fund_mock.latest = MagicMock(return_value=history_entry)
        return code, fund_mock

    def test_name_filter_applied(self, tools):
        code, fund_mock = self._make_fund_mock("GOLD", "Altın Fonu")

        mock_tefas = MagicMock()
        mock_tefas.__enter__ = MagicMock(return_value=mock_tefas)
        mock_tefas.__exit__ = MagicMock(return_value=False)
        mock_tefas.fetch.return_value = {code: fund_mock}

        with patch("fon_mcp._tefas_utils.Tefas", return_value=mock_tefas):
            result = tools["search_funds"](name_filter="altın")

        assert len(result["funds"]) == 1
        assert result["funds"][0]["fund_code"] == "GOLD"

    def test_name_filter_case_insensitive(self, tools):
        code, fund_mock = self._make_fund_mock("GOLD", "altın Fonu")  # already lowercase Turkish

        mock_tefas = MagicMock()
        mock_tefas.__enter__ = MagicMock(return_value=mock_tefas)
        mock_tefas.__exit__ = MagicMock(return_value=False)
        mock_tefas.fetch.return_value = {code: fund_mock}

        with patch("fon_mcp._tefas_utils.Tefas", return_value=mock_tefas):
            result = tools["search_funds"](name_filter="altın")

        assert len(result["funds"]) >= 1
