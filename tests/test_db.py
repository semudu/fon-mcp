"""Tests for DuckDB cache layer (_db.py)."""

from __future__ import annotations

import json

from fon_mcp import _db as db


class TestPriceCache:
    def test_set_and_get(self):
        rows = [
            {
                "date": "2025-01-02",
                "price": 10.5,
                "portfolio_size": 1e9,
                "share_count": 1e8,
                "person_count": 500,
            },
            {
                "date": "2025-01-03",
                "price": 11.0,
                "portfolio_size": 1.1e9,
                "share_count": 1e8,
                "person_count": 510,
            },
        ]
        db.price_cache_set("AAK", rows)
        result = db.price_cache_get("AAK", "2025-01-02", "2025-01-03", ttl_seconds=3600)
        assert result is not None
        assert len(result) == 2
        assert result[0]["price"] == 10.5
        assert result[1]["price"] == 11.0

    def test_cache_miss_for_unknown_fund(self):
        result = db.price_cache_get("UNKNOWN", "2025-01-01", "2025-01-31", ttl_seconds=3600)
        assert result is None

    def test_cache_returns_none_when_no_rows_in_range(self):
        rows = [
            {
                "date": "2024-01-01",
                "price": 5.0,
                "portfolio_size": None,
                "share_count": None,
                "person_count": None,
            }
        ]
        db.price_cache_set("AAK", rows)
        # Query a different range — should return None (no rows found)
        result = db.price_cache_get("AAK", "2025-06-01", "2025-06-30", ttl_seconds=3600)
        assert result is None


class TestAllocationCache:
    def test_set_and_get(self):
        data = {
            "assets": {"hs": 0.60, "dt": 0.40},
            "asset_names": {"hs": "Hisse Senedi", "dt": "Devlet Tahvili"},
        }
        db.allocation_cache_set("TI2", "2025-01-02", data)
        result = db.allocation_cache_get("TI2", "2025-01-02", ttl_seconds=3600)
        assert result is not None
        assert result["assets"]["hs"] == 0.60

    def test_miss_on_unknown_fund(self):
        result = db.allocation_cache_get("XYZ", "2025-01-01", ttl_seconds=3600)
        assert result is None


class TestSnapshotCache:
    def test_set_and_get(self):
        data = {"fund_code": "IPB", "price": 25.5, "daily_return_pct": 0.5}
        db.snapshot_cache_set("IPB", data)
        result = db.snapshot_cache_get("IPB", ttl_seconds=3600)
        assert result is not None
        assert result["price"] == 25.5

    def test_miss_on_unknown(self):
        assert db.snapshot_cache_get("NONE", ttl_seconds=3600) is None


class TestGenericCache:
    def test_set_and_get_dict(self):
        payload = [{"code": "YF", "name": "Yatırım Fonu"}]
        db.cache_set("fund_list_cache", "cache_key", "test-key", payload)
        raw = db.cache_get("fund_list_cache", "cache_key", "test-key", ttl_seconds=3600)
        assert raw is not None
        parsed = json.loads(raw)
        assert parsed[0]["code"] == "YF"

    def test_miss_returns_none(self):
        assert db.cache_get("fund_list_cache", "cache_key", "no-such-key", ttl_seconds=3600) is None


class TestFTSIndex:
    def test_index_and_search(self):
        db.fts_index_disclosure(
            {
                "index": 1001,
                "fund_code": "THF",
                "company_name": "Tacirler Portföy",
                "subject": "KESINLESEN_PORTFOY_BILGILERI",
                "summary": "Kesinleşen portföy bilgileri açıklaması",
                "publish_date": "2025-03-01",
                "url": "https://www.kap.org.tr/tr/Bildirim/1001",
            }
        )
        results = db.fts_search("portföy", "THF", None, None, 10)
        assert len(results) >= 1
        assert results[0]["fund_code"] == "THF"

    def test_search_no_results(self):
        results = db.fts_search("yokböylebirşey", None, None, None, 10)
        assert results == []
