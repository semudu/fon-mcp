"""Admin MCP tools — cache management and fund data refresh."""

from __future__ import annotations

import logging
from datetime import date, timedelta

from mcp.server.fastmcp import FastMCP
from tefas_client import Tefas

from fon_mcp import _db as db

logger = logging.getLogger(__name__)


def register(mcp: FastMCP) -> None:
    @mcp.tool()
    def get_cache_status() -> dict:
        """DuckDB cache durumunu gösterir: her tablodaki satır sayısı ve DB dosya boyutu.

        Returns:
            {tables: {table_name: row_count}, db_size_mb: float, db_file: str}
        """
        return db.cache_status()

    @mcp.tool()
    def refresh_fund(fund_code: str, price_history_days: int = 365) -> dict:
        """Bir fonun cache verisini temizler ve yeniden çeker (cache warm-up).

        Bu araç:
        1. Fondaki tüm cache kayıtlarını siler.
        2. Güncel fon snapshot'ını çeker.
        3. Belirtilen gün sayısı kadar fiyat geçmişini çeker.

        Args:
            fund_code: TEFAS fon kodu (örn. "AAK").
            price_history_days: Kaç günlük fiyat geçmişi çekilsin. Default 365.

        Returns:
            {fund_code, purged: true, snapshot_refreshed: bool, price_days_fetched: int}
        """
        code = fund_code.strip().upper()

        # Purge existing cache
        db.purge_fund(code)
        logger.info("Purged cache for %s", code)

        # Refresh snapshot
        snapshot_ok = False
        try:
            with Tefas() as tefas:
                overview = tefas.fetch_overview(code)
            snapshot_data = {
                "fund_code": code,
                "title": overview.title,
                "price": overview.price,
                "daily_return_pct": overview.daily_return,
                "portfolio_size": overview.market_cap,
                "share_count": overview.shares,
                "person_count": overview.number_of_investors,
                "market_share_pct": overview.market_share,
                "category": overview.category,
                "category_rank": overview.category_rank,
                "category_fund_count": overview.category_fund_count,
            }
            db.snapshot_cache_set(code, snapshot_data)
            snapshot_ok = True
        except Exception as e:
            logger.warning("Snapshot refresh failed for %s: %s", code, e)

        # Refresh price history
        end_date = date.today()
        start_date = end_date - timedelta(days=price_history_days)
        price_days = 0
        try:
            with Tefas() as tefas:
                funds = tefas.fetch(code, start_date=start_date, end_date=end_date)

            if code in funds:
                fund = funds[code]
                rows = [
                    {
                        "date": h.date.isoformat(),
                        "price": h.price,
                        "portfolio_size": h.market_cap,
                        "share_count": h.number_of_shares,
                        "person_count": h.number_of_investors,
                    }
                    for h in fund.history
                ]
                db.price_cache_set(code, rows)
                price_days = len(rows)
        except Exception as e:
            logger.warning("Price history refresh failed for %s: %s", code, e)

        return {
            "fund_code": code,
            "purged": True,
            "snapshot_refreshed": snapshot_ok,
            "price_days_fetched": price_days,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
