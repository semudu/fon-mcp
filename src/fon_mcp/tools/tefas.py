"""TEFAS MCP tools — 6 tools wrapping tefas-client with DuckDB cache."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from mcp.server.fastmcp import FastMCP
from tefas_client import Tefas

from fon_mcp import _db as db
from fon_mcp._settings import get as settings

logger = logging.getLogger(__name__)


def _today() -> str:
    return date.today().isoformat()


def _history_to_dict(h: Any) -> dict:
    return {
        "date": h.date.isoformat(),
        "price": h.price,
        "portfolio_size": h.market_cap,
        "share_count": h.number_of_shares,
        "person_count": h.number_of_investors,
    }


def register(mcp: FastMCP) -> None:
    cfg = settings()

    @mcp.tool()
    def get_fund_price_history(
        fund_code: str,
        start_date: str,
        end_date: str | None = None,
        include_allocation: bool = False,
    ) -> dict:
        """Bir yatırım fonunun belirli tarih aralığındaki NAV (pay değeri) geçmişini döndürür.

        Veri önce DuckDB cache'inden kontrol edilir; bulunamazsa TEFAS API'sine
        istek atılır ve sonuç cache'lenir.

        Args:
            fund_code: TEFAS fon kodu (örn. "AAK", "TI2", "GOLD").
            start_date: Başlangıç tarihi ISO formatında (YYYY-MM-DD).
            end_date: Bitiş tarihi ISO formatında. Belirtilmezse bugün kullanılır.
            include_allocation: True ise her güne ait portföy dağılımı da getirilir.

        Returns:
            {fund_code, entries: [{date, price, portfolio_size, share_count, person_count}]}
        """
        code = fund_code.strip().upper()
        end = end_date or _today()

        if not include_allocation:
            cached = db.price_cache_get(code, start_date, end, cfg.cache_ttl_price)
            if cached is not None:
                logger.debug("Cache hit: price_cache %s %s–%s", code, start_date, end)
                return {"fund_code": code, "entries": cached, "source": "cache"}

        start_dt = date.fromisoformat(start_date)
        end_dt = date.fromisoformat(end)

        with Tefas() as tefas:
            funds = tefas.fetch(
                code, start_date=start_dt, end_date=end_dt, include_allocation=include_allocation
            )

        if code not in funds:
            return {"fund_code": code, "entries": [], "source": "api"}

        fund = funds[code]
        entries = [_history_to_dict(h) for h in fund.history]

        if not include_allocation:
            db.price_cache_set(code, entries)
        else:
            db.price_cache_set(code, entries)
            for h in fund.history:
                if h.allocation:
                    db.allocation_cache_set(
                        code,
                        h.date.isoformat(),
                        {"assets": h.allocation.assets, "asset_names": h.allocation.asset_names},
                    )
            entries = [
                {
                    **_history_to_dict(h),
                    "allocation": {
                        "assets": h.allocation.assets,
                        "asset_names": h.allocation.asset_names,
                    }
                    if h.allocation
                    else None,
                }
                for h in fund.history
            ]

        return {"fund_code": code, "entries": entries, "source": "api"}

    @mcp.tool()
    def get_fund_snapshot(fund_code: str) -> dict:
        """Bir fonun anlık görünümünü döndürür: güncel fiyat, günlük getiri, portföy büyüklüğü,
        kategori sıralaması ve pazar payı.

        Veri 15 dakika boyunca cache'lenir (config ile ayarlanabilir).

        Args:
            fund_code: TEFAS fon kodu (örn. "IPB").

        Returns:
            Fon anlık verilerini içeren sözlük.
        """
        code = fund_code.strip().upper()

        cached = db.snapshot_cache_get(code, cfg.cache_ttl_snapshot)
        if cached is not None:
            logger.debug("Cache hit: snapshot_cache %s", code)
            return {**cached, "source": "cache"}

        with Tefas() as tefas:
            overview = tefas.fetch_overview(code)

        result = {
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

        db.snapshot_cache_set(code, result)
        return {**result, "source": "api"}

    @mcp.tool()
    def get_fund_allocation(fund_code: str, target_date: str | None = None) -> dict:
        """Bir fonun portföy dağılımını döndürür — hisse senedi, tahvil, repo, altın,
        döviz mevduatı vb. 60'tan fazla varlık sınıfı.

        Args:
            fund_code: TEFAS fon kodu.
            target_date: İstenen tarih (YYYY-MM-DD). Belirtilmezse bugün kullanılır.

        Returns:
            {fund_code, date, assets: {asset_code: pct}, asset_names: {asset_code: name}}
        """
        code = fund_code.strip().upper()
        date_str = target_date or _today()

        cached = db.allocation_cache_get(code, date_str, cfg.cache_ttl_allocation)
        if cached is not None:
            logger.debug("Cache hit: allocation_cache %s %s", code, date_str)
            return {"fund_code": code, "date": date_str, **cached, "source": "cache"}

        target_dt = date.fromisoformat(date_str)
        with Tefas() as tefas:
            funds = tefas.fetch(
                code, start_date=target_dt, end_date=target_dt, include_allocation=True
            )

        if code not in funds:
            return {
                "fund_code": code,
                "date": date_str,
                "assets": {},
                "asset_names": {},
                "source": "api",
            }

        fund = funds[code]
        # Find entry closest to target_date
        alloc = None
        for h in fund.history:
            if h.allocation:
                alloc = h.allocation
                break

        if alloc is None:
            return {
                "fund_code": code,
                "date": date_str,
                "assets": {},
                "asset_names": {},
                "source": "api",
            }

        data = {"assets": alloc.assets, "asset_names": alloc.asset_names}
        db.allocation_cache_set(code, date_str, data)
        return {"fund_code": code, "date": date_str, **data, "source": "api"}

    @mcp.tool()
    def list_fund_types(fund_type: str = "YAT") -> dict:
        """TEFAS üst fon türlerini (şemsiye fon kategorileri) listeler.

        Args:
            fund_type: "YAT" (yatırım fonları), "EMK" (emeklilik/BES) veya "BYF" (borsa yatırım fonları).

        Returns:
            {fund_type, types: [{code, name, fund_count}]}
        """
        cache_key = f"fund_types:{fund_type}"
        cached_json = db.cache_get(
            "fund_list_cache", "cache_key", cache_key, cfg.cache_ttl_fund_list
        )
        if cached_json:
            import json

            return {"fund_type": fund_type, "types": json.loads(cached_json), "source": "cache"}

        with Tefas() as tefas:
            types = tefas.fetch_fund_types(fund_type)  # type: ignore[arg-type]

        result = [{"code": t.code, "name": t.name} for t in types]
        db.cache_set("fund_list_cache", "cache_key", cache_key, result)
        return {"fund_type": fund_type, "types": result, "source": "api"}

    @mcp.tool()
    def list_founders(fund_type: str = "YAT") -> dict:
        """TEFAS'ta kayıtlı kurucu kurumları listeler (portföy yönetim şirketleri).

        Args:
            fund_type: "YAT" veya "EMK".

        Returns:
            {fund_type, founders: [{code, name}]}
        """
        cache_key = f"founders:{fund_type}"
        cached_json = db.cache_get(
            "fund_list_cache", "cache_key", cache_key, cfg.cache_ttl_fund_list
        )
        if cached_json:
            import json

            return {"fund_type": fund_type, "founders": json.loads(cached_json), "source": "cache"}

        with Tefas() as tefas:
            founders = tefas.fetch_founders(fund_type)  # type: ignore[arg-type]

        result = [{"code": f.code, "name": f.name} for f in founders]
        db.cache_set("fund_list_cache", "cache_key", cache_key, result)
        return {"fund_type": fund_type, "founders": result, "source": "api"}

    @mcp.tool()
    def search_funds(
        fund_type: str = "YAT",
        founder_code: str | None = None,
        umbrella_type: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        name_filter: str | None = None,
    ) -> dict:
        """TEFAS'taki fonları kurucu, tür, şemsiye kategorisi veya isim filtresine göre filtreler.

        Args:
            fund_type: "YAT", "EMK" veya "BYF".
            founder_code: Kurucu kodu (örn. "AKP"). list_founders ile bulunabilir.
            umbrella_type: Şemsiye fon tipi kodu (örn. 104 = hisse senedi fonu). list_fund_types ile bulunabilir.
            start_date: Veri başlangıç tarihi (YYYY-MM-DD). Belirtilmezse bugün.
            end_date: Veri bitiş tarihi (YYYY-MM-DD). Belirtilmezse bugün.
            name_filter: Fon adında aranacak metin (büyük/küçük harf duyarsız). Örn. "altın", "hisse", "para piyasası".

        Returns:
            {funds: [{fund_code, title, price, portfolio_size, date}]}
        """
        end = end_date or _today()
        start = start_date or end

        start_dt = date.fromisoformat(start)
        end_dt = date.fromisoformat(end)

        with Tefas() as tefas:
            funds = tefas.fetch(
                fund_type=fund_type,  # type: ignore[arg-type]
                founder_code=founder_code,
                umbrella_type=umbrella_type,
                start_date=start_dt,
                end_date=end_dt,
            )

        result = []
        for code, fund in funds.items():
            latest = fund.latest() if fund.history else None
            result.append(
                {
                    "fund_code": code,
                    "title": fund.title,
                    "price": latest.price if latest else None,
                    "portfolio_size": latest.market_cap if latest else None,
                    "date": latest.date.isoformat() if latest else None,
                }
            )

        if name_filter:
            needle = name_filter.lower()
            result = [f for f in result if needle in f["title"].lower()]

        result.sort(key=lambda x: x.get("portfolio_size") or 0, reverse=True)
        return {"funds": result, "count": len(result)}
