"""TEFAS MCP tools — 6 tools wrapping tefas-client with DuckDB cache."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from mcp.server.fastmcp import FastMCP
from tefas_client import Tefas, TefasError
from tefas_client.exceptions import EmptyResponseError

from fon_mcp import _db as db
from fon_mcp._settings import get as settings
from fon_mcp._tefas_utils import fetch_with_fallback, to_business_day

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

        ⚠️ Fon kodu bilinmiyorsa önce search_funds kullanın.
        Yanıtta error="fund_not_found" gelirse farklı kodlar tahmin etmeyin — search_funds ile doğrulayın.

        Args:
            fund_code: TEFAS fon kodu (örn. "AAK", "TI2", "GOLD").
            start_date: Başlangıç tarihi ISO formatında (YYYY-MM-DD).
            end_date: Bitiş tarihi ISO formatında. Belirtilmezse bugün kullanılır.
            include_allocation: True ise her güne ait portföy dağılımı da getirilir.

        Returns:
            {fund_code, entries: [{date, price, portfolio_size, share_count, person_count}]}
            Fon bulunamazsa: {fund_code, entries: [], error: "fund_not_found", message: ...}
        """
        code = fund_code.strip().upper()
        end = end_date or _today()

        if not include_allocation:
            cached = db.price_cache_get(code, start_date, end, cfg.cache_ttl_price)
            if cached is not None:
                logger.debug("Cache hit: price_cache %s %s–%s", code, start_date, end)
                return {"fund_code": code, "entries": cached, "source": "cache"}

        start_dt = to_business_day(date.fromisoformat(start_date))
        end_dt = to_business_day(date.fromisoformat(end))

        funds = fetch_with_fallback(code, start_dt, end_dt, include_allocation=include_allocation)

        if code not in funds:
            return {
                "fund_code": code,
                "entries": [],
                "error": "fund_not_found",
                "message": f"'{code}' fon kodu bulunamadı. Fon kodunu doğrulamak için search_funds aracını kullanın — farklı kodlar tahmin etmeyin.",
                "source": "api",
            }

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

        today_str = _today()
        result_entries = {"fund_code": code, "entries": entries, "source": "api"}
        if entries:
            latest = entries[-1]["date"]
            result_entries["latest_entry_date"] = latest
            if latest < today_str:
                result_entries["note"] = (
                    f"Bugün ({today_str}) için fiyat henüz yayımlanmamış. "
                    f"En son kayıt {latest} tarihlidir (tatil veya erken saat)."
                )
        return result_entries

    @mcp.tool()
    def get_fund_snapshot(fund_code: str) -> dict:
        """Bir fonun anlık görünümünü döndürür: güncel fiyat, günlük getiri, portföy büyüklüğü,
        kategori sıralaması ve pazar payı.

        Veri 15 dakika boyunca cache'lenir (config ile ayarlanabilir).

        ⚠️ Fon kodu bilinmiyorsa önce search_funds kullanın.
        Yanıtta error="fund_not_found" gelirse farklı kodlar tahmin etmeyin — search_funds ile doğrulayın.

        Args:
            fund_code: TEFAS fon kodu (örn. "IPB").

        Returns:
            Fon anlık verilerini içeren sözlük.
            Fon bulunamazsa: {fund_code, error: "fund_not_found", message: ...}
        """
        code = fund_code.strip().upper()

        cached = db.snapshot_cache_get(code, cfg.cache_ttl_snapshot)
        if cached is not None:
            logger.debug("Cache hit: snapshot_cache %s", code)
            return {**cached, "source": "cache"}

        today_str = _today()
        today_dt = date.fromisoformat(today_str)

        try:
            with Tefas() as tefas:
                overview = tefas.fetch_overview(code)
        except (EmptyResponseError, TefasError):
            return {
                "fund_code": code,
                "error": "fund_not_found",
                "message": f"'{code}' fon kodu TEFAS'ta bulunamadı. Fon kodunu doğrulamak için search_funds aracını kullanın — farklı kodlar tahmin etmeyin.",
                "source": "api",
            }

        # Fiyat geçmişine bak: hem price_date hem de overview fiyatı 0/None ise
        # son geçerli fiyatı bulmak için fetch_with_fallback kullan
        hist = fetch_with_fallback(code, today_dt - timedelta(days=10), today_dt)

        price_date: str | None = None
        hist_price: float | None = None
        if code in hist and hist[code].history:
            last_h = hist[code].history[-1]
            price_date = last_h.date.isoformat()
            hist_price = last_h.price if (last_h.price is not None and last_h.price > 0) else None

        # overview.price 0 ya da None ise son geçerli tarihsel fiyatı kullan
        effective_price = (
            overview.price if (overview.price is not None and overview.price > 0) else hist_price
        )

        result: dict = {
            "fund_code": code,
            "title": overview.title,
            "price": effective_price,
            "price_date": price_date,
            "daily_return_pct": overview.daily_return,
            "portfolio_size": overview.market_cap,
            "share_count": overview.shares,
            "person_count": overview.number_of_investors,
            "market_share_pct": overview.market_share,
            "category": overview.category,
            "category_rank": overview.category_rank,
            "category_fund_count": overview.category_fund_count,
        }
        if price_date and price_date < today_str:
            result["stale_data"] = True
            result["note"] = (
                f"Bugün ({today_str}) için fiyat henüz yayımlanmamış. "
                f"Gösterilen fiyat {price_date} tarihlidir (tatil veya erken saat)."
            )

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
        # Hem TefasError hem de boş veri için son 7 güne fallback yapan tek çağrı
        funds = fetch_with_fallback(
            code,
            target_dt - timedelta(days=7),
            target_dt,
            include_allocation=True,
        )

        if code not in funds:
            return {
                "fund_code": code,
                "date": date_str,
                "assets": {},
                "asset_names": {},
                "note": "Belirtilen tarih ve önceki 7 gün için portföy dağılımı bulunamadı.",
                "source": "api",
            }

        fund = funds[code]
        # En son allocation'ı bul (history ascending sıralı → sondan başa bak)
        alloc = None
        actual_date = date_str
        for h in reversed(fund.history):
            if h.allocation:
                alloc = h.allocation
                actual_date = h.date.isoformat()
                break

        if alloc is None:
            return {
                "fund_code": code,
                "date": date_str,
                "assets": {},
                "asset_names": {},
                "note": "Belirtilen tarih ve önceki 7 gün için portföy dağılımı bulunamadı.",
                "source": "api",
            }

        data = {"assets": alloc.assets, "asset_names": alloc.asset_names}
        db.allocation_cache_set(code, actual_date, data)
        result = {"fund_code": code, "date": actual_date, **data, "source": "api"}
        if actual_date < date_str:
            result["note"] = (
                f"İstenen tarih ({date_str}) için veri yok. "
                f"Gösterilen portföy dağılımı {actual_date} tarihlidir (tatil veya erken saat)."
            )
        return result

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

        start_dt = to_business_day(date.fromisoformat(start))
        end_dt = to_business_day(date.fromisoformat(end))

        funds = fetch_with_fallback(
            None,
            start_dt,
            end_dt,
            fund_type=fund_type,  # type: ignore[arg-type]
            founder_code=founder_code,
            umbrella_type=umbrella_type,
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
