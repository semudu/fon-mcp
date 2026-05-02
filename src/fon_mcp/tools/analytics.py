"""Analytics MCP tools — 4 tools using DuckDB window functions for fund analysis."""

from __future__ import annotations

import json
import logging
import math
from datetime import date, timedelta
from typing import Any

from mcp.server.fastmcp import FastMCP
from tefas_client import Tefas

from fon_mcp import _db as db
from fon_mcp._settings import get as settings

logger = logging.getLogger(__name__)


def _ensure_prices(fund_code: str, start_date: str, end_date: str, cfg: Any) -> bool:
    """Ensure price data for fund_code is in cache. Returns True if data is available."""
    cached = db.price_cache_get(fund_code, start_date, end_date, cfg.cache_ttl_price)
    if cached:
        return True

    start_dt = date.fromisoformat(start_date)
    end_dt = date.fromisoformat(end_date)
    with Tefas() as tefas:
        funds = tefas.fetch(fund_code, start_date=start_dt, end_date=end_dt)

    if fund_code not in funds:
        return False

    fund = funds[fund_code]
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
    db.price_cache_set(fund_code, rows)
    return bool(rows)


def _compute_metrics_sql(fund_code: str, start_date: str, end_date: str) -> dict | None:
    """Compute CAGR, volatility, max drawdown, Sharpe using DuckDB SQL."""
    con = db.get()

    result = con.execute(
        """
        WITH base AS (
            SELECT
                date::DATE AS dt,
                price
            FROM price_cache
            WHERE fund_code = ? AND date BETWEEN ? AND ? AND price IS NOT NULL
            ORDER BY date
        ),
        returns AS (
            SELECT
                dt,
                price,
                price / LAG(price) OVER (ORDER BY dt) - 1 AS daily_return
            FROM base
        ),
        drawdown AS (
            SELECT
                dt,
                price,
                daily_return,
                MAX(price) OVER (ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max
            FROM returns
        ),
        stats AS (
            SELECT
                MIN(dt) AS start_dt,
                MAX(dt) AS end_dt,
                FIRST(price ORDER BY dt ASC) AS start_price,
                LAST(price ORDER BY dt ASC) AS end_price,
                STDDEV_POP(daily_return) AS daily_std,
                MIN(price / running_max - 1) AS max_drawdown,
                AVG(daily_return) AS mean_daily_return,
                COUNT(*) AS trading_days
            FROM drawdown
        )
        SELECT
            start_dt::VARCHAR, end_dt::VARCHAR,
            start_price, end_price,
            daily_std,
            max_drawdown,
            mean_daily_return,
            trading_days,
            DATEDIFF('day', start_dt, end_dt) AS calendar_days
        FROM stats
        """,
        [fund_code, start_date, end_date],
    ).fetchone()

    if result is None:
        return None

    # Verify we have at least 2 data points (trading_days >= 2)
    if result[7] is None or result[7] < 2:
        return None

    (
        start_dt,
        end_dt,
        start_price,
        end_price,
        daily_std,
        max_drawdown,
        mean_daily_return,
        trading_days,
        calendar_days,
    ) = result

    if start_price is None or start_price <= 0 or end_price is None or calendar_days == 0:
        return None

    # Annualised CAGR
    cagr = (end_price / start_price) ** (365.0 / calendar_days) - 1

    # Annualised volatility (252 trading days)
    volatility = (daily_std or 0.0) * math.sqrt(252)

    return {
        "start_date": start_dt,
        "end_date": end_dt,
        "start_price": round(start_price, 6),
        "end_price": round(end_price, 6),
        "total_return_pct": round((end_price / start_price - 1) * 100, 4),
        "cagr_pct": round(cagr * 100, 4),
        "volatility_ann_pct": round(volatility * 100, 4),
        "max_drawdown_pct": round((max_drawdown or 0.0) * 100, 4),
        "trading_days": trading_days,
        "calendar_days": calendar_days,
    }


def register(mcp: FastMCP) -> None:
    cfg = settings()

    @mcp.tool()
    def calculate_metrics(
        fund_code: str,
        start_date: str,
        end_date: str,
        risk_free_rate: float | None = None,
    ) -> dict:
        """Bir fonun finansal performans metriklerini hesaplar.

        Hesaplanan metrikler:
        - **CAGR** (Bileşik Yıllık Büyüme Oranı)
        - **Toplam Getiri** (%)
        - **Yıllık Volatilite** (günlük getirilerin standart sapması × √252)
        - **Maksimum Drawdown** (tepe değerden en derin düşüş)
        - **Sharpe Oranı** (CAGR − risksiz oran) / volatilite

        Args:
            fund_code: TEFAS fon kodu (örn. "AAK", "IPB").
            start_date: Analiz başlangıç tarihi (YYYY-MM-DD).
            end_date: Analiz bitiş tarihi (YYYY-MM-DD).
            risk_free_rate: Yıllık risksiz oran (örn. 0.40 = %40). Belirtilmezse config değeri kullanılır.

        Returns:
            {fund_code, cagr_pct, total_return_pct, volatility_ann_pct, max_drawdown_pct, sharpe_ratio, ...}
        """
        code = fund_code.strip().upper()
        rfr = risk_free_rate if risk_free_rate is not None else cfg.risk_free_rate

        # Check metrics cache first
        cached = db.cache_get("metrics_cache", "fund_code", code, cfg.cache_ttl_metrics)
        if cached:
            data = json.loads(cached)
            if data.get("start_date") == start_date and data.get("end_date") == end_date:
                return {**data, "fund_code": code, "source": "cache"}

        if not _ensure_prices(code, start_date, end_date, cfg):
            return {"fund_code": code, "error": "Fiyat verisi bulunamadı", "source": "api"}

        m = _compute_metrics_sql(code, start_date, end_date)
        if m is None:
            return {
                "fund_code": code,
                "error": "Yetersiz veri (en az 2 fiyat gerekli)",
                "source": "api",
            }

        sharpe = (
            (m["cagr_pct"] / 100 - rfr) / (m["volatility_ann_pct"] / 100)
            if m["volatility_ann_pct"]
            else None
        )

        result = {
            **m,
            "sharpe_ratio": round(sharpe, 4) if sharpe is not None else None,
            "risk_free_rate_used": rfr,
        }

        # Cache the metrics (compound key workaround: store in generic metrics_cache)
        db.cache_set("metrics_cache", "fund_code", code, result)

        return {"fund_code": code, **result, "source": "api"}

    @mcp.tool()
    def compare_funds(
        fund_codes: list[str],
        start_date: str,
        end_date: str,
        risk_free_rate: float | None = None,
    ) -> dict:
        """Birden fazla fonun performans metriklerini yan yana karşılaştırır.

        Her fon için CAGR, volatilite, Sharpe oranı ve maksimum drawdown hesaplanır.
        Sonuçlar CAGR'a göre büyükten küçüğe sıralanır.

        Args:
            fund_codes: Karşılaştırılacak fon kodları listesi (örn. ["AAK", "IPB", "TI2"]).
            start_date: Analiz başlangıç tarihi (YYYY-MM-DD).
            end_date: Analiz bitiş tarihi (YYYY-MM-DD).
            risk_free_rate: Yıllık risksiz oran. Belirtilmezse config değeri kullanılır.

        Returns:
            {comparisons: [{fund_code, cagr_pct, volatility_ann_pct, sharpe_ratio, max_drawdown_pct}]}
        """
        rfr = risk_free_rate if risk_free_rate is not None else cfg.risk_free_rate
        results = []

        for raw_code in fund_codes:
            code = raw_code.strip().upper()
            _ensure_prices(code, start_date, end_date, cfg)
            m = _compute_metrics_sql(code, start_date, end_date)
            if m is None:
                results.append({"fund_code": code, "error": "Veri yok"})
                continue
            vol = m["volatility_ann_pct"] / 100
            cagr = m["cagr_pct"] / 100
            sharpe = (cagr - rfr) / vol if vol else None
            results.append(
                {
                    "fund_code": code,
                    "cagr_pct": m["cagr_pct"],
                    "total_return_pct": m["total_return_pct"],
                    "volatility_ann_pct": m["volatility_ann_pct"],
                    "max_drawdown_pct": m["max_drawdown_pct"],
                    "sharpe_ratio": round(sharpe, 4) if sharpe is not None else None,
                    "trading_days": m["trading_days"],
                }
            )

        results.sort(key=lambda x: x.get("cagr_pct") or float("-inf"), reverse=True)
        return {
            "start_date": start_date,
            "end_date": end_date,
            "risk_free_rate": rfr,
            "comparisons": results,
        }

    @mcp.tool()
    def correlate_funds(
        fund_codes: list[str],
        start_date: str,
        end_date: str,
    ) -> dict:
        """Fonların günlük getirileri arasındaki korelasyon matrisini hesaplar.

        1.0'e yakın değer = yüksek korelasyon (aynı yönde hareket)
        0'a yakın değer = bağımsız hareket
        −1.0'e yakın değer = ters yönlü hareket

        Args:
            fund_codes: En az 2 fon kodu içeren liste.
            start_date: Başlangıç tarihi (YYYY-MM-DD).
            end_date: Bitiş tarihi (YYYY-MM-DD).

        Returns:
            {matrix: {fund_a: {fund_b: correlation_value}}}
        """
        con = db.get()
        codes = [c.strip().upper() for c in fund_codes]

        # Ensure all funds are in cache
        for code in codes:
            _ensure_prices(code, start_date, end_date, cfg)

        # Build pivot of daily returns using DuckDB
        # First get all (date, fund_code, price) rows
        rows = con.execute(
            """
            SELECT fund_code, date::VARCHAR AS dt, price
            FROM price_cache
            WHERE fund_code IN ({})
              AND date BETWEEN ? AND ?
              AND price IS NOT NULL
            ORDER BY fund_code, date
            """.format(",".join("?" * len(codes))),
            codes + [start_date, end_date],
        ).fetchall()

        if not rows:
            return {"error": "Fiyat verisi bulunamadı", "matrix": {}}

        # Build per-fund daily return series
        by_fund: dict[str, dict[str, float]] = {}
        prev_price: dict[str, float] = {}
        for fund_code, dt, price in rows:
            if price is None:
                continue
            if fund_code in prev_price and prev_price[fund_code] > 0:
                ret = price / prev_price[fund_code] - 1
                by_fund.setdefault(fund_code, {})[dt] = ret
            prev_price[fund_code] = price

        # Compute correlation matrix in Python
        matrix: dict[str, dict[str, float]] = {}
        valid_codes = [c for c in codes if c in by_fund]

        for ca in valid_codes:
            matrix[ca] = {}
            for cb in valid_codes:
                if ca == cb:
                    matrix[ca][cb] = 1.0
                    continue
                common_dates = sorted(set(by_fund[ca]) & set(by_fund[cb]))
                if len(common_dates) < 2:
                    matrix[ca][cb] = None
                    continue
                xs = [by_fund[ca][d] for d in common_dates]
                ys = [by_fund[cb][d] for d in common_dates]
                corr = _pearson(xs, ys)
                matrix[ca][cb] = round(corr, 4) if corr is not None else None

        return {
            "start_date": start_date,
            "end_date": end_date,
            "matrix": matrix,
            "funds_with_data": valid_codes,
        }

    @mcp.tool()
    def rank_funds(
        fund_type: str = "YAT",
        metric: str = "cagr",
        period_days: int = 365,
        top_n: int = 20,
        risk_free_rate: float | None = None,
    ) -> dict:
        """TEFAS'taki fonları seçilen metriğe göre sıralandırır.

        Bu araç DuckDB'deki mevcut cache'ten yararlanarak analiz yapar.
        En iyi sonuç için önce search_funds ile fon listesi çekmeniz ve
        ardından calculate_metrics ile cache doldurmak gerekebilir.

        Args:
            fund_type: "YAT", "EMK" veya "BYF".
            metric: Sıralama metriği: "cagr", "sharpe", "volatility" (düşükten), "max_drawdown" (düşükten).
            period_days: Analiz penceresi (gün). Default 365.
            top_n: Döndürülecek maksimum fon sayısı. Default 20.
            risk_free_rate: Sharpe hesabı için risksiz oran.

        Returns:
            {ranked: [{rank, fund_code, metric_value, cagr_pct, sharpe_ratio, volatility_ann_pct}]}
        """
        rfr = risk_free_rate if risk_free_rate is not None else cfg.risk_free_rate
        end_date = date.today().isoformat()
        start_date = (date.today() - timedelta(days=period_days)).isoformat()

        con = db.get()

        # Get all fund codes with price data in the range
        codes_rows = con.execute(
            """
            SELECT DISTINCT fund_code
            FROM price_cache
            WHERE date BETWEEN ? AND ?
            """,
            [start_date, end_date],
        ).fetchall()

        all_codes = [r[0] for r in codes_rows]
        if not all_codes:
            return {"ranked": [], "note": "Cache boş. Önce fonları fetch edin."}

        results = []
        for code in all_codes:
            m = _compute_metrics_sql(code, start_date, end_date)
            if m is None or m["trading_days"] < 20:
                continue
            vol = m["volatility_ann_pct"] / 100
            cagr = m["cagr_pct"] / 100
            sharpe = (cagr - rfr) / vol if vol else None
            results.append(
                {
                    "fund_code": code,
                    "cagr_pct": m["cagr_pct"],
                    "volatility_ann_pct": m["volatility_ann_pct"],
                    "max_drawdown_pct": m["max_drawdown_pct"],
                    "sharpe_ratio": round(sharpe, 4) if sharpe is not None else None,
                    "trading_days": m["trading_days"],
                }
            )

        # Sort by selected metric
        metric = metric.lower()
        reverse = True
        sort_key: str
        if metric == "cagr":
            sort_key = "cagr_pct"
        elif metric == "sharpe":
            sort_key = "sharpe_ratio"
        elif metric == "volatility":
            sort_key = "volatility_ann_pct"
            reverse = False
        elif metric == "max_drawdown":
            sort_key = "max_drawdown_pct"
            reverse = False  # less negative = better
        else:
            sort_key = "cagr_pct"

        results.sort(
            key=lambda x: x.get(sort_key) or (float("-inf") if reverse else float("inf")),
            reverse=reverse,
        )
        top = results[:top_n]
        for i, item in enumerate(top, 1):
            item["rank"] = i

        return {
            "start_date": start_date,
            "end_date": end_date,
            "metric": metric,
            "risk_free_rate": rfr,
            "total_analyzed": len(results),
            "ranked": top,
        }

    @mcp.tool()
    def analyze_investor_flow(
        fund_code: str,
        start_date: str,
        end_date: str,
    ) -> dict:
        """Bir fonun yatırımcı sayısı, AUM ve pay adedi değişimini analiz eder.

        Yatırımcı kazanımı/kaybı, nakit giriş/çıkış (AUM değişimi) ve pay ihracı/itfası
        hakkında dönemsel özet sunar.

        Args:
            fund_code: TEFAS fon kodu (örn. "AAK").
            start_date: Analiz başlangıç tarihi (YYYY-MM-DD).
            end_date: Analiz bitiş tarihi (YYYY-MM-DD).

        Returns:
            {fund_code, investors: {start, end, delta, pct_change}, aum_tl: {...}, shares: {...}}
        """
        code = fund_code.strip().upper()
        _ensure_prices(code, start_date, end_date, cfg)

        con = db.get()
        rows = con.execute(
            """
            SELECT date::VARCHAR, person_count, portfolio_size, share_count
            FROM price_cache
            WHERE fund_code = ? AND date BETWEEN ? AND ?
              AND (person_count IS NOT NULL OR portfolio_size IS NOT NULL)
            ORDER BY date
            """,
            [code, start_date, end_date],
        ).fetchall()

        if len(rows) < 2:
            return {"fund_code": code, "error": "Yetersiz veri (en az 2 nokta gerekli)"}

        first, last = rows[0], rows[-1]

        def _delta(a, b):
            return (b - a) if (a is not None and b is not None) else None

        def _pct(a, b):
            if a is None or b is None or a == 0:
                return None
            return round((b - a) / a * 100, 2)

        return {
            "fund_code": code,
            "start_date": first[0],
            "end_date": last[0],
            "data_points": len(rows),
            "investors": {
                "start": first[1],
                "end": last[1],
                "delta": _delta(first[1], last[1]),
                "pct_change": _pct(first[1], last[1]),
            },
            "aum_tl": {
                "start": round(first[2], 2) if first[2] is not None else None,
                "end": round(last[2], 2) if last[2] is not None else None,
                "delta": round(_delta(first[2], last[2]), 2)
                if _delta(first[2], last[2]) is not None
                else None,
                "pct_change": _pct(first[2], last[2]),
            },
            "shares": {
                "start": first[3],
                "end": last[3],
                "delta": round(_delta(first[3], last[3]), 2)
                if _delta(first[3], last[3]) is not None
                else None,
                "pct_change": _pct(first[3], last[3]),
            },
        }

    @mcp.tool()
    def rank_by_investor_flow(
        period_days: int = 30,
        top_n: int = 20,
        metric: str = "investor_delta",
        ascending: bool = False,
    ) -> dict:
        """Cache'deki fonları yatırımcı sayısı veya AUM değişimine göre sıralar.

        "En çok yatırımcı kazanan/kaybeden fonlar" veya "en büyük AUM girişi/çıkışı"
        sorularını yanıtlar. Yalnızca DuckDB cache'indeki verilerden hesaplanır;
        veri yoksa önce get_fund_price_history veya search_funds çağrılmalıdır.

        Args:
            period_days: Analiz penceresi (gün). Default 30.
            top_n: Döndürülecek fon sayısı. Default 20.
            metric: Sıralama kriteri: "investor_delta" (yatırımcı sayısı değişimi),
                    "investor_pct" (yüzde değişim), "aum_delta" (TL cinsinden AUM değişimi),
                    "aum_pct" (AUM yüzde değişimi).
            ascending: True = en çok kaybeden önce. Default False (en çok kazanan önce).

        Returns:
            {ranked: [{rank, fund_code, investor_delta, investor_pct, aum_delta_tl, aum_pct}]}
        """
        end_date = date.today().isoformat()
        start_date = (date.today() - timedelta(days=period_days)).isoformat()

        con = db.get()
        rows = con.execute(
            """
            SELECT
                fund_code,
                FIRST(person_count ORDER BY date ASC)   AS start_investors,
                LAST(person_count ORDER BY date ASC)    AS end_investors,
                FIRST(portfolio_size ORDER BY date ASC) AS start_aum,
                LAST(portfolio_size ORDER BY date ASC)  AS end_aum,
                COUNT(*) AS data_points
            FROM price_cache
            WHERE date BETWEEN ? AND ?
              AND person_count IS NOT NULL
            GROUP BY fund_code
            HAVING COUNT(*) >= 5
            """,
            [start_date, end_date],
        ).fetchall()

        if not rows:
            return {
                "ranked": [],
                "note": "Cache boş veya yetersiz veri. Önce get_fund_price_history veya search_funds çağrın.",
            }

        results = []
        for fund_code, s_inv, e_inv, s_aum, e_aum, dp in rows:
            inv_delta = (e_inv - s_inv) if (e_inv is not None and s_inv is not None) else None
            inv_pct = round((e_inv - s_inv) / s_inv * 100, 2) if (e_inv and s_inv) else None
            aum_delta = (
                round(e_aum - s_aum, 2) if (e_aum is not None and s_aum is not None) else None
            )
            aum_pct = round((e_aum - s_aum) / s_aum * 100, 2) if (e_aum and s_aum) else None
            results.append(
                {
                    "fund_code": fund_code,
                    "investor_delta": inv_delta,
                    "investor_pct": inv_pct,
                    "aum_delta_tl": aum_delta,
                    "aum_pct": aum_pct,
                    "start_investors": s_inv,
                    "end_investors": e_inv,
                    "data_points": dp,
                }
            )

        sort_key_map = {
            "investor_delta": "investor_delta",
            "investor_pct": "investor_pct",
            "aum_delta": "aum_delta_tl",
            "aum_pct": "aum_pct",
        }
        sort_key = sort_key_map.get(metric.lower(), "investor_delta")
        null_fallback = float("inf") if ascending else float("-inf")
        results.sort(
            key=lambda x: x.get(sort_key) if x.get(sort_key) is not None else null_fallback,
            reverse=not ascending,
        )
        top = results[:top_n]
        for i, item in enumerate(top, 1):
            item["rank"] = i

        return {
            "start_date": start_date,
            "end_date": end_date,
            "period_days": period_days,
            "metric": metric,
            "ascending": ascending,
            "total_funds_analyzed": len(results),
            "ranked": top,
        }


# ---------------------------------------------------------------------------
# Internal math helpers
# ---------------------------------------------------------------------------


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    """Pearson correlation coefficient."""
    n = len(xs)
    if n < 2:
        return None
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)
