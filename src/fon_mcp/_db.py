"""DuckDB schema init, connection management, and CRUD helpers."""

from __future__ import annotations

import json
import logging
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

logger = logging.getLogger(__name__)

_conn: duckdb.DuckDBPyConnection | None = None
_db_path: str = ""


def init(db_file: str) -> None:
    """Initialise (or open) the DuckDB database and create schema if needed."""
    global _conn, _db_path
    path = Path(db_file).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    _db_path = str(path)
    _conn = duckdb.connect(_db_path)
    _create_schema(_conn)
    logger.info("DuckDB opened at %s", _db_path)


def close() -> None:
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None


def get() -> duckdb.DuckDBPyConnection:
    if _conn is None:
        raise RuntimeError("DB not initialised — call db.init() first")
    return _conn


@contextmanager
def cursor() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    yield get()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def _create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS price_cache (
            fund_code   VARCHAR NOT NULL,
            date        DATE    NOT NULL,
            price       DOUBLE,
            portfolio_size DOUBLE,
            share_count    DOUBLE,
            person_count   INTEGER,
            cached_at   TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (fund_code, date)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS allocation_cache (
            fund_code   VARCHAR NOT NULL,
            date        DATE    NOT NULL,
            asset_json  VARCHAR NOT NULL,
            cached_at   TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (fund_code, date)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS snapshot_cache (
            fund_code    VARCHAR NOT NULL PRIMARY KEY,
            snapshot_json VARCHAR NOT NULL,
            cached_at    TIMESTAMPTZ NOT NULL
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS fund_list_cache (
            cache_key   VARCHAR NOT NULL PRIMARY KEY,
            data_json   VARCHAR NOT NULL,
            cached_at   TIMESTAMPTZ NOT NULL
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS disclosure_cache (
            cache_key   VARCHAR NOT NULL PRIMARY KEY,
            data_json   VARCHAR NOT NULL,
            cached_at   TIMESTAMPTZ NOT NULL
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS disclosure_detail_cache (
            url         VARCHAR NOT NULL PRIMARY KEY,
            data_json   VARCHAR NOT NULL,
            cached_at   TIMESTAMPTZ NOT NULL
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS metrics_cache (
            fund_code   VARCHAR NOT NULL PRIMARY KEY,
            data_json   VARCHAR NOT NULL,
            cached_at   TIMESTAMPTZ NOT NULL
        )
    """)

    # FTS table for disclosures
    con.execute("""
        CREATE TABLE IF NOT EXISTS disclosure_fts (
            disclosure_index INTEGER NOT NULL PRIMARY KEY,
            fund_code   VARCHAR NOT NULL,
            company_name VARCHAR NOT NULL,
            subject     VARCHAR NOT NULL,
            summary     VARCHAR NOT NULL,
            publish_date DATE NOT NULL,
            url         VARCHAR NOT NULL
        )
    """)


# ---------------------------------------------------------------------------
# TTL-aware generic helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def cache_get(table: str, key_col: str, key_val: Any, ttl_seconds: int) -> Any | None:
    """Return cached JSON string if fresh, else None."""
    con = get()
    rows = con.execute(
        f"SELECT data_json, cached_at FROM {table} WHERE {key_col} = ?", [key_val]
    ).fetchall()
    if not rows:
        return None
    data_json, cached_at = rows[0]
    age = (_now() - cached_at).total_seconds()
    if age > ttl_seconds:
        return None
    return data_json


def cache_set(table: str, key_col: str, key_val: Any, data: Any) -> None:
    """Upsert serialised data into the given single-key cache table."""
    con = get()
    json_str = json.dumps(data, default=str)
    con.execute(
        f"""
        INSERT INTO {table} ({key_col}, data_json, cached_at)
        VALUES (?, ?, ?)
        ON CONFLICT ({key_col}) DO UPDATE SET
            data_json  = excluded.data_json,
            cached_at  = excluded.cached_at
        """,
        [key_val, json_str, _now()],
    )


# ---------------------------------------------------------------------------
# Price cache helpers
# ---------------------------------------------------------------------------


def price_cache_get(
    fund_code: str, start_date: str, end_date: str, ttl_seconds: int
) -> list[dict] | None:
    """Return price rows for fund_code between start_date and end_date if all fresh."""
    con = get()
    rows = con.execute(
        """
        SELECT fund_code, date::VARCHAR, price, portfolio_size, share_count, person_count, cached_at
        FROM price_cache
        WHERE fund_code = ? AND date BETWEEN ? AND ?
        ORDER BY date
        """,
        [fund_code, start_date, end_date],
    ).fetchall()
    if not rows:
        return None
    # Check freshness of oldest cached_at
    oldest_age = max((_now() - r[6]).total_seconds() for r in rows)
    if oldest_age > ttl_seconds:
        return None
    return [
        {
            "fund_code": r[0],
            "date": r[1],
            "price": r[2],
            "portfolio_size": r[3],
            "share_count": r[4],
            "person_count": r[5],
        }
        for r in rows
    ]


def price_cache_set(fund_code: str, rows: list[dict]) -> None:
    con = get()
    now = _now()
    for row in rows:
        con.execute(
            """
            INSERT INTO price_cache (fund_code, date, price, portfolio_size, share_count, person_count, cached_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (fund_code, date) DO UPDATE SET
                price          = excluded.price,
                portfolio_size = excluded.portfolio_size,
                share_count    = excluded.share_count,
                person_count   = excluded.person_count,
                cached_at      = excluded.cached_at
            """,
            [
                fund_code,
                row["date"],
                row.get("price"),
                row.get("portfolio_size"),
                row.get("share_count"),
                row.get("person_count"),
                now,
            ],
        )


# ---------------------------------------------------------------------------
# Allocation cache helpers
# ---------------------------------------------------------------------------


def allocation_cache_get(fund_code: str, date_str: str, ttl_seconds: int) -> dict | None:
    con = get()
    rows = con.execute(
        "SELECT asset_json, cached_at FROM allocation_cache WHERE fund_code = ? AND date = ?",
        [fund_code, date_str],
    ).fetchall()
    if not rows:
        return None
    asset_json, cached_at = rows[0]
    if (_now() - cached_at).total_seconds() > ttl_seconds:
        return None
    return json.loads(asset_json)


def allocation_cache_set(fund_code: str, date_str: str, assets: dict) -> None:
    con = get()
    con.execute(
        """
        INSERT INTO allocation_cache (fund_code, date, asset_json, cached_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (fund_code, date) DO UPDATE SET
            asset_json = excluded.asset_json,
            cached_at  = excluded.cached_at
        """,
        [fund_code, date_str, json.dumps(assets), _now()],
    )


# ---------------------------------------------------------------------------
# Snapshot cache helpers
# ---------------------------------------------------------------------------


def snapshot_cache_get(fund_code: str, ttl_seconds: int) -> dict | None:
    con = get()
    rows = con.execute(
        "SELECT snapshot_json, cached_at FROM snapshot_cache WHERE fund_code = ?",
        [fund_code],
    ).fetchall()
    if not rows:
        return None
    snapshot_json, cached_at = rows[0]
    if (_now() - cached_at).total_seconds() > ttl_seconds:
        return None
    return json.loads(snapshot_json)


def snapshot_cache_set(fund_code: str, snapshot: dict) -> None:
    con = get()
    con.execute(
        """
        INSERT INTO snapshot_cache (fund_code, snapshot_json, cached_at)
        VALUES (?, ?, ?)
        ON CONFLICT (fund_code) DO UPDATE SET
            snapshot_json = excluded.snapshot_json,
            cached_at     = excluded.cached_at
        """,
        [fund_code, json.dumps(snapshot, default=str), _now()],
    )


# ---------------------------------------------------------------------------
# Disclosure FTS helpers
# ---------------------------------------------------------------------------


def fts_index_disclosure(d: dict) -> None:
    """Insert or replace a disclosure record in the FTS table."""
    con = get()
    con.execute(
        """
        INSERT INTO disclosure_fts (disclosure_index, fund_code, company_name, subject, summary, publish_date, url)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (disclosure_index) DO UPDATE SET
            subject      = excluded.subject,
            summary      = excluded.summary,
            publish_date = excluded.publish_date
        """,
        [
            d["index"],
            d.get("fund_code", ""),
            d.get("company_name", ""),
            d.get("subject", ""),
            d.get("summary", ""),
            d.get("publish_date", ""),
            d.get("url", ""),
        ],
    )


def fts_search(
    query: str, fund_code: str | None, start_date: str | None, end_date: str | None, limit: int
) -> list[dict]:
    """Full-text search over disclosure subject + summary using DuckDB LIKE."""
    con = get()
    conditions = ["(subject ILIKE ? OR summary ILIKE ?)"]
    params: list[Any] = [f"%{query}%", f"%{query}%"]

    if fund_code:
        conditions.append("fund_code = ?")
        params.append(fund_code.upper())
    if start_date:
        conditions.append("publish_date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("publish_date <= ?")
        params.append(end_date)

    where = " AND ".join(conditions)
    rows = con.execute(
        f"""
        SELECT disclosure_index, fund_code, company_name, subject, summary, publish_date::VARCHAR, url
        FROM disclosure_fts
        WHERE {where}
        ORDER BY publish_date DESC
        LIMIT ?
        """,
        params + [limit],
    ).fetchall()

    return [
        {
            "index": r[0],
            "fund_code": r[1],
            "company_name": r[2],
            "subject": r[3],
            "summary": r[4],
            "publish_date": r[5],
            "url": r[6],
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Fund group lookup
# ---------------------------------------------------------------------------


def lookup_fund_group(fund_code: str) -> str | None:
    """fund_list_cache'ten fon koduna ait KAP grup kodunu döndürür.

    cache_key formatı: 'kap_funds:{GROUP}:active' veya 'kap_funds:{GROUP}:all'
    Dönen değer örn: 'YF', 'BYF', 'EYF'.  Bulunamazsa None.
    """
    con = get()
    rows = con.execute(
        "SELECT cache_key, data_json FROM fund_list_cache WHERE cache_key LIKE 'kap_funds:%'"
    ).fetchall()
    code_upper = fund_code.strip().upper()
    for cache_key, data_json in rows:
        try:
            funds = json.loads(data_json)
            if any(f.get("code", "").upper() == code_upper for f in funds):
                # 'kap_funds:YF:active' → 'YF'
                parts = cache_key.split(":")
                if len(parts) >= 2:
                    return parts[1]
        except Exception:
            continue
    return None


def lookup_fund_oid(fund_code: str) -> str | None:
    """fund_list_cache'ten fon koduna ait KAP OID'ini döndürür.

    Dönen değer 32 karakterli hex OID (örn. '4028328c950ba8c70195140f682921da').
    Bulunamazsa None.
    """
    con = get()
    rows = con.execute(
        "SELECT data_json FROM fund_list_cache WHERE cache_key LIKE 'kap_funds:%'"
    ).fetchall()
    code_upper = fund_code.strip().upper()
    for (data_json,) in rows:
        try:
            funds = json.loads(data_json)
            for f in funds:
                if f.get("code", "").upper() == code_upper:
                    oid = f.get("oid", "")
                    return oid if oid else None
        except Exception:
            continue
    return None


# ---------------------------------------------------------------------------
# Cache invalidation
# ---------------------------------------------------------------------------


def purge_fund(fund_code: str) -> None:
    """Delete all cached data for a single fund code."""
    con = get()
    code = fund_code.upper()
    con.execute("DELETE FROM price_cache WHERE fund_code = ?", [code])
    con.execute("DELETE FROM allocation_cache WHERE fund_code = ?", [code])
    con.execute("DELETE FROM snapshot_cache WHERE fund_code = ?", [code])
    con.execute("DELETE FROM metrics_cache WHERE fund_code = ?", [code])
    con.execute("DELETE FROM disclosure_fts WHERE fund_code = ?", [code])
    logger.info("Purged all cache for fund %s", code)


def cache_status() -> dict:
    """Return row counts and approximate size for each cache table."""
    con = get()
    tables = [
        "price_cache",
        "allocation_cache",
        "snapshot_cache",
        "fund_list_cache",
        "disclosure_cache",
        "disclosure_detail_cache",
        "metrics_cache",
        "disclosure_fts",
    ]
    result: dict[str, Any] = {}
    for t in tables:
        row = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
        result[t] = {"rows": row[0] if row else 0}

    # DuckDB database file size
    if _db_path:
        p = Path(_db_path)
        if p.exists():
            result["db_file_size_mb"] = round(p.stat().st_size / 1024 / 1024, 2)
    return result
