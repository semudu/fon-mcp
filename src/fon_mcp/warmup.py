"""Startup cache warmup — statik lookup verilerini önceden çeker.

Sunucu başlarken çağrılır.  Herhangi bir hata sunucuyu durdurmaz; sadece
loglara yazılır.  Cache TTL dolmadıkça aynı veri tekrar çekilmez.
"""

from __future__ import annotations

import logging

from kap_client import Kap
from tefas_client import Tefas

from fon_mcp import _db as db

logger = logging.getLogger(__name__)

# TEFAS lookup parametreleri
_TEFAS_FUND_TYPES = ["YAT", "EMK", "BYF"]
_TEFAS_FOUNDER_TYPES = ["YAT", "EMK"]

# KAP fon grupları
_KAP_FUND_GROUPS = ["BYF", "YF", "EYF", "OKS", "YYF"]


def run(cfg) -> None:
    """Tüm statik lookup verilerini cache'e yükler.

    Zaten taze cache varsa o grubu atlar.
    """
    logger.info("Cache warmup başlıyor…")
    _warmup_tefas_fund_types(cfg)
    _warmup_tefas_founders(cfg)
    _warmup_kap_fund_groups(cfg)
    logger.info("Cache warmup tamamlandı.")


# ---------------------------------------------------------------------------
# TEFAS
# ---------------------------------------------------------------------------


def _warmup_tefas_fund_types(cfg) -> None:
    needs = [
        ft
        for ft in _TEFAS_FUND_TYPES
        if db.cache_get("fund_list_cache", "cache_key", f"fund_types:{ft}", cfg.cache_ttl_fund_list)
        is None
    ]
    if not needs:
        return
    logger.info("TEFAS fon türleri çekiliyor: %s", needs)
    with Tefas() as tefas:
        for ft in needs:
            try:
                types = tefas.fetch_fund_types(ft)  # type: ignore[arg-type]
                result = [{"code": t.code, "name": t.name} for t in types]
                db.cache_set("fund_list_cache", "cache_key", f"fund_types:{ft}", result)
                logger.info("TEFAS fon türleri (%s): %d tür cache'e yazıldı", ft, len(result))
            except Exception:
                logger.warning("TEFAS fon türleri çekilemedi (%s)", ft, exc_info=True)


def _warmup_tefas_founders(cfg) -> None:
    needs = [
        ft
        for ft in _TEFAS_FOUNDER_TYPES
        if db.cache_get("fund_list_cache", "cache_key", f"founders:{ft}", cfg.cache_ttl_fund_list)
        is None
    ]
    if not needs:
        return
    logger.info("TEFAS kurucular çekiliyor: %s", needs)
    with Tefas() as tefas:
        for ft in needs:
            try:
                founders = tefas.fetch_founders(ft)  # type: ignore[arg-type]
                result = [{"code": f.code, "name": f.name} for f in founders]
                db.cache_set("fund_list_cache", "cache_key", f"founders:{ft}", result)
                logger.info("TEFAS kurucular (%s): %d kurucu cache'e yazıldı", ft, len(result))
            except Exception:
                logger.warning("TEFAS kurucular çekilemedi (%s)", ft, exc_info=True)


# ---------------------------------------------------------------------------
# KAP
# ---------------------------------------------------------------------------


def _warmup_kap_fund_groups(cfg) -> None:
    needs = [
        grp
        for grp in _KAP_FUND_GROUPS
        if db.cache_get(
            "fund_list_cache", "cache_key", f"kap_funds:{grp}:active", cfg.cache_ttl_fund_list
        )
        is None
    ]
    if not needs:
        return
    logger.info("KAP fon listeleri çekiliyor: %s", needs)
    with Kap() as kap:
        for grp in needs:
            try:
                funds = kap.fetch_funds(grp, include_liquidated=False)
                result = [
                    {
                        "oid": f.oid,
                        "code": f.code,
                        "title": f.title,
                        "fund_type": f.fund_type,
                        "is_active": f.is_active,
                    }
                    for f in funds
                ]
                db.cache_set("fund_list_cache", "cache_key", f"kap_funds:{grp}:active", result)
                logger.info("KAP %s: %d fon cache'e yazıldı", grp, len(result))
            except Exception:
                logger.warning("KAP fon listesi çekilemedi (%s)", grp, exc_info=True)
