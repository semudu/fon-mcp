"""Shared TEFAS fetch utilities with holiday/weekend date fallback."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from tefas_client import Tefas, TefasError

logger = logging.getLogger(__name__)

_MAX_DATE_ROLLBACK = 10  # gün


def to_business_day(d: date) -> date:
    """Cumartesi/Pazar ise önceki Cuma'ya çeker."""
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d


def fetch_with_fallback(
    fund_code: str | None,
    start_dt: date,
    end_dt: date,
    **fetch_kwargs: Any,
) -> dict:
    """TEFAS'tan veri çeker; boş yanıt (tatil/bayram) durumunda end_dt'yi geriye çekerek tekrar dener.

    fund_code None ise tüm fon listesi (fund_type/founder_code gibi kwargs ile) çekilir.

    Returns:
        ``tefas.fetch()`` çıktısıyla aynı yapıda dict (fund_code → Fund).
        Tüm denemeler başarısız olursa boş dict döner.
    """
    end_dt = to_business_day(end_dt)
    start_dt = to_business_day(start_dt)

    label = fund_code or "<all>"

    for _ in range(_MAX_DATE_ROLLBACK):
        if end_dt < start_dt:
            logger.warning(
                "No trading days found for %s in [%s, %s]",
                label,
                start_dt,
                end_dt,
            )
            return {}
        try:
            with Tefas() as tefas:
                if fund_code is not None:
                    return tefas.fetch(
                        fund_code, start_date=start_dt, end_date=end_dt, **fetch_kwargs
                    )
                else:
                    return tefas.fetch(start_date=start_dt, end_date=end_dt, **fetch_kwargs)
        except TefasError:
            logger.warning(
                "TEFAS error for %s on end_dt=%s (tatil/bayram olabilir), bir gün geri çekiliyor.",
                label,
                end_dt,
            )
            end_dt -= timedelta(days=1)
            end_dt = to_business_day(end_dt)

    return {}
