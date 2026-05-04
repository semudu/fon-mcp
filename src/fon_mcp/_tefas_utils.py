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


def prev_business_day(d: date) -> date:
    """Bir önceki iş gününe geçer; haftasonu varsa doğrudan Cuma'ya atlar.

    Örnekler:
        Pazartesi → Cuma   (haftasonunu atlıyor)
        Salı      → Pazartesi
        Cuma      → Perşembe
    """
    d -= timedelta(days=1)
    return to_business_day(d)


def _last_price_is_valid(result: dict, fund_code: str) -> bool:
    """Fonun son geçmiş kaydında sıfırdan büyük geçerli bir fiyat var mı?

    Yabancı piyasaların kapalı olduğu günlerde veya bazı resmi tatillerde TEFAS
    veriyi döndürebilir ama fiyat 0 ya da None olarak gelir — bu da geçersiz sayılır.
    """
    if fund_code not in result:
        return False
    history = result[fund_code].history
    if not history:
        return False
    # history ascending sıralı → son kayıt en güncel
    last = history[-1]
    return last.price is not None and last.price > 0


def fetch_with_fallback(
    fund_code: str | None,
    start_dt: date,
    end_dt: date,
    **fetch_kwargs: Any,
) -> dict:
    """TEFAS'tan veri çeker; boş yanıt veya sıfır fiyat durumunda end_dt'yi geriye çekerek dener.

    Aşağıdaki durumlarda bir gün geri çekip yeniden dener:
    - ``TefasError``: API boş body döndürdü (tatil/bayram/haftasonu)
    - Fiyat 0 veya None: Yabancı piyasa kapalı, bayram ya da yayımlanmamış fiyat

    fund_code None ise tüm fon listesi (fund_type/founder_code gibi kwargs ile) çekilir;
    bu durumda fiyat doğrulaması yapılmaz (liste sorgusu için anlamsız).

    Returns:
        ``tefas.fetch()`` çıktısıyla aynı yapıda dict (fund_code → Fund).
        Tüm denemeler başarısız olursa boş dict döner.
    """
    end_dt = to_business_day(end_dt)
    start_dt = to_business_day(start_dt)

    label = fund_code or "<all>"

    for attempt in range(_MAX_DATE_ROLLBACK):
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
                    result = tefas.fetch(
                        fund_code, start_date=start_dt, end_date=end_dt, **fetch_kwargs
                    )
                else:
                    result = tefas.fetch(start_date=start_dt, end_date=end_dt, **fetch_kwargs)
        except TefasError:
            logger.warning(
                "TEFAS error for %s on end_dt=%s (tatil/bayram olabilir), bir gün geri çekiliyor.",
                label,
                end_dt,
            )
            end_dt = prev_business_day(end_dt)
            continue

        # Tüm fon sorgusunda (fund_code=None): boş sonuç ve güncel tarih → tatil olabilir, geri çek
        if fund_code is None and not result:
            days_from_today = (date.today() - end_dt).days
            if days_from_today <= 5:
                logger.warning(
                    "Bos sonuc: <all> end_dt=%s (tatil/bayram olabilir), bir gun geri cekiliyor.",
                    end_dt,
                )
                end_dt = prev_business_day(end_dt)
                continue
            # Eski tarih için boş sonuç → gerçekten veri yok, devam etme
            return result

        # Belirli bir fon sorgulandığında: fiyat 0/None ise geçersiz say ve geri çek
        if fund_code is not None and not _last_price_is_valid(result, fund_code):
            logger.warning(
                "Sifir/gecersiz fiyat: %s end_dt=%s (yabanci piyasa kapali veya yayimlanmamis), "
                "bir gun geri cekiliyor. (deneme %d/%d)",
                fund_code,
                end_dt,
                attempt + 1,
                _MAX_DATE_ROLLBACK,
            )
            end_dt = prev_business_day(end_dt)
            continue

        return result

    return {}
