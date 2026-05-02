"""fon-mcp FastMCP server — Türk yatırım fonları için MCP araç sunucusu."""

from __future__ import annotations

import logging
import sys

from mcp.server.fastmcp import FastMCP

from fon_mcp import _db as db
from fon_mcp import warmup
from fon_mcp._settings import get as settings
from fon_mcp.tools import admin, analytics, kap, tefas

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

mcp = FastMCP(
    "fon-mcp",
    instructions="""
Bu sunucu Türkiye'deki yatırım fonlarına (TEFAS) ve KAP bildirimlerine erişim sağlar.

## VERİ KAYNAKLARI

**TEFAS** → canlı fiyat, getiri, portföy dağılımı (anlık/güncel sayısal veri)
**KAP** → resmi bildirimler, PDF belgeler (portföy raporu, izahname, performans raporu vb.)

## ARAÇ SEÇİM KILAVUZU

### Fon kodu bilinmiyorsa
1. TEFAS fonu için → `search_funds(name_filter="...")` veya `list_fund_types` + `list_founders`
2. KAP fonu için → `list_kap_funds(fund_group="YF")` (BYF/EYF/YF vb.)

### Canlı fiyat / getiri / sıralama
→ `get_fund_snapshot(fund_code)` — güncel NAV, getiri, sıralama

### Fiyat geçmişi ve performans metrikleri
1. `get_fund_price_history(fund_code, start_date, end_date)`
2. `calculate_metrics(fund_code, start_date, end_date)` — CAGR, Sharpe, drawdown

### Portföy dağılımı (canlı sayısal veri)
→ `get_fund_allocation(fund_code)` — hisse/bono/altın/döviz dağılımı, TEFAS'tan

### KAP'tan resmi PDF belge (tek adım)
→ `get_fund_document(fund_code, document_type)` — en son belgeyi bulur, indirir, Markdown döndürür

  Kabul edilen `document_type` değerleri:
  - "portföy" → Kesinleşen Portföy Bilgileri raporu
  - "izahname" → Fon izahnamesi
  - "yatırımcı bilgi formu" veya "kid" → Sürekli Bilgilendirme Formu
  - "performans" → Performans Sunum Raporu
  - "finansal" → Finansal Rapor
  - "gider" → Fon Gider Bilgileri

### Son bildirimler listesi
→ `get_fund_disclosures(fund_code, start_date, end_date)` — son 30/90/365 gün

### Bildirim içeriği okuma (2 adım)
1. `get_fund_disclosures(...)` → URL'leri al
2. `get_disclosure_detail(url)` → HTML metnini oku (ek olmayan bildirimler için)

### Bildirim eki okuma (3 adım — manuel olarak yapmak gerektiğinde)
1. `get_fund_disclosures(...)` → `index` ve `has_attachment` alanlarını al
2. `get_disclosure_attachments(disclosure_index)` → ek URL'lerini al
3. `download_attachment(url)` → PDF/DOCX içeriğini Markdown olarak al

### Yatırımcı akış analizi
→ `analyze_investor_flow(fund_code, start_date, end_date)` — AUM ve yatırımcı değişimi
→ `rank_by_investor_flow(period_days=30)` — en çok/az kazanan fonlar

## DOĞRU ARAÇ SEÇİMİ

| Kullanıcı sorusu | Doğru araç |
|---|---|
| "X fonunun fiyatı ne?" | `get_fund_snapshot` |
| "X fonu son 1 yılda ne kadar kazandırdı?" | `calculate_metrics` |
| "X fonunun portföy dağılımı?" | `get_fund_allocation` (TEFAS, canlı) |
| "X fonunun KAP portföy raporu?" | `get_fund_document(…, "portföy")` |
| "X fonunun izahnamesi?" | `get_fund_document(…, "izahname")` |
| "X fonunun yatırımcı bilgi formu?" | `get_fund_document(…, "yatırımcı bilgi formu")` |
| "X fonunun son bildirimleri?" | `get_fund_disclosures` son 90 gün |
| "X fonunun en son performans raporu?" | `get_fund_document(…, "performans")` |
| "Son 30 günde en çok yatırımcı kazanan fonlar?" | `rank_by_investor_flow` |
| "X ve Y fonlarını karşılaştır" | `compare_funds` |

## ÖNEMLİ NOTLAR

- Tarihler her zaman YYYY-MM-DD formatında verilmeli.
- `get_fund_document` başarısız olursa (belge bulunamadı), `days_back` parametresini artırın.
- KAP fon kodu (örn. "THF") ile TEFAS fon kodu (örn. "THF") genellikle aynıdır.
- Veriler DuckDB cache'inde saklanır; aynı sorgu tekrarlandığında API'ye gidilmez.
""",
)


def main() -> None:
    cfg = settings()

    logger.info("fon-mcp başlatılıyor — DB: %s", cfg.db_file)
    db.init(cfg.db_file)

    try:
        warmup.run(cfg)
    except Exception:
        logger.warning("Cache warmup tamamlanamadı; sunucu çalışmaya devam ediyor.", exc_info=True)

    tefas.register(mcp)
    kap.register(mcp)
    analytics.register(mcp)
    admin.register(mcp)

    logger.info("Araçlar yüklendi. TEFAS: 6 | KAP: 7 | Analitik: 6 | Admin: 2 | Toplam: 21")

    # --- MCP Prompts: common workflow templates ---
    @mcp.prompt()
    def portfoy_raporu(fon_kodu: str) -> str:
        """Bir fonun en son KAP portföy dağılım raporunu getirip yorumlar."""
        return (
            f"{fon_kodu} fonunun en son resmi portföy dağılım raporunu KAP'tan getir ve içeriğini "
            f"analiz et. get_fund_document aracını document_type='portföy' ile kullan. "
            f"Rapordaki varlık sınıfları, ağırlıklar ve dikkat çeken değişiklikler hakkında yorum yap."
        )

    @mcp.prompt()
    def izahname_ozet(fon_kodu: str) -> str:
        """Bir fonun KAP izahnamesini getirir ve temel maddeleri özetler."""
        return (
            f"{fon_kodu} fonunun izahnamesini KAP'tan getir (get_fund_document, document_type='izahname'). "
            f"Yatırım stratejisi, risk profili, yönetim ücreti, hedef kitle ve kısıtlamalar hakkında "
            f"kısa bir özet çıkar."
        )

    @mcp.prompt()
    def son_bildirimler(fon_kodu: str, gun: int = 90) -> str:
        """Bir fonun son N günlük KAP bildirimlerini listeler ve önemli olanları öne çıkarır."""
        from datetime import date, timedelta

        end = date.today().isoformat()
        start = (date.today() - timedelta(days=gun)).isoformat()
        return (
            f"{fon_kodu} fonunun {start} ile {end} tarihleri arasındaki KAP bildirimlerini "
            f"get_fund_disclosures ile getir. Özel durum açıklamaları, yönetim değişiklikleri "
            f"ve portföy bildirimleri gibi önemli gelişmeleri vurgula."
        )

    @mcp.prompt()
    def performans_analizi(fon_kodu: str, baslangic: str, bitis: str) -> str:
        """Bir fon için kapsamlı performans analizi yapar."""
        return (
            f"{fon_kodu} fonunun {baslangic}–{bitis} dönemindeki performansını analiz et. "
            f"Şu adımları takip et:\n"
            f"1. get_fund_price_history ile fiyat geçmişini al\n"
            f"2. calculate_metrics ile CAGR, Sharpe, volatilite ve max drawdown hesapla\n"
            f"3. get_fund_snapshot ile güncel durumu kontrol et\n"
            f"Sonuçları yorumla ve risk-getiri dengesi hakkında yorum yap."
        )

    mcp.run()


if __name__ == "__main__":
    main()
