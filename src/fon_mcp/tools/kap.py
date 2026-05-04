"""KAP MCP tools — 7 tools wrapping kap-client with DuckDB cache and markitdown."""

from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import re
from datetime import date, timedelta
from pathlib import Path

import httpx
from kap_client import FundGroup, FundSubject, Kap
from mcp.server.fastmcp import FastMCP

from fon_mcp import _db as db
from fon_mcp import warmup as _warmup_mod
from fon_mcp._settings import get as settings

logger = logging.getLogger(__name__)

# KAP subject OID → human-readable label (subset of most useful)
SUBJECT_LABELS: dict[str, str] = {
    FundSubject.FINANSAL_RAPOR.value: "Finansal Rapor",
    FundSubject.FINANSAL_TABLO_BILDIRIMI.value: "Finansal Tablo Bildirimi",
    FundSubject.FON_GIDER_BILGILERI.value: "Fon Gider Bilgileri",
    FundSubject.FON_SUREKLI_BILGILENDIRME_FORMU.value: "Fon Sürekli Bilgilendirme Formu",
    FundSubject.FONA_ILISKIN_BILGILER.value: "Fona İlişkin Bilgiler",
    FundSubject.IZAHNAME.value: "İzahname",
    FundSubject.KESINLESEN_PORTFOY_BILGILERI.value: "Kesinleşen Portföy Bilgileri",
    FundSubject.OZEL_DURUM_ACIKLAMASI.value: "Özel Durum Açıklaması",
    FundSubject.PERFORMANS_SUNUM_RAPORU.value: "Performans Sunum Raporu",
    FundSubject.PORTFOY_DAGILIM_RAPORU.value: "Portföy Dağılım Raporu",
    FundSubject.FON_TOPLAM_GIDER_ORANI.value: "Fon Toplam Gider Oranı",
    FundSubject.SORUMLULUK_BEYANI.value: "Sorumluluk Beyanı",
}

FUND_GROUP_LABELS: dict[str, str] = {g.value: g.name.replace("_", " ").title() for g in FundGroup}

# Human-friendly aliases → FundSubject enum name (küçük harf anahtar, tam eşleşme)
# Hem get_fund_document hem get_fund_disclosures tarafından kullanılır.
SUBJECT_ALIASES: dict[str, str] = {
    # --- Portföy Dağılım Raporu (aylık standart portföy raporu) ---
    "portföy": "PORTFOY_DAGILIM_RAPORU",
    "portfolyo": "PORTFOY_DAGILIM_RAPORU",
    "portfolio": "PORTFOY_DAGILIM_RAPORU",
    "portföy dağılım raporu": "PORTFOY_DAGILIM_RAPORU",
    "portfoy dagilim raporu": "PORTFOY_DAGILIM_RAPORU",
    "dağılım raporu": "PORTFOY_DAGILIM_RAPORU",
    "dagilim raporu": "PORTFOY_DAGILIM_RAPORU",
    "portfoy_dagilim_raporu": "PORTFOY_DAGILIM_RAPORU",
    # --- Kesinleşen Portföy Bilgileri (BYF/özel fonlar için ayrı bildirim türü) ---
    "kesinlesen portfoy": "KESINLESEN_PORTFOY_BILGILERI",
    "kesinleşen portföy": "KESINLESEN_PORTFOY_BILGILERI",
    "kesinleşen portföy bilgileri": "KESINLESEN_PORTFOY_BILGILERI",
    "kesinlesen portfoy bilgileri": "KESINLESEN_PORTFOY_BILGILERI",
    "portföy bilgileri": "KESINLESEN_PORTFOY_BILGILERI",
    "kesinlesen_portfoy_bilgileri": "KESINLESEN_PORTFOY_BILGILERI",
    # --- İzahname ---
    "izahname": "IZAHNAME",
    "prospectus": "IZAHNAME",
    "izahname_": "IZAHNAME",
    # --- Yatırımcı Bilgi Formu / KID ---
    "yatirimci bilgi formu": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "yatırımcı bilgi formu": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "bilgilendirme formu": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "surekli bilgilendirme": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "sürekli bilgilendirme": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "fon surekli bilgilendirme formu": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "fon sürekli bilgilendirme formu": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "kid": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "fon_surekli_bilgilendirme_formu": "FON_SUREKLI_BILGILENDIRME_FORMU",
    # strateji/amaç/risk soruları da KID'e yönlendirilir
    "strateji": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "fon stratejisi": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "yatırım stratejisi": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "yatirim stratejisi": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "yatırım amacı": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "yatirim amaci": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "fon amacı": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "fon amaci": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "risk profili": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "risk": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "hedef kitle": "FON_SUREKLI_BILGILENDIRME_FORMU",
    # --- Performans Sunum Raporu ---
    "performans": "PERFORMANS_SUNUM_RAPORU",
    "performans raporu": "PERFORMANS_SUNUM_RAPORU",
    "performans sunum raporu": "PERFORMANS_SUNUM_RAPORU",
    "performance": "PERFORMANS_SUNUM_RAPORU",
    "performans_sunum_raporu": "PERFORMANS_SUNUM_RAPORU",
    # --- Finansal Rapor ---
    "finansal": "FINANSAL_RAPOR",
    "finansal rapor": "FINANSAL_RAPOR",
    "financial": "FINANSAL_RAPOR",
    "finansal_rapor": "FINANSAL_RAPOR",
    # --- Fon Gider Bilgileri ---
    "gider": "FON_GIDER_BILGILERI",
    "gider bilgileri": "FON_GIDER_BILGILERI",
    "fon gider bilgileri": "FON_GIDER_BILGILERI",
    "fon_gider_bilgileri": "FON_GIDER_BILGILERI",
    # --- Fon Toplam Gider Oranı ---
    "gider oranı": "FON_TOPLAM_GIDER_ORANI",
    "gider orani": "FON_TOPLAM_GIDER_ORANI",
    "fon toplam gider orani": "FON_TOPLAM_GIDER_ORANI",
    "fon toplam gider oranı": "FON_TOPLAM_GIDER_ORANI",
    "fon_toplam_gider_orani": "FON_TOPLAM_GIDER_ORANI",
    # --- Fona İlişkin Bilgiler ---
    "fona iliskin": "FONA_ILISKIN_BILGILER",
    "fona ilişkin": "FONA_ILISKIN_BILGILER",
    "fona iliskin bilgiler": "FONA_ILISKIN_BILGILER",
    "fona ilişkin bilgiler": "FONA_ILISKIN_BILGILER",
    "fon bilgileri": "FONA_ILISKIN_BILGILER",
    "fona_iliskin_bilgiler": "FONA_ILISKIN_BILGILER",
    # --- Özel Durum Açıklaması ---
    "özel durum": "OZEL_DURUM_ACIKLAMASI",
    "ozel durum": "OZEL_DURUM_ACIKLAMASI",
    "özel durum açıklaması": "OZEL_DURUM_ACIKLAMASI",
    "ozel durum aciklamasi": "OZEL_DURUM_ACIKLAMASI",
    "ozel_durum_aciklamasi": "OZEL_DURUM_ACIKLAMASI",
    # --- Sorumluluk Beyanı ---
    "sorumluluk beyanı": "SORUMLULUK_BEYANI",
    "sorumluluk beyani": "SORUMLULUK_BEYANI",
    "sorumluluk": "SORUMLULUK_BEYANI",
    "sorumluluk_beyani": "SORUMLULUK_BEYANI",
    # --- Yıllık Rapor ---
    "yıllık rapor": "YILLIK_RAPOR",
    "yillik rapor": "YILLIK_RAPOR",
    "yillik_rapor": "YILLIK_RAPOR",
    # --- Finansal Tablo Bildirimi ---
    "finansal tablo": "FINANSAL_TABLO_BILDIRIMI",
    "finansal tablo bildirimi": "FINANSAL_TABLO_BILDIRIMI",
    "finansal_tablo_bildirimi": "FINANSAL_TABLO_BILDIRIMI",
    # --- Genel Açıklama ---
    "genel açıklama": "GENEL_ACIKLAMA",
    "genel aciklama": "GENEL_ACIKLAMA",
    "genel_aciklama": "GENEL_ACIKLAMA",
    # --- Yatırımcı Raporu ---
    "yatırımcı raporu": "YATIRIMCI_RAPORU",
    "yatirimci raporu": "YATIRIMCI_RAPORU",
    "yatirimci_raporu": "YATIRIMCI_RAPORU",
    # --- Tanıtım Formu ---
    "tanıtım formu": "TANITIM_FORMU",
    "tanitim formu": "TANITIM_FORMU",
    "tanitim_formu": "TANITIM_FORMU",
}

# Geriye dönük uyumluluk için alias
DOC_TYPE_ALIASES = SUBJECT_ALIASES


_MAX_MD_CHARS = 200_000  # yanıta eklenecek varsayılan maksimum karakter


def _file_to_markdown(path: str, full_text: bool = False) -> tuple[str, int]:
    """Dosyayı Markdown metne çevirir.

    PDF → PyMuPDF (hızlı C extension, saniyeler içinde).
    DOCX/XLSX/diğer → markitdown (60s timeout).

    Returns:
        (text, total_chars) tuple.
        full_text=False ise text _MAX_MD_CHARS'da kırpılır, total_chars ham uzunluğu gösterir.
    """
    p = Path(path)
    logger.info("Belge dönüştürülüyor: %s (%.1f KB)", p.name, p.stat().st_size / 1024)

    if p.suffix.lower() == ".pdf":
        import fitz  # PyMuPDF

        doc = fitz.open(str(p))
        parts: list[str] = []
        for page in doc:
            parts.append(page.get_text())
        doc.close()
        text = "\n".join(parts)
    else:
        # DOCX / XLSX / diğer — markitdown ayrı thread'de, 60s timeout
        with concurrent.futures.ThreadPoolExecutor(max_workers=None) as ex:
            future = ex.submit(
                lambda: __import__("markitdown").MarkItDown().convert(str(p)).text_content
            )
            try:
                text = future.result(timeout=60)
            except concurrent.futures.TimeoutError:
                future.cancel()
                raise TimeoutError(f"Dönüşüm 60s içinde tamamlanamadı: {p.name}")

    total_chars = len(text)
    if not full_text and total_chars > _MAX_MD_CHARS:
        text = text[:_MAX_MD_CHARS]

    logger.info("Dönüşüm tamam: %s → %d / %d karakter", p.name, len(text), total_chars)
    return text, total_chars


def _resolve_subject(subject: str) -> str | None:
    """Doğal dil veya enum adını FundSubject enum adına çevirir. Bulunamazsa None döner."""
    # 1) Tam küçük harf eşleşmesi (doğal dil)
    key = subject.strip().lower()
    if key in SUBJECT_ALIASES:
        return SUBJECT_ALIASES[key]
    # 2) Büyük harf → doğrudan FundSubject enum adı (KESINLESEN_PORTFOY_BILGILERI gibi)
    upper = subject.strip().upper()
    if hasattr(FundSubject, upper):
        return upper
    # 3) Underscore ile normalize edilmiş küçük harf eşleşmesi
    normalized = upper.replace(" ", "_")
    if hasattr(FundSubject, normalized):
        return normalized
    return None


def register(mcp: FastMCP) -> None:
    cfg = settings()

    @mcp.tool()
    def list_kap_funds(fund_group: str, include_liquidated: bool = False) -> dict:
        """KAP'ta kayıtlı fonları grup koduna göre listeler.

        Args:
            fund_group: Fon grubu kodu. Geçerli değerler:
                BYF (Borsa Yatırım Fonları), YF (Yatırım Fonları),
                EYF (Emeklilik Yatırım Fonları), OKS (OKS EYF),
                YYF (Yabancı Yatırım Fonları), VFF (Varlık Finansman),
                KFF (Konut Finansman), GMF (Gayrimenkul), GSF (Girişim Sermayesi),
                PFF (Proje Finansman), TEYF (Tasfiye Edilen).
            include_liquidated: True ise tasfiye edilen fonlar da dahil edilir.

        Returns:
            {fund_group, funds: [{oid, code, title, fund_type, is_active}]}
        """
        group_str = fund_group.strip().upper()
        cache_key = f"kap_funds:{group_str}:{'all' if include_liquidated else 'active'}"
        cached_json = db.cache_get(
            "fund_list_cache", "cache_key", cache_key, cfg.cache_ttl_fund_list
        )
        if cached_json:
            return {"fund_group": group_str, "funds": json.loads(cached_json), "source": "cache"}

        with Kap() as kap:
            funds = kap.fetch_funds(group_str, include_liquidated=include_liquidated)

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
        db.cache_set("fund_list_cache", "cache_key", cache_key, result)
        return {"fund_group": group_str, "funds": result, "count": len(result), "source": "api"}

    @mcp.tool()
    def get_fund_disclosures(
        fund_code: str,
        start_date: str,
        end_date: str,
        fund_group: str | None = None,
        subject: str | None = None,
    ) -> dict:
        """Bir fona ait KAP bildirimlerini getirir.

        Args:
            fund_code: Fon kodu (örn. "THF", "AFA"). Büyük/küçük harf duyarsız.
            start_date: Başlangıç tarihi (YYYY-MM-DD).
            end_date: Bitiş tarihi (YYYY-MM-DD).
            fund_group: Opsiyonel fon grubu kodu (BYF, YF, EYF vb.) — sorguyu hızlandırır.
            subject: Opsiyonel konu filtresi. Hem Türkçe doğal dil ("portföy",
                "kesinleşen portföy bilgileri", "performans", "izahname", "gider",
                "finansal", "özel durum", "yatırımcı bilgi formu" vb.) hem de
                tam enum adı (KESINLESEN_PORTFOY_BILGILERI, PERFORMANS_SUNUM_RAPORU,
                FINANSAL_RAPOR, IZAHNAME, FON_GIDER_BILGILERI, FON_SUREKLI_BILGILENDIRME_FORMU,
                OZEL_DURUM_ACIKLAMASI, SORUMLULUK_BEYANI, YILLIK_RAPOR vb.) kabul edilir.

        Returns:
            {disclosures: [{index, publish_datetime, company_name, fund_code, subject, summary, has_attachment, url}]}
        """
        code = fund_code.strip().upper()
        cache_key = f"disclosures:{code}:{start_date}:{end_date}:{fund_group or ''}:{subject or ''}"
        cached_json = db.cache_get(
            "disclosure_cache", "cache_key", cache_key, cfg.cache_ttl_disclosure
        )
        if cached_json:
            return {"disclosures": json.loads(cached_json), "source": "cache"}

        subject_oids: list[str] | None = None
        if subject:
            enum_name = _resolve_subject(subject)
            if enum_name and hasattr(FundSubject, enum_name):
                subject_oids = [getattr(FundSubject, enum_name).value]
            else:
                logger.warning("Bilinmeyen subject: %r — konu filtresi uygulanmıyor", subject)

        # KAP API fundTypeList=[]-yi desteklemiyor → 500 döndürür.
        # fund_group bilinmiyorsa önce cache'den bak, yoksa warmup ile prefetch et.
        resolved_group = fund_group
        if not resolved_group:
            resolved_group = db.lookup_fund_group(code)
            if not resolved_group:
                _warmup_mod._warmup_kap_fund_groups(cfg)
                resolved_group = db.lookup_fund_group(code)

        # --- Strateji 1: fund OID + tek subject → filter endpoint (sınır yok) ---
        days_total = (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days
        fund_oid = db.lookup_fund_oid(code)
        used_filter = False

        if fund_oid and subject_oids and len(subject_oids) == 1 and days_total <= 365:
            try:
                with Kap() as kap:
                    disclosures = kap.fetch_fund_disclosures_by_filter(
                        fund_oid=fund_oid,
                        subject_oid=subject_oids[0],
                        days=days_total + 1,
                    )
                # Eski endpoint aralık dışını dahil etmez, biz de başlangıç tarihini uygulayalım
                start_dt = date.fromisoformat(start_date)
                disclosures = [d for d in disclosures if d.publish_datetime.date() >= start_dt]
                used_filter = True
            except Exception as exc:
                logger.warning(
                    "Filter endpoint başarısız (%s), byCriteria'ya geçiliyor: %s", code, exc
                )
                used_filter = False

        if not used_filter:
            with Kap() as kap:
                if resolved_group:
                    disclosures = _fetch_disclosures_chunked(
                        kap,
                        start_date=start_date,
                        end_date=end_date,
                        fund_code=code,
                        fund_group=resolved_group,
                        subject_oids=subject_oids,
                    )
                else:
                    # Son çare: tüm grupları tara (nadir durum)
                    logger.warning("Fon grubu bulunamadı (%s), tüm gruplar taranıyor", code)
                    seen: set[int] = set()
                    disclosures = []
                    for grp in _warmup_mod._KAP_FUND_GROUPS:
                        try:
                            batch = _fetch_disclosures_chunked(
                                kap,
                                start_date=start_date,
                                end_date=end_date,
                                fund_code=code,
                                fund_group=grp,
                                subject_oids=subject_oids,
                            )
                        except Exception:
                            continue
                        for d in batch:
                            if d.index not in seen:
                                seen.add(d.index)
                                disclosures.append(d)
                    disclosures.sort(key=lambda d: d.publish_datetime, reverse=True)

        result = [
            {
                "index": d.index,
                "publish_datetime": d.publish_datetime.isoformat(),
                "company_name": d.company_name,
                "fund_code": d.fund_code,
                "subject": d.subject,
                "summary": d.summary,
                "has_attachment": d.has_attachment,
                "is_corrective": d.is_corrective,
                "url": d.url,
            }
            for d in disclosures
        ]

        db.cache_set("disclosure_cache", "cache_key", cache_key, result)

        # Index into FTS table
        for item in result:
            db.fts_index_disclosure(
                {
                    "index": item["index"],
                    "fund_code": item["fund_code"],
                    "company_name": item["company_name"],
                    "subject": item["subject"],
                    "summary": item["summary"],
                    "publish_date": item["publish_datetime"][:10],
                    "url": item["url"],
                }
            )

        return {"disclosures": result, "count": len(result), "source": "api"}

    @mcp.tool()
    def search_disclosures(
        query: str,
        fund_code: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 20,
    ) -> dict:
        """Daha önce çekilmiş KAP bildirimlerini konu ve özet içeriğinde tam metin arama yapar.

        NOT: Bu araç yalnızca get_fund_disclosures ile daha önce cache'lenmiş bildirimlerde arama yapar.
        Arama yapmadan önce ilgili fonun bildirimlerini get_fund_disclosures ile çekmeniz gerekir.

        Args:
            query: Aranacak metin (büyük/küçük harf duyarsız).
            fund_code: Opsiyonel fon kodu filtresi.
            start_date: Opsiyonel başlangıç tarihi filtresi (YYYY-MM-DD).
            end_date: Opsiyonel bitiş tarihi filtresi (YYYY-MM-DD).
            limit: Maksimum sonuç sayısı (default 20).

        Returns:
            {results: [{index, fund_code, company_name, subject, summary, publish_date, url}]}
        """
        results = db.fts_search(query, fund_code, start_date, end_date, limit)
        return {"results": results, "count": len(results), "query": query}

    @mcp.tool()
    def get_disclosure_detail(url: str) -> dict:
        """Bir KAP bildiriminin tam içeriğini getirir (HTML parse edilmiş metin olarak).

        Bu bildirim içeriği 30 gün boyunca cache'lenir çünkü yayımlanmış bildirimler değişmez.

        Args:
            url: Bildirimin URL'si (get_fund_disclosures sonucundaki her bildirimde 'url' alanı olarak gelir).

        Returns:
            {url, content}
        """
        cached = db.cache_get(
            "disclosure_detail_cache", "url", url, cfg.cache_ttl_disclosure_detail
        )
        if cached:
            return {"url": url, "content": json.loads(cached), "source": "cache"}

        with httpx.Client(timeout=30, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
            html = resp.text

        content = _html_to_text(html)
        db.cache_set("disclosure_detail_cache", "url", url, content)
        return {"url": url, "content": content, "source": "api"}

    @mcp.tool()
    def get_disclosure_attachments(disclosure_index: int) -> dict:
        """Bir KAP bildirimine ait dosya eklerinin listesini döndürür.

        Args:
            disclosure_index: Bildirimin KAP index numarası.

        Returns:
            {index, attachments: [{filename, url}]}
        """
        with Kap() as kap:
            attachments = kap.fetch_attachments(disclosure_index)

        result = [{"filename": att.filename, "url": att.url} for att in attachments]
        return {"index": disclosure_index, "attachments": result, "count": len(result)}

    @mcp.tool()
    def download_attachment(
        url: str,
        filename: str = "",
        as_markdown: bool = True,
        full_text: bool = False,
    ) -> dict:
        """Bir KAP bildirim ekini indirir ve opsiyonel olarak Markdown formatına çevirir.

        PDF, DOCX, XLSX ve diğer desteklenen formatlar otomatik olarak Markdown'a dönüştürülür.
        Bu sayede LLM doğrudan dosya içeriğini okuyabilir.

        Args:
            url: Dosyanın indirme URL'si (get_disclosure_attachments'dan gelen `url` alanı).
            filename: Dosya adı (opsiyonel). Belirtilmezse URL'den türetilir.
            as_markdown: True ise içerik Markdown formatında döndürülür (default True).
                False ise dosya diske kaydedilir ve yol döndürülür.
            full_text: True ise 200.000 karakter sınırı uygulanmaz, tam içerik döndürülür.
                İçerik çok büyükse önce False ile çağırın; uyarı gelirse True ile tekrar çağırın.

        Returns:
            as_markdown=True ise: {filename, source_url, markdown_content}
            as_markdown=False ise: {filename, source_url, saved_path}
        """
        attachments_dir = Path(cfg.attachments_dir)
        attachments_dir.mkdir(parents=True, exist_ok=True)

        safe_name = filename or url.split("/")[-1] or "attachment.bin"
        save_path = attachments_dir / safe_name

        with httpx.Client(timeout=60, follow_redirects=True) as client:
            resp = client.get(url, headers={"Referer": "https://www.kap.org.tr/"})
            resp.raise_for_status()
            save_path.write_bytes(resp.content)

        if as_markdown:
            try:
                markdown_text, total_chars = _file_to_markdown(str(save_path), full_text=full_text)
                result: dict = {
                    "filename": safe_name,
                    "source_url": url,
                    "markdown_content": markdown_text,
                }
                if not full_text and total_chars > _MAX_MD_CHARS:
                    result["content_truncated"] = True
                    result["total_chars"] = total_chars
                    result["truncation_warning"] = (
                        f"İçerik {total_chars:,} karakter olduğundan ilk {_MAX_MD_CHARS:,} "
                        f"karakteri döndürüldü. Tam içerik için full_text=True ile tekrar çağırın."
                    )
                return result
            except Exception as e:
                logger.warning("Dönüşüm hatası (%s): %s", safe_name, e)
                return {
                    "filename": safe_name,
                    "source_url": url,
                    "saved_path": str(save_path),
                    "error": f"Dönüşüm hatası: {e}",
                }

        return {"filename": safe_name, "source_url": url, "saved_path": str(save_path)}

    @mcp.tool()
    def get_fund_document(
        fund_code: str,
        document_type: str,
        fund_group: str | None = None,
        days_back: int | None = None,
        full_text: bool = False,
    ) -> dict:
        """Bir fonun belirli türdeki en son KAP belgesini bulup Markdown olarak döndürür.

        Portföy raporu, izahname, yatırımcı bilgi formu, performans raporu gibi sık
        kullanılan belgeler için birden fazla araç çağırma gerekmeden tek seferde tam
        içeriğe ulaşmayı sağlar.

        Kullanım senaryoları:
        - "X fonunun son portföy dağılım raporu nedir?" → document_type="portföy"
        - "X fonunun izahnamesini getir" → document_type="izahname"
        - "Yatırımcı bilgi formunu göster" → document_type="yatırımcı bilgi formu"
        - "Son performans sunum raporunu özetle" → document_type="performans"

        Args:
            fund_code: Fon kodu (örn. "THF", "AFA", "AAK"). Büyük/küçük harf duyarsız.
            document_type: Belge türü. Kabul edilen değerler:
                "portföy" / "portfolio" / "KESINLESEN_PORTFOY_BILGILERI"
                "izahname" / "IZAHNAME"
                "yatırımcı bilgi formu" / "kid" / "FON_SUREKLI_BILGILENDIRME_FORMU"
                "performans" / "PERFORMANS_SUNUM_RAPORU"
                "finansal" / "FINANSAL_RAPOR"
                "gider" / "FON_GIDER_BILGILERI"
            fund_group: Opsiyonel fon grubu (BYF, YF, EYF vb.) — sorguyu hızlandırır.
            days_back: Kaç gün geriye bakılacağı. Verilmezse otomatik olarak 60 → 180 → 365
                gün sırasıyla genişletilir; raporlar aylık yayınlandığından çoğu durumda
                60 gün yeterlidir.
            full_text: True ise 200.000 karakter sınırı uygulanmaz, tam içerik döndürülür.
                İçerik çok büyükse önce False ile çağırın; uyarı gelirse True ile tekrar çağırın.

        Returns:
            {fund_code, document_type, disclosure_date, disclosure_url, filename, markdown_content}
            veya hata durumunda {error, fund_code, document_type, searched_days}
        """
        code = fund_code.strip().upper()
        subject_enum_name = _resolve_subject(document_type)
        if not subject_enum_name:
            return {
                "error": (
                    f"Bilinmeyen belge türü: '{document_type}'. "
                    f"Örnekler: portföy, izahname, performans, finansal, gider, "
                    f"yatırımcı bilgi formu, özel durum, sorumluluk beyanı, yıllık rapor"
                ),
                "fund_code": code,
            }

        # Aranacak gün aralıkları: açıkça verilmişse tek seferlik, yoksa otomatik genişlet
        search_ranges = [days_back] if days_back is not None else [60, 180, 365]

        subject_oid_value: str | None = None
        subject_oids: list[str] | None = None
        if hasattr(FundSubject, subject_enum_name):
            subject_oid_value = getattr(FundSubject, subject_enum_name).value
            subject_oids = [subject_oid_value]

        # fund_group bilinmiyorsa cache'den bak, yoksa warmup ile çek (bir kez)
        resolved_group = fund_group
        if not resolved_group:
            resolved_group = db.lookup_fund_group(code)
            if not resolved_group:
                _warmup_mod._warmup_kap_fund_groups(cfg)
                resolved_group = db.lookup_fund_group(code)

        end_dt = date.today()
        with_attachment: list = []
        actual_days = search_ranges[-1]

        # --- Strateji 1: fund OID + subject OID → filter endpoint (360 gün, sınır yok) ---
        fund_oid = db.lookup_fund_oid(code)
        if fund_oid and subject_oid_value:
            try:
                filter_days = max(search_ranges)
                with Kap() as kap:
                    disclosures = kap.fetch_fund_disclosures_by_filter(
                        fund_oid=fund_oid,
                        subject_oid=subject_oid_value,
                        days=filter_days,
                    )
                with_attachment = [d for d in disclosures if d.has_attachment]
                actual_days = filter_days
                logger.info(
                    "Filter endpoint kullanıldı (%s/%s): %d sonuç, %d ekli",
                    code,
                    subject_enum_name,
                    len(disclosures),
                    len(with_attachment),
                )
            except Exception as exc:
                logger.warning(
                    "Filter endpoint başarısız (%s/%s): %s — byCriteria'ya geçiliyor",
                    code,
                    subject_enum_name,
                    exc,
                )
                with_attachment = []  # fall-back tetiklensin

        # --- Strateji 2: byCriteria endpoint (chunked, date-range) ---
        if not with_attachment:
            for days in search_ranges:
                actual_days = days
                start_dt = end_dt - timedelta(days=days)
                if days != search_ranges[0]:
                    logger.info(
                        "Belge bulunamadı (%s/%s), arama aralığı genişletiliyor: %d gün",
                        code,
                        subject_enum_name,
                        days,
                    )

                with Kap() as kap:
                    if resolved_group:
                        disclosures = _fetch_disclosures_chunked(
                            kap,
                            start_date=start_dt.isoformat(),
                            end_date=end_dt.isoformat(),
                            fund_code=code,
                            fund_group=resolved_group,
                            subject_oids=subject_oids,
                        )
                    else:
                        # Son çare: tüm grupları tara (warmup başarısız olduysa)
                        logger.warning("Fon grubu bulunamadı (%s), tüm gruplar taranıyor", code)
                        seen: set[int] = set()
                        disclosures = []
                        for grp in _warmup_mod._KAP_FUND_GROUPS:
                            try:
                                batch = _fetch_disclosures_chunked(
                                    kap,
                                    start_date=start_dt.isoformat(),
                                    end_date=end_dt.isoformat(),
                                    fund_code=code,
                                    fund_group=grp,
                                    subject_oids=subject_oids,
                                )
                            except Exception:
                                continue
                            for d in batch:
                                if d.index not in seen:
                                    seen.add(d.index)
                                    disclosures.append(d)
                        disclosures.sort(key=lambda d: d.publish_datetime, reverse=True)

                with_attachment = [d for d in disclosures if d.has_attachment]
                if with_attachment:
                    break  # Belge bulundu, genişletmeye gerek yok

        if not with_attachment:
            all_count = len(disclosures)
            return {
                "error": (
                    f"Son {actual_days} günde '{subject_enum_name}' türünde ek içeren bildirim bulunamadı. "
                    f"Toplam bulunan bildirim: {all_count}."
                ),
                "fund_code": code,
                "document_type": subject_enum_name,
                "searched_days": actual_days,
            }

        latest = with_attachment[0]

        with Kap() as kap:
            attachments = kap.fetch_attachments(latest.index)

        if not attachments:
            return {
                "error": f"Bildirim bulundu (index={latest.index}) ancak ek listesi boş geldi.",
                "fund_code": code,
                "disclosure_url": latest.url,
            }

        # Dönüştürülebilir ekleri filtrele (PDF/DOCX/XLSX); hiç yoksa hepsini al
        convertible = [
            a for a in attachments if re.search(r"\.(pdf|docx|xlsx)$", a.filename, re.IGNORECASE)
        ] or attachments

        attachments_dir = Path(cfg.attachments_dir)
        attachments_dir.mkdir(parents=True, exist_ok=True)

        sections: list[str] = []
        filenames: list[str] = []
        truncated_files: list[dict] = []

        def _download_and_convert(att) -> tuple[str, str, str | None]:
            """(safe_name, section_text, error_or_None) döndürür."""
            sname = att.filename or att.url.split("/")[-1] or "attachment.bin"
            spath = attachments_dir / sname
            try:
                with httpx.Client(timeout=60, follow_redirects=True) as c:
                    r = c.get(att.url, headers={"Referer": "https://www.kap.org.tr/"})
                    r.raise_for_status()
                    spath.write_bytes(r.content)
            except Exception as e:
                logger.warning("Ek indirilemedi (%s): %s", sname, e)
                return sname, f"### {sname}\n[İndirme hatası: {e}]", str(e)
            try:
                md_text, total_chars = _file_to_markdown(str(spath), full_text=full_text)
                if not full_text and total_chars > _MAX_MD_CHARS:
                    truncated_files.append({"filename": sname, "total_chars": total_chars})
                return sname, f"### {sname}\n{md_text}", None
            except Exception as e:
                logger.warning("Dönüşüm hatası (%s): %s", sname, e)
                return sname, f"### {sname}\n[Dönüştürme hatası: {e}]", str(e)

        workers = min(len(convertible), (os.cpu_count() or 4) * 2)
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_download_and_convert, att) for att in convertible]
            for fut in concurrent.futures.as_completed(futures):
                sname, section, _ = fut.result()
                sections.append(section)
                filenames.append(sname)

        markdown_content = "\n\n---\n\n".join(sections)

        response: dict = {
            "fund_code": code,
            "document_type": subject_enum_name,
            "disclosure_date": latest.publish_datetime.isoformat(),
            "disclosure_url": latest.url,
            "source_urls": [att.url for att in convertible],
            "filenames": filenames,
            "attachment_count": len(filenames),
            "markdown_content": markdown_content,
        }
        if truncated_files:
            response["content_truncated"] = True
            response["truncated_files"] = truncated_files
            response["truncation_warning"] = (
                f"{len(truncated_files)} dosya {_MAX_MD_CHARS:,} karakterde kesildi: "
                + ", ".join(f"{t['filename']} ({t['total_chars']:,} kar.)" for t in truncated_files)
                + " — Tam içerik için full_text=True ile tekrar çağırın."
            )
        return response


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# KAP API rejects date ranges longer than this with HTTP 500 (wrapped 400).
_KAP_MAX_CHUNK_DAYS = 90


def _fetch_disclosures_chunked(
    kap: Kap,
    start_date: str,
    end_date: str,
    fund_code: str,
    fund_group: str,
    subject_oids: list[str] | None = None,
) -> list:
    """KAP API'sinin 90-gün sınırını aşmamak için tarih aralığını chunk'lara böler.

    Toplam aralık 90 günden kısa ise tek çağrı yapar.
    Uzunsa, 90-günlük parçalar hâlinde çağırır ve sonuçları birleştirir.
    """
    from datetime import date as _date

    start_dt = _date.fromisoformat(start_date)
    end_dt = _date.fromisoformat(end_date)
    total_days = (end_dt - start_dt).days

    if total_days <= _KAP_MAX_CHUNK_DAYS:
        return kap.fetch_fund_disclosures(
            start_date=start_date,
            end_date=end_date,
            fund_code=fund_code,
            fund_group=fund_group,
            subject_oids=subject_oids,
        )

    # Chunk'lara böl
    seen: set[int] = set()
    results: list = []
    chunk_end = end_dt
    while chunk_end > start_dt:
        chunk_start = max(start_dt, chunk_end - timedelta(days=_KAP_MAX_CHUNK_DAYS))
        batch = kap.fetch_fund_disclosures(
            start_date=chunk_start.isoformat(),
            end_date=chunk_end.isoformat(),
            fund_code=fund_code,
            fund_group=fund_group,
            subject_oids=subject_oids,
        )
        for d in batch:
            if d.index not in seen:
                seen.add(d.index)
                results.append(d)
        chunk_end = chunk_start - timedelta(days=1)

    results.sort(key=lambda d: d.publish_datetime, reverse=True)
    return results


def _html_to_text(html: str) -> str:
    """Strip HTML tags and return readable plain text."""
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<(br|p|div|tr|li)[^>]*>", "\n", html, flags=re.IGNORECASE)
    html = re.sub(r"<td[^>]*>", "\t", html, flags=re.IGNORECASE)
    html = re.sub(r"<[^>]+>", "", html)
    html = html.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    html = html.replace("&nbsp;", " ").replace("&quot;", '"').replace("&#39;", "'")
    html = re.sub(r"\n{3,}", "\n\n", html)
    html = re.sub(r"[ \t]+", " ", html)
    return html.strip()
