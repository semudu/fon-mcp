"""KAP MCP tools — 7 tools wrapping kap-client with DuckDB cache and markitdown."""

from __future__ import annotations

import json
import logging
import re
from datetime import date, timedelta
from pathlib import Path

import httpx
from kap_client import FundGroup, FundSubject, Kap
from mcp.server.fastmcp import FastMCP

from fon_mcp import _db as db
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
    FundSubject.FON_TOPLAM_GIDER_ORANI.value: "Fon Toplam Gider Oranı",
    FundSubject.SORUMLULUK_BEYANI.value: "Sorumluluk Beyanı",
}

FUND_GROUP_LABELS: dict[str, str] = {g.value: g.name.replace("_", " ").title() for g in FundGroup}

# Human-friendly document type aliases → FundSubject enum name
DOC_TYPE_ALIASES: dict[str, str] = {
    # Portföy
    "portföy": "KESINLESEN_PORTFOY_BILGILERI",
    "portfolyo": "KESINLESEN_PORTFOY_BILGILERI",
    "portfolio": "KESINLESEN_PORTFOY_BILGILERI",
    "kesinlesen portfoy": "KESINLESEN_PORTFOY_BILGILERI",
    "KESINLESEN_PORTFOY_BILGILERI": "KESINLESEN_PORTFOY_BILGILERI",
    # İzahname
    "izahname": "IZAHNAME",
    "prospectus": "IZAHNAME",
    "IZAHNAME": "IZAHNAME",
    # Yatırımcı bilgi formu / KID
    "yatirimci bilgi formu": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "yatırımcı bilgi formu": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "bilgilendirme formu": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "surekli bilgilendirme": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "kid": "FON_SUREKLI_BILGILENDIRME_FORMU",
    "FON_SUREKLI_BILGILENDIRME_FORMU": "FON_SUREKLI_BILGILENDIRME_FORMU",
    # Performans sunum raporu
    "performans": "PERFORMANS_SUNUM_RAPORU",
    "performans raporu": "PERFORMANS_SUNUM_RAPORU",
    "performance": "PERFORMANS_SUNUM_RAPORU",
    "PERFORMANS_SUNUM_RAPORU": "PERFORMANS_SUNUM_RAPORU",
    # Finansal rapor
    "finansal": "FINANSAL_RAPOR",
    "finansal rapor": "FINANSAL_RAPOR",
    "financial": "FINANSAL_RAPOR",
    "FINANSAL_RAPOR": "FINANSAL_RAPOR",
    # Fon gider bilgileri
    "gider": "FON_GIDER_BILGILERI",
    "gider bilgileri": "FON_GIDER_BILGILERI",
    "FON_GIDER_BILGILERI": "FON_GIDER_BILGILERI",
    # Fona ilişkin bilgiler
    "fona iliskin": "FONA_ILISKIN_BILGILER",
    "fon bilgileri": "FONA_ILISKIN_BILGILER",
    "FONA_ILISKIN_BILGILER": "FONA_ILISKIN_BILGILER",
}


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
            subject: Opsiyonel konu filtresi. Geçerli değerler: FINANSAL_RAPOR,
                KESINLESEN_PORTFOY_BILGILERI, PERFORMANS_SUNUM_RAPORU, FON_GIDER_BILGILERI,
                IZAHNAME, OZEL_DURUM_ACIKLAMASI, FON_SUREKLI_BILGILENDIRME_FORMU vb.

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
            attr = subject.upper()
            if hasattr(FundSubject, attr):
                subject_oids = [getattr(FundSubject, attr).value]

        with Kap() as kap:
            disclosures = kap.fetch_fund_disclosures(
                start_date=start_date,
                end_date=end_date,
                fund_code=code,
                fund_group=fund_group,
                subject_oids=subject_oids,
            )

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
    def download_attachment(url: str, filename: str = "", as_markdown: bool = True) -> dict:
        """Bir KAP bildirim ekini indirir ve opsiyonel olarak Markdown formatına çevirir.

        PDF, DOCX, XLSX ve diğer desteklenen formatlar otomatik olarak Markdown'a dönüştürülür.
        Bu sayede LLM doğrudan dosya içeriğini okuyabilir.

        Args:
            url: Dosyanın indirme URL'si (get_disclosure_attachments'dan gelen `url` alanı).
            filename: Dosya adı (opsiyonel). Belirtilmezse URL'den türetilir.
            as_markdown: True ise içerik Markdown formatında döndürülür (default True).
                False ise dosya diske kaydedilir ve yol döndürülür.

        Returns:
            as_markdown=True ise: {filename, markdown_content}
            as_markdown=False ise: {filename, saved_path}
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
                from markitdown import MarkItDown

                result_md = MarkItDown().convert(str(save_path))
                return {
                    "filename": safe_name,
                    "markdown_content": result_md.text_content,
                    "source": "api+markitdown",
                }
            except Exception as e:
                logger.warning("markitdown conversion failed for %s: %s", safe_name, e)
                return {
                    "filename": safe_name,
                    "saved_path": str(save_path),
                    "error": f"Markdown conversion failed: {e}",
                }

        return {"filename": safe_name, "saved_path": str(save_path)}

    @mcp.tool()
    def get_fund_document(
        fund_code: str,
        document_type: str,
        fund_group: str | None = None,
        days_back: int = 365,
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
            days_back: Kaç gün geriye bakılacağı (default 365). Belge bulunamazsa artırın.

        Returns:
            {fund_code, document_type, disclosure_date, disclosure_url, filename, markdown_content}
            veya hata durumunda {error, fund_code, document_type, searched_days}
        """
        code = fund_code.strip().upper()
        dt_key = document_type.strip().lower()
        subject_enum_name = DOC_TYPE_ALIASES.get(dt_key) or DOC_TYPE_ALIASES.get(
            document_type.strip()
        )
        if not subject_enum_name:
            return {
                "error": (
                    f"Bilinmeyen belge türü: '{document_type}'. "
                    f"Geçerli değerler: {', '.join(sorted(set(DOC_TYPE_ALIASES.values())))}"
                ),
                "fund_code": code,
            }

        end_dt = date.today()
        start_dt = end_dt - timedelta(days=days_back)

        subject_oids: list[str] | None = None
        if hasattr(FundSubject, subject_enum_name):
            subject_oids = [getattr(FundSubject, subject_enum_name).value]

        with Kap() as kap:
            disclosures = kap.fetch_fund_disclosures(
                start_date=start_dt.isoformat(),
                end_date=end_dt.isoformat(),
                fund_code=code,
                fund_group=fund_group,
                subject_oids=subject_oids,
            )

        with_attachment = [d for d in disclosures if d.has_attachment]
        if not with_attachment:
            all_count = len(disclosures)
            return {
                "error": (
                    f"Son {days_back} günde '{subject_enum_name}' türünde ek içeren bildirim bulunamadı. "
                    f"Toplam bulunan bildirim: {all_count}. days_back değerini artırmayı deneyin."
                ),
                "fund_code": code,
                "document_type": subject_enum_name,
                "searched_days": days_back,
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

        preferred = next(
            (a for a in attachments if re.search(r"\.(pdf|docx|xlsx)$", a.filename, re.IGNORECASE)),
            attachments[0],
        )

        attachments_dir = Path(cfg.attachments_dir)
        attachments_dir.mkdir(parents=True, exist_ok=True)
        safe_name = preferred.filename or preferred.url.split("/")[-1] or "attachment.bin"
        save_path = attachments_dir / safe_name

        with httpx.Client(timeout=60, follow_redirects=True) as client:
            resp = client.get(preferred.url, headers={"Referer": "https://www.kap.org.tr/"})
            resp.raise_for_status()
            save_path.write_bytes(resp.content)

        try:
            from markitdown import MarkItDown

            md_result = MarkItDown().convert(str(save_path))
            markdown_content = md_result.text_content
        except Exception as e:
            logger.warning("markitdown conversion failed for %s: %s", safe_name, e)
            markdown_content = f"[Dönüştürme hatası: {e}. Dosya: {save_path}]"

        return {
            "fund_code": code,
            "document_type": subject_enum_name,
            "disclosure_date": latest.publish_datetime.isoformat(),
            "disclosure_url": latest.url,
            "filename": safe_name,
            "markdown_content": markdown_content,
        }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


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
