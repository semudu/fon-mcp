"""Tests for KAP MCP tools — most frequent user queries."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from fon_mcp.tools import kap
from fon_mcp.tools.kap import DOC_TYPE_ALIASES

# ---------------------------------------------------------------------------
# Helpers: build fake kap_client model objects
# ---------------------------------------------------------------------------


def _make_fund(code: str = "THF", oid: str = "abc123", fund_type: str = "Hisse Senedi Fonu"):
    f = MagicMock()
    f.oid = oid
    f.code = code
    f.title = f"{code} Test Fonu"
    f.fund_type = fund_type
    f.is_active = True
    return f


def _make_disclosure(
    index: int = 9001, fund_code: str = "THF", subject: str = "KESINLESEN_PORTFOY_BILGILERI"
):
    d = MagicMock()
    d.index = index
    d.publish_datetime = datetime(2025, 3, 1, 12, 0)
    d.company_name = "Tacirler Portföy"
    d.fund_code = fund_code
    d.stock_codes = ""
    d.subject = subject
    d.summary = "Test özeti"
    d.disclosure_type = "FR"
    d.has_attachment = True
    d.is_late = False
    d.is_corrective = False
    d.is_english = False
    d.url = f"https://www.kap.org.tr/tr/Bildirim/{index}"
    return d


def _make_attachment(filename: str = "portfoy.pdf"):
    a = MagicMock()
    a.filename = filename
    a.url = f"https://www.kap.org.tr/tr/attachments/{filename}"
    return a


def _mock_kap_cm(return_value_attr: str, return_value):
    """Build a Kap() context manager mock that returns a value for one method."""
    m = MagicMock()
    m.__enter__ = MagicMock(return_value=m)
    m.__exit__ = MagicMock(return_value=False)
    getattr(m, return_value_attr).return_value = return_value
    return m


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tools(mcp):
    kap.register(mcp)
    return mcp.tools


# ---------------------------------------------------------------------------
# Test: "YF grubundaki fonlar neler?" → list_kap_funds
# ---------------------------------------------------------------------------


class TestListKapFunds:
    def test_returns_fund_list(self, tools):
        funds = [_make_fund("THF"), _make_fund("AFA", oid="def456")]
        mock_kap = _mock_kap_cm("fetch_funds", funds)

        with patch("fon_mcp.tools.kap.Kap", return_value=mock_kap):
            result = tools["list_kap_funds"]("YF")

        assert result["fund_group"] == "YF"
        assert len(result["funds"]) == 2
        codes = [f["code"] for f in result["funds"]]
        assert "THF" in codes
        assert result["source"] == "api"

    def test_result_cached_on_repeat(self, tools):
        funds = [_make_fund("THF")]
        mock_kap = _mock_kap_cm("fetch_funds", funds)

        with patch("fon_mcp.tools.kap.Kap", return_value=mock_kap):
            tools["list_kap_funds"]("YF")
            result = tools["list_kap_funds"]("YF")

        assert result["source"] == "cache"
        mock_kap.fetch_funds.assert_called_once()  # API called only once

    def test_fund_group_uppercased(self, tools):
        mock_kap = _mock_kap_cm("fetch_funds", [])

        with patch("fon_mcp.tools.kap.Kap", return_value=mock_kap):
            result = tools["list_kap_funds"]("yf")

        assert result["fund_group"] == "YF"


# ---------------------------------------------------------------------------
# Test: "THF fonunun son bildirimleri" → get_fund_disclosures
# ---------------------------------------------------------------------------


class TestGetFundDisclosures:
    def test_returns_disclosures(self, tools):
        disclosures = [_make_disclosure(9001, "THF"), _make_disclosure(9002, "THF")]
        mock_kap = _mock_kap_cm("fetch_fund_disclosures", disclosures)

        with patch("fon_mcp.tools.kap.Kap", return_value=mock_kap):
            result = tools["get_fund_disclosures"]("THF", "2025-01-01", "2025-03-31")

        assert len(result["disclosures"]) == 2
        assert result["disclosures"][0]["fund_code"] == "THF"
        assert result["source"] == "api"

    def test_disclosures_cached(self, tools):
        mock_kap = _mock_kap_cm("fetch_fund_disclosures", [_make_disclosure()])

        with patch("fon_mcp.tools.kap.Kap", return_value=mock_kap):
            tools["get_fund_disclosures"]("THF", "2025-01-01", "2025-03-31")
            result = tools["get_fund_disclosures"]("THF", "2025-01-01", "2025-03-31")

        assert result["source"] == "cache"

    def test_fts_indexed_on_fetch(self, tools):
        from fon_mcp import _db as db

        disclosures = [_make_disclosure(7777, "THF", "KESINLESEN_PORTFOY_BILGILERI")]
        mock_kap = _mock_kap_cm("fetch_fund_disclosures", disclosures)

        with patch("fon_mcp.tools.kap.Kap", return_value=mock_kap):
            tools["get_fund_disclosures"]("THF", "2025-01-01", "2025-03-31")

        hits = db.fts_search("Test özeti", "THF", None, None, 10)
        assert any(h["fund_code"] == "THF" for h in hits)


# ---------------------------------------------------------------------------
# Test: "X fonunun portföy raporu" → get_fund_document
# ---------------------------------------------------------------------------


class TestGetFundDocument:
    def _setup_kap_mock(self, disclosure_list, attachments):
        """Return a Kap mock that alternates fetch_fund_disclosures / fetch_attachments calls."""
        m = MagicMock()
        m.__enter__ = MagicMock(return_value=m)
        m.__exit__ = MagicMock(return_value=False)
        m.fetch_fund_disclosures.return_value = disclosure_list
        m.fetch_attachments.return_value = attachments
        return m

    def test_unknown_document_type_returns_error(self, tools):
        result = tools["get_fund_document"]("THF", "bilinmeyen_tur")
        assert "error" in result
        assert "Bilinmeyen belge türü" in result["error"]

    def test_portfoy_alias_resolved(self, tools):
        disclosure = _make_disclosure(8001)
        attachment = _make_attachment("portfoy.pdf")
        mock_kap = self._setup_kap_mock([disclosure], [attachment])

        mock_response = MagicMock()
        mock_response.content = b"%PDF-1.4 test content"
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_response

        with (
            patch("fon_mcp.tools.kap.Kap", return_value=mock_kap),
            patch("fon_mcp.tools.kap.httpx.Client", return_value=mock_client),
        ):
            result = tools["get_fund_document"]("THF", "portföy")

        assert "error" not in result or "Markdown conversion failed" in result.get("error", "")
        assert result.get("fund_code") == "THF"

    def test_izahname_alias_resolves(self):
        assert DOC_TYPE_ALIASES.get("izahname") == "IZAHNAME"

    def test_kid_alias_resolves(self):
        assert DOC_TYPE_ALIASES.get("kid") == "FON_SUREKLI_BILGILENDIRME_FORMU"

    def test_performans_alias_resolves(self):
        assert DOC_TYPE_ALIASES.get("performans") == "PERFORMANS_SUNUM_RAPORU"

    def test_gider_alias_resolves(self):
        assert DOC_TYPE_ALIASES.get("gider") == "FON_GIDER_BILGILERI"

    def test_no_attachment_returns_error(self, tools):
        disc_no_attachment = _make_disclosure(8002)
        disc_no_attachment.has_attachment = False
        mock_kap = self._setup_kap_mock([disc_no_attachment], [])

        with patch("fon_mcp.tools.kap.Kap", return_value=mock_kap):
            result = tools["get_fund_document"]("THF", "portföy")

        assert "error" in result


# ---------------------------------------------------------------------------
# Test: search_disclosures (FTS)
# ---------------------------------------------------------------------------


class TestSearchDisclosures:
    def test_fts_search(self, tools):
        from fon_mcp import _db as db

        db.fts_index_disclosure(
            {
                "index": 6001,
                "fund_code": "AFA",
                "company_name": "Ak Portföy",
                "subject": "PERFORMANS_SUNUM_RAPORU",
                "summary": "2024 yılı performans raporu açıklaması",
                "publish_date": "2025-02-01",
                "url": "https://www.kap.org.tr/tr/Bildirim/6001",
            }
        )

        result = tools["search_disclosures"]("performans raporu", fund_code="AFA")
        assert len(result["results"]) >= 1
        assert result["results"][0]["fund_code"] == "AFA"

    def test_empty_result_for_no_match(self, tools):
        result = tools["search_disclosures"]("hiçbirşeybulunamazsın12345")
        assert result["results"] == []
