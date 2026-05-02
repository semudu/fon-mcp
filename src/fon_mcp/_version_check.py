"""GitHub yeni versiyon kontrolü — startup sırasında arka planda çalışır."""

from __future__ import annotations

import json
import logging
import threading
import urllib.error
import urllib.request
from importlib.metadata import PackageNotFoundError, version

logger = logging.getLogger(__name__)

_PACKAGE_NAME = "fon-mcp"
_TIMEOUT = 5  # saniye


def _current_version() -> str | None:
    try:
        return version(_PACKAGE_NAME)
    except PackageNotFoundError:
        return None


def _latest_github_version(github_repo: str) -> str | None:
    """GitHub Releases API'den en son sürüm etiketini döndürür."""
    url = f"https://api.github.com/repos/{github_repo}/releases/latest"
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/vnd.github+json", "User-Agent": _PACKAGE_NAME},
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            data = json.loads(resp.read())
            tag: str = data.get("tag_name", "")
            return tag.lstrip("v") if tag else None
    except Exception as exc:
        logger.debug("Versiyon kontrolü başarısız: %s", exc)
        return None


def _compare(current: str, latest: str) -> bool:
    """latest > current ise True döner (PEP 440 benzeri sayısal karşılaştırma)."""

    def to_tuple(v: str) -> tuple[int, ...]:
        parts = []
        for segment in v.split("."):
            try:
                parts.append(int(segment))
            except ValueError:
                parts.append(0)
        return tuple(parts)

    return to_tuple(latest) > to_tuple(current)


def _check(github_repo: str) -> None:
    current = _current_version()
    latest = _latest_github_version(github_repo)
    if not current or not latest:
        return
    if _compare(current, latest):
        logger.warning(
            "⚠️  fon-mcp yeni sürüm mevcut: %s → %s  (https://github.com/%s/releases/latest)",
            current,
            latest,
            github_repo,
        )
    else:
        logger.debug("fon-mcp güncel (%s).", current)


def check_in_background(github_repo: str) -> None:
    """Yeni versiyon kontrolünü arka planda (daemon thread) çalıştırır."""
    if not github_repo:
        return
    t = threading.Thread(target=_check, args=(github_repo,), daemon=True, name="version-check")
    t.start()
