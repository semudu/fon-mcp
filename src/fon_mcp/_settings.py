"""Application settings via Pydantic Settings.

Priority (highest → lowest):
    1. Environment variables  (prefix: ``FON_MCP_``)
    2. config.toml            (looked up at paths below in order)
    3. Built-in defaults

Config file search order:
    1. Path given by ``FON_MCP_CONFIG_FILE`` env var
    2. ``./config.toml``  (cwd — useful for Docker volume mounts)
    3. ``~/.fon-mcp/config.toml``
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pydantic import field_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)


def _find_config_file() -> str | None:
    explicit = os.environ.get("FON_MCP_CONFIG_FILE")
    if explicit:
        return explicit
    candidates = [Path("config.toml"), Path.home() / ".fon-mcp" / "config.toml"]
    for p in candidates:
        if p.exists():
            return str(p)
    return None


class CacheTTL(BaseSettings):
    """Cache TTL values in seconds — all individually overridable."""

    snapshot: int = 900  # 15 min
    price: int = 86_400  # 1 day
    allocation: int = 86_400  # 1 day
    fund_list: int = 604_800  # 7 days
    disclosure: int = 3_600  # 1 hour
    disclosure_detail: int = 2_592_000  # 30 days
    metrics: int = 86_400  # 1 day

    model_config = SettingsConfigDict(env_prefix="FON_MCP_CACHE_TTL_")


class Settings(BaseSettings):
    # --- paths ---
    db_file: str = "~/.fon-mcp/cache.duckdb"
    attachments_dir: str = "~/.fon-mcp/attachments"

    # --- markitdown ---
    convert_on_download: bool = True  # auto-convert attachments to markdown

    # --- analytics ---
    risk_free_rate: float = 0.40  # annual, TCMB policy rate default

    # --- cache TTL (nested, also overridable flat via env) ---
    cache_ttl_snapshot: int = 900
    cache_ttl_price: int = 86_400
    cache_ttl_allocation: int = 86_400
    cache_ttl_fund_list: int = 604_800
    cache_ttl_disclosure: int = 3_600
    cache_ttl_disclosure_detail: int = 2_592_000
    cache_ttl_metrics: int = 86_400

    model_config = SettingsConfigDict(
        env_prefix="FON_MCP_",
        toml_file=_find_config_file() or "config.toml",
        toml_file_encoding="utf-8",
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (init_settings, env_settings, TomlConfigSettingsSource(settings_cls))

    @field_validator("db_file", "attachments_dir", mode="before")
    @classmethod
    def expand_path(cls, v: Any) -> str:
        return str(Path(str(v)).expanduser())


_settings: Settings | None = None


def get() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
