# fon-mcp

**Model Context Protocol (MCP) sunucusu — Türk yatırım fonu verileri (TEFAS + KAP)**

LLM'lerin [TEFAS](https://www.tefas.gov.tr) ve [KAP](https://www.kap.org.tr) verilerine doğrudan erişmesini sağlar. Fon fiyat geçmişi, portföy dağılımı, performans metrikleri, yatırımcı akış analizi ve KAP bildirimleri için 20 MCP aracı içerir. Tüm veriler yerel DuckDB cache'inde saklanır; tekrar sorgularda API çağrısı yapılmaz.

---

## Araçlar

### TEFAS (6 araç)
| Araç | Açıklama |
|------|----------|
| `get_fund_price_history` | NAV (pay değeri) geçmişi, portföy büyüklüğü, yatırımcı sayısı |
| `get_fund_snapshot` | Güncel anlık görünüm: getiri, sıralama, pazar payı |
| `get_fund_allocation` | Portföy dağılımı (hisse, bono, altın, döviz…) |
| `list_fund_types` | Şemsiye fon türleri |
| `list_founders` | Fon kurucuları |
| `search_funds` | Fon kodu, şemsiye türü veya isim filtresiyle arama |

### KAP (6 araç)
| Araç | Açıklama |
|------|----------|
| `list_kap_funds` | KAP'ta kayıtlı fonlar (BYF, YF, EYF…) |
| `get_fund_disclosures` | Fona ait KAP bildirimleri, konu filtresiyle |
| `get_disclosure_detail` | Bildirim sayfasının tam metin içeriği |
| `list_companies` | KAP'ta kayıtlı şirketler ve kurucu listesi |
| `get_company_disclosures` | Şirkete ait bildirimler |
| `download_attachment` | KAP bildirim ekini indir (PDF/DOCX → Markdown) |

### Analitik (6 araç)
| Araç | Açıklama |
|------|----------|
| `calculate_metrics` | CAGR, volatilite, max drawdown, Sharpe oranı |
| `compare_funds` | İki fon arasında metrik karşılaştırması |
| `correlate_funds` | İki fonun fiyat korelasyonu |
| `rank_funds` | Birden fazla fonun performans sıralaması |
| `analyze_investor_flow` | Dönemsel yatırımcı sayısı ve AUM değişim analizi |
| `rank_by_investor_flow` | Cache'deki tüm fonları yatırımcı/AUM akışına göre sırala |

### Admin (2 araç)
| Araç | Açıklama |
|------|----------|
| `get_cache_status` | DuckDB tablo satır sayıları ve dosya boyutu |
| `refresh_fund` | Bir fonun cache verisini temizle ve yeniden çek |

---

## Kurulum

### Ön Gereksinimler
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (önerilen) **veya** pip / pipx

`kap-client` ve `tefas-client` PyPI üzerinden otomatik olarak kurulur; ayrıca klonlanması gerekmez.

---

### Yöntem 1 — uvx (önerilen, kurulum gerektirmez)

`uvx` komutu paketi indirip izole bir ortamda çalıştırır. Herhangi bir `pip install` veya klonlama adımı gerekmez.

```bash
# Doğrudan çalıştır
uvx fon-mcp
```

MCP istemcileri bu yöntemi kullanarak fon-mcp'yi otomatik güncel tutar.

---

### Yöntem 2 — pip / pipx

```bash
pip install fon-mcp
fon-mcp

# veya
pipx install fon-mcp
fon-mcp
```

---

### Yöntem 3 — Kaynak koddan (geliştirici)

```bash
git clone https://github.com/your-org/fon-mcp
cd fon-mcp
uv sync
uv run fon-mcp
```

### Yöntem 4 — Docker

```bash
docker run -i --rm \
  -v fon_mcp_data:/data \
  ghcr.io/your-org/fon-mcp
```

#### Docker Compose

```bash
git clone https://github.com/your-org/fon-mcp
cd fon-mcp
docker compose up --build
```

`docker-compose.yml` içinde ortam değişkenleriyle yapılandırabilirsiniz:

```yaml
environment:
  FON_MCP_RISK_FREE_RATE: "0.40"
  FON_MCP_CACHE_TTL_PRICE: "86400"
```

---

## Yapılandırma

Tüm ayarlar üç kanaldan verilebilir (öncelik sırası):

1. **Ortam değişkenleri** — `FON_MCP_` ön ekiyle
2. **`config.toml`** — `FON_MCP_CONFIG_FILE` ile belirtilen yol, çalışma dizini veya `~/.fon-mcp/config.toml`
3. **Varsayılanlar**

### Ortam Değişkenleri Referansı

| Değişken | Tür | Varsayılan | Açıklama |
|----------|-----|-----------|----------|
| `FON_MCP_CONFIG_FILE` | `string` | — | `config.toml` için özel yol. Belirtilmezse çalışma dizini ve `~/.fon-mcp/` sırayla denenir. |
| `FON_MCP_DB_FILE` | `string` | `~/.fon-mcp/cache.duckdb` | DuckDB veritabanı dosyasının tam yolu. Docker'da genellikle `/data/cache.duckdb` yapılır. |
| `FON_MCP_ATTACHMENTS_DIR` | `string` | `~/.fon-mcp/attachments` | KAP eklerinin indirileceği dizin. |
| `FON_MCP_CONVERT_ON_DOWNLOAD` | `bool` | `true` | `true` — PDF/DOCX/XLSX ekleri indirilirken otomatik Markdown'a dönüştürülür. `false` — ham dosya saklanır. |
| `FON_MCP_RISK_FREE_RATE` | `float` | `0.40` | Sharpe oranı hesabında kullanılan **yıllık** risksiz getiri oranı (ondalık). `0.40` = %40. TCMB politika faizine yakın bir değer kullanın. |
| `FON_MCP_CACHE_TTL_SNAPSHOT` | `int` | `900` | `get_fund_snapshot` sonuçlarının cache'de tutulma süresi (saniye). Varsayılan: 15 dakika. |
| `FON_MCP_CACHE_TTL_PRICE` | `int` | `86400` | `get_fund_price_history` verilerinin cache süresi (saniye). Varsayılan: 1 gün. |
| `FON_MCP_CACHE_TTL_ALLOCATION` | `int` | `86400` | `get_fund_allocation` verilerinin cache süresi (saniye). Varsayılan: 1 gün. |
| `FON_MCP_CACHE_TTL_FUND_LIST` | `int` | `604800` | `list_kap_funds`, `search_funds` gibi fon listelerinin cache süresi (saniye). Varsayılan: 7 gün. |
| `FON_MCP_CACHE_TTL_DISCLOSURE` | `int` | `3600` | `get_fund_disclosures` sonuçlarının cache süresi (saniye). Varsayılan: 1 saat. |
| `FON_MCP_CACHE_TTL_DISCLOSURE_DETAIL` | `int` | `2592000` | `get_disclosure_detail` içeriğinin cache süresi (saniye). Yayımlanan bildirimler değişmediği için varsayılan: 30 gün. |
| `FON_MCP_CACHE_TTL_METRICS` | `int` | `86400` | `calculate_metrics` sonuçlarının cache süresi (saniye). Varsayılan: 1 gün. |

### config.toml

```toml
# config.toml — config.example.toml'u kopyalayarak oluşturun
# cp config.example.toml ~/.fon-mcp/config.toml

db_file               = "~/.fon-mcp/cache.duckdb"
attachments_dir       = "~/.fon-mcp/attachments"
convert_on_download   = true
risk_free_rate        = 0.40   # %40 → 0.40 | %45 → 0.45

# Cache TTL (saniye) — ihtiyaç duymadığınız satırları silebilirsiniz
cache_ttl_snapshot           = 900        # 15 dk
cache_ttl_price              = 86400      # 1 gün
cache_ttl_allocation         = 86400      # 1 gün
cache_ttl_fund_list          = 604800     # 7 gün
cache_ttl_disclosure         = 3600       # 1 saat
cache_ttl_disclosure_detail  = 2592000    # 30 gün
cache_ttl_metrics            = 86400      # 1 gün
```

---

## İstemci Yapılandırması

Her istemci için iki yöntem gösterilmiştir: **uvx** (önerilen — kurulum/klonlama gerektirmez) ve **Docker**.

> **Config dosyası:** Tüm istemciler ortam değişkeni (`FON_MCP_*`) veya yapılandırma dosyası (`~/.fon-mcp/config.toml`) ile özelleştirilebilir.

---

### Claude Desktop

`~/Library/Application Support/Claude/claude_desktop_config.json` (macOS)
`%APPDATA%\Claude\claude_desktop_config.json` (Windows)

**uvx (önerilen — kurulum gerektirmez):**

```json
{
  "mcpServers": {
    "fon-mcp": {
      "command": "uvx",
      "args": ["fon-mcp"],
      "env": {
        "FON_MCP_RISK_FREE_RATE": "0.40"
      }
    }
  }
}
```

**Docker:**

```json
{
  "mcpServers": {
    "fon-mcp": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm",
        "-v", "fon_mcp_data:/data",
        "-e", "FON_MCP_RISK_FREE_RATE=0.40",
        "ghcr.io/your-org/fon-mcp"
      ]
    }
  }
}
```

---

### Cursor

`~/.cursor/mcp.json` veya proje kökünde `.cursor/mcp.json`:

**uvx (önerilen):**

```json
{
  "mcpServers": {
    "fon-mcp": {
      "command": "uvx",
      "args": ["fon-mcp"]
    }
  }
}
```

**Docker:**

```json
{
  "mcpServers": {
    "fon-mcp": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm",
        "-v", "fon_mcp_data:/data",
        "ghcr.io/your-org/fon-mcp"
      ]
    }
  }
}
```

---

### VS Code / GitHub Copilot

`.vscode/mcp.json` (proje kökünde):

**uvx (önerilen):**

```json
{
  "servers": {
    "fon-mcp": {
      "type": "stdio",
      "command": "uvx",
      "args": ["fon-mcp"],
      "env": {
        "FON_MCP_RISK_FREE_RATE": "0.40"
      }
    }
  }
}
```

**Docker:**

```json
{
  "servers": {
    "fon-mcp": {
      "type": "stdio",
      "command": "docker",
      "args": [
        "run", "-i", "--rm",
        "-v", "fon_mcp_data:/data",
        "ghcr.io/your-org/fon-mcp"
      ]
    }
  }
}
```

---

### Zed

`~/.config/zed/settings.json` içine ekleyin:

**uvx (önerilen):**

```json
{
  "context_servers": {
    "fon-mcp": {
      "command": {
        "path": "uvx",
        "args": ["fon-mcp"]
      }
    }
  }
}
```

**Docker:**

```json
{
  "context_servers": {
    "fon-mcp": {
      "command": {
        "path": "docker",
        "args": [
          "run", "-i", "--rm",
          "-v", "fon_mcp_data:/data",
          "ghcr.io/your-org/fon-mcp"
        ]
      }
    }
  }
}
```

---

### Windsurf

`~/.codeium/windsurf/mcp_config.json`:

**uvx (önerilen):**

```json
{
  "mcpServers": {
    "fon-mcp": {
      "command": "uvx",
      "args": ["fon-mcp"]
    }
  }
}
```

**Docker:**

```json
{
  "mcpServers": {
    "fon-mcp": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm",
        "-v", "fon_mcp_data:/data",
        "ghcr.io/your-org/fon-mcp"
      ]
    }
  }
}
```

---

### MCP CLI (test / geliştirici)

```bash
# uvx ile doğrudan test
uvx fon-mcp

# Veya kaynak koddan
pip install "mcp[cli]"
mcp dev src/fon_mcp/server.py
```

---

## Örnek Sorgular

Yapılandırma tamamlandıktan sonra LLM'e şunları sorabilirsiniz:

```
Son 1 yılda en yüksek Sharpe oranına sahip 5 hisse fonu hangileri?

AAK fonunun Ocak–Mart 2025 dönemindeki portföy dağılımını göster.

THF fonu için son 6 aydaki KAP bildirimlerini listele.

Son 30 günde en çok yatırımcı kazanan 10 fon hangisi?

TI2 ile GOLD fonlarının fiyat korelasyonu nedir?

AFA fonunun son açıkladığı performans sunum raporunun içeriğini özetle.
```

---

## Mimari

```
fon-mcp (FastMCP)
├── tools/tefas.py      # TEFAS API → DuckDB cache
├── tools/kap.py        # KAP API → DuckDB cache + markitdown
├── tools/analytics.py  # DuckDB SQL window functions
├── tools/admin.py      # Cache yönetimi
├── _db.py              # DuckDB şema + generic cache helpers
└── _settings.py        # Pydantic Settings (env + toml)
```

- **Transport:** stdio (MCP standart)
- **Cache:** DuckDB (embedded, dosya tabanlı, sıfır bağımlılık)
- **Belge dönüşümü:** markitdown (PDF, DOCX, XLSX → Markdown)

---

## Lisans

MIT
