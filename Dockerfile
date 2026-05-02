FROM python:3.12-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy project files
COPY pyproject.toml uv.lock* ./
COPY src/ ./src/

# Install dependencies (no local path deps in Docker — they must be installed separately
# or the kap-client / tefas-client wheels must be provided)
RUN uv sync --no-dev

# Default data directory
ENV FON_MCP_DB_FILE=/data/cache.duckdb
ENV FON_MCP_ATTACHMENTS_DIR=/data/attachments

VOLUME ["/data"]

ENTRYPOINT ["uv", "run", "fon-mcp"]
