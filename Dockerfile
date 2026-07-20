FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Change working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock ./
COPY src/ ./src/
COPY templates/ ./templates/

# Sync dependencies and compile bytecode
RUN uv sync --frozen --no-dev

# Ensure the entrypoint is run using uv
CMD ["uv", "run", "src/telegram_clone/main.py"]
