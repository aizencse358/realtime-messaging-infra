FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.4.29 /uv /uvx /usr/local/bin/

WORKDIR /app

ENV UV_PROJECT_ENVIRONMENT=/usr/local \
    UV_COMPILE_BYTECODE=1

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

COPY src ./src
COPY loadtest ./loadtest

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
