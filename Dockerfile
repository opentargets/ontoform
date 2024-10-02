FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ADD . /app

WORKDIR /app
RUN uv sync --frozen

ENTRYPOINT ["uv", "run", "ontoform"]
