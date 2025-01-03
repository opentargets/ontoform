FROM ghcr.io/astral-sh/uv:0.5.14-python3.13-bookworm-slim

ADD . /app

WORKDIR /app
RUN uv sync --frozen

ENTRYPOINT ["uv", "run", "ontoform"]
