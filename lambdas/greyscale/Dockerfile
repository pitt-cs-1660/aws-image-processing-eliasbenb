FROM public.ecr.aws/lambda/python:3.13

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_LINK_MODE=copy

WORKDIR /app

COPY pyproject.toml ./

RUN uv sync --no-dev

COPY handler.py ./

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONPATH="/app/.venv/lib/python3.13/site-packages:/app"

CMD ["handler.greyscale_handler"]
