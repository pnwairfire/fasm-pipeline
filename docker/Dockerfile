FROM python:3.12-slim
WORKDIR /app

# PYTHONPATH lets `from fasm_pipeline import ...` and `python -m fasm_pipeline`
# resolve without an editable install.
ENV PYTHONPATH=/app

# Install uv from official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml .
COPY fasm_pipeline/ fasm_pipeline/
RUN uv pip install --system --no-cache .

# The container runs one pipeline and logs to stdout, e.g.:
#   docker run --rm --env-file .env fasm-pipeline:latest airnow
# Prefect (on ECS/Fargate/EC2) provisions and launches this image; it never
# needs to live in this image. Secrets are injected at run time via --env-file
# or the orchestrator's env config — never baked in (see .example.env).
ENTRYPOINT ["python", "-m", "fasm_pipeline"]
