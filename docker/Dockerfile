FROM python:3.14-slim
WORKDIR /app

# PYTHONPATH lets `from fasm_pipeline import ...` and `python -m fasm_pipeline`
# resolve without an editable install.
ENV PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fasm_pipeline/ fasm_pipeline/

# The container runs one pipeline and logs to stdout, e.g.:
#   docker run --rm --env-file .env fasm-pipeline:latest airnow
# Prefect (on ECS/Fargate/EC2) provisions and launches this image; it never
# needs to live in this image. Secrets are injected at run time via --env-file
# or the orchestrator's env config — never baked in (see .example.env).
ENTRYPOINT ["python", "-m", "fasm_pipeline"]
