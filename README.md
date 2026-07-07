# fasm-pipeline

FASM **ETL pipelines**. Each pipeline pulls source data, transforms it, and loads to the tileserver DB or appropriate endpoints. Vanilla and agnostic implementation

## Streams

Each stream is a container command (also runnable as `python -m fasm_pipeline <name>`):

| Command | Pulls from | Loads to |
|---|---|---|
| `airnow` | S3 (AirNow geojson) | `pwfsl_map.airnow_monitors` |
| `clarity` | S3 (Clarity geojson) | `pwfsl_map.clarity_sensors` |
| `mobile-monitors` | S3 (AIRSIS + WRCC geojson) | `pwfsl_map.airsis_monitors`, `pwfsl_map.wrcc_monitors` |
| `purple-air` | S3 (PurpleAir csv) | `pwfsl_map.purple_air` |
| `fire-points` | AirFire DB | `pwfsl_map.fasm_fire_points` |
| `fire-perimeters` | AirFire DB | `pwfsl_map.fasm_fire_perimeters` |
| `hms-fire-detects` | S3 (HMS geojson) | `pwfsl_map.fire_detects` |
| `historical-fire-detects-grid` | AirFire DB | `pwfsl_map.fire_detects_historical_grid` |
| `hms-smoke-plumes` | S3 (HMS geojson) | `pwfsl_map.hms_smoke_plume` + S3 status file |
| `outlooks` | AirFire DB (outlook schema) | `pwfsl_map.outlooks` + S3 geojson |
| `exclusion-lists` | S3 (json) | `pwfsl_map.purple_air_exclusion`, `pwfsl_map.elwood_exclusion` |
| `historical-aq-sync` | TS DB | `pwfsl_map.*` (SQL sync) |

## Quick start (Docker)

```bash
# 1. build the image
docker build -f docker/Dockerfile -t fasm-pipeline:latest .

# 2. configure secrets
cp .example.env .env

# 3. run a stream
docker run --rm --env-file .env fasm-pipeline:latest airnow
```

For right now, secrets are injected at run time via `--env-file`: not baked into the image.

## Running locally (optional)

```bash
pip install -e .                # pyenv env pinned via .python-version
python -m fasm_pipeline airnow  # or: fasm-pipeline airnow
```

The process loads `.env` automatically. Add `--log-level DEBUG` for more detail.

## Environment variables

Copy `.example.env` to `.env` and fill in the **required** values. Everything
else is optional (defaults reproduce production behavior).

> **Production:** don't ship a `.env`. Inject secrets from SSM Parameter Store /
> Secrets Manager via the ECS task — see [SECRETS.md](SECRETS.md).

### Required — credentials & buckets

| Variable | Purpose |
|---|---|
| `TS_DB_HOST` / `TS_DB_PORT` / `TS_DB_USER` / `TS_DB_PW` / `TS_DB_DATABASE` | Tile Server DB — **destination** for every stream (schema `pwfsl_map`) |
| `AIRFIRE_DB_HOST` / `AIRFIRE_DB_PORT` / `AIRFIRE_DB_USER` / `AIRFIRE_DB_PW` / `AIRFIRE_DB_DATABASE` | AirFire DB — **source** for fire + outlook streams (schemas `fire_info`, `outlook_v7`) |
| `AWS_ACCESS_KEY` / `AWS_SECRET_ACCESS_KEY` / `AFE_BUCKET` | AirFire S3 — source data |
| `EPA_AWS_ACCESS_KEY` / `EPA_AWS_SECRET_ACCESS_KEY` / `EPA_BUCKET` | EPA S3 — HMS status / layer output |

(A given stream only needs the credentials for the services it touches — see the
Streams table.)

### Optional — S3 endpoint / region

For non-AWS or S3-compatible stores (MinIO, VPC endpoints). Unset → AWS defaults.

`AWS_ENDPOINT_URL`, `AWS_REGION`, `EPA_ENDPOINT_URL`, `EPA_REGION`

### Optional — repoint sources & targets

Every source/target is overridable via an env var of the same name (defaults in
`fasm_pipeline/config.py`), so you can move a stream without a code change:

- **Schemas**: `DEST_SCHEMA` (`pwfsl_map`), `FIRE_SCHEMA` (`fire_info`), `OUTLOOK_SCHEMA` (`outlook_v7`)
- **Destination tables**: `AIRNOW_TABLE`, `CLARITY_TABLE`, `AIRSIS_TABLE`, `WRCC_TABLE`, `PURPLE_AIR_TABLE`, `FIRE_POINTS_TABLE`, `FIRE_PERIMETERS_TABLE`, `FIRE_DETECTS_TABLE`, `FIRE_DETECTS_GRID_TABLE`, `HMS_SMOKE_TABLE`, `OUTLOOKS_TABLE`, `PURPLE_AIR_EXCLUSION_TABLE`, `ELWOOD_EXCLUSION_TABLE`
- **S3 source keys**: `AIRNOW_S3_KEY`, `CLARITY_S3_KEY`, `AIRSIS_S3_KEY`, `WRCC_S3_KEY`, `PURPLE_AIR_S3_KEY`, `HMS_FIRE_S3_KEY`, `HMS_SMOKE_S3_KEY`, `PURPLE_AIR_EXCLUSION_S3_KEY`, `ELWOOD_EXCLUSION_S3_KEY`
- **S3 output keys**: `OUTLOOKS_OUTPUT_S3_KEY`, `HMS_STATUS_S3_KEY`

Table/schema names are validated as SQL identifiers on startup. See `.example.env`
for the full list with default values.

> Note: `historical-aq-sync` runs bundled `.sql` files that reference
> `pwfsl_map.*` / `pwfsl_historical.*` directly, so `DEST_SCHEMA` does **not**
> repoint that stream — edit the SQL in `fasm_pipeline/sql/` instead.

## Logging

Pipelines log to stdout under the `fasm_pipeline` logger (level via
`--log-level`, default `INFO`). Each run logs an `EXTRACTED … / TRANSFORMED … /
LOADED …` trail and a final summary.
