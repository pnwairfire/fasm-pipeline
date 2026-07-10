"""CLI entrypoint: run a single FASM pipeline end-to-end and log to stdout.

    python -m fasm_pipeline <stream>
    fasm-pipeline <stream>            # via the installed console script

This is the interface an orchestrator invokes. It is deliberately
Prefect-agnostic — Prefect (or cron, or a bare `docker run`) launches this and
reads the logs; nothing here knows about work pools or deployments.
"""

import argparse
import logging
import sys

from fasm_pipeline import (
    airnow,
    clarity,
    exclusion_lists,
    fire_perimeters,
    fire_points,
    historical_aq_sync,
    historical_fire_detects_grid,
    hms_fire_detects,
    hms_smoke_plumes,
    mobile_monitors,
    outlooks,
    purple_air,
)

# Command name -> module exposing run(). Command names match the pipeline/table
# domains and are what an orchestrator passes as the container command.
REGISTRY = {
    "airnow": airnow,
    "clarity": clarity,
    "mobile-monitors": mobile_monitors,
    "purple-air": purple_air,
    "fire-points": fire_points,
    "fire-perimeters": fire_perimeters,
    "hms-fire-detects": hms_fire_detects,
    "historical-fire-detects-grid": historical_fire_detects_grid,
    "hms-smoke-plumes": hms_smoke_plumes,
    "outlooks": outlooks,
    "exclusion-lists": exclusion_lists,
    "historical-aq-sync": historical_aq_sync,
}


def main(argv=None):
    parser = argparse.ArgumentParser(prog="fasm-pipeline", description=__doc__)
    parser.add_argument("stream", choices=sorted(REGISTRY), help="which pipeline to run")
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="stdlib logging level (default: INFO)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    log = logging.getLogger("fasm_pipeline")

    module = REGISTRY[args.stream]
    log.info(f"Starting pipeline: {args.stream}")
    summary = module.run()
    log.info(f"Finished pipeline: {args.stream} — {summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
