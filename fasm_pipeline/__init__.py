"""fasm_pipeline — Prefect-agnostic ETL logic for the FASM data streams.

Each stream module exposes plain ``extract`` / ``transform`` / ``load`` style
functions plus a ``run()`` that chains them end-to-end. Nothing in this package
imports Prefect (or any orchestrator), so pipelines can be run and tested
standalone:

    from fasm_pipeline import airnow
    airnow.run()

or from the command line / a container:

    python -m fasm_pipeline airnow

Logging uses the standard library ``logging`` module under the ``fasm_pipeline``
root logger. The ``__main__`` CLI configures a stdout handler; when an
orchestrator imports and calls ``run()`` in-process, it owns log configuration.
"""
