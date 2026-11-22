from bytewax.testing import run_main

from recruiting.common import metrics
from recruiting.services.analytics.flow import flow

if __name__ == "__main__":
    metrics.start_metrics_server(8003)  # PROD: bytewax also has built-in OTLP tracing
    try:
        run_main(flow)  # single worker, blocks (tail=True). Use `python -m bytewax.run` for multiple workers.
    except KeyboardInterrupt:
        pass
