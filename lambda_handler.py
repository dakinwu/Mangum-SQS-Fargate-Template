"""AWS Lambda entrypoint for the API (Mangum + FastAPI).

DISABLE_POLLER must be set BEFORE `app.main` is imported: app.main's
lifespan closure reads it at import/startup time to decide whether to spawn
poll_loop() in-process. On Lambda the worker runs as its own long-lived
process (Fargate/ECS via worker_entry.py, or any container/VM), not
co-located with the API - a Lambda invocation is exactly the wrong lifetime
for a "poll forever" loop.

lifespan="off" is intentional too: Lambda invocations are short-lived and
bursty, so there's no amortization benefit to FastAPI's lifespan
startup/shutdown cost the way there is on a long-lived process - it would
just add latency to (some) cold starts for no ongoing benefit.
"""

import os

os.environ.setdefault("DISABLE_POLLER", "true")

from mangum import Mangum  # noqa: E402

from app.main import app  # noqa: E402

handler = Mangum(app, lifespan="off")
