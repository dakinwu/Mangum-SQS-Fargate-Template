"""Standalone entrypoint for the SQS worker (Fargate/ECS, or any container/VM
you run it on). Runs the same poll_loop() that app/main.py's lifespan can
also start in legacy single-process mode, but as a dedicated process with no
HTTP server attached.

Handles SIGINT/SIGTERM so an orchestrator's graceful-stop signal (e.g. ECS
task drain) gets a real chance to finish in-flight work before hard-killing
the process.
"""

import asyncio
import signal

from app.base.utils.logging import log_info
from app.worker.sqs_consumer import poll_loop


async def main() -> None:
    log_info("Worker starting (poll_loop)", module="Worker")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    poller = asyncio.create_task(poll_loop())

    await stop_event.wait()
    log_info("Shutdown signal received, stopping poll_loop", module="Worker")

    poller.cancel()
    try:
        # shield: the outer cancellation above already stopped waiting on
        # stop_event; shielding the poller here lets it run its own
        # CancelledError cleanup (releasing in-flight message visibility)
        # to completion instead of being cancelled a second time mid-cleanup.
        await asyncio.wait_for(asyncio.shield(poller), timeout=60)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass

    log_info("Worker stopped cleanly", module="Worker")


if __name__ == "__main__":
    asyncio.run(main())
