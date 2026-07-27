"""Placeholder job processor that demonstrates BOTH failure paths the
dispatcher distinguishes (see dispatcher.py:_is_transient_infra_error):

- ~30% of runs raise a plain RuntimeError -> terminal failure. The task is
  marked FAILED and the SQS message is still deleted (retrying a
  deterministic bug wastes the visibility-timeout window for no benefit).
- ~10% of runs raise a ConnectionError -> transient infra failure. The task
  is left re-claimable and the SQS message is kept for prompt redelivery.
"""

import asyncio
import random

from app.base.utils.logging import log_info
from app.core.semaphore_manager import get_semaphore


class ExampleFlakyJob:
    @staticmethod
    async def process_job(*, task_id: str, run_id: str) -> dict:
        async with get_semaphore("job_processing"):
            duration = random.uniform(2, 5)
            log_info("Processing flaky job", module="ExampleFlakyJob", task_id=task_id, duration_s=round(duration, 2))
            await asyncio.sleep(duration)

            roll = random.random()
            if roll < 0.10:
                raise ConnectionError("simulated transient network blip talking to a downstream dependency")
            if roll < 0.40:
                raise RuntimeError("simulated deterministic processing failure")

        return {"outcome": "ok", "simulated_duration_s": round(duration, 2)}
