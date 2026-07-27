"""Placeholder job processor - stands in for real work (e.g. an OCR+LLM
pipeline in a production system). Always succeeds after a short simulated
delay, to exercise the happy path end to end."""

import asyncio
import random

from app.base.utils.logging import log_info
from app.core.semaphore_manager import get_semaphore


class ExampleQuickJob:
    @staticmethod
    async def process_job(*, task_id: str, run_id: str) -> dict:
        async with get_semaphore("job_processing"):
            duration = random.uniform(1, 3)
            log_info("Processing quick job", module="ExampleQuickJob", task_id=task_id, duration_s=round(duration, 2))
            await asyncio.sleep(duration)
        return {"outcome": "ok", "simulated_duration_s": round(duration, 2)}
