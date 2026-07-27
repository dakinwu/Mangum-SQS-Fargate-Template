"""Global concurrency gate for job processors.

The real-world version of this pattern (see docs/architecture-notes.md)
typically has several named semaphores, each protecting a different scarce
resource (an LLM API's rate limit, memory-bound PDF conversion, a DB
connection pool, ...). This template keeps a single example semaphore,
`job_processing`, to demonstrate the shape without pretending there's a real
external dependency to protect.

Lazily initialized (not at import time) because asyncio.Semaphore binds to
the running event loop; constructing it at module import time - before
uvicorn/asyncio.run has started a loop - would bind it to the wrong one.
"""

import asyncio

from app.config import get_settings

_semaphores: dict[str, asyncio.Semaphore] = {}


def get_semaphore(name: str = "job_processing") -> asyncio.Semaphore:
    if name not in _semaphores:
        limit = get_settings().max_concurrent_tasks
        _semaphores[name] = asyncio.Semaphore(limit)
    return _semaphores[name]


def reset_semaphores() -> None:
    """Test-only: clear cached semaphores so a new event loop gets fresh ones."""
    _semaphores.clear()
