import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import get_settings
from app.routers import job_run, sub_task, sub_task_type

VERSION = "0.1.0"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Lazy import: on Lambda (lambda_handler.py sets DISABLE_POLLER=true
    # before importing this module) this import never runs, saving a little
    # cold-start time. On the worker (worker_entry.py) or in legacy
    # single-process mode, it does.
    from app.worker.sqs_consumer import poll_loop

    poller = None
    if not os.getenv("DISABLE_POLLER"):
        poller = asyncio.create_task(poll_loop())

    yield

    if poller:
        poller.cancel()
        try:
            await asyncio.wait_for(asyncio.shield(poller), timeout=30)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass


_is_prod = get_settings().env == "prod"

app = FastAPI(
    title="Mangum + SQS + Worker Template API",
    version=VERSION,
    docs_url=None if _is_prod else "/docs",
    redoc_url=None if _is_prod else "/redoc",
    openapi_url=None if _is_prod else "/openapi.json",
    lifespan=lifespan,
    # API Gateway exposes the app under a stage path (e.g. "/dev"); FastAPI
    # needs root_path to generate correct absolute URLs in Swagger UI.
    root_path=os.getenv("API_ROOT_PATH", ""),
)

app.include_router(job_run.router)
app.include_router(sub_task.router)
app.include_router(sub_task_type.router)


@app.get("/")
def read_root():
    return {"version": VERSION}
