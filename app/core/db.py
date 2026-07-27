"""DB session factories.

Two engines against the *same* database, deliberately:

- The sync engine/Session (psycopg2) backs API request handlers. Requests are
  short and one-shot; a plain blocking session is simpler and there's no
  event loop to protect.
- The async engine/AsyncSession (asyncpg) backs the worker. The worker runs
  many jobs concurrently in a single process (see app/worker/sqs_consumer.py)
  - a blocking DB call there would stall the poll loop's ability to keep
  receiving messages and extending visibility heartbeats for *other*
  in-flight jobs. Async I/O is load-bearing here, not a style choice.
"""

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import Session

from app.config import get_settings

_settings = get_settings()

engine = create_engine(_settings.database_url_sync, pool_pre_ping=True)

async_engine = create_async_engine(_settings.database_url_async, pool_pre_ping=True)
AsyncSessionLocal = async_sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
)


def get_db():
    """FastAPI dependency: sync Session for API routes."""
    with Session(engine) as session:
        yield session


def get_async_db():
    """Async Session for worker / job-dispatcher code."""
    return AsyncSessionLocal()
