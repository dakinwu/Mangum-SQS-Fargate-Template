"""Application settings.

Loading strategy (mirrors a real dev/prod split without actually wiring up a
secrets backend):
    ENV=local        -> read from `.env` (via pydantic-settings' env_file).
    ENV=dev|prod     -> read from process environment variables, then
                        overlaid with `_load_secrets_manager_overrides()`.

`_load_secrets_manager_overrides()` is a stub here (returns `{}`) so the
"secrets win over env vars" pattern is documented and the seam exists, even
though this template has no real Secrets Manager integration to demo.
"""

from functools import lru_cache
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    env: str = "local"

    # Database
    db_host: str = ""
    db_port: int = 5432
    db_user: str = ""
    db_password: str = ""
    db_database: str = ""

    # SQS
    sqs_queue_url: str = ""
    aws_endpoint_url: str | None = None  # set for LocalStack; unset for real AWS
    aws_region: str = "us-east-1"

    # API
    api_root_path: str = ""

    # Worker
    max_concurrent_tasks: int = 5

    # Secrets Manager (dev/prod only)
    secret_arn: str = ""

    @property
    def database_url_sync(self) -> str:
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_database}"
        )

    @property
    def database_url_async(self) -> str:
        return (
            f"postgresql+asyncpg://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_database}"
        )


def _load_secrets_manager_overrides(env: str) -> dict[str, Any]:
    """Stub for a real Secrets Manager lookup. Returns {} outside dev/prod so
    local development never depends on AWS credentials being present."""
    if env not in ("dev", "prod"):
        return {}
    return {}


@lru_cache()
def get_settings() -> Settings:
    settings = Settings()
    for key, value in _load_secrets_manager_overrides(settings.env).items():
        setattr(settings, key, value)
    return settings
