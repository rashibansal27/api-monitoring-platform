"""
Centralised configuration via Pydantic BaseSettings.
All values can be overridden through environment variables or a .env file.
"""

from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ElasticsearchSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ES_", env_file=".env", extra="ignore")

    host: str = Field(default="http://localhost:9200", description="ES base URL")
    username: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    api_key: Optional[str] = Field(default=None)
    index_pattern: str = Field(
        default="payment-logs-*",
        description="Kibana index pattern to poll",
    )
    # How far back to look on each poll (seconds)
    poll_lookback_seconds: int = Field(default=60)
    # Max hits per ES query
    max_hits: int = Field(default=1000)
    timeout_seconds: int = Field(default=10)


class PrometheusSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PROM_", env_file=".env", extra="ignore")

    url: str = Field(default="http://localhost:9090", description="Prometheus base URL")
    timeout_seconds: int = Field(default=10)
    # Step resolution for range queries
    step_seconds: int = Field(default=60)


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_", env_file=".env", extra="ignore")

    bootstrap_servers: str = Field(default="localhost:9092")
    group_id: str = Field(default="api-monitoring-consumer")
    topics: list[str] = Field(default=["payment.transactions", "gateway.logs"])
    auto_offset_reset: str = Field(default="latest")
    # Use mock queue instead of real Kafka
    use_mock: bool = Field(default=True)


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DB_", env_file=".env", extra="ignore")

    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    name: str = Field(default="api_monitoring")
    user: str = Field(default="postgres")
    password: str = Field(default="postgres")
    pool_size: int = Field(default=10)
    max_overflow: int = Field(default=20)
    echo_sql: bool = Field(default=False)

    @property
    def async_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )

    @property
    def sync_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="REDIS_", env_file=".env", extra="ignore")

    host: str = Field(default="localhost")
    port: int = Field(default=6379)
    db: int = Field(default=0)
    password: Optional[str] = Field(default=None)
    decode_responses: bool = Field(default=True)

    @property
    def url(self) -> str:
        auth = f":{self.password}@" if self.password else ""
        return f"redis://{auth}{self.host}:{self.port}/{self.db}"


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="APP_", env_file=".env", extra="ignore")

    name: str = Field(default="API Monitoring Platform")
    env: str = Field(default="development")       # development | staging | production
    debug: bool = Field(default=False)
    log_level: str = Field(default="INFO")

    # API server
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000)

    # Polling scheduler — set False to disable background jobs
    enable_scheduler: bool = Field(default=True)

    @field_validator("env")
    @classmethod
    def validate_env(cls, v: str) -> str:
        allowed = {"development", "staging", "production"}
        if v not in allowed:
            raise ValueError(f"env must be one of {allowed}")
        return v

    @property
    def is_production(self) -> bool:
        return self.env == "production"


class Settings(BaseSettings):
    """Root settings object — composes all sub-settings."""
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app: AppSettings = Field(default_factory=AppSettings)
    elasticsearch: ElasticsearchSettings = Field(default_factory=ElasticsearchSettings)
    prometheus: PrometheusSettings = Field(default_factory=PrometheusSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Returns a cached singleton Settings instance.
    Call get_settings() anywhere; import overhead is zero after first call.
    """
    return Settings()
