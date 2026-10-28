"""Centralized Configuration Module for MergeWhisper.

Provides type-safe environment parsing, validation, and zero-crash fallbacks
for autonomous DevOps orchestration.
"""

from functools import lru_cache
from typing import Any, Optional

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Export alias for compatibility with specifications referencing pydanticSettingsConfigDict
pydanticSettingsConfigDict = SettingsConfigDict


class Settings(BaseSettings):
    """Centralized runtime settings loaded from environment variables and .env file.
    
    Guarantees safe instantiation even when external API credentials are omitted.
    """

    # Optional external AI service credentials (single key or comma-separated rotation list)
    gemini_api_key: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("gemini_api_key", "GEMINI_API_KEY"),
        description="Optional API key for Google Gemini model orchestration. If omitted, deterministic offline fallback is utilized.",
    )
    gemini_api_keys: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("gemini_api_keys", "GEMINI_API_KEYS"),
        description="Optional comma-separated list of Google Gemini API keys for active round-robin rotation (e.g., 'key1,key2').",
    )

    # Optional GitHub authorization token for ephemeral cloning and authenticated remote push
    github_token: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("github_token", "GITHUB_TOKEN", "gh_token", "GH_TOKEN"),
        description="Optional personal access token for GitHub operations (cloning private repos and pushing PR branches).",
    )

    # External LLM generation network timeout (30s threshold per tier)
    llm_timeout_seconds: float = Field(
        default=30.0,
        validation_alias=AliasChoices(
            "llm_timeout_seconds",
            "LLM_TIMEOUT_SECONDS",
            "llm_timeout",
            "LLM_TIMEOUT",
        ),
        description="Network read/execution timeout for external LLM generation in seconds.",
    )

    # Server binding interface ('hash' per specification with 'host' alias support)
    hash: str = Field(
        default="0.0.0.0",
        validation_alias=AliasChoices("hash", "host", "HASH", "HOST"),
        description="Network binding address or host hash for the ASGI runtime listener.",
    )

    # Server port specification
    port: int = Field(
        default=8000,
        ge=1,
        le=65535,
        validation_alias=AliasChoices("port", "PORT"),
        description="TCP port on which the Fast API application server binds.",
    )

    # Environment identifier
    active_environment: str = Field(
        default="development",
        validation_alias=AliasChoices(
            "active_environment",
            "environment",
            "env",
            "ACTIVE_ENVIRONMENT",
            "ENVIRONMENT",
            "ENV",
        ),
        description="Current runtime lifecycle stage (e.g., development, staging, production).",
    )

    # Application metadata
    app_name: str = Field(
        default="MergeWhisper",
        description="Application identifier used in headers and telemetry.",
    )

    app_version: str = Field(
        default="0.1.0",
        description="Semantic application release version.",
    )

    # Pydantic Settings configuration: Ingest from local .env with extra parameters ignored
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    @field_validator("gemini_api_key", mode="before")
    @classmethod
    def sanitize_api_key(cls, value: Any) -> Optional[str]:
        """Sanitize external API key input to guarantee safe instantiation without validation crashes."""
        if value is None:
            return None
        if isinstance(value, str):
            cleaned = value.strip()
            return cleaned if cleaned else None
        return str(value)

    @property
    def host(self) -> str:
        """Convenience property aligning 'hash' binding parameter to standard network terminology."""
        return self.hash

    def get_api_keys(self) -> list[str]:
        """Parse and return valid API keys from GEMINI_API_KEYS or GEMINI_API_KEY."""
        raw = self.gemini_api_keys or self.gemini_api_key or ""
        keys: list[str] = []
        for part in raw.split(","):
            cleaned = part.strip()
            if cleaned and cleaned != "your_api_key_here" and cleaned not in keys:
                keys.append(cleaned)
        return keys

    @property
    def is_gemini_available(self) -> bool:
        """Determines if live Gemini orchestration can be initiated."""
        return len(self.get_api_keys()) > 0

    @property
    def is_production(self) -> bool:
        """Returns True if the active environment is configured for production."""
        return self.active_environment.lower() in {"production", "prod"}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Thread-safe singleton accessor for application settings.
    
    Caches configuration state to avoid recurring disk I/O on hot execution paths.
    """
    return Settings()
