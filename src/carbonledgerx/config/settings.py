"""Runtime configuration for CarbonLedgerX."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    """Application settings with validated local project paths."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CARBONLEDGERX_",
        extra="ignore",
    )

    project_root: Path = Field(default_factory=lambda: DEFAULT_PROJECT_ROOT)
    raw_data_dir: Path | None = None
    interim_data_dir: Path | None = None
    processed_data_dir: Path | None = None
    outputs_dir: Path | None = None

    @model_validator(mode="after")
    def resolve_and_validate_paths(self) -> Self:
        """Resolve path defaults and ensure required directories exist."""

        project_root = self.project_root.resolve()

        self.project_root = project_root
        self.raw_data_dir = (self.raw_data_dir or project_root / "data" / "raw").resolve()
        self.interim_data_dir = (
            self.interim_data_dir or project_root / "data" / "interim"
        ).resolve()
        self.processed_data_dir = (
            self.processed_data_dir or project_root / "data" / "processed"
        ).resolve()
        self.outputs_dir = (self.outputs_dir or project_root / "outputs").resolve()

        invalid_paths = [
            f"{name}: {path}"
            for name, path in self.resolved_paths().items()
            if not path.exists() or not path.is_dir()
        ]
        if invalid_paths:
            message = "Missing required project directories:\n" + "\n".join(invalid_paths)
            raise ValueError(message)

        return self

    def resolved_paths(self) -> dict[str, Path]:
        """Return the canonical project path mapping."""

        return {
            "project_root": self.project_root,
            "raw_data_dir": self.raw_data_dir,
            "interim_data_dir": self.interim_data_dir,
            "processed_data_dir": self.processed_data_dir,
            "outputs_dir": self.outputs_dir,
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached settings instance."""

    return Settings()
