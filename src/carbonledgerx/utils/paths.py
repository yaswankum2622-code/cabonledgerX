"""Helpers for resolving project-relative filesystem paths."""

from __future__ import annotations

from os import PathLike, fspath
from pathlib import Path

from carbonledgerx.config.settings import get_settings


PathPart = str | PathLike[str]


def _normalize_parts(parts: tuple[PathPart, ...]) -> tuple[str, ...]:
    """Normalize path-like arguments into strings for joining."""

    return tuple(fspath(part) for part in parts)


def resolve_project_path(*parts: PathPart) -> Path:
    """Resolve a path relative to the project root."""

    settings = get_settings()
    return settings.project_root.joinpath(*_normalize_parts(parts)).resolve()


def raw_data_path(*parts: PathPart) -> Path:
    """Resolve a path inside ``data/raw``."""

    settings = get_settings()
    return settings.raw_data_dir.joinpath(*_normalize_parts(parts)).resolve()


def interim_data_path(*parts: PathPart) -> Path:
    """Resolve a path inside ``data/interim``."""

    settings = get_settings()
    return settings.interim_data_dir.joinpath(*_normalize_parts(parts)).resolve()


def processed_data_path(*parts: PathPart) -> Path:
    """Resolve a path inside ``data/processed``."""

    settings = get_settings()
    return settings.processed_data_dir.joinpath(*_normalize_parts(parts)).resolve()


def output_path(*parts: PathPart) -> Path:
    """Resolve a path inside ``outputs``."""

    settings = get_settings()
    return settings.outputs_dir.joinpath(*_normalize_parts(parts)).resolve()
