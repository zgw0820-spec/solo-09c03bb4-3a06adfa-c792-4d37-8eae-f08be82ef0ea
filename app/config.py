"""Runtime configuration, sourced from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    ppm_staging_bytes: int  # shared quota for staged/in-conversion PPM bytes
    workers: int            # number of local converter processes
    max_active_batches: int  # how many batches may pack concurrently


def load_settings() -> Settings:
    return Settings(
        data_dir=Path(os.environ.get("DATA_DIR", "/data")),
        ppm_staging_bytes=int(os.environ.get("PPM_STAGING_BYTES", str(64 * 1024 * 1024))),
        workers=int(os.environ.get("WORKERS", "2")),
        max_active_batches=int(os.environ.get("MAX_ACTIVE_BATCHES", "2")),
    )
