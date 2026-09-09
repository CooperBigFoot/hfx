"""parse : evidence bytes -> dataset identity and delineation request."""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator

CONSUMER_SHA = "7a43834a870d0f5d46ed6d23926205ee58c8fb93"
GRIT_URI = "https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/"


class Evidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class DatasetIdentity(Evidence):
    uri: str
    fabric_name: str = Field(min_length=1)
    fabric_version: str | None = Field(min_length=1)
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    attribution: str = Field(min_length=1)
    license: str = Field(min_length=1)

    @model_validator(mode="after")
    def no_secrets_in_uri(self):
        parsed = urlsplit(self.uri)
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError(
                "dataset URI must not contain credentials, query or fragment"
            )
        if parsed.scheme not in ("", "s3", "https"):
            raise ValueError("unsupported dataset URI")
        return self


class ConsumerIdentity(Evidence):
    source_sha: Literal[CONSUMER_SHA]
    wheel_path: str
    wheel_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    extension_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    build_receipt_path: str
    build_receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    runtime_data_receipt_path: str
    runtime_data_receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class DelineationSettings(Evidence):
    # Literal fields freeze normal pinned-source semantics for both fabrics.
    refine: Literal[True] = True
    snap_radius: Literal[1000.0] = 1000.0
    snap_strategy: Literal["weight-first"] = "weight-first"
    snap_threshold: Literal[1000] = 1000
    clean_epsilon: Literal[0.00001] = 0.00001
    repair_geometry: Literal["auto"] = "auto"
    parquet_cache: Literal[True] = True
    parquet_cache_max_mb: Literal[512] = 512


class DelineationRequest(Evidence):
    dataset: DatasetIdentity
    consumer: ConsumerIdentity
    input_outlet: tuple[float, float]
    settings: DelineationSettings

    @model_validator(mode="after")
    def geographic_point(self):
        lon, lat = self.input_outlet
        if not (-180 <= lon <= 180 and -90 <= lat <= 90):
            raise ValueError("input_outlet must be (longitude, latitude) in EPSG:4326")
        return self


class ResourceLimits(Evidence):
    max_rss_gib: float = Field(default=40, gt=0, le=48)
    min_available_gib: float = Field(default=12, ge=8)
    max_swap_growth_gib: float = Field(default=1, ge=0, le=2)
    min_free_disk_gib: float = Field(default=100, ge=50)
    max_elapsed_seconds: float = Field(default=14400, gt=0)
    sample_seconds: float = Field(default=1, gt=0, le=5)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(path: Path, value: object) -> None:
    with path.open("x") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")


def read_request(path: Path) -> DelineationRequest:
    return DelineationRequest.model_validate_json(path.read_bytes())


def finite_point(value, name: str):
    if len(value) != 2 or not all(math.isfinite(v) for v in value):
        raise ValueError(f"{name} must contain two finite coordinates")
    lon, lat = value
    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
        raise ValueError(f"{name} outside EPSG:4326")
    return [lon, lat]
