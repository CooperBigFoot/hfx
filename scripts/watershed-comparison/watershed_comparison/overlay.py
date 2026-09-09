"""compare : SavedWatershed × SavedWatershed → WatershedComparison.

Render complete geographic boundaries and measure their EPSG:3035 overlay.
Input evidence is never changed. Failed comparisons retain a failure record.
"""

import hashlib
import json
import math
import textwrap
from dataclasses import dataclass
from pathlib import Path

import cartopy.crs as ccrs
from matplotlib import rc_context
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from pyproj import CRS, Transformer
from shapely import from_wkb
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform


@dataclass(frozen=True)
class SavedWatershed:
    directory: Path
    geometry: BaseGeometry
    metadata: dict


def _point(value, field: str) -> tuple[float, float]:
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise ValueError(f"{field}: expected [longitude, latitude]")
    if any(
        isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)
        for v in value
    ):
        raise ValueError(f"{field}: coordinates must be finite numbers")
    lon, lat = value
    if not -180 <= lon <= 180 or not -90 <= lat <= 90:
        raise ValueError(f"{field}: coordinates outside EPSG:4326")
    return lon, lat


def _polygon(geometry: BaseGeometry, artifact: str) -> None:
    if (
        geometry.geom_type not in ("Polygon", "MultiPolygon")
        or geometry.is_empty
        or not geometry.is_valid
    ):
        raise ValueError(
            f"{artifact}: expected nonempty valid Polygon/MultiPolygon; no repair is permitted"
        )
    if not all(math.isfinite(v) for v in geometry.bounds):
        raise ValueError(f"{artifact}: nonfinite geometry bounds")


def _verified_bytes(path: Path, expected: str) -> bytes:
    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != expected:
        raise ValueError(f"{path}: SHA-256 does not match retained evidence")
    return payload


def _load(directory: Path) -> SavedWatershed:
    supervisor_failure = directory.parent / "supervision_failure.json"
    if supervisor_failure.exists():
        raise ValueError(
            f"{supervisor_failure}: supervisor failed after worker execution"
        )
    comparison = directory.parent.parent
    if (comparison / "comparison_request.json").exists() and (
        comparison / "failure.json"
    ).exists():
        raise ValueError(f"{comparison}/failure.json: enclosing comparison failed")
    if (directory / "failure.json").exists():
        raise ValueError(f"{directory}/failure.json: failed run cannot be compared")
    if not (directory / "success.json").is_file():
        raise ValueError(
            f"{directory}/success.json: completed run evidence is required"
        )
    success = json.loads((directory / "success.json").read_bytes())
    trace = _verified_bytes(directory / "trace.jsonl", success["trace_sha256"])
    if not trace.strip():
        raise ValueError(
            f"{directory}/trace.jsonl: completed run requires a nonempty trace"
        )
    metadata = json.loads(
        _verified_bytes(directory / "metadata.json", success["metadata_sha256"])
    )
    for field, filename in (
        ("runtime_data_after_sha256", "runtime-data-after.json"),
        ("loaded_libraries_after_sha256", "loaded-libraries-after.json"),
    ):
        if field not in metadata:
            raise ValueError(
                f"{directory}: required runtime evidence binding {field} is absent"
            )
        _verified_bytes(directory / filename, metadata[field])
    wkb = _verified_bytes(
        directory / "watershed.wkb", metadata["geometry_sha256"]["wkb"]
    )
    geojson = _verified_bytes(
        directory / "watershed.geojson", metadata["geometry_sha256"]["geojson"]
    )
    ids = json.loads(
        _verified_bytes(
            directory / "upstream_unit_ids.json", metadata["upstream_unit_ids_sha256"]
        )
    )
    request = json.loads((directory / "request.json").read_bytes())
    for field in ("dataset", "consumer", "input_outlet", "settings"):
        if request[field] != metadata[field]:
            raise ValueError(
                f"{directory}: request.json {field} disagrees with result metadata"
            )
    observations = json.loads(
        _verified_bytes(
            directory / "observations.json", metadata["observed_manifest_sha256"]
        )
    )
    for phase in ("before", "after"):
        manifest_bytes = _verified_bytes(
            directory / f"manifest-{phase}.json", metadata["dataset"]["manifest_sha256"]
        )
        observation = observations[phase]
        if observation["sha256"] != metadata["dataset"][
            "manifest_sha256"
        ] or observation["bytes"] != len(manifest_bytes):
            raise ValueError(
                f"{directory}: {phase} manifest observation identity disagrees"
            )
        manifest = json.loads(manifest_bytes)
        for field in ("fabric_name", "fabric_version"):
            if manifest[field] != metadata["dataset"][field]:
                raise ValueError(
                    f"{directory}: observed manifest {field} disagrees with dataset identity"
                )
    for field in (
        "fabric_name",
        "fabric_version",
        "uri",
        "manifest_sha256",
        "attribution",
        "license",
    ):
        value = metadata["dataset"][field]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"{directory}/metadata.json: dataset.{field} must be nonempty text"
            )
    for field in ("source_sha", "wheel_sha256"):
        value = metadata["consumer"][field]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"{directory}/metadata.json: consumer.{field} must be nonempty text"
            )
    _point(metadata["input_outlet"], "input_outlet")
    _point(metadata["resolved_outlet"], "resolved_outlet")
    if metadata.get("refined_outlet") is not None:
        _point(metadata["refined_outlet"], "refined_outlet")
    refinement = metadata["refinement"]
    if not isinstance(refinement["status"], str) or not refinement["status"]:
        raise ValueError(f"{directory}: missing refinement status")
    if refinement["seed_kind"] is not None and not isinstance(
        refinement["seed_kind"], str
    ):
        raise ValueError(f"{directory}: refinement.seed_kind must be text or null")
    reason = refinement["skip_reason"]
    if reason is not None:
        if (
            not isinstance(reason, dict)
            or not isinstance(reason.get("kind"), str)
            or not reason["kind"]
        ):
            raise ValueError(
                f"{directory}: refinement.skip_reason must be a typed object with kind or null"
            )
        if "category" not in reason or (
            reason["category"] is not None and not isinstance(reason["category"], str)
        ):
            raise ValueError(
                f"{directory}: refinement.skip_reason.category must be text or null"
            )
    area = metadata["area_km2"]
    if (
        isinstance(area, bool)
        or not isinstance(area, (int, float))
        or not math.isfinite(area)
        or area <= 0
    ):
        raise ValueError(f"{directory}: engine area_km2 must be finite and positive")
    count = metadata["upstream_unit_count"]
    if type(count) is not int or count < 1:
        raise ValueError(
            f"{directory}: upstream_unit_count must demonstrate nonempty traversal"
        )
    if type(metadata["terminal_unit_id"]) is not int:
        raise ValueError(f"{directory}: terminal_unit_id must be an integer")
    if not isinstance(ids, list) or any(type(unit_id) is not int for unit_id in ids):
        raise ValueError(
            f"{directory}: upstream_unit_ids must be integer drainage-unit IDs"
        )
    if (
        len(ids) != count
        or len(set(ids)) != count
        or metadata["terminal_unit_id"] not in ids
    ):
        raise ValueError(
            f"{directory}: upstream_unit_ids count, uniqueness, or terminal membership disagrees"
        )
    refined = metadata.get("refined_outlet")
    if refined is None:
        consistent = (
            refinement["status"] == "best_effort_skipped"
            and reason is not None
            and refinement["seed_kind"] == "coarse"
        )
    else:
        consistent = (
            refinement["status"] == "applied"
            and reason is None
            and refinement["seed_kind"] in ("vector_quantized", "raster_ranked")
        )
    if not consistent:
        raise ValueError(f"{directory}: inconsistent best-effort refinement outcome")
    geometry = from_wkb(wkb)
    document = json.loads(geojson)
    if document.get("type") != "Feature":
        raise ValueError(f"{directory}: expected consumer GeoJSON Feature")
    properties = document["properties"]
    if (
        properties["terminal_unit_id"] != metadata["terminal_unit_id"]
        or properties["upstream_unit_count"] != count
    ):
        raise ValueError(
            f"{directory}: GeoJSON traversal properties disagree with result metadata"
        )
    geographic = shape(document["geometry"])
    _polygon(geometry, f"{directory}/watershed.wkb")
    _polygon(geographic, f"{directory}/watershed.geojson")
    if not geometry.equals_exact(geographic, tolerance=0):
        raise ValueError(f"{directory}: WKB and GeoJSON full geometry mismatch")
    return SavedWatershed(directory, geometry, metadata)


def _extent(
    watersheds: tuple[SavedWatershed, SavedWatershed], area_crs: CRS
) -> list[float]:
    west, south, east, north = area_crs.area_of_use.bounds
    xs, ys = [], []
    for watershed in watersheds:
        xmin, ymin, xmax, ymax = watershed.geometry.bounds
        if not (west <= xmin <= xmax <= east and south <= ymin <= ymax <= north):
            raise ValueError(
                f"{watershed.directory}: geometry outside EPSG:3035 area_of_use bounds {area_crs.area_of_use.bounds}"
            )
        xs.extend((xmin, xmax))
        ys.extend((ymin, ymax))
        for field in ("input_outlet", "resolved_outlet", "refined_outlet"):
            if watershed.metadata.get(field) is not None:
                lon, lat = _point(watershed.metadata[field], field)
                if not (west <= lon <= east and south <= lat <= north):
                    raise ValueError(
                        f"{watershed.directory}: {field} outside EPSG:3035 area_of_use"
                    )
                xs.append(lon)
                ys.append(lat)
    dx, dy = max(max(xs) - min(xs), 0.01), max(max(ys) - min(ys), 0.01)
    return [
        min(xs) - dx * 0.08,
        max(xs) + dx * 0.08,
        min(ys) - dy * 0.08,
        max(ys) + dy * 0.08,
    ]


def _draw(
    watersheds: tuple[SavedWatershed, SavedWatershed], extent: list[float], output: Path
) -> None:
    geographic_crs = ccrs.PlateCarree()
    notes = []
    for watershed in watersheds:
        dataset, refinement = (
            watershed.metadata["dataset"],
            watershed.metadata["refinement"],
        )
        reason = refinement["skip_reason"]
        reason_caption = "none" if reason is None else reason["kind"]
        if reason is not None and reason["category"] is not None:
            reason_caption += f" ({reason['category']})"
        notes.append(
            textwrap.fill(
                f"{dataset['fabric_name']} {dataset['fabric_version']} | Attribution: {dataset['attribution']} | "
                f"License: {dataset['license']} | Refinement: {refinement['status']}; "
                f"seed kind: {refinement['seed_kind']}; skip reason: {reason_caption}",
                width=130,
            )
        )
    notes.append(
        "Observed total output difference; fabric, snapping, and refinement differ. Neither result is ground truth."
    )
    footer = "\n\n".join(notes)
    footer_height = 0.16 * (footer.count("\n") + 2)
    figure = Figure(figsize=(12, 9 + footer_height))
    FigureCanvasAgg(figure)
    axes = figure.add_axes(
        [
            0.08,
            (footer_height + 0.45) / (9 + footer_height),
            0.85,
            7.5 / (9 + footer_height),
        ],
        projection=ccrs.LambertAzimuthalEqualArea(
            central_longitude=10, central_latitude=52
        ),
    )
    handles = []
    try:
        for watershed, color, style in zip(
            watersheds, ("#0072B2", "#D55E00"), ("-", "--"), strict=True
        ):
            name = watershed.metadata["dataset"]["fabric_name"]
            axes.add_geometries(
                [watershed.geometry],
                crs=geographic_crs,
                facecolor="none",
                edgecolor=color,
                linewidth=1.1,
                linestyle=style,
            )
            handles.append(
                Line2D(
                    [], [], color=color, linestyle=style, label=f"{name} full boundary"
                )
            )
            for field, marker, label in (
                ("resolved_outlet", "o", "resolved outlet"),
                ("refined_outlet", "^", "refined outlet"),
            ):
                point = watershed.metadata.get(field)
                if point is None or (
                    field == "refined_outlet"
                    and point == watershed.metadata["resolved_outlet"]
                ):
                    continue
                axes.plot(
                    *point,
                    marker=marker,
                    markersize=8,
                    fillstyle="none",
                    color=color,
                    linestyle="none",
                    transform=geographic_crs,
                )
                handles.append(
                    Line2D(
                        [],
                        [],
                        marker=marker,
                        markersize=8,
                        markerfacecolor="none",
                        color=color,
                        linestyle="none",
                        label=f"{name} {label}",
                    )
                )
        axes.plot(
            *watersheds[0].metadata["input_outlet"],
            marker="x",
            markersize=10,
            color="black",
            linestyle="none",
            transform=geographic_crs,
        )
        handles.append(
            Line2D(
                [],
                [],
                marker="x",
                color="black",
                linestyle="none",
                label="Requested outlet",
            )
        )
        axes.set_extent(extent, crs=geographic_crs)
        axes.gridlines(crs=geographic_crs, draw_labels=True, linewidth=0.4, alpha=0.4)
        axes.legend(handles=handles, loc="best", fontsize=8)
        axes.set_title("Saved watershed comparison (complete boundaries)")
        figure.text(0.06, 0.03, footer, fontsize=8, va="bottom")
        with rc_context({"path.simplify": False}):
            for suffix in ("png", "pdf"):
                figure.savefig(output / f"overlay.{suffix}", dpi=180)
    finally:
        figure.clear()


def compare_saved_watersheds(first: Path, second: Path, output: Path) -> dict:
    """Write an offline overlay and area metrics from two saved consumer runs.

    Raises on existing output, malformed evidence, different requested points or
    consumer builds, geometry mismatch, or an extent outside EPSG:3035 use bounds.
    Inputs and partial outputs are retained on failure; failure.json records why.
    """
    output.mkdir(parents=True, exist_ok=False)
    try:
        watersheds = (_load(first), _load(second))
        a, b = watersheds
        if a.metadata["input_outlet"] != b.metadata["input_outlet"]:
            raise ValueError("Saved runs have different input_outlet points")
        for field in ("source_sha", "wheel_sha256"):
            if a.metadata["consumer"][field] != b.metadata["consumer"][field]:
                raise ValueError(f"Saved runs have different consumer {field}")
        if a.metadata["settings"] != b.metadata["settings"]:
            raise ValueError("Saved runs have different consumer settings")
        crs = CRS.from_epsg(3035)
        extent = _extent(watersheds, crs)
        projector = Transformer.from_crs("EPSG:4326", crs, always_xy=True)

        def project(x, y, z=None):
            return projector.transform(x, y, errcheck=True)

        projected = tuple(
            transform(project, watershed.geometry) for watershed in watersheds
        )
        for watershed, geometry in zip(watersheds, projected, strict=True):
            _polygon(geometry, f"{watershed.directory}: EPSG:3035 projected geometry")
        left, right = projected
        areas = {
            "first_km2": left.area / 1_000_000,
            "second_km2": right.area / 1_000_000,
            "intersection_km2": left.intersection(right).area / 1_000_000,
            "union_km2": left.union(right).area / 1_000_000,
            "first_only_km2": left.difference(right).area / 1_000_000,
            "second_only_km2": right.difference(left).area / 1_000_000,
            "symmetric_difference_km2": left.symmetric_difference(right).area
            / 1_000_000,
        }
        if (
            not all(math.isfinite(v) and v >= 0 for v in areas.values())
            or areas["union_km2"] <= 0
        ):
            raise ValueError("EPSG:3035 overlay produced invalid areas")
        metrics = {
            "area_method": "EPSG:4326 vertices transformed to EPSG:3035 (ETRS89 / LAEA Europe), planar polygon overlay; square metres divided by 1000000; no geometry repair or simplification",
            "area_of_use_bounds_lon_lat": list(crs.area_of_use.bounds),
            "geometric_area": areas,
            "intersection_over_union": areas["intersection_km2"] / areas["union_km2"],
            "engine_reported_area_km2": {
                "first": a.metadata["area_km2"],
                "second": b.metadata["area_km2"],
            },
            "first": a.metadata,
            "second": b.metadata,
            "map_extent_lon_lat": extent,
            "interpretation": "Total observed output difference includes source fabric, snapping, and available refinement. This comparison cannot isolate refinement causality. Neither watershed is a ground-truth oracle. Engine-reported areas are retained separately from geometric areas.",
        }
        _draw(watersheds, extent, output)
        (output / "metrics.json").write_text(
            json.dumps(metrics, indent=2, allow_nan=False) + "\n"
        )
        return metrics
    except Exception as error:
        (output / "failure.json").write_text(
            json.dumps(
                {
                    "first": str(first),
                    "second": str(second),
                    "error_type": type(error).__name__,
                    "error": str(error),
                },
                indent=2,
            )
            + "\n"
        )
        raise
