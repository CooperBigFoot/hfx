"""Exercise saved evidence through real Shapely, pyproj, and Cartopy paths."""

import hashlib
import json

import pytest
from pyproj import Transformer
from shapely.geometry import MultiPolygon, Polygon, box, mapping
from shapely.ops import transform

from watershed_comparison.models import CONSUMER_SHA, DelineationSettings
from watershed_comparison.overlay import compare_saved_watersheds


def save_run(directory, geometry, name="tdx_hydro", **overrides):
    directory.mkdir()
    metadata = {
        "dataset": {
            "fabric_name": name,
            "fabric_version": "synthetic-v1",
            "uri": "/synthetic",
            "manifest_sha256": "a" * 64,
            "attribution": "Synthetic test source attribution",
            "license": "CC BY-SA 4.0" if name == "tdx_hydro" else "CC BY-NC 4.0",
        },
        "consumer": {
            "source_sha": CONSUMER_SHA,
            "wheel_sha256": "c" * 64,
            "wheel_path": "/synthetic/consumer.whl",
            "extension_sha256": "d" * 64,
            "build_receipt_path": "/synthetic/build.json",
            "build_receipt_sha256": "e" * 64,
        },
        "settings": DelineationSettings().model_dump(),
        "input_outlet": [7.589, 47.5596],
        "resolved_outlet": [7.59, 47.56],
        "refined_outlet": [7.60, 47.57] if name == "grit" else None,
        "refinement": {
            "status": "applied" if name == "grit" else "best_effort_skipped",
            "seed_kind": "raster_ranked" if name == "grit" else "coarse",
            "skip_reason": None
            if name == "grit"
            else {
                "kind": "auxiliary_unavailable",
                "category": None,
                "schema": "hfx.aux.d8_raster.v2",
                "failure_kind": None,
                "requested_threshold": None,
                "effective_threshold": None,
                "units": None,
                "mapped_cell": None,
                "measured_accumulation": None,
            },
        },
        "area_km2": 42.0,
        "upstream_unit_count": 2,
        "terminal_unit_id": 100,
    }
    manifest = json.dumps(
        {"fabric_name": name, "fabric_version": "synthetic-v1"}
    ).encode()
    metadata["dataset"]["manifest_sha256"] = hashlib.sha256(manifest).hexdigest()
    observations = {}
    for phase in ("before", "after"):
        (directory / f"manifest-{phase}.json").write_bytes(manifest)
        observations[phase] = {
            "sha256": hashlib.sha256(manifest).hexdigest(),
            "bytes": len(manifest),
            "source_uri": "/synthetic/manifest.json",
            "observed_at": "2026-09-09T00:00:00Z",
        }
    (directory / "observations.json").write_text(json.dumps(observations))
    metadata["observed_manifest_sha256"] = digest(directory / "observations.json")
    (directory / "trace.jsonl").write_text('{"event":"synthetic_completed"}\n')
    for filename, field, value in (
        (
            "runtime-data-after.json",
            "runtime_data_after_sha256",
            {
                "gdal_anchor": "/synthetic/gdal",
                "proj_search_paths": ["/synthetic/proj"],
                "proj_network_enabled": False,
                "guarantee": "synthetic",
            },
        ),
        (
            "loaded-libraries-after.json",
            "loaded_libraries_after_sha256",
            {
                "/synthetic/libgdal.dylib": {
                    "loaded_path": "/synthetic/libgdal.dylib",
                    "sha256": "f" * 64,
                }
            },
        ),
    ):
        (directory / filename).write_text(json.dumps(value))
        metadata[field] = digest(directory / filename)
    metadata.update(overrides)
    (directory / "watershed.wkb").write_bytes(geometry.wkb)
    (directory / "watershed.geojson").write_text(
        json.dumps(
            {
                "type": "Feature",
                "properties": {"terminal_unit_id": 100, "upstream_unit_count": 2},
                "geometry": mapping(geometry),
            }
        )
    )
    (directory / "upstream_unit_ids.json").write_text(json.dumps([100, 101]))
    metadata["geometry_sha256"] = {
        "wkb": digest(directory / "watershed.wkb"),
        "geojson": digest(directory / "watershed.geojson"),
    }
    metadata["upstream_unit_ids_sha256"] = digest(directory / "upstream_unit_ids.json")
    (directory / "metadata.json").write_text(json.dumps(metadata))
    bind_metadata(directory)
    return directory


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def bind_metadata(directory):
    metadata = json.loads((directory / "metadata.json").read_text())
    (directory / "request.json").write_text(
        json.dumps(
            {
                key: metadata[key]
                for key in ("dataset", "consumer", "input_outlet", "settings")
            }
        )
    )
    (directory / "success.json").write_text(
        json.dumps(
            {
                "metadata_sha256": digest(directory / "metadata.json"),
                "trace_sha256": digest(directory / "trace.jsonl"),
            }
        )
    )


@pytest.fixture
def runs(tmp_path):
    first = MultiPolygon(
        [
            Polygon(
                [(7, 47), (8, 47), (8, 48), (7, 48)],
                holes=[[(7.1, 47.1), (7.2, 47.1), (7.2, 47.2), (7.1, 47.2)]],
            ),
            box(8.1, 48.1, 8.2, 48.2),
        ]
    )
    second = box(7.5, 47.5, 8.5, 48.5)
    return (
        save_run(tmp_path / "first", first),
        save_run(tmp_path / "second", second, "grit"),
        first,
        second,
    )


def test_real_overlay_full_multipart_holes_metrics_and_markers(
    runs, tmp_path, monkeypatch
):
    import cartopy.crs as ccrs
    from cartopy.io import Downloader
    from cartopy.mpl.geoaxes import GeoAxes

    def no_download(*args, **kwargs):
        pytest.fail("Overlay attempted a basemap download")

    monkeypatch.setattr(Downloader, "acquire_resource", no_download)
    seen, markers = [], []
    original_add, original_plot = GeoAxes.add_geometries, GeoAxes.plot

    def record_add(self, geoms, crs, **kwargs):
        seen.extend(geoms)
        assert isinstance(crs, ccrs.PlateCarree)
        return original_add(self, geoms, crs, **kwargs)

    def record_plot(self, *args, **kwargs):
        assert isinstance(kwargs["transform"], ccrs.PlateCarree)
        markers.append(kwargs["marker"])
        return original_plot(self, *args, **kwargs)

    monkeypatch.setattr(GeoAxes, "add_geometries", record_add)
    monkeypatch.setattr(GeoAxes, "plot", record_plot)
    first, second, left, right = runs
    snapshots = {p: p.read_bytes() for run in (first, second) for p in run.iterdir()}
    result = compare_saved_watersheds(first, second, tmp_path / "comparison")
    assert len(seen) == 2
    assert seen[0].equals_exact(left, 0)
    assert seen[1].equals_exact(right, 0)
    assert sorted(markers) == sorted(["o", "o", "^", "x"])
    projection = Transformer.from_crs(4326, 3035, always_xy=True)
    a, b = transform(projection.transform, left), transform(projection.transform, right)
    areas = result["geometric_area"]
    assert areas["first_km2"] == pytest.approx(a.area / 1e6)
    assert areas["intersection_km2"] == pytest.approx(a.intersection(b).area / 1e6)
    assert areas["symmetric_difference_km2"] == pytest.approx(
        areas["first_only_km2"] + areas["second_only_km2"]
    )
    assert areas["union_km2"] == pytest.approx(
        areas["first_km2"] + areas["second_km2"] - areas["intersection_km2"]
    )
    assert result["engine_reported_area_km2"] == {"first": 42.0, "second": 42.0}
    assert result["second"]["dataset"]["license"] == "CC BY-NC 4.0"
    extent = result["map_extent_lon_lat"]
    assert extent[0] < 7 and extent[1] > 8.5 and extent[2] < 47 and extent[3] > 48.5
    output = tmp_path / "comparison"
    assert (output / "overlay.png").read_bytes().startswith(b"\x89PNG")
    assert (output / "overlay.pdf").read_bytes().startswith(b"%PDF")
    assert json.loads((output / "metrics.json").read_text()) == result
    assert all(p.read_bytes() == content for p, content in snapshots.items())


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("input_outlet", [8, 48], "different input_outlet"),
        (
            "consumer",
            {"source_sha": "d" * 40, "wheel_sha256": "c" * 64},
            "different consumer source_sha",
        ),
        (
            "consumer",
            {"source_sha": CONSUMER_SHA, "wheel_sha256": "d" * 64},
            "different consumer wheel_sha256",
        ),
        ("settings", {"snap_radius_m": 2000}, "different consumer settings"),
        ("upstream_unit_count", 0, "nonempty traversal"),
        ("resolved_outlet", [180, 0], "outside EPSG:3035"),
        ("resolved_outlet", [float("nan"), 47], "finite numbers"),
        ("area_km2", -1, "finite and positive"),
    ],
)
def test_metadata_failure_retains_evidence(runs, tmp_path, field, value, error):
    first, second, _, _ = runs
    path = second / "metadata.json"
    metadata = json.loads(path.read_text())
    metadata[field] = value
    path.write_text(json.dumps(metadata))
    bind_metadata(second)
    original = path.read_bytes()
    output = tmp_path / "comparison"
    with pytest.raises(ValueError, match=error):
        compare_saved_watersheds(first, second, output)
    assert path.read_bytes() == original
    assert error in json.loads((output / "failure.json").read_text())["error"]
    assert not (output / "metrics.json").exists()


@pytest.mark.parametrize(
    "geometry,error",
    [
        (Polygon(), "nonempty valid"),
        (Polygon([(7, 47), (8, 48), (8, 47), (7, 48), (7, 47)]), "nonempty valid"),
        (box(-100, 30, -99, 31), "outside EPSG:3035"),
    ],
)
def test_invalid_or_unsupported_extent(tmp_path, geometry, error):
    first = save_run(tmp_path / "first", geometry)
    second = save_run(tmp_path / "second", box(7, 47, 8, 48), "grit")
    with pytest.raises(ValueError, match=error):
        compare_saved_watersheds(first, second, tmp_path / "output")
    assert (tmp_path / "output/failure.json").exists()


def test_geojson_mismatch_is_fatal(runs, tmp_path):
    first, second, _, _ = runs
    (first / "watershed.geojson").write_text(
        json.dumps(
            {
                "type": "Feature",
                "properties": {"terminal_unit_id": 100, "upstream_unit_count": 2},
                "geometry": mapping(box(7, 47, 7.1, 47.1)),
            }
        )
    )
    metadata = json.loads((first / "metadata.json").read_text())
    metadata["geometry_sha256"]["geojson"] = digest(first / "watershed.geojson")
    (first / "metadata.json").write_text(json.dumps(metadata))
    bind_metadata(first)
    with pytest.raises(ValueError, match="full geometry mismatch"):
        compare_saved_watersheds(first, second, tmp_path / "output")


def test_existing_output_is_never_modified(runs, tmp_path):
    first, second, _, _ = runs
    output = tmp_path / "output"
    output.mkdir()
    sentinel = output / "sentinel"
    sentinel.write_text("prior evidence")
    with pytest.raises(FileExistsError):
        compare_saved_watersheds(first, second, output)
    assert list(output.iterdir()) == [sentinel]
    assert sentinel.read_text() == "prior evidence"


def test_plot_failure_retains_partial_outputs(runs, tmp_path, monkeypatch):
    from matplotlib.figure import Figure

    original = Figure.savefig

    def fail_pdf(self, path, **kwargs):
        if path.suffix == ".pdf":
            raise RuntimeError("synthetic PDF write failure")
        return original(self, path, **kwargs)

    monkeypatch.setattr(Figure, "savefig", fail_pdf)
    first, second, _, _ = runs
    output = tmp_path / "output"
    with pytest.raises(RuntimeError, match="synthetic PDF"):
        compare_saved_watersheds(first, second, output)
    assert (output / "overlay.png").exists()
    assert (output / "failure.json").exists()
    assert not (output / "metrics.json").exists()


def test_topologically_equal_but_changed_vertices_are_refused(tmp_path):
    geometry = box(7, 47, 8, 48)
    first = save_run(tmp_path / "first", geometry)
    second = save_run(tmp_path / "second", geometry, "grit")
    reversed_geometry = Polygon(list(geometry.exterior.coords)[::-1])
    assert geometry.equals(reversed_geometry)
    (first / "watershed.geojson").write_text(
        json.dumps(
            {
                "type": "Feature",
                "properties": {"terminal_unit_id": 100, "upstream_unit_count": 2},
                "geometry": mapping(reversed_geometry),
            }
        )
    )
    metadata = json.loads((first / "metadata.json").read_text())
    metadata["geometry_sha256"]["geojson"] = digest(first / "watershed.geojson")
    (first / "metadata.json").write_text(json.dumps(metadata))
    bind_metadata(first)
    with pytest.raises(ValueError, match="full geometry mismatch"):
        compare_saved_watersheds(first, second, tmp_path / "output")


def test_native_typed_skip_reason_is_retained_and_captioned(
    runs, tmp_path, monkeypatch
):
    from matplotlib.figure import Figure

    first, second, _, _ = runs
    reason = {
        "kind": "auxiliary_unavailable",
        "category": "capability",
        "schema": "hfx.aux.d8_raster.v2",
        "failure_kind": None,
        "requested_threshold": None,
        "effective_threshold": None,
        "units": None,
        "mapped_cell": None,
        "measured_accumulation": None,
    }
    path = first / "metadata.json"
    metadata = json.loads(path.read_text())
    metadata["refinement"] = {
        "status": "best_effort_skipped",
        "seed_kind": "coarse",
        "skip_reason": reason,
    }
    path.write_text(json.dumps(metadata))
    bind_metadata(first)
    captions = []
    original = Figure.text

    def record_text(self, x, y, text, **kwargs):
        captions.append(text)
        return original(self, x, y, text, **kwargs)

    monkeypatch.setattr(Figure, "text", record_text)
    metrics = compare_saved_watersheds(first, second, tmp_path / "output")
    assert metrics["first"]["refinement"]["skip_reason"] == reason
    assert "auxiliary_unavailable (capability)" in captions[0]
    assert "requested_threshold" not in captions[0]


@pytest.mark.parametrize(
    "mutation,match",
    [
        ("failure", "failure.json"),
        ("no_success", "success.json"),
        ("metadata", "metadata.*SHA-256"),
        ("matching_geometry", "watershed.wkb.*SHA-256"),
        ("upstream", "upstream_unit_ids.json.*SHA-256"),
    ],
)
def test_composed_evidence_chain_rejects_changed_or_failed_run(
    runs, tmp_path, mutation, match
):
    first, second, _, _ = runs
    if mutation == "failure":
        (first / "failure.json").write_text('{"stage": "session_close"}')
    elif mutation == "no_success":
        (first / "success.json").unlink()
    elif mutation == "metadata":
        (first / "metadata.json").write_text(
            (first / "metadata.json").read_text() + " "
        )
    elif mutation == "matching_geometry":
        replacement = box(7, 47, 7.8, 47.8)
        (first / "watershed.wkb").write_bytes(replacement.wkb)
        document = json.loads((first / "watershed.geojson").read_text())
        document["geometry"] = mapping(replacement)
        (first / "watershed.geojson").write_text(json.dumps(document))
    else:
        (first / "upstream_unit_ids.json").write_text("[100, 102]")
    with pytest.raises(ValueError, match=match):
        compare_saved_watersheds(first, second, tmp_path / "output")
    assert (tmp_path / "output/failure.json").exists()
    assert not (tmp_path / "output/overlay.png").exists()


@pytest.mark.parametrize("ids", [[100], [100, 100], [101, 102], [100, True]])
def test_bound_upstream_ids_must_prove_traversal(runs, tmp_path, ids):
    first, second, _, _ = runs
    (first / "upstream_unit_ids.json").write_text(json.dumps(ids))
    metadata = json.loads((first / "metadata.json").read_text())
    metadata["upstream_unit_ids_sha256"] = digest(first / "upstream_unit_ids.json")
    (first / "metadata.json").write_text(json.dumps(metadata))
    bind_metadata(first)
    with pytest.raises(ValueError, match="upstream_unit_ids"):
        compare_saved_watersheds(first, second, tmp_path / "output")


@pytest.mark.parametrize(
    "artifact", ["observations.json", "manifest-before.json", "manifest-after.json"]
)
def test_manifest_evidence_changes_are_refused(runs, tmp_path, artifact):
    first, second, _, _ = runs
    path = first / artifact
    path.write_text(path.read_text() + " ")
    with pytest.raises(ValueError, match="SHA-256"):
        compare_saved_watersheds(first, second, tmp_path / "output")


def test_request_point_disagreement_is_refused(runs, tmp_path):
    first, second, _, _ = runs
    path = first / "request.json"
    request = json.loads(path.read_text())
    request["input_outlet"] = [8, 48]
    path.write_text(json.dumps(request))
    with pytest.raises(ValueError, match="request.json input_outlet disagrees"):
        compare_saved_watersheds(first, second, tmp_path / "output")


def test_bound_inconsistent_refinement_is_refused(runs, tmp_path):
    first, second, _, _ = runs
    path = first / "metadata.json"
    metadata = json.loads(path.read_text())
    metadata["refinement"]["status"] = "applied"
    path.write_text(json.dumps(metadata))
    bind_metadata(first)
    with pytest.raises(ValueError, match="inconsistent best-effort refinement"):
        compare_saved_watersheds(first, second, tmp_path / "output")


@pytest.mark.parametrize(
    "mutation,match",
    [
        ("supervisor", "supervision_failure.json"),
        ("comparison", "failure.json"),
        ("missing_trace", "trace.jsonl"),
        ("empty_trace", "trace.jsonl"),
        ("changed_trace", "trace.jsonl"),
        ("runtime-data-after.json", "runtime-data-after.json"),
        ("loaded-libraries-after.json", "loaded-libraries-after.json"),
    ],
)
def test_supervised_run_trace_and_runtime_evidence(runs, tmp_path, mutation, match):
    first, second, _, _ = runs
    campaign = tmp_path / "campaign"
    parent = campaign / "first-supervision"
    parent.mkdir(parents=True)
    nested = parent / "worker"
    first.rename(nested)
    first = nested
    if mutation == "supervisor":
        (parent / "supervision_failure.json").write_text('{"reason":"rss_limit"}')
    elif mutation == "comparison":
        (campaign / "comparison_request.json").write_text("{}")
        (campaign / "failure.json").write_text('{"stage":"second_run"}')
    elif mutation == "missing_trace":
        (first / "trace.jsonl").unlink()
    elif mutation == "empty_trace":
        (first / "trace.jsonl").write_bytes(b"")
        bind_metadata(first)
    elif mutation == "changed_trace":
        (first / "trace.jsonl").write_text('{"event":"changed"}')
    else:
        (first / mutation).write_text("{}")
    with pytest.raises((ValueError, FileNotFoundError), match=match):
        compare_saved_watersheds(first, second, tmp_path / "output")
    assert not (tmp_path / "output/overlay.png").exists()


def test_unmarked_ancestor_failure_does_not_reject_independent_run(runs, tmp_path):
    first, second, _, _ = runs
    ancestor = tmp_path / "unrelated"
    parent = ancestor / "supervision"
    parent.mkdir(parents=True)
    nested = parent / "worker"
    first.rename(nested)
    (ancestor / "failure.json").write_text('{"unrelated":"evidence"}')
    compare_saved_watersheds(nested, second, tmp_path / "output")
    assert (tmp_path / "output/overlay.png").exists()


@pytest.mark.parametrize(
    "field", ["runtime_data_after_sha256", "loaded_libraries_after_sha256"]
)
def test_missing_bound_runtime_receipt_is_refused(runs, tmp_path, field):
    first, second, _, _ = runs
    path = first / "metadata.json"
    metadata = json.loads(path.read_text())
    del metadata[field]
    path.write_text(json.dumps(metadata))
    bind_metadata(first)
    with pytest.raises(ValueError, match=field):
        compare_saved_watersheds(first, second, tmp_path / "output")
    assert not (tmp_path / "output/overlay.png").exists()
