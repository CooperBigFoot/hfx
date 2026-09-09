"""Exercise saved evidence through real Shapely, pyproj, and Cartopy paths."""

import json

import pytest
from pyproj import Transformer
from shapely.geometry import MultiPolygon, Polygon, box, mapping
from shapely.ops import transform

from watershed_comparison.overlay import compare_saved_watersheds


def save_run(directory, geometry, name="tdx_hydro", **overrides):
    directory.mkdir()
    metadata = {
        "dataset": {
            "fabric_name": name,
            "fabric_version": "synthetic-v1",
            "uri": "file:///synthetic",
            "manifest_sha256": "a" * 64,
            "attribution": "Synthetic test source attribution",
            "license": "CC BY-SA 4.0" if name == "tdx_hydro" else "CC BY-NC 4.0",
        },
        "consumer": {"source_sha": "b" * 40, "wheel_sha256": "c" * 64},
        "settings": {"snap_radius_m": 1000},
        "input_outlet": [7.589, 47.5596],
        "resolved_outlet": [7.59, 47.56],
        "refined_outlet": [7.60, 47.57] if name == "grit" else None,
        "refinement": {
            "status": "applied" if name == "grit" else "skipped",
            "seed_kind": "resolved_outlet",
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
    metadata.update(overrides)
    (directory / "metadata.json").write_text(json.dumps(metadata))
    (directory / "watershed.wkb").write_bytes(geometry.wkb)
    (directory / "watershed.geojson").write_text(
        json.dumps({"type": "Feature", "properties": {}, "geometry": mapping(geometry)})
    )
    return directory


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
            {"source_sha": "b" * 40, "wheel_sha256": "d" * 64},
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
                "properties": {},
                "geometry": mapping(box(7, 47, 7.1, 47.1)),
            }
        )
    )
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
                "properties": {},
                "geometry": mapping(reversed_geometry),
            }
        )
    )
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
        "status": "skipped",
        "seed_kind": "coarse",
        "skip_reason": reason,
    }
    path.write_text(json.dumps(metadata))
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
