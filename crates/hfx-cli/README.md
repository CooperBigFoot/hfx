# hfx-cli

CLI tool that validates HFX dataset directories against `spec/HFX_SPEC.md`.

## Purpose

Reads an HFX dataset directory (`manifest.json`, `catchments.parquet`,
`graph.parquet`, and declared auxiliary artifacts) and reports all spec
violations in a single pass.

## Quickstart

Install the crate from a repository checkout:

```bash
cargo install --path crates/hfx-cli
```

This installs the `hfx` binary:

```bash
hfx ./path/to/dataset
hfx --format json ./path/to/dataset
```

Use `--strict` to promote warnings to errors. Exit code `0` means valid; exit code `1` means invalid.

## Architecture

- `main.rs` provides the clap CLI and hands off to `lib.rs`.
- `lib.rs` exposes `validate()` as the crate entry point.
- `reader/` loads JSON, Parquet, and TIFF inputs into `ParsedDataset`.
- `check/` runs pure validation passes against that parsed representation.
- `report.rs` formats the resulting diagnostics as text or JSON.

Two layers are decoupled by `ParsedDataset`:

- **`reader/`** reads Parquet, TIFF, and JSON into lightweight intermediate representations (`CatchmentsData`, `GraphData`, `SnapData`, `RasterMeta`). TIFF structural tags are parsed with `tiff`, while CRS and geotransform metadata are read via GDAL so raster CRS/extent checks can run without loading pixel data.
- **`check/`** contains pure validation logic. Each module is free functions that take `&`-references to intermediate data and return `Vec<Diagnostic>`. No I/O, no trait objects.

## Key Types

| Type | Module | Role |
|---|---|---|
| `Diagnostic` | `diagnostic.rs` | Universal finding type (severity, category, artifact, location, message) |
| `ValidationReport` | `report.rs` | Aggregated result with text/JSON rendering |
| `ParsedDataset` | `dataset.rs` | Bridge between readers and checks |
| `RawManifest` | `reader/manifest.rs` | Serde struct with `Option<T>` fields for graceful error reporting |

## Validation Phases

Checks run in dependency order inside `check/mod.rs::run_checks()`:

1. File presence (manifest, catchments, graph, declared auxiliary artifacts)
2. Manifest field validation (13 checks)
3. Schema validation (column types, row group stats/sizes, unit_count match)
4. ID + value constraints (positivity, uniqueness, bbox validity, areas)
5. Cross-file referential integrity (graph-catchment coverage, upstream refs, snap auxiliary FKs, bbox enclosure)
6. Graph acyclicity (Kahn's algorithm)
7. Geometry spot-check (WKB type + geozero validity, 1% sample for catchments)
8. Raster checks (dtype, tiling, nodata, CRS, manifest-bbox containment)

## Usage

```
hfx <DATASET_PATH> [--format text|json] [--strict] [--skip-rasters] [--sample-pct N]
```

Exit codes: `0` = valid, `1` = invalid.

## v0.3.0 Changes

- `manifest.json::format_version` is hard-cut to `"0.3.0"`; `"0.2.1"`, `"0.2"`,
  and `"0.1"` are rejected with `manifest.unsupported_format_version`.
- `catchments.parquet` and snap files carry the bounding box as a single `bbox`
  struct with four `float32` leaves (`xmin`, `ymin`, `xmax`, `ymax`) declared as
  a [GeoParquet 1.1](https://geoparquet.org/releases/v1.1.0/) `covering`, so
  standard spatial tools recognize the bbox for predicate pushdown. The required
  row-group statistics now sit on the struct leaves (`bbox.xmin`, `bbox.ymin`,
  `bbox.xmax`, `bbox.ymax`), and the validator checks the covering metadata at
  `geo.columns.geometry.covering.bbox`.
- Snap features are declared with `hfx.aux.snap.v2`; the previous v1 snap schema
  is no longer blessed.
- D8 rasters are declared with `hfx.aux.d8_raster.v2`; the previous v1 schema is
  no longer blessed and is rejected with `manifest.auxiliary_schema`. A v2
  declaration must carry `crs`, `flow_dir_encoding`, and `flow_acc_units` as
  manifest metadata, while dtype and nodata stay authoritative in each GeoTIFF
  header. Rasters are no longer assumed to be WGS84: footprints are read in
  their declared CRS and projected for the extent check.
- `graph.parquet` is unchanged: it carries no geometry column, so it keeps its
  four flat `bbox_minx`, `bbox_miny`, `bbox_maxx`, `bbox_maxy` columns plus their
  row-group statistics and layout checks.
- Multi-level `catchments.parquet` and `graph.parquet` must be ordered by
  non-decreasing `level`. Hilbert ordering remains deferred until curve
  parameters are specified.
- `stem_role` allows `mainstem`, `tributary`, `distributary`, or `unknown`, and
  the validator enforces the enum. Snap `weight` values must be finite and
  non-negative, and `metadata.references_levels` must include every referenced
  catchment level.

## Diagnostic Capping

The validator caps repetitive diagnostics to keep reports readable:

- **Per-row null violations**: When a non-nullable column contains more than 10 null values, the first 10 are reported individually with row indices. Remaining violations are summarised as a single diagnostic stating how many were suppressed.
- **Batch-read failures**: If 3 consecutive Parquet/Arrow record-batch reads fail (e.g., due to an unsupported compression codec or file corruption), the reader aborts early with a summary diagnostic rather than continuing to accumulate identical errors.

## Known Conformance Gaps

The following spec-required checks are **not implemented** and will emit warnings rather than errors:

| Spec Rule | Status | Reason | Tracking |
|---|---|---|---|
| Hilbert sort order on catchments/graph rows | Deferred | Curve parameters not yet specified in the spec; `ordering.catchments.hilbert_unsorted` and `ordering.graph.hilbert_unsorted` are registered but not emitted in v0.2.1 | Deferred |
| Polygon self-intersection / geometric validity | Partial | `geozero` checks WKB structural validity, not topological validity | Partial |
| Snap bbox strictness (`<=` for line features) | Fixed | Snap bboxes now correctly use `<=` rather than `<` for line-feature bbox enclosure | — |
| Parquet compression codecs (zstd, snappy, lz4, gzip) | Fixed | All four codecs are now supported; codec detection errors are reported via diagnostic capping | — |

A dataset that passes this validator with `--strict` is conformant on all checked rules. The unchecked rules above mean a passing result does **not** guarantee full spec conformance.

## Build Requirement

`hfx-cli` requires GDAL at build/runtime for raster CRS and extent validation. The validator links against the system GDAL installed on the host.
