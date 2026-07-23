# Changelog

This file tracks TOOLKIT releases — the `hfx` and `hfx-cli` crates and the
`hfx` CLI binary — following [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
conventions. See [`docs/spec/versioning.md`](./docs/spec/versioning.md) for the
two-track version policy and [`spec/CHANGELOG.md`](./spec/CHANGELOG.md) for
specification changes.

Entries at 0.4.0 and earlier were written under the crates' pre-rename names
`hfx-core` and `hfx-validator`; the crates were renamed to `hfx` and `hfx-cli`
before the 0.4.0 publish.

## [Unreleased]

Nothing yet.

## [0.5.0] - 2026-07-23

Breaking MINOR bump. The spec track does not move: 0.5.0 still implements
**HFX spec 0.3.0**. The break is the D8 raster auxiliary schema, a breaking
auxiliary-schema change *within* format 0.3.0 (see the
[spec changelog](./spec/CHANGELOG.md) D8 raster v2 entry). `hfx` and `hfx-cli`
release as 0.5.0 in lockstep; `hfx-cli` pins `hfx` with an exact `=0.5.0`
dependency.

### Changed

- [`hfx.aux.d8_raster.v2`](./spec/aux/d8_raster/v2.md) replaces v1 as the
  blessed D8 raster schema. Raster CRS, flow-direction encoding, and
  flow-accumulation units become required manifest metadata on a D8
  declaration; dtype and nodata are authoritative in each GeoTIFF header
  rather than declared in the manifest.
- `hfx` replaces `BlessedAuxSchema::D8RasterV1` with
  `BlessedAuxSchema::D8RasterV2`. `AuxiliarySchemaId::parse` now rejects
  `hfx.aux.d8_raster.v1` with `AuxiliaryError::MalformedSchemaId`, and
  `hfx-cli` surfaces that rejection through the existing
  `manifest.auxiliary_schema` diagnostic.
- `hfx-cli` no longer assumes D8 rasters are in WGS84. Raster footprints are
  read in their native CRS and projected to WGS84 for the manifest-extent
  overlap check, so projected-CRS rasters validate. `RasterMeta::bbox` is
  renamed to `RasterMeta::bbox_wgs84` to name what it holds, and
  `D8RasterEntry` gains a `metadata: D8RasterMetadataV2` field.
- `raster.crs_mismatch` now compares each GeoTIFF's CRS against the declared
  `crs` metadata rather than against a fixed EPSG:4326 assumption.
- The `hfx-cli` → `hfx` dependency pin moves to `=0.5.0`.

### Added

- `FlowDirEncoding::Grass`, alongside the existing `Esri` and `Taudem`.
- `D8RasterMetadataV2` and its parsed components `EpsgCode` and
  `FlowAccumulationUnits`, with `D8RasterMetadataV2Error`, `EpsgCodeError`, and
  `FlowAccumulationUnitsError`, all exported from the `hfx` crate root.
- `hfx-cli` diagnostic `raster.crs_pair_mismatch`: `flow_dir` and `flow_acc`
  within one D8 entry must declare the same CRS.
- `hfx-cli` diagnostic `raster.flow_acc_units_dtype`: `flow_acc_units: cells`
  requires a float32 `flow_acc` raster.
- Conformance fixtures for the v2 contract:
  `valid/tiny-with-aux-d8-projected-grass` (projected CRS, GRASS encoding),
  `invalid/tiny-with-legacy-aux-d8-v1` (a v1 declaration must fail), and
  `invalid/tiny-with-aux-d8-missing-nodata`.

### Removed

- `BlessedAuxSchema::D8RasterV1`. Datasets declaring `hfx.aux.d8_raster.v1`
  are rejected; recompile them against v2. The v1 schema document is retained
  at [`spec/aux/d8_raster/v1.md`](./spec/aux/d8_raster/v1.md) for historical
  reference.

## [0.4.0] - 2026-07-06

Breaking MINOR bump implementing **HFX spec 0.3.0** (the GeoParquet bbox
covering break). `hfx-core` and `hfx-validator` both release as 0.4.0 in
lockstep; the validator pins `hfx-core` with an exact `=0.4.0` dependency.

> Corrected at 0.5.0 prep: this entry was dated 2026-06-28 and written under
> the pre-rename crate names, but both crates were published to crates.io on
> 2026-07-06 as `hfx` and `hfx-cli`. The `v0.4.0` git tag points at 525740e,
> which predates the rename and is *not* the published source. Read the
> published crates, not the tag, for what 0.4.0 shipped.

### Changed

- `hfx-validator` now reads the `catchments.parquet` and snap `bbox` as a
  single [GeoParquet 1.1](https://geoparquet.org/releases/v1.1.0/) `covering`
  struct (`xmin`, `ymin`, `xmax`, `ymax`) instead of four flat `bbox_minx…`
  columns, validates the covering metadata at
  `geo.columns.geometry.covering.bbox`, and reads the required row-group
  statistics from the struct leaves. `graph.parquet` stays flat (no geometry).
- `manifest.json::format_version` is hard-cut to `"0.3.0"`; datasets declaring
  `"0.2.1"` or earlier are rejected with `manifest.unsupported_format_version`.
- Snap auxiliary declarations move to `hfx.aux.snap.v2`; `hfx.aux.snap.v1` is no
  longer blessed.
- `hfx-core` adds `FormatVersion::V0_3_0` and replaces
  `BlessedAuxSchema::SnapV1` with `BlessedAuxSchema::SnapV2`.
- The `hfx-validator` → `hfx-core` dependency pin moves to `=0.4.0`.

## [0.3.0] - 2026-06-12

First curated release of the HFX toolkit under the two-track version policy
(see [`docs/VERSIONING.md`](./docs/VERSIONING.md)). `hfx-core` and
`hfx-validator` both release as 0.3.0 in lockstep and implement HFX spec
0.2.1 (frozen wire shape). Highlights cover everything since the published
crates.io snapshots (`hfx-core` 0.2.0, `hfx-validator` 0.1.26).

### Added

- HFX spec 0.2.1 with RFC 2119 normative hardening, a Version Compatibility
  section, and a dedicated spec changelog
  ([`spec/CHANGELOG.md`](./spec/CHANGELOG.md)).
- Manifest JSON schema
  ([`schemas/manifest.schema.json`](./schemas/manifest.schema.json)) and a
  conformance suite of valid/invalid fixtures exercising it.
- `hfx` validator CLI, installed by the `hfx-validator` crate.
- `grit-v2` and `merit-v2` reference adapters.
- `examples/tiny` reference dataset.
- GitHub Actions CI plus issue and pull-request templates.
- Governance docs: [`docs/VERSIONING.md`](./docs/VERSIONING.md) (two-track
  version policy), `CODE_OF_CONDUCT.md`, `GOVERNANCE.md`.

### Changed

- `hfx-validator` is now version-locked to `hfx-core` via an exact `=0.3.0`
  dependency pin; the two crates move in lockstep.

## Pre-0.3.0 history

Before the curated-release policy (adopted 2026-06), every commit carried a
patch version bump and a `v*` tag, reaching workspace version 0.2.64 across 87
tags. The crates.io snapshots from that era are `hfx-core` 0.2.0 and
`hfx-validator` 0.1.26. Curated releases begin with 0.3.0.
