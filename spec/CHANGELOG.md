# HFX Specification Changelog

Specification-level changes, newest first. See the
[Version Compatibility](./HFX_SPEC.md#version-compatibility) section of the
spec for the versioning policy. The 0.2.0 → 0.2.1 hard cut predates that
policy and is recorded here as it happened.

## 0.2.1 — 2026-05-25

Hard-cut manifest version over 0.2.0.

- `bbox_minx`, `bbox_miny`, `bbox_maxx`, and `bbox_maxy` added as required
  columns on `graph.parquet`, with spatial-partitioning and row-group
  statistics rules matching `catchments.parquet`.
- Root-level `snap.parquet` retired as a core artifact. Snap features move to
  manifest-declared `hfx.aux.snap.v1` auxiliary declarations
  ([`spec/aux/snap/v1.md`](./aux/snap/v1.md)).
- `stem_role` enum enforced by validators, with `distributary` added alongside
  `mainstem`, `tributary`, and `unknown`.
- `format_version` const changed from `"0.2"` to `"0.2.1"` in the manifest
  JSON Schema.

### Editorial — 2026-06-12

Language-only hardening of the 0.2.1 text. The wire shape is unchanged and
`format_version` remains `0.2.1`.

- RFC 2119 conventions clause added to the spec Overview.
- Normative keywords uppercased throughout `HFX_SPEC.md`,
  `aux/d8_raster/v1.md`, and `aux/snap/v1.md`; non-normative prose rephrased
  to avoid keyword ambiguity.
- `Version Compatibility` section added to `HFX_SPEC.md`.
- This changelog created.

## 0.2.0 — 2026-05-14

Defines the v0.2 contract.

- `graph.parquet` replaces the v0.1 `graph.arrow` adjacency artifact.
- Manifest `auxiliary[]` declarations introduced. `hfx.aux.d8_raster.v1`
  ([`spec/aux/d8_raster/v1.md`](./aux/d8_raster/v1.md)) replaces first-class
  `flow_dir.tif` / `flow_acc.tif` artifacts.
- v0.1 manifest fields removed: `has_rasters`, `has_snap`, `fabric_level`,
  `flow_dir_encoding`, `terminal_sink_id`, and `atom_count`.
- Drainage-unit terminology adopted; replaces the v0.1 "catchment atom"
  vocabulary.
- `format_version` set to `"0.2"`.
