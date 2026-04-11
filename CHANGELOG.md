# Changelog

All notable changes to this repository will be documented in this file.

## Unreleased

### Added
- Implemented all `hfx-core` domain types with parse-don't-validate invariants.
  - Newtypes: `AtomId`, `SnapId`, `AreaKm2`, `Weight`, `Longitude`, `Latitude`, `BoundingBox`, `WkbGeometry`, `AtomCount`.
  - Enums: `Topology`, `FormatVersion`, `FlowDirEncoding`, `MainstemStatus`, `UpAreaAvailability`, `RasterAvailability`, `SnapAvailability`.
  - Structs: `CatchmentAtom`, `SnapTarget`, `AdjacencyRow`, `DrainageGraph`, `Manifest` (via `ManifestBuilder`).
  - Traits: `HasBbox`, `HasAtomId`.
  - Per-module error types: `IdError`, `MeasureError`, `GeoError`, `GraphError`, `ManifestError`.
- 78 unit tests covering construction, invariants, and edge cases.

### Changed
- `snap.parquet` is now optional. Added `has_snap` boolean to `manifest.json`. When absent, the engine falls back to point-in-polygon on `catchments.parquet`.
- Updated `manifest.schema.json` with `has_snap` field.

### Infrastructure
- Reorganized the repository as a spec-first monorepo.
- Moved the canonical development specification to `spec/HFX_SPEC.md`.
- Added placeholder areas for schemas, examples, conformance fixtures, adapters, and decision records.
- Split the Rust workspace into `hfx-core` and `hfx-validator`.
