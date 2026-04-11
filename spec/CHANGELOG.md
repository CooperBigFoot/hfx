# Spec Changelog

## Unreleased

### Changed
- `snap.parquet` is now optional. Added `has_snap` boolean to manifest. When absent, engine falls back to point-in-polygon on `catchments.parquet` for outlet resolution.
- Updated manifest JSON example to include `has_snap` field.
- Updated engine behaviour contract (step 1) with two-path resolution: tiered snap ranking when `has_snap = true`, spatial containment query when `has_snap = false`.
- Validation rules for snap referential integrity are now conditional on `has_snap = true`.

### Infrastructure
- Established `spec/HFX_SPEC.md` as the canonical development path for the HFX specification.
- Began organizing the repository around a spec-first monorepo structure.
