# Spec Changelog

## Unreleased

### Changed
- `snap.parquet` is now optional. Added `has_snap` boolean to manifest. When absent, engine falls back to point-in-polygon on catchments.

- Established `spec/HFX_SPEC.md` as the canonical development path for the HFX specification.
- Began organizing the repository around a spec-first monorepo structure.
