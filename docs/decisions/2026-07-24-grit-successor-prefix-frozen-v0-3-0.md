# GRIT planetary D8 ships under a successor prefix; hfx-v0.3.0 is frozen

**Status:** Superseded

**Superseded by:** [2026-08-07-grit-v0-3-0-authority-driven-manifest.md](2026-08-07-grit-v0-3-0-authority-driven-manifest.md)

**Date:** 2026-07-24

## Context

The 2026-07-24 live fire of the human-gated manifest amendment on
`grit/hfx-v0.3.0` was rolled back within ~25 minutes. Released pourpoint
0.2.0 parses `hfx.aux.d8_raster.v2` entries but cannot open the planetary COG
pair: the BigTIFF tile index (2,041,930 tiles per raster) extends to byte
~24.5 MiB, the Rust `tiff` crate loads it eagerly on IFD open, and 0.2.0
bounds remote reads at 256 KiB for extent selection and 16 MiB for the carve
window. Its best-effort mode escalates the selection failure to a fatal
error, so every default-mode GRIT carve failed while the amendment was live.
The reader gate assumed by the in-place plan was satisfied only at the schema
level; it was never proven against the actual staged artifacts.

## Decision

`grit/hfx-v0.3.0` stays byte-stable for deployed 0.2.0 readers. The
planetary D8 entry publishes under successor prefix `grit/hfx-v0.3.1` via
server-side copy of the staged objects plus an amended manifest, once a
released reader has been proven against the actual staged COGs. Re-firing
the amendment on `grit/hfx-v0.3.0` was rejected: an addition a deployed
parser rejects is a breaking change, and deployed 0.2.0 rejects this one
fatally. The reader gate is behavioral; a release that merely parses the
entry schema does not satisfy it.

This supersedes `2026-07-20-grit-reader-gated-in-place-publish.md`.
