# D8 raster auxiliaries gain a declarative v2 schema (CRS, encoding, units)

**Status:** Accepted

**Date:** 2026-07-20

## Decision

The `hfx.aux.d8_raster` auxiliary schema is revised to a v2 that describes the
rasters it points at instead of normalizing them. v2 declares in metadata: the
raster CRS (no longer required to be EPSG:4326), the flow-direction encoding
(enum widened beyond `esri`/`taudem` to include `grass`), and the
flow-accumulation units (upstream cell count or km²). A D8 pair may remain on
its native grid, including projected equal-area grids; engines read the
metadata, transform query coordinates into the raster CRS at refinement time,
and normalize encoding and units on read. Producers may still emit v1-shaped
data where lossless; the spec no longer requires it. Core HFX vector artifacts
remain EPSG:4326; this applies to D8 raster auxiliaries only.

## Context

GRIT v1.0 publishes its 30 m drainage-direction and drainage-area rasters in
EPSG:8857 (Equal Earth). A D8 raster is not a field of location properties: each
cell value is a pointer to a specific neighbor on that exact grid. Warping the
grid to another CRS rotates grid directions relative to the target grid (in
Equal Earth, increasingly with distance from the central meridian) and
duplicates or drops cells during resampling, producing artificial pits, parallel
channels, and severed flow paths. Standard hydrology practice is to never
reproject a flow-direction grid; directions are re-derived from a reprojected
DEM instead, which is out of scope here.

## Alternatives considered

- **Warp GRIT rasters to EPSG:4326 to satisfy the v1 schema:** rejected because
  it corrupts the D8 neighbor semantics that terminal refinement traces, and
  refinement accuracy is the sole reason the rasters are being added.
- **Re-derive flow direction on a 4326 grid from FABDEM:** rejected as a
  separate, much larger data-production effort that duplicates the published,
  peer-reviewed GRIT processing.
- **Keep the spec unchanged and treat GRIT rasters as an out-of-contract
  side channel:** rejected because the auxiliary contract exists precisely so
  engines can consume declared rasters without fabric-specific logic.
- **Minimal v2 (CRS field only, adapter normalizes encoding and units to v1
  semantics):** rejected in favor of a uniformly declarative schema — auxiliary
  data is deliberately less opinionated than core artifacts, and a consumer
  should learn everything it needs from metadata alone rather than relying on
  some fields being normalized and others declared.
