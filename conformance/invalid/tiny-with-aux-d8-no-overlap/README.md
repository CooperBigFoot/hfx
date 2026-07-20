# Invalid tiny v0.3.0 fixture with non-overlapping D8 auxiliary rasters

Expected diagnostic: `raster.extent_no_overlap`.

This fixture exercises the relaxed `hfx.aux.d8_raster.v2` spatial contract's
remaining guardrail. The manifest bbox is `[0, 0, 2, 2]`, while the D8 rasters
cover `[-10, -10, -5, -5]`, so their extents are entirely outside the manifest
bbox.
