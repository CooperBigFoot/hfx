# Valid tiny v0.3.0 fixture with tiled D8 auxiliary rasters

Expected diagnostic: `none`.

This fixture exercises the relaxed `hfx.aux.d8_raster.v1` spatial contract.
The manifest bbox is `[0, 0, 2, 2]`; the `west` D8 entry covers `[0, 0, 1, 2]`
and the `east` D8 entry covers `[1, 0, 2, 2]`. Each raster overlaps the
manifest bbox but neither entry contains it.
