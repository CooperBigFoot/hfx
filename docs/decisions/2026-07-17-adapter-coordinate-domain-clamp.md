# HydroBASINS source coordinates use a one-cell domain clamp

**Status:** Accepted

**Date:** 2026-07-17

## Decision

The HydroBASINS adapter clamps marginal EPSG:4326 coordinate overshoot once at
each source-normalization boundary: `load_region_units`, `load_pour_points`, and
`_normalize_rivers_layer`. Each clamp occurs after conversion to EPSG:4326 and
geometry-type validation, but before IDs, outlet coordinates, Hilbert sorting,
bounds, or river source order propagate the coordinates downstream.

The legal domains are longitude `[-180, 180]` and latitude `[-90, 90]`. The
clamp envelope is exactly one source DEM cell: 15 arc-seconds, or
`15 / 3600 = 0.004166666666666667` degrees. Marginal excess at or below that
envelope snaps to the exact domain edge. Larger excess is a source-contract
violation and raises `AdapterError`.

Each boundary invocation that changes coordinates emits one warning containing
the layer, altered vertex count, and sorted unique source IDs. Calls that do not
change a coordinate do not emit a clamp warning.

## Context

The observed `au`/Fiji source issue affects HydroBASINS units with `HYBAS_ID`
5120082160 and 5120082230. Their source polygons contain longitude values around
`180.0006`, approximately `0.0006` degrees east of +180. This is harmless source
precision noise within one source cell and approximately seven times smaller
than the selected envelope, but leaving it unchanged produces out-of-domain
coordinates and bbox values downstream.

The tolerance is grounded in the 15 arc-second source resolution. It is neither
a convenient rounded epsilon nor runtime configuration. Grossly invalid or
wrongly projected coordinates remain fatal and carry the layer, source ID,
coordinate excess, and tolerance in the diagnostic.

True antimeridian unwrap or split behavior is separate. This decision does not
change `guard_antimeridian` or how units spanning the antimeridian are handled.

## Alternatives considered

- **Unbounded clamping:** rejected because it would silently conceal corrupt or
  wrongly projected inputs.
- **Zero-tolerance rejection:** rejected because it would fail known harmless
  precision noise in the source product.
- **A larger arbitrary or configurable tolerance:** rejected because it weakens
  the source contract without a domain rationale and permits behavior to vary
  between adapter runs.

## Why this is easy to get wrong later

Applying a clip only while writing bounds or outlet columns leaves the source
geometry invalid for other consumers. Applying independent clipping in each
consumer also risks inconsistent geometry and derived columns. The normalization
therefore remains a shared parse-boundary operation, with rejection measured
before clipping so a gross excess cannot lose its diagnostic evidence.

The executable regressions are
`CoordinateDomainClampTests.test_au_fiji_polygon_vertices_are_clamped`,
`test_marginal_overshoot_is_clamped_at_point_and_river_boundaries`,
`test_over_tolerance_coordinates_are_rejected_at_the_source_boundary`, and
`test_in_domain_geometry_does_not_emit_a_clamp_warning` in
`adapters/hydrobasins/test_build_adapter.py`. The existing
`PourPointTests.test_invalid_selected_coordinates_are_rejected` also protects
the rule that gross coordinates fail at the load boundary, including for points
that would otherwise be unmatched.
