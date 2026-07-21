# HydroBASINS → HFX: Unit Outlets from Pour Points, and Endorheic Routing

**Status:** Accepted

**Date:** 2026-07-15

**Follow-up:** Implemented by the core HydroBASINS adapter (Effort ticket #32,
Program #31). Snap features (HydroRIVERS) and the operational planetary build
are separate Efforts (#33, #34).

## Decision

The core HydroBASINS → HFX adapter sources every drainage unit's outlet
coordinate (`outlet_lon` / `outlet_lat`) from the **HydroBASINS Pour Points**
ancillary product, joined to the sub-basin polygons by `HYBAS_ID`. Outlets are
**not** derived geometrically from polygon boundaries and do **not** depend on
HydroRIVERS.

Three sub-decisions follow from this:

1. **Input set is two HydroBASINS layers.** The adapter ingests the standard
   sub-basin polygons (`hybas_<region>_lev12_v1.shp`) *and* the pour-point
   layer (`hybas_pour_lev12_v1.shp`). Both carry the same HydroSHEDS license.
   Every Pfafstetter-12 sub-basin has at least one pour point, so the join is
   total; a unit with no matching pour point is a build error.

2. **Lumped coastal units collapse to one outlet.** A HydroBASINS sub-basin
   that aggregates multiple small coastal watersheds carries multiple pour
   points under one shared `HYBAS_ID`. HFX requires exactly one outlet per unit,
   so for any `HYBAS_ID` resolving to more than one pour point the adapter
   selects one deterministically — the pour point nearest the sub-basin
   centroid, tie-broken by lowest `(lon, lat)` — and records the collapse in the
   adapter README.

3. **Virtual endorheic edges are cut.** When building `graph.parquet` from
   `NEXT_DOWN`, edges whose source unit is an endorheic sink (`ENDO = 2`) are
   dropped; those units are terminal in the graph even when they carry a
   non-zero `NEXT_DOWN` (a HydroBASINS "virtual" connection across which no
   surface water flows).

## Rationale

The HydroBASINS *polygon* attribute table has no outlet coordinate — it carries
only topological and metric fields (`HYBAS_ID`, `NEXT_DOWN`, `SUB_AREA`,
`UP_AREA`, `PFAF_ID`, `ENDO`, `COAST`, …). The naive conclusion is that the
outlet must be reconstructed geometrically (the midpoint of the boundary a unit
shares with its `NEXT_DOWN` neighbour) or deferred until HydroRIVERS is
available.

Both alternatives are inferior to using the ancillary product. The **HydroBASINS
Pour Points** layer (Lehner, Technical Documentation v1.0, December 2024) defines
a pour point as *"the outlet point of a sub-basin, i.e., the location at which
the river network drains into the next downstream sub-basin,"* extracted from
the underpinning HydroSHEDS grid *"at the center of the grid cell which shows
the highest flow accumulation value within the sub-basin."* That is the true,
DEM-derived pour cell — strictly better than any boundary-midpoint
approximation, and available without HydroRIVERS. It keeps the unit outlet (a
core, permanent field owned by this adapter) cleanly separated from snap
features (the HydroRIVERS query-time channel network, ticket #33), which never
revise outlets.

The coastal collapse is an accepted, documented deviation from HFX spec §4
(*"A polygon with multiple physical exit points SHOULD be represented as
multiple drainage units"*). Faithful splitting would require re-delineating
sub-watersheds per pour point on the DEM, which a vector-only adapter does not
do; the multiplicity is a HydroBASINS coding-scheme artifact (many independent
coastal micro-basins lumped into one `ORDER = 0` conglomerate), not one basin
with many hydrological outlets. Collapsing preserves the 1:1
`HYBAS_ID → id` mapping the whole adapter depends on.

Cutting virtual endorheic edges keeps `graph.parquet` a faithful surface-drainage
graph and keeps it **consistent with `UP_AREA`**, which HydroBASINS documents as
already excluding area reached only through virtual connections. Retaining those
edges would route upstream accumulation across a hydrological discontinuity and
disagree with the stored `up_area_km2`.

## Alternatives Rejected

- **Geometric outlet derivation** (shared-boundary midpoint between a unit and
  its `NEXT_DOWN` neighbour, interior/coastal points for terminals). Rejected:
  it is a coarse approximation of a point the source already provides precisely,
  and only the finite-and-in-range coordinate domain is validator-checked, so
  the approximation would silently degrade a core field.
- **Deferring outlets to the HydroRIVERS snap Effort (#33).** Rejected: the
  unit outlet is a core `catchments.parquet` field this Effort must populate,
  distinct from snap; coupling it to #33 would leave core artifacts incomplete.
- **Splitting lumped coastal units into one HFX unit per pour point.** Rejected:
  infeasible without DEM re-delineation and would break the 1:1
  `HYBAS_ID → id` mapping and the single-global-dataset decision (Program #31).
- **Preserving virtual endorheic connections as graph edges.** Rejected:
  contradicts surface-drainage semantics and the `UP_AREA` accounting.
