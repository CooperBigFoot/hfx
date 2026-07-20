# HydroBASINS nested levels use attribute parents, per-level outlets, and Pfaf-12 snap gating

**Status:** Accepted

**Date:** 2026-07-20

## Decision

The HydroBASINS adapter compiles one singleton or contiguous ascending source Pfafstetter range within levels 1 through 12. Source Pfaf level `P` maps to zero-based HFX level `P - min(selected range)`.

For every selected source Pfaf level above the range minimum, each unit's `parent_id` is resolved within its region by dropping the last decimal digit of the child's `PFAF_ID` and requiring that prefix to match exactly one unit at the immediately preceding source Pfaf level. An unresolved or ambiguous parent is fatal. Parent assignment is attribute-only and has no spatial fallback.

Every selected source Pfaf level obtains outlets independently from `hybas_pour_lev<NN>_v1.shp` through a total `HYBAS_ID` join. A coarser unit's outlet is selected from that level's own pour points and is never derived from a finest-level descendant.

HydroRIVERS snap output is permitted only when the selected range includes source Pfaf level 12. Snap keying remains `HYBAS_L12` to source Pfaf-12 units. The manifest records the corresponding zero-based HFX level with `references_levels = [12 - min(selected range)]`; the default source range `1-12` therefore declares `[11]`.

## Context

The previous adapter emitted only source Pfaf level 12 as HFX level 0, with null `parent_id`, one pour-point layer, and snap metadata fixed to `references_levels: [0]`. A nested HFX dataset needs explicit parent relations and an outlet and same-level graph for every selected level, while HydroRIVERS continues to identify its containing fine unit through `HYBAS_L12`.

HydroBASINS supplies the required attribute contracts. Adjacent source Pfaf levels encode parentage through decimal `PFAF_ID` prefixes. The ancillary Pour Points product supplies a separate global `HYBAS_ID`-keyed layer for each source Pfaf level. HydroRIVERS supplies `HYBAS_L12`, not a level-independent or coarser HydroBASINS key.

The committed `adapters/hydrobasins/attribute_join_scan_report.json` evaluated 3,786,218 child joins across 99 adjacent-level pairs, covered 3,786,228 basins across 12 pour-point levels, and checked 3,786,228 basins collision-free across 108 regional level layers. All checks reported zero findings.

## Rationale

Attribute-only parent assignment makes the published Pfaf hierarchy the source of truth and turns any source-contract break into a reproducible diagnostic. A spatial containment fallback could silently choose a different hierarchy and make results depend on geometric tolerances or repair behavior.

Per-level pour points preserve the DEM-derived outlet definition at every compiled granularity. Deriving a coarser outlet from a finest-level descendant would discard the dedicated ancillary observation for that level and couple coarse output to a separate descendant-selection rule.

Pfaf-12 snap gating preserves the meaning of `HYBAS_L12` and avoids pretending that a HydroRIVERS reach is keyed directly to a coarser unit. Computing the HFX reference level from the selected range keeps the physical key unchanged while accurately describing the nested dataset's zero-based level numbering.

## Alternatives considered

- **Spatial containment fallback for missing or ambiguous parents:** rejected because it can conceal a broken `PFAF_ID` contract and produce tolerance-dependent hierarchy.
- **Derive coarser outlets from finest-level descendants:** rejected because each source Pfaf level has its own authoritative DEM-derived pour-point layer.
- **Use only the finest selected level's pour points for every level:** rejected because the join key and outlet meaning are level-specific.
- **Allow snap output without source Pfaf level 12:** rejected because HydroRIVERS exposes `HYBAS_L12`, so no authoritative selected unit exists for the join.
- **Re-key snap reaches spatially or to a coarser level:** rejected because it changes the established snap contract and introduces an unverified spatial association.
- **Keep `references_levels: [0]` for every snap build:** rejected because source Pfaf level 12 is HFX level 11 in the default `1-12` build and HFX level 6 in a `6-12` build.

## Why this is easy to get wrong later

Source Pfaf numbering and HFX numbering coincide only accidentally for some selected minima and must remain explicitly distinguished. `HYBAS_L12` names the source Pfaf-12 unit even when that unit is not HFX level 0. Parent and outlet joins also have different scopes: parents resolve within a region across adjacent source levels, while pour points are global inputs joined independently at each level. Future changes must preserve those scopes and fail rather than substitute spatial inference.
