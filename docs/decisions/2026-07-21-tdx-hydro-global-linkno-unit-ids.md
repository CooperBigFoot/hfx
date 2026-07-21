# TDX-Hydro HFX unit IDs are global LINKNOs from the first compile

**Status:** Accepted

**Date:** 2026-07-21

## Context

Native TDX-Hydro `LINKNO` values are unique only within one of the 62
processing basins, and the planetary build (#107) concatenates all of them into
one dataset. The single-basin pilot (#106) could have shipped native LINKNOs
and deferred renumbering to the merge.

## Decision

Every TDX-Hydro compile assigns drainage-unit IDs with the GEOGLOWS numbering
convention `LINKNO + header_number × 10_000_000`, using a per-processing-basin
header crosswalk vendored into this repository. `-1` sentinel links are never
transformed. The fabric data itself remains the pristine NGA distribution.

## Why

A pilot validated under native IDs proves the wrong artifact: the planetary
merge would renumber every `id`, graph edge, and snap `unit_id`, invalidating
the pilot's validation and any recorded references. Adopting a published,
deterministic convention now makes the pilot byte-compatible with the merge.
Vendoring the crosswalk keeps builds offline-reproducible and keeps the
pristine-NGA source decision intact, since the crosswalk is a numbering aid,
not fabric data.
