# Changelog

All notable changes to `hfx-core` are documented here.

## 0.2.1 — 2026-05-25

- Add `StemRole::Distributary` for branches diverging at bifurcations.
- Add `BlessedAuxSchema::SnapV1` for `hfx.aux.snap.v1` manifest declarations.

## 0.2.0 — 2026-04-20

- Tighten Weight contract: higher weight MUST indicate greater hydrological dominance (previously "typically" upstream area).
- Flip default snap strategy in HFX_SPEC §3 from distance-first tiered ranking to weight-first cascade with mainstem / distance / id tie-breakers. Distance-first remains available as an opt-in engine strategy for datasets whose weights are not rank-meaningful.
- No schema changes. Adapters that already write weight = upstream_area (GRIT, MERIT, HydroSHEDS) remain conformant.
