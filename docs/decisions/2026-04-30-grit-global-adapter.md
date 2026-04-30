# Global GRIT HFX Adapter

## Status

Accepted on 2026-04-30. The global GRIT HFX adapter has completed preflight, per-region build, merge, strict validation, and sanity checks.

## Context

GRIT publishes seven EPSG:4326 regional slices (`AF`, `AS`, `EU`, `NA`, `SA`, `SI`, `SP`) whose segment `global_id` values can be used directly as HFX atom ids if they are globally unique and graph references do not require regional id translation. The project already established a MERIT global adapter pattern: compile each source partition independently, empirically verify merge safety, concatenate into one global HFX dataset, and omit `manifest.region` for the final global artifact.

## Decision

GRIT will mirror the MERIT global-build shape: keep a parameterized per-region builder, add a `run_all_regions.py` orchestrator, and merge successful regional HFX outputs into one global dataset. The old Europe-specific filename was renamed to `build_adapter.py` to match the shared adapter convention and make GRIT a canonical adapter-guide example rather than a one-off EU script.

An empirical preflight gate is required before global merge. `verify_cross_region.py` scans the source archives for duplicate ids, cross-region graph references, unresolved graph references, and antimeridian geometry indicators before the expensive build/merge steps.

## Consequences

The global GRIT dataset is a single source-fabric HFX artifact with `bbox = [-180.0, -90.0, 180.0, 90.0]` and no `region` field in the manifest. The graph can be concatenated without id remapping because the preflight found no collisions or unresolved/cross-region references.

The merge implementation must rebuild each output row group from fresh Arrow arrays while preserving the `binary` geometry schema. A direct concatenation of large WKB arrays initially failed with PyArrow binary offset overflow.

## Verification

Preflight passed with:

```bash
uv run --project adapters/grit python adapters/grit/verify_cross_region.py \
    --outer-archive /Users/nicolaslazaro/Desktop/grit-hfx/17435232.zip \
    --root /Users/nicolaslazaro/Desktop/grit-hfx
```

Reports were written to `/Users/nicolaslazaro/Desktop/grit-hfx/preflight/cross_region_report.json` and `.txt`. The verifier scanned `AF`, `AS`, `EU`, `NA`, `SA`, `SI`, and `SP`; found `total_unique_ids = 1,767,065`, `max_id_value = 600000654`, `id collisions = 0`, `resolved cross-region refs = 0`, `unresolved refs = 0`, and `antimeridian regions = 0`.

Phase 6 run `20260430T162007Z` succeeded for all seven regions with summary `/Users/nicolaslazaro/Desktop/grit-hfx/batch_logs/20260430T162007Z/summary.json`. Atom counts were: `AF=351755`, `AS=353827`, `EU=150325`, `NA=303473`, `SA=237943`, `SI=235185`, `SP=134557`.

The merged output at `/Users/nicolaslazaro/Desktop/grit-hfx/global/grit-hfx-global` passed the strict validator, wrote `/Users/nicolaslazaro/Desktop/grit-hfx/global/validator-report.json`, and passed sanity assertions for `atom_count = 1,767,065`, exact global bbox, and omitted `manifest.region`.
