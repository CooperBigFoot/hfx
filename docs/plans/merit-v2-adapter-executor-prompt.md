# MERIT v2 Adapter — Executor Prompt

You are the executor for the MERIT v2 adapter implementation. Your authoritative work order is `docs/plans/merit-v2-adapter.md` in this repository — that document contains the full design, the §0 resolved decisions, the §5 risk register, the §6 test strategy, and the §8 coordinator resolutions. **Read it end-to-end before writing any code, and treat it as the spec.** This prompt is a thin wrapper that highlights five non-negotiables and lists the standard project gates.

---

## Required reading order

1. `docs/plans/merit-v2-adapter.md` — full work order.
2. `CLAUDE.md` (project conventions, including the per-commit patch-version-bump rule).
3. `spec/HFX_SPEC.md` (v0.2.1 baseline), `spec/aux/snap/v1.md` (snap aux schema), `spec/aux/d8_raster/v1.md` (d8 raster aux schema — load this carefully, your 60 raster aux entries must conform exactly).
4. `adapters/_template/build_adapter.py` (nine-stage contract).
5. `adapters/merit/build_adapter.py`, `adapters/merit/README.md`, `adapters/merit/merge_basins.py`, `adapters/merit/build_global_rasters.py`, `adapters/merit/run_missing_basins.py` (v0.1 helpers worth vendoring — especially the raster transcode helpers, the COMID/uparea handling, the rclone/curl orchestrator, and the cross-basin merge logic in `merge_basins.py`).
6. `adapters/grit-v2/build_adapter.py` (the just-shipped reference implementation of the nine-stage contract at planetary scale; reuse the `_downstream_endpoint`, `_node_key`, `_load_id_map`, `_write_balanced_table`, and `validate` patterns where they apply).
7. `docs/decisions/2026-04-30-grit-global-adapter.md` (decision record for the global-build pattern this plan mirrors).

---

## Five non-negotiables — do not skim past these

### 1. Phase 0 cross-basin preflight is your day-1 work item

Before writing any adapter code beyond skeleton stubs, implement and run `verify_cross_basin.py` (see plan §2 Phase 0 and §5 Risks 1, 2, 3). The output gates the rest of the build. Specifically the preflight must HARD-FAIL if:

- Any COMID is duplicated across the 60 selected Pfaf basins.
- Any `NextDownID > 0` in any basin's `riv_pfaf_<NN>` resolves to a COMID in a *different* basin.
- Any shapefile schema (column names + dtypes) drifts from the pfaf-27 reference.

If any of those fire, **stop and escalate**. The fallback path (dense ID re-issue, or cross-basin edge resolution) materially changes the plan and the coordinator must sign off.

### 2. 60 D8 raster aux entries — one per basin, NOT a planetary mosaic

The plan's §0 decisions #6, #13, and #14 are explicit: ship 60 `hfx.aux.d8_raster.v1` entries, one per basin, with COGs under `aux/d8/pfaf_<NN>/{flow_dir,flow_acc}.tif`. **You must not** stitch them into a single planetary mosaic. The v0.1 `build_global_rasters.py` script exists and is tempting, but it is OUT OF SCOPE for this workstream — it stays as a future-work tool. Engines consuming this dataset want per-basin streamable D8, not 30 GB monolithic COGs.

Each raster must conform exactly to `spec/aux/d8_raster/v1.md`:
- `flow_dir`: COG, `uint8`, NoData=`255`, EPSG:4326, internal tiling 256×256 or 512×512.
- `flow_acc`: COG, `float32`, NoData=`-1.0`, EPSG:4326, same tiling as flow_dir.
- Metadata `flow_dir_encoding` must be `"esri"` (matches MERIT's 1/2/4/8/16/32/64/128 convention).

### 3. `id = COMID` directly — no dense re-issue

The plan's §0 decision #9 keeps COMIDs as HFX `id`s verbatim, unlike GRIT v2 which re-issues. This is correct because (a) MERIT-Basins COMIDs are positive int64 already, (b) they are globally unique (verified by Phase 0), and (c) preserving source IDs has high audit value. **Do not "improve" this by re-issuing dense IDs in Hilbert order** — you would be silently changing the data contract and breaking auditability against v1 datasets.

If Phase 0 finds COMID collisions, **escalate** rather than silently re-issuing. The plan's §9 "Open questions for coordinator" item 1 documents this explicitly.

### 4. Phase 1 writes COGs directly to the dataset path. No symlinks. Ever.

Plan §2 Phase 1 step 8 is explicit: per-basin COG transcoding writes to `<dataset_root>/aux/d8/pfaf_<NN>/{flow_dir,flow_acc}.tif` — the **final dataset location**, not a staging area, not a `<basin_workdir>`. Stage 8 is snap-only and never touches the COG files.

**Do not use `os.symlink`.** Symlinks break `aws s3 sync` (either error out or silently dereference, losing provenance). If for some operational reason you absolutely need an indirection layer (you shouldn't — direct write is the design), the only acceptable fallback is `os.link` (hardlink, same-filesystem only) or `shutil.copy2`. The plan's design eliminates the need for any of those.

The natural consequence: if Phase 1 fails partway, the partial `aux/d8/` directory is the resume point. Make the orchestrator idempotent — skip basins whose COGs already exist and pass `cog_validate`. This is described in the plan §2 Phase 1 "Implication for failure recovery" paragraph.

### 5. Single-basin Tier 1 smoke uses a real partial-fabric manifest, not a fake "wrapped global"

Plan §6 Tier 1 is explicit: the pfaf-27 single-basin smoke ships as a proper HFX v0.2.1 partial-fabric dataset per spec §Deployment Patterns:
- `region = "pfaf-27"`
- `bbox` = the actual pfaf-27 polygon-union extent (compute from data; do NOT use `[-180, -90, 180, 90]`)
- One d8 aux entry named `"pfaf-27"`, one snap aux entry named `"stems"`
- `unit_count` = pfaf-27 row count only

**Do not** fake a planetary manifest with one basin's worth of data and call it a "smoke build" — that violates the spec (planetary bbox with non-planetary coverage). The orchestrator's single-basin mode produces a conformant partial dataset, full stop.

---

## Quality gates

- **Per-commit:** `./scripts/bump-version.sh patch`, `cargo test --workspace` green (if any Rust changes; pure-Python adapter work does not need cargo tests but must keep them green), tag the commit with `v<version>`. Patch bumps only — never minor/major without explicit user approval.
- **Per stage:** validate with `pytest adapters/merit-v2/` if tests exist, and exercise the stage on pfaf-27 smoke data before claiming it done.
- **Per phase boundary:** the dataset produced at that boundary should still load with the GeoParquet 1.1 validator (`validate_geoparquet`); the HFX validator only needs to be green at the end of Phase 3.
- **End of Phase 3:** `cargo run -p hfx-validator -- <dataset> --strict --sample-pct 100 --format text` must return `Result: VALID` first on the pfaf-27 single-basin smoke (a proper partial-fabric dataset with `region="pfaf-27"` and the actual pfaf-27 bbox — see non-negotiable #5), then on the 3-basin smoke (partial-fabric with `region="smoke-3basin"` covering pfaf-27 + pfaf-42 + pfaf-91), then on the planetary build (global, `region` omitted, `bbox=[-180,-90,180,90]`).
- **No `.unwrap()` / `.expect()` in library code, no `println!`, use `tracing`, follow the project's type-driven and `thiserror`/`anyhow` conventions** as described in `CLAUDE.md`. (Note: this is a Python adapter so most of the Rust-specific rules don't apply, but the spirit — structured logging via `logging` not `print`, named exceptions over generic `RuntimeError`, no swallowing of errors — does.)

## Escalation protocol

Escalate to the coordinator (do not silently re-decide) when any of the following happens:

- Phase 0 preflight finds COMID collisions OR cross-basin `NextDownID` references OR shapefile schema drift.
- The MERIT-Basins `uparea` field turns out to be in different units or with different semantics than HFX's `up_area_km2` (Risk 7).
- A `--strict` validator failure persists after a fix attempt that you believe should have resolved it.
- Any of the §0 resolved decisions becomes infeasible to honour (e.g. the 60-aux-entries manifest blows up the validator beyond the 60-minute budget).
- Memory or wall-clock exceeds the §4 estimates by more than 2×.
- Workstation free disk drops below 100 GB before Phase 1.5 cleanup can recover it (this trigger is expected NOT to fire on the ≥250 GB-free budget after the direct-write architecture removed ~100 GB of staging — see plan §4).
- You identify a needed change to the HFX spec or validator itself (file it; do not fold into this workstream).

Otherwise: prefer to make a documented call locally and surface it in the final report.

## Reporting format

At the end of each completed phase, produce a short status update (one to two paragraphs) covering:

- What you ran (per-basin timings + RAM peaks; validator output).
- Any deviation from the plan, with justification.
- Open follow-ups you would want filed as separate issues (do not fold them into the MERIT v2 workstream).

Final deliverable: a working `adapters/merit-v2/build_adapter.py` (plus `verify_cross_basin.py` and `run_all_basins.py` helpers) that, when run end-to-end against the 60 MERIT Pfaf basins, produces an HFX v0.2.1-conformant global dataset at `<root>/merit-hfx-global/` passing `cargo run -p hfx-validator -- <dataset> --strict --sample-pct 100`. The dataset is staged locally only; publishing to `s3://basin-delineations-public/merit/0.2.0/` is a coordinator action gated on the validator-green report.
