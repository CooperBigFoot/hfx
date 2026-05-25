# GRIT v2 Adapter — Executor Prompt

You are the executor for the GRIT v2 adapter implementation. Your authoritative work order is `docs/plans/grit-v2-adapter.md` in this repository — that document contains the full design, the §0 resolved decisions, the §5 risk register, the §6 test strategy, and the §8 coordinator resolutions. **Read it end-to-end before writing any code, and treat it as the spec.** This prompt is a thin wrapper that highlights two non-negotiables and lists the standard project gates.

---

## Required reading order

1. `docs/plans/grit-v2-adapter.md` — full work order.
2. `CLAUDE.md` (project conventions, including the per-commit patch-version-bump rule).
3. `spec/HFX_SPEC.md` (v0.2.1 baseline) and `spec/aux/snap/v1.md` (snap aux schema).
4. `adapters/_template/build_adapter.py` (nine-stage contract).
5. `adapters/grit/build_adapter.py`, `adapters/grit/merge_regions.py`, `adapters/grit/run_all_regions.py` (v1 helpers worth vendoring).
6. `adapters/grit/GRIT_HFX_SPEC_VALIDATION.md` (v1 friction log — read for known landmines).

---

## Two non-negotiables — do not skim past these

### 1. `up_area_km2` for reaches: reverse-topological DP, NOT segment inheritance

The plan's §0 decision #8 and the new §2 "Phase 2.5" section are explicit: reach `up_area_km2` is computed in a reverse-topological DP over the reach graph, with reach `area_km2` as the per-node contribution. **You must not inherit the parent segment's `drainage_area_out` for reach rows.** Inheritance silently misleads consumers because, within a single segment, the downstream-most reach should hold close to the full segment area while the upstream reach should hold only a small fraction.

If Path B (derive reach edges from segment topology — see Risk 1) is chosen AND derivation turns out fragile, the fallback is **NULL for all reach rows**, documented in the dataset-root `README.md`. The catchments schema permits this (`up_area_km2` is `float32 nullable`). Risk 8 in the plan formalises this gate.

### 2. Tier 0 source-schema probe is your day-1 work item

Before writing any adapter code, run the schema probe described in §5 Risk 1 and §6 Tier 0 of the plan: inspect the reach `lines` layer schema in the EU, AS, and NA GPKGs to determine whether GRIT exposes an explicit reach upstream-ids column (Path A) or whether reach edges must be derived from segment topology (Path B). The outcome fixes the implementation strategy for `stage_7_write_graph` and the Phase 2.5 DP, and substantially changes the effort estimate (see §7 in the plan: Path A is M, Path B is L). **Do not start `stage_1_inspect_source` until this is resolved**, since the per-region intermediate shard schema (specifically `tmp/<region>/reaches.parquet`'s `upstream_source_global_ids` column) depends on Path A vs Path B.

Report the probe outcome back before proceeding to Stage 1.

---

## Quality gates

- **Per-commit:** `./scripts/bump-version.sh patch`, `cargo test --workspace` green (if any Rust changes touch the workspace; pure-Python adapter work does not need cargo tests but must keep them green), tag the commit with `v<version>`. Patch bumps only — never minor/major without explicit user approval.
- **Per stage:** validate with `pytest adapters/grit-v2/` if tests exist, and exercise the stage on EU smoke data before claiming it done.
- **Per phase boundary:** the dataset produced at that boundary should still load with the v1 GeoParquet validator (`validate_geoparquet`); the HFX validator only needs to be green at the end of Phase 3.
- **End of Phase 3:** `cargo run -p hfx-validator -- <dataset> --strict --sample-pct 100 --format text` must return `Result: VALID` on the EU smoke build first, then the planetary build.
- **No `.unwrap()` / `.expect()` in library code, no `println!`, use `tracing`, follow the project's type-driven and `thiserror`/`anyhow` conventions** as described in `CLAUDE.md`.

## Escalation protocol

Escalate to the coordinator (do not silently re-decide) when any of the following happens:

- The Tier 0 schema probe finds neither Path A nor a workable Path B (i.e. you cannot reconstruct reach upstream relations at all).
- A `--strict` validator failure persists after a fix attempt that you believe should have resolved it (the validator has known UX gaps — see §1 out-of-scope in the v0.2.1 release plan).
- Any of the §0 resolved decisions becomes infeasible to honour (e.g. snappy compression conflicts with a downstream consumer, or the dense ID re-issue order cannot match Hilbert order for a defensible technical reason).
- Memory or wall-clock exceeds the §4 estimates by more than 2×.
- You identify a needed change to the HFX spec or validator itself (file it; do not fold into this workstream).

Otherwise: prefer to make a documented call locally and surface it in the final report.

## Reporting format

At the end of each completed phase, produce a short status update (one to two paragraphs) covering:

- What you ran (per-region timings + RAM peaks; validator output).
- Any deviation from the plan, with justification.
- Open follow-ups you would want filed as separate issues (do not fold them into the GRIT v2 workstream).

Final deliverable: a working `adapters/grit-v2/build_adapter.py` (plus supporting helpers) that, when run end-to-end against the seven GRIT regions, produces an HFX v0.2.1-conformant global dataset at `<root>/grit-hfx-global/` passing `cargo run -p hfx-validator -- <dataset> --strict --sample-pct 100`. The dataset is staged locally only; publishing to `s3://basin-delineations-public/grit/2.0.0/` is a coordinator action gated on the validator-green report.
