# HFX v0.2.1 Release Plan

*Produced from the planning/grill session that resolved 8 open questions on the v0.2.1 amendment. This document is the authoritative work order for the executor agent; deviations require coordinator approval.*

**Status:** Approved — ready for execution

**Date:** 2026-05-25

**Companion:** [`docs/decisions/2026-05-07-v02-units-and-refinement.md`](../decisions/2026-05-07-v02-units-and-refinement.md) (v0.2 baseline this amendment builds on)

---

## 0. Resolved decisions (do not relitigate)

| # | Decision | Implication |
|---|---|---|
| 1 | Hilbert curve params **deferred to v0.3+** | `ordering.*.hilbert_unsorted` diagnostic IDs are registered but **not emitted** in v0.2.1; README "Known Conformance Gaps" entry kept |
| 2 | `hilbert_index` is **not** a stored column | Spec text must say so explicitly for both `catchments.parquet` and `graph.parquet` |
| 3 | Version bump per commit: **patch only** | Executor never minor- or major-bumps; that decision belongs to the user |
| 4 | v0.2 manifests are **hard-cut** | v0.2.1 validator accepts `"0.2.1"` only; v0.2 returns `manifest.unsupported_format_version` |
| 5 | `metadata.references_levels` violations are **ERROR** | Diagnostic ID `aux.snap.level_not_declared` |
| 6 | Weight monotonicity check is **dropped** | Replaced with `aux.snap.weight_invalid` (ERROR): finite + non-negative. Spec adds a one-line clarification that the "monotonically increasing in drainage dominance" phrase describes intent, not a structural constraint |
| 7 | `metadata.name` is **unique per dataset** | Duplicates emit `aux.snap.duplicate_name` (ERROR) |
| 8 | **One-step migration** | No intermediate 0.2.x deprecation release. v0.1 grandfathered; no active v0.2 datasets in production |
| 9 | **JSON Schema files are doc artifacts only** | The validator does not load `schemas/*.json` at runtime (verified: no `include_str!`, no `jsonschema` dep). Updating `schemas/manifest.schema.json` is a publishing concern for downstream tooling. The v0.2 hard-cut integration test exercises `check::manifest::check_format_version`, not the JSON Schema |

**Format version:** the on-disk `manifest.json::format_version` becomes `"0.2.1"`. The v0.2.1 validator accepts that value only and hard-cuts `"0.2"`.

---

## 1. Out-of-scope (do not touch in this workstream)

Filed in `adapters/grit/GRIT_HFX_SPEC_VALIDATION.md` during GRIT v1 work — **follow-on PRs, not v0.2.1**:

1. JSON-mode stdout cleanup (tracing logs interleave with the JSON payload).
2. zstd Parquet codec support in the validator binary.
3. Batch-read error flooding (one error per failing record-batch read).
4. `--strict` ergonomics for row-group warnings.

These are validator UX/robustness work, scheduled separately. Out of scope here:

- Hilbert curve parameter pinning (deferred to v0.3+).
- Adding a stored `hilbert_index` column (the rule is about ordering, not storage).
- Any minor or major version bump.
- Refactors, bug fixes, or cleanups unrelated to the listed PRs. If you notice something, file it in your final report; do not fix it in this workstream.

---

## 2. PR sequence

Each PR includes the standing per-commit patch bump (`./scripts/bump-version.sh patch`) and the corresponding git tag. **Every PR must keep `cargo test --workspace` green at its boundary.** Mid-PR commits may temporarily fail tests; PR boundaries may not.

### PR 1 — Spec text + JSON Schemas (**S–M**)

Files:

- `spec/HFX_SPEC.md`
  - Header → `**Version 0.2.1**`.
  - §1 catchments: add multi-level ordering paragraph (single-level keeps Hilbert-only rule; multi-level requires `(level ASC, hilbert_index ASC)`). Add one sentence: *"`hilbert_index` is a sort key, not a stored column; no `hilbert_index` field is present on `catchments.parquet`."*
  - §1 unit invariants: append the DAG `up_area_km2` paragraph. **Must enumerate three semantic options** — (a) partitioned by flow physics at bifurcations, (b) geometric union of upstream catchments, (c) mainstem-routed single-threaded — and **require** producers to document their choice in the manifest or accompanying README. Engines comparing across DAG datasets must reconcile semantics first.
  - §2 graph: add 4 `bbox_*` columns (float32, NOT NULL) to schema table; add a Spatial Partitioning subsection mirroring catchments' (Hilbert + 4096–8192 row groups + bbox stats); add the same `hilbert_index`-is-not-a-stored-column sentence.
  - §3 snap: **delete entire section**; replace with one-line pointer to `spec/aux/snap/v1.md`.
  - §4 outlet rules: replace "on the dataset boundary for a coastal terminus" with: *"on the polygon boundary at the coast — for regional datasets cut at the coastline, this is also the dataset boundary; for global datasets, it is the unit polygon's coastal edge."*
  - §5 manifest: `format_version` row → `"0.2.1"`; example JSON updated.
  - Artifact Summary table: remove `snap.parquet` row.
  - Validation §File Presence: remove `snap.parquet`; add legacy-snap migration note.
  - Validation §Schema and Values: extend `stem_role` enum to four values (`mainstem`, `tributary`, `distributary`, `unknown`); add finite/ordered rule for graph bbox columns; add the weight clarification sentence: *"weight values must be finite and non-negative; the 'monotonically increasing in drainage dominance' phrase describes the semantic intent of the column, not a structural constraint enforced by validators."*
  - Validation §Auxiliary Validation: add `hfx.aux.snap.v1` row.
  - Add a `## Migration from v0.2` subsection covering format_version bump, graph bbox cols, snap → aux, distributary enum, and the newly-enforced `stem_role` enum check.

- `spec/aux/snap/v1.md` *(new)* — mirror `spec/aux/d8_raster/v1.md` shape:
  - Schema ID `hfx.aux.snap.v1`.
  - Manifest declaration example.
  - Required artifact keys: `snap`.
  - Metadata block: `{name: kebab-case string, description: string, references_levels: int[], weight_semantics: string}`.
  - Parquet schema table: `id int64 NN`, `unit_id int64 NN`, `weight float32 NN`, `stem_role string nullable enum {mainstem|tributary|distributary|unknown}`, `bbox_minx/miny/maxx/maxy float32 nullable`, `geometry binary WKB NN` (Point or LineString).
  - Validation Expectations section listing each check the validator performs.
  - **MUST include a worked multi-entry example** showing two `hfx.aux.snap.v1` entries (`segment-stems` referencing level 0, `reach-stems` referencing level 1). This is the headline new capability driven by GRIT v2 and must be demonstrated, not just described.

- `schemas/manifest.schema.json`: `format_version.const` → `"0.2.1"`. *(Doc artifact only.)*

- `schemas/aux/snap.v1.schema.json` *(new)*: metadata-block JSON Schema — required `name` (pattern `^[a-z][a-z0-9-]*$`), `description`, `references_levels` (`array[integer >= 0]`, `minItems: 1`), `weight_semantics` (string). *(Doc artifact only.)*

**Definition of done**
- Markdown renders cleanly (visual scan).
- Both JSON Schemas validate with `python -c 'import json; json.load(open(...))'`.
- One commit with patch bump and tag.

---

### PR 2 — `hfx-core` type changes (**S**)

Files:

- `crates/hfx-core/src/snap.rs`
  - Add `StemRole::Distributary` with a doc comment ("branch diverging at a bifurcation").
  - Fix the misleading `StemRole::Tributary` doc comment that currently says "tributary or distributary" — they are now distinct variants.
  - Extend `Display` and `FromStr` for `"distributary"`.
  - Add unit tests: round-trip `Distributary`; inequality vs `Tributary`.

- `crates/hfx-core/src/auxiliary.rs`
  - Add `BlessedAuxSchema::SnapV1`.
  - Parse arm for `"hfx.aux.snap.v1"` in `AuxiliarySchemaId::parse`.
  - Update `Display` impl on `BlessedAuxSchema`.
  - Add unit tests next to the existing ones.

**Definition of done**
- `cargo test -p hfx-core` green.
- Patch bump + tag per commit.

---

### PR 3 — Validator: graph bbox columns + multi-level ordering (**L**)

Split into 5 commits. Every commit must compile; PR boundary must be green.

**Commit 3a — Reader: graph bbox columns**
- Lift the duplicated `row_group_has_bbox_stats` helper (currently in `crates/hfx-validator/src/reader/catchments.rs` and `crates/hfx-validator/src/reader/snap.rs:33-48`) into `crates/hfx-validator/src/reader/schema.rs` as a shared `pub(crate)` fn. Update both existing call sites.
- `crates/hfx-validator/src/reader/graph.rs::expected_columns()`: add 4 columns `bbox_minx`, `bbox_miny`, `bbox_maxx`, `bbox_maxy` (`DataType::Float32`, `nullable=false`).
- Plumb bbox values through batch reading into `Vec<[f32; 4]>` (mirror `crates/hfx-validator/src/reader/catchments.rs`).
- Capture per-row-group `bbox_stats` and `row_count` using the shared helper.
- `crates/hfx-validator/src/dataset.rs::GraphData`: add fields `bboxes: Vec<[f32; 4]>`, `row_group_sizes: Vec<usize>`, `row_group_has_bbox_stats: Vec<bool>`.

**Commit 3b — Schema checks: graph row-groups + bbox stats**
- `crates/hfx-validator/src/check/schema.rs`: add a `dataset.graph` block mirroring the existing catchments block. Diagnostic IDs:
  - `schema.graph.bbox_stats_missing` (ERROR)
  - `schema.graph.rg_size` (WARN, ERROR under `--strict`)
  - `schema.graph.rg_count` (WARN, ERROR under `--strict`)

**Commit 3c — Value checks: graph bbox finite/ordered**
- `crates/hfx-validator/src/check/ids.rs`: add `check_graph_bboxes` reusing `validate_bbox_f32` (defined for catchments around line 131). Diagnostic ID: `ids.graph_bbox`.
- Wire into `crates/hfx-validator/src/check/mod.rs::run_checks()` immediately after `ids::check_graph_ids`.

**Commit 3d — Multi-level ordering**
- New module `crates/hfx-validator/src/check/ordering.rs` with two functions:
  - `check_catchments_ordering(catchments: &CatchmentsData) -> Vec<Diagnostic>` — if catchments span more than one distinct level, levels must be non-decreasing across rows. Emit `ordering.catchments.level_unsorted` (ERROR) with first offending row index.
  - `check_graph_ordering(graph: &GraphData) -> Vec<Diagnostic>` — same rule. Diagnostic ID: `ordering.graph.level_unsorted` (ERROR).
- **Hilbert handling (per resolution #1):** register the diagnostic IDs `ordering.catchments.hilbert_unsorted` and `ordering.graph.hilbert_unsorted` in the diagnostic catalog with severity WARN, but **do not emit them** until Hilbert curve parameters are pinned in v0.3+. Document this deferred behavior in the validator README's Known Conformance Gaps table — the same row that already exists.
- Add `pub mod ordering` to `crates/hfx-validator/src/check/mod.rs`; call both functions from `run_checks()` after `check_schemas`.

**Commit 3e — Update existing fixtures to keep tests green**
- Regenerate `conformance/valid/tiny/graph.parquet` and `conformance/valid/tiny-with-aux-d8/graph.parquet` with the 4 new bbox columns. Single row-group keeps it simple (file sizes are under 4096 rows, so the existing rule applies).
- Update `conformance/generate_fixtures.py::write_graph()` to write bbox columns alongside `id`, `level`, `upstream_ids`. Bbox values per row come from the matching catchment row's bbox.
- Do **not** add new fixtures in this PR — that is PR 5's job. Only minimum updates needed to keep `cargo test --workspace` green.

**Definition of done**
- `cargo test --workspace` green at the PR boundary.
- New diagnostic IDs (`schema.graph.bbox_stats_missing`, `schema.graph.rg_size`, `schema.graph.rg_count`, `ids.graph_bbox`, `ordering.catchments.level_unsorted`, `ordering.graph.level_unsorted`) covered by unit tests using in-memory `GraphData`/`CatchmentsData` synthesis.

---

### PR 4 — Validator: `hfx.aux.snap.v1` dispatcher + core snap removal (**L**)

Split into 5 commits.

**Commit 4a — File presence + core snap removal**
- `crates/hfx-validator/src/check/file_presence.rs`: keep `snap_path` discovery; if `files.snap_path.is_some()`, emit `file_presence.legacy_snap_parquet` (ERROR) with message "snap.parquet at dataset root is a v0.2 artifact; move to hfx.aux.snap.v1".
- `crates/hfx-validator/src/reader/mod.rs`: remove the unconditional `snap::read_snap` call. Snap reads now happen only inside the aux dispatcher.
- `crates/hfx-validator/src/check/mod.rs`: remove the `dataset.snap` blocks (`ids::check_snap_data`, `geometry::check_snap_geometries`, `referential::check_snap_refs` as core calls). They will be invoked by the aux dispatcher instead.
- `crates/hfx-validator/src/dataset.rs`: keep `FilePresenceMap.snap_path` for the deprecation diagnostic. Drop `ParsedDataset.snap` (it's now per-aux-entry); the dispatcher in PR 4c will hold parsed snaps in its own scope.

**Commit 4b — Reader refactor**
- `crates/hfx-validator/src/reader/snap.rs::read_snap`: change signature to `read_snap(path: &Path, label: &str) -> (Option<SnapData>, Vec<Diagnostic>)`. Use `label` in every diagnostic message so multi-entry datasets can be diagnosed unambiguously. The diagnostic `check_id` values stay the same shape; only the message text incorporates the label.

**Commit 4c — `hfx.aux.snap.v1` dispatcher**
- `crates/hfx-validator/src/check/auxiliary.rs`: add an arm for `"hfx.aux.snap.v1"` calling a new `check_snap_v1` function.
- `check_snap_v1` orchestrates per-entry checks:
  - Schema validation via existing `expected_columns()` (no change to snap schema for aux).
  - `aux.snap.weight_invalid` (ERROR) — weight finite and non-negative.
  - `aux.snap.stem_role_invalid` (ERROR) — must be one of `{mainstem, tributary, distributary, unknown}` or null. **This is a check that did not exist in v0.2** — call out in PR 6 CHANGELOG.
  - bbox finite/ordered (reuse existing `check_snap_data` logic).
  - WKB Point/LineString (reuse existing `check_snap_geometries`).
  - Referential `unit_id → catchments` (reuse existing `check_snap_refs`).
  - `aux.snap.level_not_declared` (ERROR) — every `unit_id`'s `catchment.level` must be in `metadata.references_levels`. Reads catchment levels from `dataset.catchments`.

**Commit 4d — Multi-entry handling**
- Iterate all `hfx.aux.snap.v1` entries.
- Track `metadata.name` across entries; emit `aux.snap.duplicate_name` (ERROR) on collision.
- All diagnostic messages include the entry's `metadata.name` (or `auxiliary[idx]` if name missing).

**Commit 4e — Update existing fixtures to keep tests green**
- Ensure no existing fixture ships a core `snap.parquet`. Audit `conformance/valid/*` and `conformance/invalid/*`; current state (per `generate_fixtures.py`) has no core snap, so this should be a no-op confirmation. If any future fixture introduces one, convert to `hfx.aux.snap.v1`.
- Bump every existing fixture manifest's `format_version` from `"0.2"` to `"0.2.1"` (this is the latest moment to do it without breaking PR 4's hard-cut behavior). Update `conformance/generate_fixtures.py::write_manifest` default.
- Update the `invalid/legacy-format-version` fixture: it currently tests v0.1 → v0.2 cut, which still works since v0.1 is still rejected. Add a new sibling fixture? No — that's PR 5. Here, only update the existing manifests' `format_version` value where they should now be `"0.2.1"`.

**Definition of done**
- `cargo test --workspace` green at the PR boundary.
- New diagnostic IDs (`file_presence.legacy_snap_parquet`, `aux.snap.weight_invalid`, `aux.snap.stem_role_invalid`, `aux.snap.level_not_declared`, `aux.snap.duplicate_name`) covered by unit tests.

---

### PR 5 — Conformance fixtures: purely additive (**M**)

By the time this PR runs, PRs 3 and 4 have already kept existing fixtures green. PR 5 only adds new fixtures.

File: `conformance/generate_fixtures.py` (extend the existing script).

**New valid fixtures**
- `valid/grit-two-level`: segments at level 0 (root), reaches at level 1 (children). Graph file with bbox columns. Multi-level monotone ordering. Manifest `format_version = "0.2.1"`.
- `valid/grit-two-snap`: same dataset + two `hfx.aux.snap.v1` entries: `{name: "segment-stems", references_levels: [0]}` and `{name: "reach-stems", references_levels: [1]}`. Snap parquet files contain Point geometries referencing the appropriate level's units. **Keep this fixture realistic** — it simulates the GRIT v2 production scenario (the first downstream consumer of multi-level + multi-snap-aux).

**New invalid fixtures (one per new check class)**

| Fixture | Expected diagnostic |
|---|---|
| `invalid/graph-missing-bbox-cols` | graph schema column-missing diagnostic |
| `invalid/graph-bbox-stats-missing` | `schema.graph.bbox_stats_missing` |
| `invalid/catchments-multi-level-unsorted` | `ordering.catchments.level_unsorted` |
| `invalid/graph-level-unsorted` | `ordering.graph.level_unsorted` |
| `invalid/legacy-core-snap` | `file_presence.legacy_snap_parquet` |
| `invalid/aux-snap-bad-stem-role` (contains `"primary"`) | `aux.snap.stem_role_invalid` |
| `invalid/aux-snap-level-not-declared` | `aux.snap.level_not_declared` |
| `invalid/aux-snap-duplicate-name` | `aux.snap.duplicate_name` |
| `invalid/aux-snap-bad-geometry` (Polygon WKB) | existing snap geometry diagnostic ID |
| `invalid/aux-snap-weight-negative` | `aux.snap.weight_invalid` |
| `invalid/v02-format-version` | `manifest.unsupported_format_version` |

**Tests**
- `crates/hfx-validator/tests/conformance_fixtures.rs`: extend the existing arrays with the new fixtures.
- `crates/hfx-validator/tests/integration.rs`: add `v02_manifest_is_hard_cut` mirroring the existing `legacy_v01_manifest_is_hard_cut`.

**Definition of done**
- `uv run conformance/generate_fixtures.py` runs cleanly and writes all fixtures.
- `cargo test --workspace` green with the new fixtures.

---

### PR 6 — Version bump finalization + CHANGELOG + tag (**S**)

- Confirm `Cargo.toml` workspace version reflects all incremental patch bumps from PRs 1–5.
- `crates/hfx-core/CHANGELOG.md`: add v0.2.1 entries for `StemRole::Distributary` and `BlessedAuxSchema::SnapV1`.
- `crates/hfx-validator/README.md`:
  - Keep the Hilbert sort row in "Known Conformance Gaps" (still deferred per resolution #1).
  - Add a "v0.2.1 changes" section summarizing: graph bbox columns, multi-level ordering, `hfx.aux.snap.v1`, core snap removal, distributary stem role, newly-enforced `stem_role` enum (previously unchecked in v0.2).
- Commit message: `feat(spec): release HFX v0.2.1 (graph bbox, snap aux, distributary)`.
- Final `./scripts/bump-version.sh patch` and `git tag` per the standing rule. **Do not push.**

---

## 3. Quality gates (applies to every PR)

- `cargo build --workspace` clean.
- `cargo test --workspace` green at every PR boundary.
- `cargo clippy --workspace --all-targets -- -D warnings` clean (if not already enforced, do not introduce new warnings).
- `cargo fmt --check` clean.
- Patch bump applied to root `Cargo.toml` in the same commit as code changes; `hfx-core` version pin in `crates/hfx-validator/Cargo.toml` stays in sync via `./scripts/bump-version.sh`.
- Git tag created after every commit (`v$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')`). **Tags are not pushed.**
- New diagnostic IDs all appear in at least one fixture-driven integration test by the end of PR 5.

---

## 4. Riskiest items

1. **PR 3 (graph bbox + ordering)** — touches reader, schema check, value check, new ordering check, plus existing fixture regeneration. Split into 5 commits per §2; the fixture commit (3e) is what keeps the PR boundary green.
2. **PR 4 (aux-snap dispatcher)** — depends on PR 3 multi-level support; refactors `read_snap` signature; introduces 5 new diagnostic IDs; restructures `ParsedDataset` to drop the core `snap` field. Sequence strictly after PR 3.
3. **Stealth fix: `stem_role` enum was never enforced in v0.2.** Adding the enum check in v0.2.1 may turn previously-passing v0.2 datasets non-conformant once they re-emit as v0.2.1. Call out explicitly in PR 6 CHANGELOG.

---

## 5. Reporting format the executor must use

After each PR (or stacked commit group), produce a short report in this exact shape:

```
## PR <N>: <title>

**Changed:**
- file:line — what changed and why

**Tests added:**
- test_name in tests/<file>.rs — what it exercises

**Diagnostic IDs introduced:**
- diag.id (severity) — when it fires

**Deviations from plan (if any):**
- <what you did differently and why>

**Verification:**
- cargo test output summary
- manual smoke (if applicable)
```

Final report after PR 6: list of git tags created (not pushed), summary of cumulative diagnostic IDs added, and any open items the coordinator should know about (including any out-of-scope friction observed but not fixed).

---

## 6. Escalation protocol

If you hit any of the following, **STOP and report back to the coordinator** — do not improvise:

- A spec ambiguity not covered by the 9 resolved decisions in §0.
- A test failure that would require changing a decision (e.g., a fixture that can't be made valid under the level-monotonicity ERROR rule).
- A `cargo` build failure that you can't resolve without changing an interface the plan didn't mention.
- A discovery that an existing v0.2 behavior was buggy and your fix would broaden the scope of v0.2.1.
- Any question of the form "should I also fix X while I'm here?" — answer: no, file it, keep moving.

Format an escalation as:

> **PAUSE:** \<one-sentence question\>.
> **Context:** \<2–3 sentences\>.
> **Options I see:** A) ..., B) ..., C) ...

Then wait for coordinator response. Do not proceed past the blocking question.

---

## 7. Suggested order of operations

1. Read `CLAUDE.md`, `spec/HFX_SPEC.md`, `spec/aux/d8_raster/v1.md`, `crates/hfx-validator/README.md` end-to-end before touching anything.
2. Re-read this plan in full.
3. Run `cargo test --workspace` on `main` to confirm a green baseline.
4. PR 1 → PR 2 → PR 3 (5 commits) → PR 4 (5 commits) → PR 5 → PR 6.
5. Report after each PR per §5.
