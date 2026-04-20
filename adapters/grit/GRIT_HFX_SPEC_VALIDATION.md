# GRIT Europe HFX Spec Validation

This file is a running log of concrete findings while wrangling the Europe `EPSG:4326` GRIT slice into HFX.

## Outcome

- A strictly valid HFX dataset was produced at `/tmp/grit-hfx-eu/hfx`.
- Validation command:
  - `cargo run -p hfx-validator -- /tmp/grit-hfx-eu/hfx --format text --strict --sample-pct 100`
- Final text-mode result:
  - `0 error(s), 0 warning(s), 0 info(s)`
  - `Result: VALID`
- The current working dataset uses:
  - `catchments.parquet` from GRIT `segment_catchments`
  - `graph.arrow` from GRIT segment `upstream_line_ids`
  - `snap.parquet` from GRIT segment lines as a pragmatic fallback

## Source Data Facts Confirmed

- Europe segment lines: `150,325`
- Europe segment catchments: `150,325`
- Europe reach lines: `1,922,187`
- `segment_catchments.global_id == segments.global_id`
- Every parsed segment `upstream_line_id` resolves within the Europe segment-catchment ID set
- The Europe reach `lines` layer does **not** contain `is_mainstem`
- The Europe reach `lines` layer exposes `drainage_area_in` / `drainage_area_out` columns in schema, but they are null for all `1,922,187` rows in this slice

## Confirmed Validator / CLI Bugs

### 1. Parquet codec support mismatch

- Reproduced on `2026-04-13`
- Trigger: validate a dataset written with `zstd`-compressed Parquet
- Observed behavior:
  - `hfx-validator` emits repeated `catchments.batch_read` / `snap.batch_read` errors:
    - `Parquet error: Disabled feature at compile time: zstd`
  - The validator then continues and emits many downstream referential errors caused by the failed reads.
- Why this is a validator issue:
  - The HFX spec does not currently constrain the Parquet compression codec.
  - A dataset can be structurally valid yet unreadable by the validator because the validator binary was compiled without the needed codec.
- Consequence:
  - Validation output becomes noisy and misleading; one environmental/runtime capability problem cascades into hundreds of secondary diagnostics.

### 2. JSON mode is not clean JSON

- Reproduced on `2026-04-13`
- Command:
  - `cargo run -p hfx-validator -- /tmp/grit-hfx-eu/hfx --format json --strict`
- Observed behavior:
  - tracing logs and cargo run chatter are emitted ahead of the JSON payload on stdout
  - the captured output is therefore not directly parseable as JSON
- Why this is a validator/CLI issue:
  - `--format json` implies machine-readable stdout
  - mixing logs with the JSON payload breaks downstream automation
- Example captured output:
  - first line is a tracing info log
  - JSON begins on the second line

### 3. Batch-read error flooding

- Reproduced on `2026-04-13`
- Trigger: any Parquet read failure repeated across row groups
- Observed behavior:
  - the validator emits one error per failing record-batch read
  - this can generate hundreds of nearly identical diagnostics for a single root cause
- Consequence:
  - the primary failure is obscured
  - downstream diagnostics become harder to trust during triage

### 4. `--strict` turns advisory row-group messages into hard failures

- Reproduced on `2026-04-13`
- Observed behavior:
  - `schema.*.rg_size` diagnostics are warnings in normal mode
  - `--strict` upgrades them into blocking failures
- Why this matters:
  - in practice, row-group sizing becomes part of required producer behavior for any “strictly valid” dataset
  - that is fine if intentional, but should be understood as a de facto hard requirement

## Confirmed Spec / Data Friction

### 1. Snap bbox strictness is too rigid for line features

- Reproduced on segment-line snap targets from GRIT Europe
- Observed behavior:
  - many valid horizontal or vertical `LineString` features have `minx == maxx` or `miny == maxy`
  - the validator flags these as invalid because the spec currently requires strict inequality on all bbox axes
- Why this matters:
  - zero-width or zero-height bbox extents are normal for valid line geometries
  - the current bbox rule fits polygons better than lines
- Workaround required here:
  - artificially pad degenerate line bboxes by a small epsilon before writing `snap.parquet`
- **Resolved in v0.2:** spec updated to allow `<=` (non-strict inequality) for snap bboxes, making padding a recommended producer courtesy rather than a required workaround. The adapter retains `inflate_degenerate_bounds()` as belt-and-suspenders.

### 2. `up_area_km2` is underspecified for bifurcating DAG fabrics

- GRIT publishes drainage-area attributes that are partitioned at bifurcations.
- HFX v0.1 says `up_area_km2` is total upstream drainage area.
- Those are not the same thing for GRIT.
- Consequence:
  - the safe mapping for this exercise was `up_area_km2 = null` and `has_up_area = false`
  - the spec should say more explicitly how DAG fabrics with partitioned area attributes are expected to map into HFX

### 3. GRIT snap guidance in the spec is more optimistic than the source layers

- The problem is not that GRIT cannot support reach-based snap.
- The problem is that reach-based snap is not a direct one-layer field mapping in this slice.
- A reach line knows its parent `segment_id`, but the reach row itself does not carry the metadata needed for HFX snap output.
- To build reach-based `snap.parquet`, the adapter must:
  - read reach lines
  - read segment lines
  - join each reach to its parent segment through `segment_id`
  - inherit `is_mainstem` and a snap weight from the segment layer
- So the missing piece is adapter join logic, not missing global capability in GRIT.
- The spec text currently implies GRIT snap targets should come from reach segments with `weight = drainage_area_km2` and `is_mainstem` from the GRIT mainstem flag.
- In the Europe reach layer:
  - `is_mainstem` is absent
  - reach drainage-area fields are null in this slice
- Consequence:
  - a reach-based snap adapter needs cross-layer joins and inherited metadata, not a direct copy from one layer
  - for the working validation pass, segment lines were used as snap targets instead
- **Resolved in v0.2 (partially):** the `weight` field contract was tightened from a hint ("typically upstream area") to a MUST: weight MUST be monotonically increasing in drainage dominance. The segment-line `drainage_area_out` mapping is now explicitly conformant, not merely typical. The default engine snap strategy also flipped to weight-first cascade (filter → rank by weight → mainstem tie-break → distance tie-break → id tie-break), replacing the old distance-first tiered ranking. The data gap in the Europe reach layer (null drainage-area, missing `is_mainstem`) remains an open item requiring cross-layer join work.

### 4. Hilbert sort is required in prose but under-specified operationally

- The spec requires Hilbert sorting by centroid coordinates.
- The validator does not currently enforce it.
- Practical friction:
  - the spec does not define the exact Hilbert level/curve parameters
  - centroid calculation on geographic geometries raises implementation warnings in common Python geometry stacks
- Consequence:
  - different adapters could plausibly choose different Hilbert implementations yet all claim conformance

### 5. Manifest bbox comparisons are sensitive to floating-point detail

- A manifest bbox derived directly from source geometry bounds failed enclosure checks by tiny margins.
- Workaround required here:
  - pad the manifest bbox outward by a small epsilon before writing
- Consequence:
  - the spec is simple, but in practice producers need explicit outward-padding behavior to avoid false negatives from float rounding

## Working Notes

- Current pragmatic snap fallback is segment lines rather than reach lines so the exercise can reach end-to-end validation faster and still fully exercise the validator.
- The graph ID mapping itself has been checked directly against GRIT source data:
  - `segments.global_id == segment_catchments.global_id`
  - parsed `upstream_line_ids` all resolve within the Europe segment-catchment ID set
- The first successful strict validation required these producer-side accommodations:
  - write Parquet without `zstd`
  - balance row groups so every group stays within the validator’s strict range
  - pad degenerate line bboxes in `snap.parquet`
  - pad the manifest bbox outward slightly
