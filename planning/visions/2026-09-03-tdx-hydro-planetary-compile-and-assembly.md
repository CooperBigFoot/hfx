# Vision: TDX-Hydro planetary compile and assembly
Program: https://github.com/CooperBigFoot/hfx/issues/103
Effort: https://github.com/CooperBigFoot/hfx/issues/107
Status: completed

Historical outcome: Landed and closed on 2026-08-07. The retained result was the strict-validated 12,748,154-unit partial fabric from 55 of 62 attempted basins. The remaining seven-basin gap moved to Effort #195.

## Goal / Why

Effort ticket #107 of Program #103. The `adapters/tdx-hydro` adapter compiles **one**
TDX-Hydro processing basin into a validated HFX v0.3.0 dataset, proven end-to-end on
basin `7020000010`. Program #103's Destination is **one** planetary HFX dataset over
all 62 processing basins (~16 M drainage units), delivered to `pourpoint-hfx` so the
pourpoint-web-app VM can read it by S3 byte-range request.

This vision closes the gap between those two facts: it scales the proven single-basin
compile across all 62 basins and assembles the results into one HFX dataset that
passes planetary `hfx validate`.

Success means the artifact #108 delivers exists, is validated, and truthfully
declares what it covers. Until it does, #108 is blocked and Program #103 has no
deliverable.

## Scope — In

1. **Bulk acquisition of all 62 processing basins** onto the ephemeral fsn1 build
   VM — `basins` + `streamnet` GeoPackages per basin from the pristine NGA endpoint,
   with per-file integrity evidence (bytes, SHA-256, layer name) as the pilot
   recorded for `7020000010`.
2. **Per-basin compile stage** — 62 invocations of the landed `build_adapter.py`
   compile, one per processing basin, each producing its own HFX output and its own
   JSON diagnostics report. Failure is isolated to the basin.
3. **Planetary assembly stage** — new work. Concatenates the per-basin outputs into
   one `catchments.parquet`, one `graph.parquet`, one `aux/snap_stems.parquet`, and
   one `manifest.json`, applying one dataset-wide row order and re-issuing sequential
   snap stem ids across the merged artifact.
4. **Coverage-truthful manifest** — the assembled manifest declares `region`/`bbox`
   according to the compiled coverage, per
   `docs/decisions/2026-07-23-best-effort-coverage-truthful-manifest.md`.
5. **Planetary validation** — release-mode `hfx <dataset> --strict --sample-pct 100`
   plus the adapter's GeoParquet 1.1 wrapper, both passing on the assembled dataset.
6. **VM and volume sizing** for the planetary pass, with the campaign lifecycle in
   `scripts/hetzner/` extended as needed for a multi-basin campaign.
7. **A committed campaign record** in `scripts/hetzner/`, following the
   `CAMPAIGN-tdx-hydro-7020000010.md` precedent: acquisition evidence, per-basin
   outcomes, the retained diagnostic reports, the named excluded basins, and
   teardown proof.
8. **Retention of the per-basin diagnostics** the adapter README obliges #107 to
   re-check independently for every basin: DSContArea unit selection and divergence,
   orientation proof counts, short-successor and near-degenerate resolutions,
   contraction counts, clamp counts.

## Scope — Out (explicit non-goals)

- **Delivery to the `hfx/` prefix.** That is #108. This vision ends with the
  validated dataset parked in the campaign's scratch prefix.
- **Publishing the 62 per-basin datasets.** They are campaign scaffolding; only the
  assembled dataset is an artifact.
- **Gating assembly on human diagnostic review.** The build runs unattended;
  diagnostic review is a post-delivery obligation.
- **Any diagnostic-threshold exclusion rule.** Only hard failure excludes a basin.
- **Adapter contract changes that relax fatal cases.** A reach-side ambiguity beyond
  the bounded near-degenerate rule, a non-coincident successor, or a clamp overshoot
  beyond one source cell stays fatal. A basin that trips one fails; it is not
  accommodated by widening the contract.
- **Multi-level / zoom-tier aggregation.** TDX ships one resolution tier; any coarser
  level would be fabricated (Map #103, deliberately deferred).
- **Raster auxiliaries**, SPEC-track changes, GEOGLOWS v2 as source fabric, and
  pourpoint application integration.

## Constraints

- **Build compute is the ephemeral fsn1 VM only.** No wrangling on the workstation.
  Nothing durable survives on the VM; every input is re-fetchable and every output
  re-creatable. Teardown is mandatory and must be proven.
- **Hetzner quota.** The pourpoint project's dedicated-core limit is below 16, so
  `ccx33` (8 cores) is the largest usable type and concurrent campaigns contend for
  it. The `grit-d8-m3` volume is intentionally retained and must never be swept.
- **NGA endpoint has no HTTP range support** — no resume, no segmented transfer, and
  erratic ~1–6 MB/s per connection (drill-proven, `scripts/hetzner/DRILL.md`).
  Acquisition parallelism is across basins, not within a file.
- **Scale, extrapolated from the `7020000010` pilot** (331,263 links → 2.16 GB
  catchments, 674 MB snap, 16.5 min compile, 7.6 GB input, ~16 min acquisition):
  roughly 100 GB catchments, 32 GB snap, and 13 h of compile at ~16 M units, over
  several hundred GB of GeoPackage input. Volume sizing and wall-clock planning must
  start from measurement, not from this extrapolation.
- **The Hilbert key is currently basin-local.** `build_adapter.py:1415` and `:1558`
  normalize against each basin's own `total_bounds`, so per-basin orders are not
  comparable and concatenation alone does not produce a valid planetary order. How
  the planetary order is produced is an implementation choice; that it must be one
  coherent dataset-wide order is not.
- **Global LINKNO already solves cross-basin ID uniqueness** via the vendored 62-entry
  crosswalk. Assembly must not re-derive or re-map ids.
- **Cross-basin graph stitching is neither needed nor permitted.** Processing basins
  are drainage-closed; a leaked edge already surfaces as a fatal missing downstream
  target at compile time.
- **`--report` must stay outside `--out`**, and dataset + report are staged and
  published together.
- **Ship NOTICE/CITATION** per the TDX-Hydro Data License alongside the dataset.
- **`format_version` is 0.3.0 as-is.**

## Acceptance criteria (vision-level "done")

1. All 62 processing basins were acquired and attempted; each basin's outcome is
   recorded as landed or hard-failed, with the failure reason for each exclusion.
2. One assembled HFX dataset exists containing exactly the drainage units of the
   compiled coverage — every landed basin's units present, no unit from an excluded
   basin, no duplicate ids.
3. `hfx <dataset> --strict --sample-pct 100` exits 0 in release mode on the assembled
   dataset, and the adapter's GeoParquet 1.1 validation exits 0.
4. The manifest declares coverage truthfully: `region` omitted with
   `bbox = [-180, -90, 180, 90]` if and only if all 62 basins landed; otherwise a
   `region` label and the corresponding float32 covering-union bbox.
5. Rows are in one coherent dataset-wide order, verifiable as monotonic over the
   whole file rather than only within each basin's block.
6. Snap stems form one merged `hfx.aux.snap.v2` artifact declared at
   `aux/snap_stems.parquet` with `references_levels = [0]`, sequential ids from 1..N
   across the whole artifact, and `unit_id` values that all resolve in
   `catchments.parquet`.
7. The per-basin diagnostic reports are retained and committed alongside the campaign
   record, including the per-basin DSContArea divergence and orientation counts the
   adapter README requires #107 to re-check.
8. The assembled dataset and its attribution files are verified present in the
   campaign scratch prefix, outside `hfx/`.
9. Campaign teardown completed and independent listings confirm zero footprint.

## Decomposition hints

Risky-first ordering. The two genuine unknowns are bulk acquisition against a
range-less endpoint and the planetary order at ~16 M rows; neither is de-risked by
the pilot.

1. **Multi-basin campaign lifecycle** — extend `scripts/hetzner/` for a 62-basin
   campaign: sizing, per-basin acquisition with integrity evidence, per-basin compile
   driving, resumability across interruptions. Acquisition parallelism is tuned here
   against the real endpoint, not assumed.
2. **Assembly on a small subset first** — prove the assembly stage on 2–3 basins
   before spending a full acquisition. This is where the planetary order, the merged
   snap artifact with re-issued ids, and the coverage-truthful manifest all get
   built and validated cheaply. A 3-basin assembly that passes `--strict
   --sample-pct 100` retires most of the assembly risk at a fraction of the cost.
3. **Full campaign** — acquire, compile 62, assemble, validate, park in scratch,
   record, tear down.
4. **Campaign record** — the evidence artifact, written from the campaign's own logs
   before teardown.

Implementation note: the per-basin compile is the landed #106 adapter. If the
planetary order is best produced at compile time rather than assembly time, changing
the adapter's ordering is in scope — but the compile's contract (fatal cases, ids,
areas, outlets, diagnostics) is not to be touched.

## Open questions / risks

- **Basin-specific real-data failures are expected, not exceptional.** Basin
  `7020000010` alone forced three adapter fix PRs (#141 zero-length reaches, #142
  DSContArea recalibration, #144 short-successor orientation). Sixty-one unexamined
  basins will surface cases the pilot did not. Best-effort coverage is the mitigation,
  but the count of basins that fail is genuinely unknown and directly determines
  whether the artifact is planetary or partial-fabric.
- **The planetary order at ~100 GB is the hardest technical unknown** and does not
  fit in RAM by any obvious route. This is the single
  most likely place for the vision to stall.
- **Acquisition wall-clock is unbounded by evidence.** One basin took ~16 min for
  7.6 GB; 62 basins against an endpoint with no resume, erratic throughput, and
  observed HEAD timeouts / 504s may dominate the campaign. A mid-transfer failure has
  no resume path, only a restart of that file.
- **Antimeridian and coastal-edge behavior is untested.** The pilot recorded zero
  clamp vertices on both layers, so the coordinate-domain clamp path has never
  actually fired on TDX data. Pacific basins are where it will, and an overshoot
  beyond one 0.4-arcsecond cell is fatal by contract.
- **Endorheic basins** may exercise the root/isolated-root orientation trust
  assumptions at magnitudes unlike the pilot's 1,767 trusted isolated roots.
- **`--strict --sample-pct 100` at ~100 GB** may itself be a multi-hour operation
  with unproven memory behavior at this scale.
- **Whether a bad-but-valid basin is caught at all** now depends on the post-delivery
  diagnostic review actually happening. That is an obligation with no enforcement
  point in the build; it needs an owner and a deadline outside this vision.
- **Hetzner quota contention** blocked the pilot's first two provision attempts. A
  campaign of this length is more exposed to it, not less.
