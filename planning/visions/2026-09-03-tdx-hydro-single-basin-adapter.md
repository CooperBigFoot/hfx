# Vision: TDX-Hydro single-basin adapter
Program: https://github.com/CooperBigFoot/hfx/issues/103
Effort: https://github.com/CooperBigFoot/hfx/issues/106
Status: completed

Historical outcome: Landed and closed on 2026-07-23. The conformant adapter and strict-validated 331,263-unit basin 7020000010 pilot were delivered.

## Goal / Why

Prove the TDX-Hydro → HFX compile end to end on one real processing basin.
Program #103 delivers a planetary HFX v0.3.0 dataset from pristine NGA
TDX-Hydro; before the 62-basin build (#107) can run, one adapter must exist
that turns a processing basin's two GeoPackages (`basins` + `streamnet`) into a
validated single-level HFX dataset with snap stems, with every source
convention (IDs, topology, outlets, areas, snap weights) pinned and verified
rather than assumed. Success makes the pilot artifact byte-compatible with the
eventual planetary merge, so the planetary build is repetition, not new design.

## Scope — In

1. A new `adapters/tdx-hydro/` Python adapter following the existing adapter
   layout (`build_adapter.py`, tests, `README.md`, `pyproject.toml` + `uv.lock`),
   compiling one processing basin's `basins` + `streamnet` GeoPackages into an
   HFX v0.3.0 dataset: `catchments.parquet`, `graph.parquet`, `manifest.json`,
   and an `hfx.aux.snap.v2` snap artifact.
2. Acquisition of pilot basin `7020000010`'s two GeoPackages from
   earth-info.nga.mil onto the campaign VM
   (`https://earth-info.nga.mil/php/download.php?file=<basin>-<product>-gpkg`,
   no range support, per-file sequential streams).
3. Global LINKNO unit IDs: `id = LINKNO + header_number × 10_000_000` via a
   header crosswalk vendored into the repo (one-time provenance fetch from the
   GEOGLOWS `tdx_header_numbers.json`, then committed). `-1` sentinels are
   never transformed. Applies to `id`, graph edges, and snap `unit_id`.
4. Level-0-only units from polygon-bearing streamnet links (join
   `basins.streamID` = `streamnet.LINKNO`); `parent_id` null everywhere.
5. Same-level graph from `LINKNO → DSLINKNO`, with edges contracted through
   polygon-less links to the first polygon-bearing downstream link; a chain
   ending at `-1` makes the unit a root. Contraction counts reported.
6. Unit outlets as the downstream endpoint of each reach linestring, with
   linestring orientation proven per compile by endpoint-coincidence (within
   tolerance) against the `DSLINKNO` reach; failure of the coincidence check is
   a fatal build error.
7. `up_area_km2 = DSContArea` (inclusive), with the source unit (m² vs km²)
   verified empirically against summed catchment-polygon areas at build time.
8. Snap stems compiled from polygon-bearing reaches only: `weight = DSContArea`
   in km², `weight_semantics` documenting it, `stem_role` null everywhere,
   polygon-less reaches dropped with counts reported.
9. Diagnostics: structured log warnings plus a per-basin machine-readable build
   report written outside the dataset root (contraction count, dropped-reach
   count, clamp counts, empirical unit check result, orientation check result).
10. Adapter `README.md` documenting every pinned convention above.
11. A campaign run on the ephemeral fsn1 VM: acquire, compile, `hfx validate`
    (region = `7020000010`), then copy the validated dataset to the
    `pourpoint-hfx` bucket under a scratch prefix (outside `hfx/`) before
    teardown.
12. Local unit tests on small synthetic fixtures exercising the ID transform,
    contraction, orientation check, and snap compilation.

## Scope — Out (explicit non-goals)

- The planetary 62-basin build and merge (#107) and delivery to the
  consumer-facing `hfx/` prefix (#108).
- Multi-level / zoom-tier aggregation; TDX ships one resolution tier and any
  coarser tier would be fabricated.
- Raster auxiliaries (flow direction, accumulation, DEM); the public NGA
  distribution is vector-only.
- Any MNSI/mainstem derivation or other fabricated attributes; `stem_role`
  stays null.
- SPEC-track changes; the compile targets `format_version` 0.3.0 as-is.
- GEOGLOWS v2 data as a source fabric (numbering crosswalk only, per ADR).
- pourpoint-web-app integration beyond the scratch-prefix inspection copy.
- Full-basin processing on the local machine.

## Constraints

- Pristine NGA TDX-Hydro is the source fabric
  (`docs/decisions/2026-07-21-tdx-hydro-pristine-nga-source.md`);
  `fabric_name`/`fabric_version` identify NGA's product.
- Global LINKNO IDs per
  `docs/decisions/2026-07-21-tdx-hydro-global-linkno-unit-ids.md`; the
  crosswalk is vendored, never fetched at build time.
- All full-basin wrangling runs on an ephemeral Hetzner fsn1 campaign VM via
  the committed `scripts/hetzner` lifecycle; nothing durable survives on the VM
  (Map #103). NGA endpoint has no range support, ~1 MB/s per stream.
- Hetzner dedicated-core quota caps usable VM size at ccx33 (8 cores) until a
  console quota raise.
- Existing adapter discipline applies: coordinate-domain clamp at load
  boundaries (`docs/decisions/2026-07-17-adapter-coordinate-domain-clamp.md`),
  regional manifest bbox as float32 covering union
  (`docs/decisions/2026-07-16-regional-manifest-bbox-float32-covering.md`),
  Hilbert row ordering, no spatial fallbacks for failed joins.
- Diagnostics stay out of the user-facing dataset: log warnings plus a sidecar
  build report outside the dataset root, following the clamp-warning and
  `adapters/hydrobasins` sidecar precedents.
- Ship NOTICE/CITATION per the TDX-Hydro Data License alongside the dataset,
  following the `hosting/` precedent.
- Glossary language in `CONTEXT.md` is canonical: "drainage unit",
  "TDX-Hydro reach catchment", "TDX-Hydro processing basin", "Global LINKNO".

## Acceptance criteria (vision-level "done")

1. `hfx validate` exits zero on the compiled dataset for processing basin
   `7020000010` with `region = "7020000010"`, including the `hfx.aux.snap.v2`
   declaration.
2. Every unit `id` is a global LINKNO; the vendored crosswalk file is committed
   with its provenance documented.
3. The graph is a forest under `topology = "tree"`: every unit has at most one
   downstream edge, verified during the build, with contraction applied and
   counted.
4. The orientation coincidence check and the empirical `DSContArea` unit check
   both ran and passed, and their results appear in the build report.
5. The build report records contraction, dropped-reach, and clamp counts for
   the pilot basin.
6. The validated dataset exists in the `pourpoint-hfx` bucket under a scratch
   prefix outside `hfx/`, and the campaign VM was torn down afterward.
7. Local unit tests pass without touching full-basin data.
8. The adapter README documents all pinned conventions (IDs, contraction,
   outlets, `up_area_km2`, snap weight/`stem_role`, diagnostics).

## Decomposition hints

- Risky-first: vendor the crosswalk and build the streamnet graph model
  (ID transform, contraction, orientation check) against synthetic fixtures
  before touching real data; these encode every novel convention.
- The catchments/graph/manifest compile path can largely follow
  `adapters/merit-v2/build_adapter.py` (single-level, v0.3.0, Hilbert sort,
  float32 bbox covering); reuse its structure rather than inventing one.
- Snap compilation is separable from the core compile and can land as its own
  step once unit IDs are stable.
- The campaign run (acquire → compile → validate → scratch upload → teardown)
  is the last slice and reuses the drill-proven `scripts/hetzner` lifecycle;
  keep it thin by making the adapter a single CLI invocation on the VM.
- Acquisition of the `basins` GeoPackage for `7020000010` is unproven (only
  `streamnet` was drilled); fetch it early in the campaign slice.

## Open questions / risks

- Exact `fabric_version` string: pinned from NGA source metadata during
  implementation; the NGA distribution's version labeling has not been
  inspected yet.
- `topology = "tree"` is assumed from TauDEM convergence and must be verified
  at build time; a discovered bifurcation would force a `dag` declaration and
  revisiting the up-area convention note.
- The magnitude of the polygon-less link set for basin `7020000010` is unknown;
  if contraction turns out to rewire a large fraction of edges, the convention
  deserves a second look before the planetary build.
- `DSContArea` unit (m² vs km²) is unverified until the empirical check runs.
- Whether the GEOGLOWS header crosswalk covers all 62 processing-basin IDs
  exactly as NGA names them; verified when vendoring.
- NGA endpoint reliability for the `basins` product file size and transfer time
  is extrapolated from the `streamnet` drill, not measured.
