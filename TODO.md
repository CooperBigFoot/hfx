# HFX TODO

Strategic direction (settled 2026-04-22): **one global HFX dataset per fabric, served from object storage (R2), with cloud-native lazy reads via HTTP byte ranges.** shed reads from local paths or remote URLs through a single code path (`object_store` Rust crate).

## Settled decisions

- **Graph file**: keep Arrow IPC. ~56 MB for global MERIT-Basins; tolerable as a one-time download per dataset version, cached locally. No spec change.
- **Rasters**: build one global COG mosaic per fabric for `flow_dir.tif` and `flow_acc.tif`. Engine refinement keeps working. Storage cost handled upstream.
- **`region` manifest field**: leave as-is (informational free-form string). Global datasets can omit it; partial-fabric adapters keep using it.
- **Byte-range derisk** (verified at source level, agents + spot-check):
  - GDAL on COG over `/vsis3/` issues `Range:` HTTP requests for only intersecting tiles. R2 supported via `AWS_S3_ENDPOINT` + `AWS_VIRTUAL_HOSTING=FALSE`. (`port/cpl_vsil_curl.cpp:2017`, `frmts/gtiff/gtiffdataset_read.cpp:6780`, `port/cpl_aws.cpp:2321,2392`.)
  - Rust `parquet` `ParquetObjectReader::with_row_groups` against `AmazonS3` (R2) issues range requests for only selected row groups, ~1–5 MB per query. (`parquet/src/arrow/async_reader/store.rs:190`, `object_store/src/util.rs` 1 MiB coalesce default.)
- **MERIT global merge is mechanically safe**: zero cross-basin `NextDownID` references in pfaf-27 (1,973 atoms) or pfaf-31 (81,443 atoms). Structural reason: Pfaf-L2 boundaries are drainage divides; COMID prefix encodes basin code; concatenation requires no ID translation.

## Settled facts that anchor the plan

- Global MERIT-Basins ≈ 2.94 M atoms (Lin et al. 2019). Vector artifacts ≈ 3.7 GB on disk; `graph.arrow` ≈ 56 MB uncompressed.
- pfaf-27 raster footprint (mghydro per-basin rehost): `flowdir27.tif` 8.2 MB, `accum27.tif` 29 MB. Global mosaic size needs measurement on a larger basin or sum of all 61 mghydro files; not yet pinned down precisely.
- shed peak RSS for one local outlet on GRIT-EU: 271 MB (CLI), 419 MB (Python). Caused by redundant in-memory copies in the assembly path. See `../shed/scratchpad/bugs/20260416-02-memory-footprint.md`.

---

## Phase 1 — Document the deployment shape in the spec

Goal: state in `spec/HFX_SPEC.md` that the canonical deployment is one global HFX dataset per fabric, served from object storage. No breaking format changes.

Deliverables:
- Add a "Deployment patterns" section to the spec describing global vs partial-fabric datasets and the role of the `region` field.
- Add a sentence to the manifest section noting that `bbox` for a global dataset spans the full source-fabric extent (effectively planet-wide for global builds).
- No code changes, no validator changes, no adapter changes.

## Phase 2 — Build the first global MERIT-Basins HFX dataset ✓ COMPLETE

Goal: produce the first real global HFX dataset (vector + raster) and validate it.

Deliverables — all done:
- Ran `adapters/merit/build_adapter.py` for all 60 reachable per-basin datasets via `run_missing_basins.py`.
- Wrote `adapters/merit/merge_basins.py`: concatenates per-basin `catchments.parquet`, re-Hilbert sorts across the global table, concatenates `graph.arrow`, writes a global `manifest.json`.
- Built the global flow_dir + flow_acc COG mosaic via `adapters/merit/build_global_rasters.py` (polygon-masking before VRT-stitching to handle bbox-overlap between adjacent basins).
- Validated with `hfx --strict --sample-pct 100` → `Result: VALID`.

Results: 2,876,771 atoms, 60 of 61 Pfaf-L2 basins, ~65 GB total. Dataset at `~/Desktop/merit-hfx/global/hfx/`. pfaf-35 (anti-meridian), pfaf-87, pfaf-88 (Antarctic) excluded — see parking lot.

## Phase 3 — Shed cloud-native + memory fix

Goal: deliver the user-visible "batteries-included" experience — `pip install pyshed`, point at a local path or R2 URL, get a delineation back without giant downloads or large RSS spikes.

Two independent workstreams that can run in parallel:

**3a. object_store integration**
- Replace `std::fs::File::open` paths in `../shed/crates/core/src/session.rs` and `crates/core/src/reader/*.rs` with an `object_store::ObjectStore`-backed reader. Construct `LocalFileSystem` for `file://` and local paths, `AmazonS3` for `s3://` and R2 URLs.
- Switch the catchments Parquet reader from sync `std::fs::File` to `ParquetObjectReader` + `ParquetRecordBatchStreamBuilder`.
- Switch GDAL raster opens to use `/vsis3/` paths when the dataset URL is remote, with the R2 config options (`AWS_S3_ENDPOINT`, `AWS_VIRTUAL_HOSTING=FALSE`, etc.).
- Add a download cache for `manifest.json` and `graph.arrow` at `~/.cache/hfx/<fabric_name>/<adapter_version>/`. ~56 MB graph download once per dataset version per machine.
- 10-minute smoke tests with `RUST_LOG=object_store=trace` (Parquet) and mitmproxy (GDAL) against a real R2 bucket to confirm bytes-on-wire match expectations.

**3b. Assembly memory bloat**
- Independent of cloud work. Driven by `../shed/scratchpad/bugs/20260416-02-memory-footprint.md`.
- Audit `query_by_ids`, `index_catchments_by_id`, `decode_wkb_multi_polygon`, `assemble_from_geometries` for redundant copies.
- Stream geometries through dissolve where possible.
- Add a leaner Python result path that returns WKB or area metadata without the full GeoJSON inflation.

## Phase 4 — Discovery and ship

Goal: put the global dataset on R2, address it from a user's machine, prove the whole stack end-to-end.

Deliverables:
- Pick a discovery mechanism. Cheapest: a `--dataset-url` CLI flag. Slightly nicer: a small `hfx-registry.toml` shipped with shed mapping fabric names to canonical URLs. Avoid a network-served registry until there is a reason.
- Upload Phase 2 global MERIT-Basins HFX dataset to R2.
- End-to-end smoke test: cold cache, single delineation against R2, measure bytes downloaded, peak RSS, wall time. Compare against the local-disk baseline in the memory-footprint bug doc.
- Document the deployment in shed's README.

---

## Cross-cutting / parking lot

- **`pfaf-35` anti-meridian wrap** — catchment bbox maxx=190.3° wraps past 180°E; mghydro raster is clipped to 180°E. Basin excluded from the global mosaic. Revisit when MERIT Hydro 5° source tiles or a wrap-aware raster assembly is on the table.
- **`pfaf-87` / `pfaf-88` missing from mghydro** — Antarctic sub-basins, HTTP 404 for both flowdir and accum at mghydro's directory. Vector shapefiles may still exist in the Lin et al. 2019 release; the global dataset excludes these two basins.
- **`flow_acc.tif` encoding** — the global raster is 45 GB because raw float32 upstream pixel counts compress poorly. A km² conversion (the adapter already has `_compute_area_row_km2` for per-row cosine-weighted pixel area) or int32 encoding would roughly halve the file. Revisit once a shed consumer profiles query cost.
- **GRIT filename parity** — `adapters/grit/build_grit_eu_hfx.py` doesn't match the `_template/build_adapter.py` convention that MERIT follows. Rename during the next GRIT touchup. Same review should consider folding GRIT's `WORKFLOW.md` into `README.md` for consistency with MERIT.
- **Cross-region edges silently dropped.** Largely defused by the global-dataset direction — within a single global HFX there are no regions to cross. Keep the warn-log in the adapter as a defensive check; consider promoting to a hard error so a future partial-fabric adapter doesn't ship truncated graphs without anyone noticing.
  - Adapter: `adapters/merit/build_adapter.py:647-655`
  - Engine: `../shed/crates/core/src/engine.rs` (BFS terminates at missing IDs without surfacing the truncation)
- **Future graph format**: if a fabric appears with a graph >> 200 MB (e.g., NHD HR ~30M reaches), revisit Arrow IPC vs Parquet sorted by id with row-group stats. Defer until a real user case appears.
- **Future raster sidecar catalog**: if the global mosaic build proves too painful to maintain, the sidecar tile catalog (`raster_tiles: [{bbox, url}, ...]` in manifest) is the fallback.
