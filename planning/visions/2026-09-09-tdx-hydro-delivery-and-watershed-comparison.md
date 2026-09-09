# TDX-Hydro delivery and watershed comparison
Program: https://github.com/CooperBigFoot/hfx/issues/103
Effort: https://github.com/CooperBigFoot/hfx/issues/108

## Outcome

Deliver the preserved 62-basin TDX-Hydro HFX dataset privately under `s3://pourpoint-hfx/hfx/`, prove its integrity and actual consumption, and remove approved unnecessary bucket data. Demonstrate consumption on the maintainer's laptop with a real watershed delineation at latitude **47.5596**, longitude **7.5890**. Delineate the same point using GRIT and produce a Cartopy figure overlaying the two resulting watersheds.

This Effort includes operational delivery and an inspectable comparison, followed by approved bucket cleanup. Creating this vision authorizes none of those implementation actions by itself.

## Selected artifact and evidence

Use the already compiled, strict-validated extension preserved at:

`s3://pourpoint-hfx/scratch/tdx-hydro-seven-basin-extension/extension-hfx-v0-3-0/dataset/`

Recorded identity:

- 62 of 62 processing basins; 15,936,428 drainage units.
- Single-level, vector-only HFX `format_version` 0.3.0, EPSG:4326, tree topology.
- `fabric_name` tdx_hydro; `fabric_version` NGA-TDX-Hydro-20230126.
- No `region`; bbox `[-180,-90,180,90]`.
- `hfx.aux.snap.v2` stems; no ancillary rasters or D8 refinement data.
- Six preserved objects totaling 142,657,755,797 bytes: `manifest.json`, `catchments.parquet`, `graph.parquet`, `aux/snap_stems.parquet`, `NOTICE`, and `CITATION.txt`.
- Manifest SHA-256 `af443be357742550ea76ef774b83a1f86e683ea828bb796c9ca893425777be85`.

The target-merged authority is `scripts/hetzner/CAMPAIGN-tdx-hydro-extension.{md,json}` and `scripts/hetzner/evidence/tdx-hydro-extension/`, audited at HFX commit `bb1bbe458cb57ab3370525334c7bfed4b6134ceb`. Read Effort #195's delivery and landed comments together with these files. Effort #107 establishes only the older 55-basin baseline.

Strict whole-dataset validation completed on 2026-09-07 with native assembly exit zero. Its successful raw validator stdout was not preserved; the final assembly state, native launch log, and reviewed execution path establish the pass. The later campaign driver exited 126 while generating its final record. The bucket's operational assembly-state snapshot still says running; the tracked final state supersedes it. Preserve these distinctions rather than manufacturing missing historical records.

The frozen 55-basin portion retains historical outlet conventions for near-degenerate reaches and isolated roots. The seven added basins use the corrected conventions. Maintainer adjudication in `scripts/hetzner/seven-basin-control-adjudication.json` accepted 7,192 changed outlets in one control basin with unchanged topology and polygons. That control count does not estimate errors across the full baseline. Carry the accepted limitation into dataset documentation and comparison interpretation. No rebuild or further outlet correction is part of this Effort.

## Delivery contract

Promote the preserved artifact to a distinct, stable dataset prefix under `hfx/`, preserving all manifest-relative paths. Use storage-to-storage promotion where supported. The historical build VM and attached volume are already deleted; the old upload-from-build-VM sketch is superseded. Existing campaign publication tooling targets scratch only and is not a ready-made final publication contract.

Before mutation, inspect the current destination and establish exact object identities. Preserve unrelated prefixes and refuse unintended overwrite. Keep incomplete delivery distinguishable from an accepted dataset. Document the final dataset URI, endpoint configuration, artifact identity, object inventory, and reproducible verification commands without credential values.

Verify destination content against retained per-object SHA-256 evidence. Names, sizes, multipart ETags, or one successful range request alone do not prove full content integrity. Select a supported verification mechanism that establishes the complete delivered payload matches the preserved validated artifact. Record its actual guarantees. Retain source preservation until destination verification, real delineation, comparison, and approved cleanup prerequisites succeed.

Include NOTICE, CITATION, and a README describing source provenance, coverage, modifications, validation evidence, the mixed outlet conventions, and how to consume the data. Retain the TDX-Hydro CC BY-SA 4.0 attribution and license obligations; toolkit MIT/Apache licensing does not replace dataset licensing. Keep access private. Public hosting, bucket-wide policy changes, and announcements are outside scope.

## Real consumer comparison

The authoritative consumer repository is the sibling `../pourpoint`, regardless of the older pourpoint-web-app name in the ticket. Pin and record one exact consumer build for both delineations. The current source and released Python wheels may differ; do not silently mix versions. The application wrapper and deployment are outside scope.

Inputs:

- TDX-Hydro: the final verified private `s3://pourpoint-hfx/hfx/<dataset-id>` destination.
- GRIT: `https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/`.
- Requested location for both: latitude 47.5596, longitude 7.5890.

Run on the maintainer's laptop. Discovery observed 64 GiB RAM and about 600 GiB free disk, which supports an attempt but does not establish peak memory or completion time. This is explicit permission for local consumer delineation and plotting, superseding the earlier proposed verification VM. Heavy source processing and planetary rebuilding remain outside scope. No paid cloud compute lifecycle is authorized.

The current reader supports S3-compatible storage through its S3 URI path and endpoint configuration. The exact public GRIT HTTPS host is separately supported without credentials. Avoid leaking private credentials into public requests. Remote reads avoid requiring a full 143 GB download, but full session startup loads graph and reference data. The requested Basel location may produce a large watershed. Monitor runtime, memory, disk, and transfers; do not treat the Parquet cache size as a total memory cap. Run the engines sequentially in separate processes and preserve their outputs before plotting so both planetary sessions need not coexist in memory. Recheck local capacity before execution and stop for an operator decision if safe resource bounds cannot be maintained.

Use normal best-effort delineation behavior with the same consumer settings for both fabrics. GRIT may use its D8 refinement; TDX-Hydro has none available. Record actual refinement status and skip reason where supported, plus the snapping policy. Do not fabricate refinement for TDX-Hydro or silently disable GRIT refinement. A vector-only third run is not required by this vision.

Preserve each complete resulting watershed geometry and machine-readable metadata: exact dataset/build identity, input point, resolved outlet, refined outlet if present, terminal unit identity, upstream count, reported area, actual refinement outcome, timing, and relevant diagnostics. Current bindings expose geometry WKB and GeoJSON; record the actual API behavior of the pinned build.

Produce an inspectable Cartopy overlay with:

- both full watershed boundaries, distinguishable styling, and a legend;
- the requested point and each resolved outlet, with refined outlets when distinct;
- a suitable map extent and correctly declared coordinate transforms;
- each dataset's refinement status and source attribution, including GRIT's CC BY-NC terms;
- a reproducible script and saved figure, plus the underlying watershed outputs.

Report each watershed's area and spatial overlap/difference using a suitable area method, stating that method and units. Compare the reported engine areas separately if their definition differs from geometric area. Do not compute area directly in longitude/latitude degrees.

The figure and metrics show the total observed difference between normal TDX-Hydro and GRIT outputs. Their source fabrics, snapping results, and available refinement differ, so this two-run comparison cannot isolate refinement's causal contribution. Neither result is a ground-truth oracle. No numeric equality or predetermined area threshold is required. Inspect nonempty valid geometry, outlet resolution, and successful real traversal rather than accepting a plot produced from a failed or substituted run.

If either delineation fails, stop and ask Nicolas Lazaro how to proceed. Preserve the failure evidence. Do not automatically repair the engine, retry with changed semantics, provision compute, or substitute a simpler point. Consumer failure leaves #108's acceptance incomplete.

## Bucket cleanup and retention

Cleanup is an explicit #108 objective. This confirmed retention decision supersedes earlier unconditional preservation only for the bucket objects covered by a later exact deletion approval.

The approximately 143 GB selected artifact is distinct from total bucket usage. The discovery screenshot showed 467.67 GB and 560 objects. Historical non-overlapping receipt groups account for about 380.84 GB: selected extension 142.66 GB, frozen baseline 114.06 GB, source corpus 84.10 GB, per-basin outputs 31.44 GB, and controls 8.58 GB. These are historical summaries, not a complete current bucket inventory. Do not assume the remaining objects are disposable or all exact duplicates.

After successful destination verification and the real comparison:

1. Produce a current prefix-by-prefix inventory with exact keys, counts, sizes, purposes, and any known consumers. Identify the final dataset and small records to retain.
2. Propose deletion of superseded TDX-Hydro datasets, source copies, scratch copies, controls, and intermediate outputs that are unnecessary after accepted delivery.
3. Retain the selected final dataset, attribution, and small provenance, checksum, validation, delivery, and comparison records in documented durable locations.
4. Present the exact deletion inventory to Nicolas Lazaro and obtain explicit approval before any deletion. Treat unrelated datasets separately; unknown purpose or unknown consumers do not establish safe deletion.
5. Delete only approved objects, rechecking their identity and protecting the retained set. Record actual deletion results and a post-cleanup inventory and byte count. Verify that the final dataset and retained records remain accessible.

This scope concerns the bucket. Local historical evidence, Git refs, and unrelated cloud resources are not cleanup targets. No deletion occurs merely because the user accepted this retention policy. Stop on changed inventory or ambiguity rather than expanding approval. Do not claim cleanup complete while the approval or deletion remains pending.

## Completion evidence and boundaries

Completion requires reviewed, durable evidence of final object identity and integrity, successful real destination consumption at the specified point, the GRIT comparison and Cartopy figure, actual refinement disclosure, exact cleanup approval and results, and a verified retained dataset after cleanup. Distinguish historical validation evidence from new delivery checks. Record the actual final size rather than assuming the six-object starting size is unchanged after adding documentation.

No HFX spec/version changes, source reacquisition, planetary rebuild, streaming-validator redesign, public release, web-app rollout, automatic consumer repair, paid verification VM, or unapproved deletion is authorized. If the chosen execution path cannot satisfy this outcome within these boundaries, stop with evidence and ask the maintainer for direction.
