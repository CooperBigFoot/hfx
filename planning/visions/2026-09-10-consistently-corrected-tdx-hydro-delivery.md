# Consistently corrected TDX-Hydro delivery
Program: https://github.com/CooperBigFoot/hfx/issues/103
Effort: https://github.com/CooperBigFoot/hfx/issues/251

## Outcome

Replace the privately delivered mixed-convention TDX-Hydro artifact with one complete HFX dataset whose outlets consistently follow the corrected adapter rules across all 62 processing basins. Deliver the replacement under `s3://pourpoint-hfx/hfx/` on Hetzner in fsn1, verify actual local pourpoint consumption at Basel, and retire approved obsolete TDX/build objects. Retain the HydroBASINS dataset.

The artifact remains pristine NGA TDX-Hydro compiled to HFX `format_version` 0.3.0: one level of reach catchments, vectors, native LINKNO to DSLINKNO topology, unit outlets, and `hfx.aux.snap.v2` stems. The specification remains authoritative. This Effort changes neither the format nor its version.

## Historical starting point

Effort #107 produced a 55-basin partial fabric. Effort #195 added seven corrected basins while deliberately retaining the frozen 55-basin input unchanged. Its corrected control was excluded from the final assembly. Effort #108 successfully delivered that mixed-convention artifact under its then-approved scope. This replacement supersedes the artifact while preserving those historical outcomes and disclosures.

The delivered reference contains 15,936,428 drainage units across 62/62 processing basins and seven objects totaling 142,657,764,763 bytes, including delivery documentation. Its private prefix is:

`s3://pourpoint-hfx/hfx/tdx-hydro-nga-20230126-global-62basin-hfx-0.3.0-af443be35774/`

Endpoint: `https://fsn1.your-objectstorage.com`, region `fsn1`.

The latest #108 delivery and landing records govern source availability. Exact-approved cleanup already deleted immediate source, per-basin, control, extension-copy and frozen-baseline payloads. Historical campaign statements that those payloads were preserved describe an earlier state. Current availability must be checked rather than inferred from those statements.

Relevant evidence:

- #108 authoritative delivery: https://github.com/CooperBigFoot/hfx/issues/108#issuecomment-5617231200
- #195 authoritative delivery: https://github.com/CooperBigFoot/hfx/issues/195#issuecomment-5601008939
- `scripts/hetzner/CAMPAIGN-tdx-hydro-extension.md` and accompanying JSON/evidence records.
- `adapters/tdx-hydro/README.md`, `build_adapter.py`, and outlet comparison/control evidence.
- `scripts/dataset-delivery/README.md` and its reviewed private-publication tooling.
- Private historical operations evidence: `/Users/nicolaslazaro/.local/share/hfx/operations/2026-09-09-tdx-delivery`.

Discovery inspected `origin/main` at `529fe1a05c1e333dbcf82ccbb0bc96d182ebcaae`. The canonical local checkout was clean but its `main` was one commit ahead and 106 behind that ref. Preserve its unique commit and other users' work; reconstruct current target evidence before implementation.

## Correction and invariance

Apply the already-adopted corrected outlet semantics throughout the complete dataset. Native successor endpoint evidence, successor-upstream conditioning and exact coincidence establish connected-reach orientation. Healthy isolated roots use unanimous per-basin evidence polarity; degenerate reaches retain the established one-coordinate convention. These decisions precede graph contraction.

Native polygon-less links contribute orientation evidence but are absent from delivered topology and snap stems. Consequently, the final HFX artifact alone does not generally contain enough information to reconstruct corrected native orientation safely. Plan for native source reacquisition where retained evidence is insufficient.

Choose safe regeneration or full rebuilding from technical evidence. A full source rebuild is not required by the outcome. Reusing unchanged polygons, connectivity, attributes or snap content is acceptable only with complete identity and invariance evidence. No outlet-regeneration command existed at discovery; any narrower regeneration path must establish equivalence to the corrected native-source rules rather than guess from the final graph.

Preserve complete coverage, drainage-unit identities, polygons, connectivity, non-outlet attributes and snap v2 semantics. Compare the replacement against the delivered reference over the complete relevant content. Investigate unexpected differences and stop for direction if resolving them would change the agreed outcome. Do not silently expand the correction into unrelated data transformations.

The historical corrected control established 7,192 outlet changes with zero polygon, graph, non-outlet attribute or snap differences. The earlier 5,425 count describes an intermediate correction subset. Neither count nor the historical maximum shift is a global acceptance threshold. Establish and retain the actual replacement differences and per-basin correction provenance.

## Observable acceptance

- One complete replacement covers all 62 processing basins, with no silently omitted units or basins and corrected outlet derivation demonstrated throughout.
- Full strict validation succeeds against the complete replacement. Retain actual validator stdout, stderr, command/build identity, exit status and an unambiguous final status in durable evidence. The existing adapter path discarded successful validator output at discovery; wrapper success alone is insufficient.
- Uploaded content is verified through complete-content cryptographic digests. Multipart ETags alone are insufficient. Preserve artifact identity, manifests, build/source provenance and required NOTICE, CITATION and README.
- The dataset remains private and supports pourpoint's actual S3 byte-range consumption.
- Run the sibling pourpoint consumer locally on the maintainer's machine at Basel, latitude 47.5596, longitude 7.5890, against the verified replacement on Hetzner. Use normal defaults and record the actual consumer build, dataset identity, requested/resolved outlet, returned geometry, diagnostics and run result. Require a successful delineation with valid, nonempty output. Apply resource supervision appropriate to the laptop. The maintainer's own validation remains separate; no additional location suite or scientific accuracy assessment is required.
- Preserve failed consumer evidence and report blockers. Do not repair the consumer, silently alter defaults, substitute a location, or turn this check into maintained delineation/comparison tooling inside HFX. The previous Basel geometry defect was repaired downstream before #108's accepted run; select and record a current suitable consumer build from evidence.
- Approved cleanup completes with exact deletion receipts and retained-content checks. Historical and current operational status are clearly distinguished.

## Spending and execution authority

The maintainer set a hard **US$15 total incremental campaign spending ceiling**, covering compute, storage and transfer costs attributable to this work. Before provisioning, establish current pricing, currency conversion, available quota, source availability, resource sizing, expected duration and a conservative shutdown reserve. Account for replacement coexistence and retained temporary resources. Do not reuse consumed historical compute authority.

Heavy source processing stays off the local machine. Use ephemeral Hetzner compute in fsn1 where feasible. The local machine is authorized for the consumer sanity check, not planetary compilation.

For context only, #195 used a ccx33 with eight vCPUs/32 GB RAM and a 600 GB volume, took 38.3625 hours and had an estimated gross cost of EUR11.37. It needed 55 GB of volume swap. Its assembly/validation launch spanned about 20 hours. These observations are not a quote or sizing proof for the new campaign. Historical source transfer was erratic at roughly 1–6 MB/s per connection without HTTP range support. Previous resources were deleted.

Choose acquisition, reuse and resource mechanics from evidence. Monitor accrued and projected spending and stop early enough to remain within the ceiling. If safe completion cannot fit US$15, return for maintainer direction before incurring costs that exceed it. An unsuccessful attempt cannot authorize an additional budget. High memory consumption during offline validation is accepted; streaming-validator redesign is outside scope. Release invocation-owned paid resources safely and retain the evidence needed to explain any incomplete outcome.

This vision does not itself start implementation or paid compute. Execution remains subject to the normal implementation workflow.

## Publication, retirement and cleanup

Use a fresh private replacement prefix under `pourpoint-hfx/hfx/`. Keep the current dataset unchanged until strict validation, complete uploaded-content verification and the local consumer sanity check succeed. Publish the manifest only after its payloads are verified. Select concrete safeguards from technical evidence without reopening reversible engineering choices as interview questions.

Hetzner previously ignored `CompleteMultipartUpload IfNoneMatch="*"`. Do not claim provider-enforced atomic no-overwrite. Existing reviewed tooling supports explicit cooperating exclusive-writer publication with fresh-prefix and identity checks. Use the strongest supported protection, prevent cooperating concurrent writers, disclose residual external-writer risk, and record the actual invocation authority. The prior #108 decision was scoped to its own publication; it is not a reusable approval token for arbitrary writes.

Inventory current obsolete TDX datasets, pilots, scratch/build leftovers, stale operational objects and relevant multipart leftovers. Investigate backup ownership before proposing deletion. Unknown data and unrelated backups have no implied deletion authorization. Preserve:

- The replacement dataset and required documentation/attribution.
- HydroBASINS at `hfx/hydrobasins-pfaf1-12-nested-global-snap-2026-07-20/`.
- Durable provenance, validation, integrity, consumer and cleanup evidence outside disposable bucket clutter.
- Any unrelated or unapproved objects.

Present an exact current deletion inventory for maintainer approval, including the superseded TDX dataset only after its replacement passes acceptance. Bind execution to approved object identities and recheck before deletion. Changed identities or unknown objects require reconciliation, not expanded permission. Verify deletion outcomes and retained content. No bucket deletion is authorized merely by this vision or the US$15 spending ceiling.

Correct or explicitly supersede misleading operational status records. Preserve the fact that historical successful validator stdout was not retained. The reviewed historical final state and native exit-zero log support the earlier outcome; new validation evidence must not be represented as a recovered historical transcript.

## Boundaries and handoff state

Excluded: ancillary rasters, multi-level aggregation, spec/schema/version changes, streaming-validator redesign, public release or announcement, application deployment, GRIT comparison, Cartopy study, global scientific accuracy assessment and unrelated consumer repairs. The GRIT area defect at https://github.com/CooperBigFoot/pourpoint/issues/157 is separate and does not block this Effort.

This vision records confirmed discovery. Before substantive implementation, verify the Effort’s single commit-pinned vision link, matching Program/Effort provenance, and the exact target-branch copy through the normal implementation workflow. Continue with `implement-vision 251` using this same vision; do not create a competing vision. Publication does not start implementation, paid compute, bucket operations, or consumer execution. Program closure requires later delivery, landing and explicit maintainer confirmation.
