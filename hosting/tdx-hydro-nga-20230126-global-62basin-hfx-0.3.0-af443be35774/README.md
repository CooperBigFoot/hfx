# TDX-Hydro global HFX dataset

## Delivery status and private access

This README describes the selected preserved artifact and its intended private
destination. Repository documentation alone does not establish completed delivery.
The new delivery receipt must have `status: verified-dataset` before consumption.
The comparison and approved cleanup have separate acceptance records.

Dataset ID: `tdx-hydro-nga-20230126-global-62basin-hfx-0.3.0-af443be35774`

Intended URI:
`s3://pourpoint-hfx/hfx/tdx-hydro-nga-20230126-global-62basin-hfx-0.3.0-af443be35774/`

Endpoint: `https://fsn1.your-objectstorage.com`; region: `fsn1`.
Use a private authorized S3 profile, such as `hetzner-pourpoint`. Credential
values are never part of this document or the delivery receipt. Do not publish
presigned URLs or change bucket visibility. Existing HydroBASINS objects under
`hfx/` remain separate and retained.

## Identity and coverage

| Field | Value |
| --- | --- |
| HFX `format_version` | 0.3.0 |
| `fabric_name` | tdx_hydro |
| `fabric_version` | NGA-TDX-Hydro-20230126 |
| Coverage | 62 of 62 TDX-Hydro processing basins |
| Drainage units | 15,936,428 |
| Levels | Single level, level 0 |
| CRS | EPSG:4326 |
| Topology | tree |
| Planetary bbox | [-180, -90, 180, 90] |
| `region` | absent |
| Auxiliary | `hfx.aux.snap.v2` snap stems |
| Ancillary rasters and D8 refinement data | absent |

The preserved six-object bundle contains 142,657,755,797 bytes. Delivery adds
this README; the final receipt records the actual resulting total. The original
manifest and all six preserved objects retain their bytes and relative paths.

| Object | Bytes | SHA-256 |
| --- | ---: | --- |
| `aux/snap_stems.parquet` | 33,828,897,608 | `9c3459148b528d54f7da4bf019bd761f557b6d139e23f592bef2136bd6ef9b3f` |
| `catchments.parquet` | 108,307,447,136 | `456a163cb627dd8128dd59f1cd820f7f156f44ebd7a2887e77041cc9259767bd` |
| `CITATION.txt` | 714 | `26cf72c81cb2841878979987a0e9345366738e6379ef6f016744d6903449e213` |
| `graph.parquet` | 521,408,174 | `b21754976fb176a762bbd6e5bcdd1737dcad82551fdc21fac4bbb7bc8df038ed` |
| `manifest.json` | 827 | `af443be357742550ea76ef774b83a1f86e683ea828bb796c9ca893425777be85` |
| `NOTICE` | 1,338 | `d3d3e7a6c45aaf9e13fe471e8b427f7526addb60aa8fa9f47534890775cd4d93` |

`source-inventory.json` in the repository pins the preserved source observations
and historical SHA-256 evidence. Multipart ETags are object condition tokens;
they do not prove SHA-256. Current HEAD requests returned no provider checksum.
The delivery tool therefore verifies every complete destination object through
bounded, ordered range reads and a sequential SHA-256 digest. It records exact
byte counts and conditional object identities, without a full local download.
See `scripts/dataset-delivery/README.md` for reproducible commands, required
provider conditional-write evidence, resource bounds, and interrupted-run rules.

## Source, modifications, and licensing

Source: NGA TDX-Hydro release 20230126, reach-catchment `basins` and `streamnet`
products derived from TanDEM-X elevation data. TDX-Hydro reach catchments are
TauDEM stream-reach drainage units with a 5 km² delineation threshold. Processing
basins are distribution tiles, rather than drainage-unit levels.

The adapter normalizes source topology, polygons, geographic bounds and stream
stems into HFX. Global unit IDs use native LINKNO plus the processing-basin header
number times 10,000,000. The compile handles valid multipart identities and
contracts links through polygon-less reaches. The source corpus remains the NGA
fabric; these normalization steps and outlet choices are compiler modifications.
No new source acquisition, rebuild, or further outlet correction occurs during
delivery. The original `NOTICE` and `CITATION.txt` travel with this dataset.

Dataset license: **CC BY-SA 4.0**, inherited from TDX-Hydro.
https://creativecommons.org/licenses/by-sa/4.0/

Retain source attribution, the license reference, and a description of
modifications. Sharing adapted material must satisfy the applicable ShareAlike
obligations. The toolkit's MIT/Apache licenses apply to software; dataset use and
redistribution remain subject to the source dataset license. Consult the
preserved `NOTICE` and `CITATION.txt` for the original citation and attribution.

## Compilation and validation evidence

Compilation revision: `c3e04bee941e95e19ebd88dca11b3d9cc7619b99`.
Assembly and preservation revision: `a1ebc116d326c9b4b192dcc942f2a42b40d9b2b2`.
Adapter and crate trees were unchanged between these revisions.

The tracked authority is
`scripts/hetzner/CAMPAIGN-tdx-hydro-extension.{md,json}` and
`scripts/hetzner/evidence/tdx-hydro-extension/`, audited at
`bb1bbe458cb57ab3370525334c7bfed4b6134ceb`.

Strict whole-dataset validation completed on 2026-09-07. The native assembly
launch finished at 11:17:46 UTC with exit 0. The reviewed native code path runs
`hfx <dataset> --strict --sample-pct 100 --format text` before marking assembly
succeeded. Successful validator stdout was not retained. The exact final
assembly state, launch log, and reviewed path establish the historical pass.
The later campaign driver exited 126 during final record generation. A stale
operational bucket snapshot still says running; the tracked final succeeded
state supersedes it. Delivery does not fabricate missing historical stdout or
relabel that driver failure as a pass. New full-byte delivery checks prove
identity with retained checksums, independently of historical format validation.

## Accepted outlet limitation

The frozen 55-processing-basin portion retains historical final-vertex outlet
conventions for near-degenerate reaches and isolated roots. The seven added
processing basins use corrected evidence-based conventions. The accepted
control adjudication records exactly 7,192 changed outlets in one processing
basin, with unchanged topology and polygons. This control count does not
estimate error frequency across the full baseline. The preserved artifact
intentionally contains these mixed conventions.

## Consumption and interpretation

Use a pinned `pourpoint` build with the final verified private S3 URI, the above
endpoint and region, and authorized credentials. Preserve the exact build
identity and dataset manifest SHA in the consumer receipt. Remote reads avoid
storing the entire dataset locally, but session startup still loads graph and
reference data. Bound and monitor memory, runtime, disk and transfers.

TDX-Hydro supports vector traversal and snapping through its declared stems.
It supplies no D8 refinement data. Record the consumer's actual refinement skip
reason. The planned Basel comparison uses latitude 47.5596, longitude 7.5890,
and normal best-effort behavior for both fabrics. GRIT can use its own D8
refinement. The two-run difference combines source fabrics, snapping and
available refinement; it cannot isolate refinement's causal contribution.
Neither result is a ground-truth oracle. GRIT attribution and CC BY-NC terms
must also appear in comparison outputs. No successful comparison is claimed by
this README.

Retain the preserved source until destination verification, real consumption,
comparison, and exact human-approved cleanup prerequisites succeed. This
artifact's delivery tooling has no source deletion or cleanup command.

## Recorded publication protection decision

The 2026-09-09 tiny provider probe rejected a conditional PUT replacement but
accepted a multipart completion replacement despite `IfNoneMatch="*"`.
The probe remains refused. No dataset payload was part of that test.
Nicolas Lazaro then approved proceeding with operational single-writer exclusion
for this exact source and destination. The original
[decision](https://github.com/CooperBigFoot/hfx/blob/7fde62b2b3acf79715ddd6830eb7845b891fd7ba/hosting/tdx-hydro-nga-20230126-global-62basin-hfx-0.3.0-af443be35774/exclusive-writer-decision.json) and separate
[authorization binding](https://github.com/CooperBigFoot/hfx/blob/7fde62b2b3acf79715ddd6830eb7845b891fd7ba/hosting/tdx-hydro-nga-20230126-global-62basin-hfx-0.3.0-af443be35774/exclusive-writer-authorization-binding.json) retain
that scope and the raw failed-probe checksum.

This is weaker than provider-enforced atomic destination no-overwrite.
A conditional private scratch ownership object coordinates cooperating tool
invocations; it does not prevent external writers. The operator must ensure
that only the approved invocation writes the chosen dataset prefix. HEAD checks
before multipart completion still have a TOCTOU interval. The complete delivery
receipt must disclose its actual `publication_protection` mode and retain the
operator decision, probe and ownership evidence. Full SHA-256 verification,
manifest-last activation, private access and all cleanup prerequisites remain
required. This decision grants no deletion authority and does not claim that
publication or the watershed comparison has completed.
