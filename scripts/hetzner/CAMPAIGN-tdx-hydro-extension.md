# TDX-Hydro extension campaign evidence

This record preserves the `seven-basin-extension` campaign outcome for
[Effort #195](https://github.com/CooperBigFoot/hfx/issues/195) in
[Program #103](https://github.com/CooperBigFoot/hfx/issues/103). The workload
completed on 2026-09-07. Final driver record generation failed afterward.
This record grants no compute authority and does not close the Effort.

## Recorded outcome and verified identity

| Field | Evidence-supported value |
|---|---|
| Processing-basin coverage | 62 of 62, derived from the frozen 55-item roster plus seven disjoint successful inputs |
| Added drainage units | 3,188,274, summed from seven retained per-basin manifests |
| Final drainage units | 15,936,428, observed in the extension manifest and equal to the baseline count plus additions |
| Dataset bytes | 142,657,755,797 across six objects in the preserved bucket listing |
| Strict whole-dataset validation | Passed; assembly launch finished 2026-09-07T11:17:46Z, exit 0 |
| Final driver | Exit 126 at 2026-09-07T11:39:25Z |
| Exact-resource teardown | Zero footprint at 2026-09-07T11:39:49Z |
| Preservation destination at campaign completion | `s3://pourpoint-hfx/scratch/tdx-hydro-seven-basin-extension/extension-hfx-v0-3-0/dataset/` |

Authenticated read-only observations at 2026-09-09T11:10:27Z confirm the
[extension manifest](evidence/tdx-hydro-extension/extension-manifest.json) SHA-256
`af443be357742550ea76ef774b83a1f86e683ea828bb796c9ca893425777be85`.
It declares 15,936,428 units, absent `region`, planetary bbox
`[-180, -90, 180, 90]`, format 0.3.0 and fabric version
`NGA-TDX-Hydro-20230126`. Its count agrees with the retained input derivation.
The extension listing contains six objects totaling 142,657,755,797 bytes.
These observations confirm small manifest bytes and object names/sizes at that
time. Full large-object digests were not reread; the campaign-time streamed
readbacks remain the full-byte preservation evidence. No public availability
or publication under `hfx/` is claimed.

The [small receipt](CAMPAIGN-tdx-hydro-extension.json) contains provenance,
rosters, per-basin manifest and report digests, validation and control results,
object digest manifests, cost and teardown evidence. It is a retrospective study
record, not a replacement `lifecycle-result.json` or a runtime contract.

## Current processing-basin dispositions

| Processing basin | Units | Current disposition |
|---|---:|---|
| `1020018110` | 515,349 | Compiled and preserved |
| `2020003440` | 336,922 | Compiled and preserved |
| `2020065840` | 433,202 | Compiled and preserved |
| `2020071190` | 663,991 | Compiled and preserved |
| `4020050470` | 78,181 | Compiled and preserved |
| `5020049720` | 933,755 | Compiled and preserved |
| `6020000010` | 226,874 | Compiled and preserved |

Each state records `succeeded`, attempt 1, with no failure reason. Each output has
four retained object digests and successful streamed bucket readback. The eight
per-basin manifest digests, including the control, were rechecked from small local
records. The control `7020000010` compiled 331,263 units separately. It already
belongs to the baseline roster and was excluded only from the new assembly inputs.
The resulting roster has no missing processing basin and does not double-count
the control.

The [55-basin baseline](CAMPAIGN-tdx-hydro-planetary.md) remains separate:
12,748,154 units, region `tdx-hydro-partial-4dbff0d6ec31`, and 114,063,230,627 bytes.
Assembly consumed it once as a frozen input. The campaign read its preservation
prefix without writing or deleting there. The receipt retains the six digests
from that pull. The authenticated baseline manifest matches its retained digest
`8e8cdf67d5036d73dd584b16f5318a232796cc417c515e5fb4791d5de195d687`,
including count, region and format. The current six-object baseline listing
sums to 114,063,230,627 bytes. This confirms small-record identity and listed
sizes without claiming a fresh full-byte integrity audit of the large objects.

## Validation and provenance

Compilation used `c3e04bee941e95e19ebd88dca11b3d9cc7619b99`. Assembly and bucket-only
preservation resumed on the same resources at
`a1ebc116d326c9b4b192dcc942f2a42b40d9b2b2`. The `adapters/` and `crates/` trees have no
difference between those revisions. The planetary control used
`43a98aff8c15a1a196f47b10217ad2f5553b6611` with the recorded ARG_MAX hotpatch,
commit `bde61149d3fefc5e3f30435bf7ed3d0bb32a519c`.

The retained assembly launch log records start at 2026-09-06T15:24:44Z, the
extension command with the baseline and roster arguments, `assemble_succeeded=1`,
and finish at 2026-09-07T11:17:46Z with exit 0. The runner's
`verify_assembly_dataset()` calls the adapter's `validate_dataset()` before
writing succeeded state. That path executes:

```text
hfx <assembled dataset> --strict --sample-pct 100 --format text
```

A nonzero HFX exit raises an error. GeoParquet checks must also succeed. The
retained final state is succeeded and validation classification is passed.
Successful HFX stdout and stderr were captured internally and discarded, so this
record does not fabricate a raw validator transcript. The final launch log and
code path establish the completed pass independently of the operator milestone.
All 67 state files, nine reports and nine logs match their retained SHA manifests.
The exact [final assembly state](evidence/tdx-hydro-extension/final-assembly-state.json)
and complete [assembly launch log](evidence/tdx-hydro-extension/assembly-launch.log)
are retained verbatim in Git. The receipt pins their digests and native execution
revisions, making the final state and exit-zero evidence independently accessible.

The driver later failed during final record generation. Its original
`campaign-record.json` has zero bytes, and `lifecycle-result.json` is absent.
Those historical facts remain unchanged. No passing driver or lifecycle result
is manufactured by this retrospective record. The independent record-generation
repair belongs to [PR #242](https://github.com/CooperBigFoot/hfx/pull/242).

## Accepted deviations from the published vision

The [published vision](../../planning/visions/2026-09-03-close-the-seven-basin-coverage-gap.md)
is unchanged. Later reviewed decisions explain two differences from its discovery
account and literal success criteria.

### Source-defect finding superseded

[PR #228](https://github.com/CooperBigFoot/hfx/pull/228) established generic
single-part multipart encoding support. The two parts of `1020018110` streamID 9
and the 17 parts of `5020049720` streamID 24 have no interior overlap and dissolve
to MultiPolygon units. Their current adjudication is adapter strictness. The
adapter refuses interior-overlapping parts rather than relaxing contradictions.
The real compiles dissolved 122,259 identities from 531,466 parts in the first
processing basin and 211,758 identities from 731,121 parts in the second.

The [schema 2 verdict ledger](../../adapters/tdx-hydro/seven-basin-verdicts.json)
keeps historical absence separate from current evidence and preserves the
superseded source-defect finding as history. Five other current ledger fields
remain null; the successful campaign states above provide their final compile
disposition. [PR #229](https://github.com/CooperBigFoot/hfx/pull/229) withdrew the
erroneous author packet and records that none was sent. No current confirmed
source defect remains in this reviewed finding set, so no author report is
warranted. The withdrawn packet must not be sent.

### Corrected control difference adjudicated

[PR #231](https://github.com/CooperBigFoot/hfx/pull/231) records Nicolas Lazaro's
2026-09-04 acceptance of exactly 7,192 changed outlets, pinned in the
[control adjudication](seven-basin-control-adjudication.json). The campaign
comparison accepted exactly that set: maximum shift 0.17020920315745225 degrees,
zero differences outside the set, and no polygon, graph, downstream, non-outlet
attribute or snap-stem differences. The corrected catchments file differs in
bytes; graph, snap stems and manifest are byte-identical to the preserved control.
The corrected control must not be called byte-identical.

The planetary rebuild matched the three parquet digests and differed only in
manifest `created_at`, under the recorded allowance. This explicit tolerance
also prevents a literal whole-tree identity claim. The corrected orientation
digest matched the tracked adjudication and the source `orient` report.
The frozen baseline retains the earlier final-vertex convention for
near-degenerate reaches and isolated roots. The added processing basins use the
corrected evidence-based convention. [Effort #108](https://github.com/CooperBigFoot/hfx/issues/108)
must preserve that distinction in delivery documentation. This accepted change
must remain explicit when evaluating the vision's byte-identity criterion.

## Preservation receipts and limits

All prefixes below are relative to
`s3://pourpoint-hfx/scratch/tdx-hydro-seven-basin-extension/`.

| Prefix | Objects read back with matching SHA-256 | Listed bytes |
|---|---:|---:|
| `source-corpus/` | 16 of 16 | 84,101,885,952 |
| `basin-outputs/` | 32 of 32 | 31,439,612,280 |
| `control-builds/` | 20 of 20 | 8,577,720,771 |
| `extension-hfx-v0-3-0/dataset/` | 6 of 6 | 142,657,755,797 |

The source digest list equals the preserved expected list for all 16 GeoPackages.
The control prefix holds corrected, rebuilt planetary and preserved planetary
trees plus their small comparison records. The receipt carries each object
digest and the hashes of the manifests and readback records. Object paths are
relative to each bucket prefix. The extension receipt records the original
`assembly/dataset/` source-manifest prefix separately, keeping its SHA-list hash
unchanged. No dataset was
processed or downloaded during this retrospective audit.

The authenticated small bucket-state readback confirms a **stale operational
snapshot**: `status: running`, 1,761 bytes, SHA-256
`79ff2376b8ebdafd763adf3891077c5fa2b068e991275c959e0e24841351062d`.
It was uploaded before validation completed. The exact final succeeded state is
1,763 bytes; it differs from the bucket snapshot only in `status`. The final
state and exit-zero launch log preserved in Git supersede that snapshot as the
campaign outcome authority. The [observation receipt](evidence/tdx-hydro-extension/verification-result.json)
and both state versions remain available to verify this distinction.

This is a known non-dataset operational risk: reading the old bucket state alone
would misreport an ongoing assembly. It does not indicate incomplete validation
or a damaged dataset. No S3 object or historical local record was overwritten,
and no validation was rerun. The empty preservation marker supplies no digest
proof by itself.

No preserved output, baseline or source was deleted. Pre-directive workstation
copies and partial salvage remain historical retention matters outside this
record. Any cleanup requires separate explicit authority.

## Lifecycle, cost and authority

Server `164714525` and volume `106799807` were the named ccx33/fsn1/600 GB resources.
The recorded teardown detached that volume, deleted that server and volume, and
reported zero footprint. A fresh read-only hcloud CLI check at
2026-09-09T11:11:19Z confirms both exact IDs are not found; its
[receipt](evidence/tdx-hydro-extension/exact-resource-absence.json) is retained.
Preflight and final unrelated-server snapshots differ
only in traffic counters. No unrelated configuration change is observed.

Provisioning request at 2026-09-05T21:18:04Z to zero footprint at
2026-09-07T11:39:49Z is 38.3625 hours, below 72 hours. Captured gross prices project
EUR 20.9923565918 for 72 hours. The conservative estimate at teardown is
EUR 11.3708598205 using 39 rounded billed hours and 730 hours per volume billing
month; the outbound estimate is below the included allowance. This is a
price-based estimate, without an invoice claim.

The operator added 55 GB of volume swap while strict validation ran after the
initial swap headroom became low. Preserve this sizing deviation independently
of the successful result. No runbook Bash fence is changed by this record.

[PR #244](https://github.com/CooperBigFoot/hfx/pull/244) landed the independently
reviewed executable authority guard. Against this completed contract,
`authority-is-pinned` and `scope-permits-compilation` pass;
`authority-is-current` exits 1 because authority is consumed. The composed driver
checks current authority before effects in every mode. No driver was executed
during this evidence verification.

The authenticated small-object evidence resolves the earlier manifest and
metadata verification gap. The stale bucket-state risk is preserved explicitly,
with the exact final state and launch log supplying the outcome authority.
Independent review evaluates these receipts together with the accepted control
and source-adjudication deviations above.

No further paid lifecycle is authorized. This record performs no artifact
publication under `hfx/`, pourpoint integration, external author communication,
Program Map transition, or Effort closure.
