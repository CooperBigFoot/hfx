# TDX-Hydro planetary campaign evidence

This record preserves the completed outcome of the `tdx-m5-planetary` campaign,
S5 attempt 4, for [Program #103](https://github.com/CooperBigFoot/hfx/issues/103)
and [Effort #107](https://github.com/CooperBigFoot/hfx/issues/107). The campaign
completed on 2026-08-07.

## Landed outcome

The campaign attempted all 62 TDX-Hydro processing basins. It retained a
strict-validated HFX v0.3.0 partial fabric with these recorded properties:

| Field | Value |
|---|---|
| Processing basins | 55 of 62 |
| Drainage units | 12,748,154 |
| Region | `tdx-hydro-partial-4dbff0d6ec31` |
| Format version | 0.3.0 |
| Fabric version | `NGA-TDX-Hydro-20230126` |
| Total exported bytes | 114,063,230,627 |
| Strict validation completed | 2026-08-07T04:57:16Z |
| Export destination at campaign completion | `s3://pourpoint-hfx/scratch/tdx-hydro-tdx-m5-planetary/planetary-hfx-v0-3-0/` |

The S3 destination is a preservation record. This document makes no claim that the
object is public or currently available.

Coverage is partial. The remaining recovery and adjudication moved to
[Effort #195](https://github.com/CooperBigFoot/hfx/issues/195). That scope change allows
Effort #107 to remain a completed, truthful 55-of-62 outcome.

## Evidence derivation

The legacy campaign mirror remains outside Git. The following checks were repeated from
that mirror during the Program #103 record migration:

1. `mirror/state/pipeline.json` contains 62 unique campaign basin identifiers.
2. `mirror/state/assembly.json` has status `succeeded` and contains 55 unique input basin
   identifiers. `mirror/reports/assembly.json` contains the same roster.
3. All 55 roster members have build reports. Summing
   `diagnostics.streamnet.polygon_bearing_link_count` over those reports gives
   `12,748,154`.
4. Hashing the sorted comma-separated 55-basin roster gives the partial-region suffix:

   ```text
   sha256(",".join(sorted(input_basin_ids)))[:12] = 4dbff0d6ec31
   ```

5. The pipeline and assembly roster difference is exactly:
   `1020018110`, `2020003440`, `2020065840`, `2020071190`, `4020050470`,
   `5020049720`, and `6020000010`.
6. Campaign-time state records `1020018110`, `2020003440`, and `2020071190` as
   compile failures. The other four each exhausted two interrupted acquisition attempts
   and remained pending compilation.
7. `mirror/logs/export-deadline.log` records successful validation, six exported objects,
   and 114,063,230,627 exported bytes.

The strict validator path used by the campaign runs `hfx --strict --sample-pct 100`; see
[`adapters/tdx-hydro/build_adapter.py`](../../adapters/tdx-hydro/build_adapter.py). The
partial-fabric label follows
[`docs/decisions/2026-07-23-best-effort-coverage-truthful-manifest.md`](../../docs/decisions/2026-07-23-best-effort-coverage-truthful-manifest.md).

The current causal classifications for the seven absent basins are recorded in
[`adapters/tdx-hydro/seven-basin-verdicts.json`](../../adapters/tdx-hydro/seven-basin-verdicts.json).
They supersede the campaign-time interpretation without changing the retained artifact.

## Recovered source identities

| Legacy record | SHA-256 |
|---|---|
| `CAMPAIGN-OUTCOME.md` | `31d99b13a8ed9c6989424c46ecfdd3267be0d7507d75b5ad404939e184b9335b` |
| `mirror/state/pipeline.json` | `4272c702098dcb5b47d970aaaff00842b12e33a58c0a4de0f806445a101b7246` |
| `mirror/state/assembly.json` | `0a12503471ceed0ff07b2fcfbed0187a09f11a68bce6f8382e9dd84fbeeb535c` |
| `mirror/reports/assembly.json` | `8e5a2a4d1e0fb5551149094d7fee11fcccfceeda82f04c0f2289ff02e86de691` |
| `mirror/logs/export-deadline.log` | `543bf9499f46cd37e365837bb9aa739052ff44850416abbcc01bfc02a1c9cea6` |
| `DEVIATION-argmax-hotpatch.md` | `4df49a154cfdb2c0349fa619a1cab6dc0b23c754dd4350ca6082cc4ef6584f3f` |

The source identities allow comparison with the retained local mirror without placing
bulk operational evidence in Git.

## Provenance boundary

The VM did not remain at pinned ref `43a98aff8c15a1a196f47b10217ad2f5553b6611`
for the full campaign. An authorized ARG_MAX hot patch was applied during the run. The
campaign must not be described as having executed entirely at that ref.

Merged delivery history includes PRs
[#162](https://github.com/CooperBigFoot/hfx/pull/162),
[#163](https://github.com/CooperBigFoot/hfx/pull/163),
[#167](https://github.com/CooperBigFoot/hfx/pull/167),
[#182](https://github.com/CooperBigFoot/hfx/pull/182),
[#193](https://github.com/CooperBigFoot/hfx/pull/193), and
[#214](https://github.com/CooperBigFoot/hfx/pull/214). Those changes provide the campaign
and assembly machinery plus the later seven-basin verdict authority. This record adds the
previously local final campaign outcome.
