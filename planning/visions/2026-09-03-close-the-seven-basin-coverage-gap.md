# Close the seven-basin coverage gap
Program: https://github.com/CooperBigFoot/hfx/issues/103
Effort: https://github.com/CooperBigFoot/hfx/issues/195

## Outcome

Recover as much of the seven-basin TDX-Hydro coverage gap as the source data can support, then preserve an enlarged HFX v0.3.0 fabric with exact evidence of its coverage.

The work favors usable coverage over a predetermined basin count. A basin with internally coherent NGA source data should compile after correcting any adapter strictness that blocks it. A basin with a genuine source contradiction may remain excluded. Operational limits may also stop a basin from compiling, but they must never be presented as proof of a source defect.

Verdict labels are implementation records rather than the product goal. They must remain truthful and must distinguish the historical reason a basin was absent from the 55-basin campaign from the current reason it is included or excluded. The outcome is that every recoverable basin is added and every remaining absence has current evidence.

When a genuine source defect is found, prepare a concise, adapter-independent report for the authors of the original TDX-Hydro paper. The maintainer, Nicolas Lazaro, will review and send that communication. Preserve what was sent. No acknowledgement, reply, or upstream correction is required to complete this Effort.

## Starting point

Effort #107 retained a strict-validated partial fabric with these recorded properties:

- 55 of 62 TDX-Hydro processing basins
- 12,748,154 drainage units
- region `tdx-hydro-partial-4dbff0d6ec31`
- `format_version` 0.3.0
- 114,063,230,627 exported bytes
- preservation destination `s3://pourpoint-hfx/scratch/tdx-hydro-tdx-m5-planetary/planetary-hfx-v0-3-0/`

The merged authority for that outcome is `scripts/hetzner/CAMPAIGN-tdx-hydro-planetary.md`. The 55-basin artifact must remain intact.

The absent processing basins are:

- `1020018110`
- `2020003440`
- `2020065840`
- `2020071190`
- `4020050470`
- `5020049720`
- `6020000010`

All seven absent basins and control basin `7020000010` were later acquired successfully. The preserved corpus contains all 16 source GeoPackages and is 84,101,885,952 bytes, about 78.3 GiB. At discovery time it is under `/Users/nicolaslazaro/hfx-campaign-evidence/2026-08-07-close-the-seven-basin-coverage-gap/off-vm/acquired-source/`. The evidence root also contains campaign records, logs, a passing planetary-revision control output, and exact teardown evidence. Treat any approval or credential material there as opaque. Never print or copy its contents into Git.

The merged verdict ledger at `adapters/tdx-hydro/seven-basin-verdicts.json` establishes:

- `1020018110` has two valid `streamID 9` polygons that cover different ground. This is a confirmed source defect.
- `2020003440` and `2020071190` exposed adapter strictness in the original adjudication.
- `2020065840`, `4020050470`, `5020049720`, and `6020000010` were historically absent because acquisition attempts failed, although their source files were later acquired.

Two later bounded compile campaigns reached deeper failures. They produced no corrected output for any absent basin. The latest campaign record shows:

- the known source defect for `1020018110`;
- later orientation refusals for `2020003440`, `2020065840`, `2020071190`, `4020050470`, and `6020000010`;
- a newly exposed duplicate unit identity for `streamID 24` in `5020049720`; and
- a later root-successor orientation refusal in control basin `7020000010`.

The second campaign preserved the complete source corpus and a passing planetary-revision control output off the VM. Both campaigns reached exact-resource teardown with zero remaining Hetzner footprint. No enlarged fabric exists yet.

## What the implementation must establish

### Recover sound source data

Use the preserved source corpus unless its integrity cannot be proved. Reacquisition is a fallback, not the default.

Correct adapter behavior only where source attributes and geometry support one general, deterministic interpretation. Corrections must fail closed when the evidence is absent, contradictory, or tied. Per-basin exceptions and guessed topology are unacceptable. Each correction must include regression evidence showing the old real failing path and must preserve behavior for inputs that already compiled.

The newly exposed failures are part of this Effort. In particular, inspect the `streamID 24` duplication in `5020049720` from source geometry. If the features cover different ground under one identity, treat it as a source defect. If they represent the same ground and the adapter is too strict, correct the generic adapter rule. Apply the same source-backed standard to every later orientation refusal.

Prove the final corrected adapter against the preserved real basin data and against control basin `7020000010`. The corrected control output must be byte-identical to the preserved output produced by planetary revision `43a98aff8c15a1a196f47b10217ad2f5553b6611` with the recorded ARG_MAX hotpatch, or the difference must stop the work for adjudication. Do not explain a difference away.

A fixed coverage number is not required. The implementation should keep resolving correctable failures within the approved campaign boundary. If a basin still cannot be compiled when that boundary is reached, preserve the exact failure and classify only what the evidence proves.

### Produce a truthful final fabric

If at least one basin compiles, extend the frozen 55-basin artifact with every newly compiled basin. Do not rebuild the 55 basins as a substitute for extension. Partial-fabric extension support and large compile-diagnostic persistence are already merged into `main` and should be treated as starting capabilities. If no basin compiles, retain the strict-validated 55-basin artifact as the final fabric and preserve the complete current disposition evidence instead of creating a no-op derivative.

For an extended artifact, derive its constituent basin roster, partial-region digest, float32 covering-union bbox, and unit count from the actual inputs. Refuse an already represented basin rather than double-counting it. Coverage below 62 of 62 must retain a partial-fabric region and must not claim the planetary bbox.

Attempt strict whole-dataset validation of any extended artifact. The existing validator has previously required about 265 GB of resident memory and swap for the 55-basin artifact. A resource-limited or interrupted validation attempt does not permit a passing claim. Preserve the extended artifact and the complete validation record even if strict validation cannot finish. Keep the strict-validated 55-basin artifact available as the known-good baseline and as the final artifact when no new basin compiles.

### Report genuine source defects

For every confirmed NGA source defect, create an author-ready evidence packet that:

- identifies the processing basin and source file;
- identifies the contradictory features and their coordinates;
- explains the contradiction without referring to adapter internals or tracebacks;
- identifies the original TDX-Hydro paper and its authors from reliable publication evidence; and
- provides a concise message suitable for the maintainer to send.

No automated run or agent sends external communication. Pause for the maintainer to review each report. Nicolas Lazaro sends the message to the paper authors. After he confirms transmission, preserve the exact sent material, date, and addressed authors through the repository's normal reviewed path. A response is outside the completion gate.

## Existing work that may be recovered

Substantial work exists outside current `main`, but it is evidence rather than merge authority.

- PRs #215 through #218 were merged into `pce/close-the-seven-basin-coverage-gap/milestone-7`, not into `main`.
- The historical graph composed later orientation work through local commit `bebe796724c3611b3b1628ace01ec3112d6903d3`. Its component commits include `db4fb9c`, `13fc27c`, `bc8b9f4`, `054765a`, `26cb497`, `e74c5ed`, and `9c1ddb2`.
- A source-defect report draft for `1020018110` exists on the separate local chain ending at `d465f36`.
- The historical planning directory `planning/2026-08-07-close-the-seven-basin-coverage-gap/` is ignored, its driver is stopped, and its graph is non-authoritative.

Do not merge the historical composite wholesale. It is based on a stale branch, includes large frozen verifier copies, and predates later changes on `origin/main`. Extract the smallest useful changes, rebase or reimplement them against the current target branch, reproduce the real regressions, run current repository gates, and obtain independent review. Preserve local refs and evidence until their useful content has been recovered and verified.

At discovery time the primary worktree's local `main` was one commit ahead of and thirteen commits behind `origin/main`. Start implementation from current `origin/main` in a dedicated worktree without discarding the local-only commit.

## Compute and preservation constraints

Heavy source processing, compilation, assembly, and validation run on ephemeral Hetzner compute. Use the `hcloud` CLI and the tracked lifecycle scripts. Do not perform local data wrangling.

Carry forward the previously approved single additional bounded lifecycle with these limits:

- one `ccx33` server in `fsn1`;
- one 600 GB attached volume;
- less than 72 hours from provisioning request;
- projected and actual gross cost below EUR 40 after a current-price preflight; and
- exact-resource teardown on success, failure, refusal, interruption, or timeout.

If another paid lifecycle becomes necessary, stop and obtain new maintainer authority before provisioning it. A repository change cannot manufacture that authority.

Only the named campaign server and volume may be mutated. `pourpoint-web-1` is outside scope. Preserve source data, logs, campaign state, every produced per-basin output, both control outputs, and each assembled artifact off the VM with digest evidence before teardown. Effort #195 performs no deletion of the preserved baseline, source corpus, or outputs. Any later retention cleanup requires a separate explicit decision.

Keep shell tracing disabled around credential paths. Use opaque credential-file inputs and never record their values. Confirm zero remaining footprint by exact resource identity after teardown.

## Evidence of success

A completed Effort has merged, reviewed evidence showing all of the following:

1. Each of the seven basins has a current source-backed disposition. A compiled basin has a preserved output and digest. An excluded basin has evidence for the actual remaining cause.
2. Every basin with internally coherent source data that could be recovered within the approved bound is included in the final fabric. If none can be recovered, the strict-validated 55-basin baseline remains the final fabric.
3. Genuine source contradictions were never relaxed into the dataset.
4. The corrected control output matches the preserved planetary control byte for byte, with no unexplained path or digest difference.
5. Existing successful adapter behavior remains unchanged, and ambiguous or contradictory evidence still refuses.
6. For any extended artifact, the roster, region digest, bbox, unit count, and exclusion record recompute from the retained inputs.
7. Strict validation of any extended artifact has either passed or has a preserved, accurately described incomplete or failing result. Only a completed pass is called strict-validated.
8. The original 55-basin artifact and every new output remain preserved outside the ephemeral machine.
9. The maintainer has confirmed transmission of an author-ready report for every confirmed source defect. The exact sent material, date, and addressed authors are preserved. No upstream response is required.
10. The named campaign server and volume are absent after teardown, and no unrelated Hetzner resource was changed.

## Boundaries

This Effort does not change the HFX specification or `format_version` 0.3.0. It does not substitute GEOGLOWS for pristine NGA TDX-Hydro. It does not redesign the validator for streaming operation. It does not publish the selected artifact under `hfx/`, announce it publicly, or integrate it into pourpoint. Those delivery concerns remain with Effort #108.
