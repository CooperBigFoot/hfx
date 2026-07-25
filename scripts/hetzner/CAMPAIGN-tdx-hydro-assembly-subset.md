# TDX-Hydro three-basin assembly subset campaign record

## Scope

Attempt 4 of campaign `tdx-m4-subset` succeeded on 2026-07-25 from checkout `8a614e72f989420dccb84c1f7f2ca28043a36a63`.
The campaign used a Hetzner `ccx33` with 8 vCPU and 32 GB of memory.
It selected processing basins `1020000010`, `7020000010`, and `9020000010`, which were three of the fixed 62 processing basins.
This record describes a completed zero-footprint validation subset and does not authorize M5.
The subset was not published.
The authoritative retained evidence remains outside the repository at `/Users/nicolaslazaro/Desktop/work/hfx-campaign-evidence/tdx-m4-subset/20260725T184128Z`.

## Campaign parameters

| Parameter | Executed value |
|---|---|
| Campaign | `tdx-m4-subset` |
| Date | 2026-07-25 |
| Successful attempt | 4 |
| Server type | Hetzner `ccx33` |
| Compute shape | 8 vCPU and 32 GB of memory |
| Checkout SHA | `8a614e72f989420dccb84c1f7f2ca28043a36a63` |
| Basin selection | `1020000010`, `7020000010`, `9020000010` |
| Campaign role | Three-basin zero-footprint validation subset |
| Publication status | Not published |
| External evidence directory | `/Users/nicolaslazaro/Desktop/work/hfx-campaign-evidence/tdx-m4-subset/20260725T184128Z` |

## Phase ordering and durations

### 1. Provision and bootstrap

Four attempts were made.
Attempt 1 stopped during initialization because the VM lacked `jq`.
Attempt 2 reached acquisition but lost two of the three basins.
The kernel out-of-memory path killed one compile, and one manifest bounding-box enclosure check failed.
Attempt 3 never provisioned because Hetzner refused a `ccx43` request with `resource_limit_exceeded`.
Attempt 4 succeeded on `ccx33` after the dependency, bounding-box, and bounded-streaming compiler fixes were present in the ground-truth checkout.
Every attempt ended with proven zero Hetzner footprint.

### 2. Acquisition

Successful attempt 4 acquired `23,950,475,264` bytes in `1h35m47s`.
This elapsed total included one transient NGA retry.

| Basin | Product | Bytes | Attempt outcome | Elapsed |
|---|---|---:|---|---:|
| `1020000010` | basins | `6,979,305,472` | succeeded on attempt 1 | `15m40s` |
| `1020000010` | streamnet | `1,880,039,424` | succeeded on attempt 1 | `30m17s` |
| `7020000010` | basins | `5,907,767,296` | succeeded on attempt 1 | `14m17s` |
| `7020000010` | streamnet | `1,676,398,592` | succeeded on attempt 1 | `2m30s` |
| `9020000010` | basins | `5,873,713,152` | succeeded on attempt 1 | `6m41s` |
| `9020000010` | streamnet | `1,633,251,328` | succeeded on attempt 2 | `25m54s` |

The `775 MiB` size is quoted verbatim from the acquisition log for the first `9020000010` streamnet attempt, which timed out after `14m22s`.
The `25m54s` duration measures the successful second attempt.
The `1h35m47s` duration measures acquisition wall clock for the campaign.
Acquisition used bounded concurrency, so the individual product durations do not define the campaign wall clock.

### 3. Compile

All three selected basins passed compile on the 32 GB machine.
RSS means resident set size.
The process-tree high-water measurement is the retained maximum observed across the compile process tree.

| Basin | Result | Elapsed | Peak RSS bytes | Process-tree high-water bytes | Peak scratch bytes | Scratch high-water bytes |
|---|---|---:|---:|---:|---:|---:|
| `1020000010` | PASS | `17m36s` | `6,257,164,288` | `6,316,322,816` | `6,530,924,497` | `6,546,571,029` |
| `7020000010` | PASS | `14m53s` | `5,989,613,568` | `6,027,456,512` | `5,541,794,376` | not supplied |
| `9020000010` | PASS | `14m33s` | `7,649,370,112` | `7,678,582,784` | `5,396,674,014` | not supplied |

`catchment_graph_merge_write` was the largest allocating phase for all three basins.

| Basin | `catchment_graph_merge_write` allocation delta bytes |
|---|---:|
| `1020000010` | `1,294,221,312` |
| `7020000010` | `1,005,346,816` |
| `9020000010` | `217,096,192` |

`catchment_graph_merge_write` also held peak RSS for every basin.
During attempt 2, before the bounded streaming compiler from PR `#178`, the kernel killed the `1020000010` compile at `anon-rss 31,671,528 kB` on the same machine class and input.
The successful compile peaked at `6,257,164,288` RSS bytes.

```text
31,671,528 kB * 1,024 = 32,431,644,672 bytes
32,431,644,672 / 6,257,164,288 = about 5.18
```

This comparison shows roughly a fivefold reduction.

### 4. Assembly

Assembly passed in `5m13s`.
Its input list contained exactly `1020000010`, `7020000010`, and `9020000010`.
The assembled output contained `944,905` units and had a reported aggregate size of `9,037,936,480` bytes.

| Object | Bytes |
|---|---:|
| `catchments.parquet` | `6,882,220,800` |
| `aux/snap_stems.parquet` | `2,124,914,739` |
| `graph.parquet` | `30,797,959` |
| `manifest.json` metadata | `3,982` |

The component measurements sum to `9,037,937,480` bytes.
This sum is `1,000` bytes greater than the supplied aggregate measurement of `9,037,936,480` bytes.
The difference is a source-measurement discrepancy because all three supplied validation results passed.
The manifest bounding box was `[-124.40817260742188, -18.22283363342285, 54.46583557128906, 83.65983581542969]`.
The manifest recorded `hfx.aux.snap.v2` at `aux/snap_stems.parquet`.
It recorded `references_levels=[0]` exactly.

### 5. Validation

The M3 whole-file verifier completed.
Strict 100 percent HFX validation reported `0 errors, 0 warnings, 0 info` and `Result: VALID`.
Explicit adapter GeoParquet validation exited `0`.

### 6. Teardown and zero footprint

Successful attempt 4 cost about `EUR 0.81`.
The split was about `EUR 0.78` for compute and `EUR 0.03` for the prorated volume.
Cumulative spend across all four attempts was about `EUR 1.98`.
The campaign stayed below the `EUR 10.00` ceiling.
Successful teardown reported this exact line:

```text
hfx: campaign tdx-m4-subset has zero Hetzner footprint: server hfx-build-tdx-m4-subset absent; volume hfx-build-tdx-m4-subset-data absent
```

Independent exact-name server and volume checks also found no campaign resources.
These checks covered the named campaign resources.

## Phase results and evidence

The external evidence directory is the retained evidence source for every row.

| Phase | Evidence description | Result |
|---|---|---|
| Provision and bootstrap | Attempt history and successful `ccx33` checkout convergence | completed |
| Acquisition | Six product measurements and one transient retry | PASS |
| Compile `1020000010` | Compile result, elapsed time, memory, and scratch measurements | PASS |
| Compile `7020000010` | Compile result, elapsed time, memory, peak scratch, and absent scratch high-water | PASS |
| Compile `9020000010` | Compile result, elapsed time, memory, peak scratch, and absent scratch high-water | PASS |
| Assembly | Exact three-basin input list, unit count, byte inventory, and manifest metadata | PASS |
| M3 verification | Whole-file verifier result | completed |
| Strict HFX validation | `0 errors, 0 warnings, 0 info` and `Result: VALID` | PASS |
| Adapter GeoParquet validation | Explicit adapter validation | exit `0` |
| Evidence retention | `/Users/nicolaslazaro/Desktop/work/hfx-campaign-evidence/tdx-m4-subset/20260725T184128Z` | completed |
| Teardown | Exact teardown line and independent exact-name checks | PASS |

## Deviations and failed attempts

Attempt 1 exposed the missing `jq` dependency during initialization.
Attempt 2 exposed the pre-PR `#178` compile memory failure and a manifest bounding-box enclosure failure.
Attempt 3 exposed the Hetzner `ccx43` resource limit before a VM was provisioned.
Attempt 4 included the dependency, bounding-box, and bounded-streaming compiler fixes and completed the subset.
The component byte inventory differs from the supplied aggregate by `1,000` bytes, as disclosed in the assembly section.

The following values are weaker-provenance campaign-record inputs because tracked material at the ground-truth ref does not independently cross-check them.
Their inclusion preserves the supplied campaign measurements and log observations.

| Supplied value | Provenance limitation |
|---|---|
| Scratch high-water `6,546,571,029` | Campaign-record input with no independent tracked cross-check |
| Acquisition log quotation `775 MiB` | Campaign-record input quoted from the external acquisition log |
| Allocation deltas `1,294,221,312`, `1,005,346,816`, and `217,096,192` | Campaign-record inputs with no independent tracked cross-check |
| Spend split `EUR 0.78` compute and `EUR 0.03` prorated volume | Campaign-record input with no independent tracked cross-check |
| `references_levels=[0]` | Campaign-record input with no independent tracked cross-check |
| `hfx.aux.snap.v2` | Campaign-record input with no independent tracked cross-check |

## M5 planetary sizing guidance

This planning guidance combines three-basin measurements with figures carried forward from a prior projection.
It does not authorize a campaign, freeze an M5 machine selection, or settle a retention policy.
The provenance boundary applies to every calculation below.

The three-basin campaign supplied the measurements in this section.
The four capacity terms used for 62-basin planning identified below were carried forward unchanged from the M4-S4 capacity disclosure and were not derived by this three-basin campaign.
One of those terms is the active compile-scratch reserve for a single compile.

| Provenance label | Included values |
|---|---|
| Measured on the three-basin subset | Campaign observations and arithmetic computed directly from them |
| Carried forward from the M4-S4 capacity disclosure | `549,279,383,552`, `206,220,202,290`, `30,000,000,000`, and `785,499,585,842` |

### Memory and scratch

The memory and scratch sizing uses these measured maxima.

```text
maximum peak RSS = max(6,257,164,288, 5,989,613,568, 7,649,370,112)
                 = 7,649,370,112 bytes
maximum process-tree high-water = max(6,316,322,816, 6,027,456,512, 7,678,582,784)
                                = 7,678,582,784 bytes
maximum measured peak scratch = max(6,530,924,497, 5,541,794,376, 5,396,674,014)
                              = 6,530,924,497 bytes
maximum supplied scratch high-water = 6,546,571,029 bytes
```

The observed per-compile memory envelope is `7,678,582,784` bytes.
The observed scratch envelope is `6,546,571,029` bytes because it is the only supplied scratch high-water measurement.
Only basin `1020000010` has a supplied scratch high-water value, so the three-basin scratch envelope has weaker coverage than the memory envelope.
The supplied scratch high-water value has weaker provenance than the peak-scratch values because no tracked input at the ground-truth ref independently cross-checks it.

For a 32 GB decimal machine class:

```text
32,000,000,000 - 7,678,582,784 = 24,321,417,216 bytes
7,678,582,784 / 32,000,000,000 = about 23.996 percent
7,678,582,784 + 6,546,571,029 = 14,225,153,813 bytes
```

The largest observed compile used about 24.0 percent of the nominal 32 GB memory class.
Memory and scratch are different resources.
The `14,225,153,813`-byte sum combines separate resource envelopes for capacity planning and does not predict RSS.
The three observed compiles fit comfortably when run serially on this class.
These measurements do not authorize parallel compile.
Three geographically varied basins do not prove that every one of the 62 basins will fit.
M5 should retain the [runbook's](RUNBOOK-tdx-hydro-assembly-subset.md) serial-compile model, operating-system reserve, live process-tree measurement, and fail-closed memory preflight.

### Storage

The [M4-S4 capacity disclosure](../../adapters/tdx-hydro/COMPILE-SCALE-REHEARSAL.md) is the source of the four carried-forward capacity terms in this subsection.
The three-basin measurements provide comparison points and do not turn those prior projections into campaign measurements.

The following retained-download arithmetic is the documented basis for the carried-forward retained-download term.

```text
6,979,305,472 + 1,880,039,424 = 8,859,344,896 bytes per conservative basin
8,859,344,896 * 62 = 549,279,383,552 retained download bytes
```

The projection uses the largest measured combined download, basin `1020000010`, for every processing basin.
The `206,220,202,290`-byte retained-output term was carried forward unchanged from the M4-S4 capacity disclosure.
It was not derived from the three measured basins.

All four displayed terms below reproduce carried-forward figures.
The first, second, and total terms are 62-basin planning aggregates.
The third term reserves scratch for one active compile.

```text
549,279,383,552 retained downloads
+ 206,220,202,290 retained outputs
+ 30,000,000,000 active compile-scratch reserve
= 785,499,585,842 projected bytes
```

The [subset runbook](RUNBOOK-tdx-hydro-assembly-subset.md) supplies the `150 GB` volume and `140,000,000,000`-byte usable-space check.

```text
785,499,585,842 - 140,000,000,000 = 645,499,585,842 bytes beyond the usable-space check
785,499,585,842 / 150,000,000,000 = 5.2367 times the nominal 150 GB volume
785,499,585,842 / 140,000,000,000 = 5.6107 times the 140,000,000,000-byte usable check
```

The current 150 GB volume and `140,000,000,000`-byte usable check cannot retain the projected 62-basin downloads, outputs, and active scratch together.
M5 requires either a much larger volume or an explicit retention policy that reclaims downloads and completed outputs as the campaign advances.
This choice is open and must be settled before M5.
This record does not choose a retention strategy.
The current runner persists `retain-all-through-publication`, and its harness asserts that retain-all sizing has no reclaim interface.
Assembly scratch, the final assembled artifact, logs, reports, filesystem overhead, and safety margin would increase the capacity requirement beyond the `785,499,585,842`-byte subtotal.

### Wall-clock

The acquisition wall clock converts and scales by basin count as follows.

```text
1h35m47s = 5,747 seconds
5,747 * 62 / 3 = about 118,771 seconds
118,771 seconds is 32h59m31s
```

The measured aggregate acquisition throughput supports a second simple model.

```text
23,950,475,264 / 5,747 = about 4,167,474 bytes per second observed aggregate throughput
549,279,383,552 / 4,167,474 = about 131,802 seconds
131,802 seconds is about 36h36m42s
```

The `32h59m31s` estimate is a basin-count extrapolation.
The `36h36m42s` estimate is a conservative projected-byte extrapolation.
These models give a roughly 33 to 37 hour acquisition planning range.
Both assume the same acquisition concurrency and comparable endpoint behavior.
The one observed retry is already present in the measured wall clock.
NGA's lack of range support, complete-file restarts, variable product sizes, and highly variable throughput make acquisition the weakest linear estimate.

The serial compile mean scales as follows.

```text
17m36s + 14m53s + 14m33s = 47m02s = 2,822 seconds
2,822 * 62 / 3 = about 58,321 seconds
58,321 seconds is 16h12m01s
```

The maximum observed basin supplies a second compile bound.

```text
17m36s = 1,056 seconds
1,056 * 62 = 65,472 seconds
65,472 seconds is 18h11m12s
```

These models give a roughly 16 to 18 hour serial-compile planning range.
They assume that the three measured basins represent the 62-basin work distribution.
Input bytes alone do not determine compile cost.
Reach and catchment complexity can also change it.
A larger unmeasured basin can exceed the bound.

The assembly wall clock scales by basin count as follows.

```text
5m13s = 313 seconds
313 * 62 / 3 = about 6,469 seconds
6,469 seconds is about 1h47m49s
```

The measured count-linear output alternative and carried-forward output term produce a second model.

```text
three-basin measured aggregate output = 9,037,936,480 bytes
count-linear 62-basin alternative = 9,037,936,480 * 62 / 3 = about 186,784,020,587 bytes
carried-forward 62-basin retained-output term = 206,220,202,290 bytes
count-linear basin factor = 62 / 3 = about 20.667
206,220,202,290 / 9,037,936,480 = about 22.817 times the subset output bytes
313 * 22.817 = about 7,142 seconds
7,142 seconds is about 1h59m02s
```

The `22.817` multiplier comes from the carried-forward retained-output term and exceeds the count-linear factor of `20.667`.
The more conservative carried-forward term preserves the prior capacity disclosure and avoids understating planning time until M5 replaces it with separately authorized sizing evidence.
These models give a roughly 1 hour 48 minute to 2 hour assembly planning range.
The estimate comes from one three-basin assembly and has no independent repeat.
Global merge, sort, validation, filesystem, and memory behavior may scale nonlinearly.
Neither extrapolation is a deadline or guarantee.

For orientation, the three simple serial phase ranges total roughly 51 to 57 hours before provisioning, bootstrap, full validation, evidence retention, retries beyond the observed acquisition retry, or teardown.
This orientation does not create a total campaign budget.
M5 still requires a separately authorized runbook with fresh capacity, price, and time ceilings.

## Acceptance criteria map

| Claim | Evidence description | Result |
|---|---|---|
| Exact three-basin selection | Campaign selection and assembly input list | PASS |
| Acquisition bytes and retry | Six product byte measurements, campaign wall clock, and quoted timeout observation | recorded |
| All three compiles passed on 32 GB | Per-basin compile results on `ccx33` | PASS |
| Per-basin memory and scratch observations | Compile table, including explicitly absent scratch high-water values | recorded |
| Bounded streaming historical comparison | Attempt 2 kernel measurement and successful PR `#178` measurement | recorded |
| Assembly input IDs, unit count, and byte inventory | Assembly report measurements and disclosed `1,000`-byte discrepancy | recorded |
| Manifest bounding box and snap auxiliary metadata | Exact bounding box, `hfx.aux.snap.v2`, path, and `references_levels=[0]` | recorded |
| M3 whole-file verification | Complete verifier result | PASS |
| Strict 100 percent HFX validation | `0 errors, 0 warnings, 0 info` and `Result: VALID` | PASS |
| Adapter GeoParquet validation | Explicit adapter validation exit | PASS |
| Attempt and cumulative spend | Attempt 4 split and four-attempt cumulative amount | recorded |
| Exact teardown plus independent zero-footprint checks | Teardown line and exact-name server and volume checks | PASS |
| M5 memory derivation | Three-basin measured memory and scratch calculations | recorded |
| M5 storage derivation | Carried-forward capacity terms and runbook comparison | open decision |
| M5 wall-clock derivation | Measured extrapolations with carried-forward assembly comparison | recorded |

## Conclusion

The three selected basins compiled, assembled, and validated from checkout `8a614e72f989420dccb84c1f7f2ca28043a36a63`.
Teardown left zero Hetzner footprint for the named campaign resources.
The subset demonstrates serial compile viability on the measured 32 GB class while leaving full-planet storage retention unresolved.
This record does not claim planetary readiness.
