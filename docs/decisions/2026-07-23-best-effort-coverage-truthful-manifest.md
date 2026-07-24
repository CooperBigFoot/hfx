# Multi-basin builds are best-effort, and the manifest declares coverage truthfully

A planetary TDX-Hydro build spans 62 independently compiled processing basins, and
the single-basin pilot showed that basin-specific real-data surprises are the norm
rather than the exception, so requiring all 62 to compile before anything ships
would let one pathological basin block the whole dataset indefinitely. A build
therefore ships its compiled coverage: only a hard failure excludes a basin, and the
excluded basins are named in the campaign record.

Because coverage is no longer guaranteed complete, the manifest must state it. Only a
complete compiled coverage omits `region` and claims the planetary
`bbox = [-180, -90, 180, 90]`; any incomplete coverage is a partial-fabric dataset
carrying a `region` label. The alternative — declaring planetary identity regardless
of what compiled, keeping the dataset's identity stable across campaigns — was
rejected because an engine reads only the manifest: a query inside a missing basin
would surface as `NoSnapCandidates`, which the consumer contract defines as a
genuinely off-network point. That failure tells the user their point is not on a
river when the truth is that their continent was not compiled, and no amount of prose
in a campaign record reaches that code path.
