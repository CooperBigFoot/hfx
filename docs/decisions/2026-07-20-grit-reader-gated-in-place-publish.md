# GRIT D8 uses reader-gated in-place publication

**Status:** Accepted

**Date:** 2026-07-20

## Context

The hosted dataset already occupies `grit/hfx-v0.3.0/` and retains HFX
`format_version` 0.3.0. Its amended manifest adds one
`hfx.aux.d8_raster.v2` entry for the planetary EPSG:8857 pair at
`aux/d8/flow_dir.tif` and `aux/d8/flow_acc.tif`.

Deployed pourpoint 0.1.0 links released HFX toolkit 0.4.0. It hard-errors while
opening a dataset that contains an unknown `hfx.aux.*` schema ID. Pourpoint
ticket #45 produces the v2-capable reader release required for publication.

The earlier OD-8 rehost decision selected a new additive prefix for an HFX
format transition. This decision deliberately diverges because this change
preserves the HFX 0.3.0 dataset identity and adds reader capability inside the
existing format.

## Decision

Publication amends `grit/hfx-v0.3.0/` in place. A sibling prefix falls outside
this decision. The two COGs and `NOTICE`, `CITATION.txt`, and `README.md` may be
staged before reader availability because the live manifest references none of
the new objects.

`manifest.json` is the atomic reader-visible switch. It stays unpublished
until a v2-capable pourpoint release produced by ticket #45 exists, and it is
the final upload after every raster and attribution object.

The runbook enforces the publication phase with
`--execute --publish-manifest` and a distinct typed manifest confirmation.
Human release verification remains the authority for satisfying the ticket
#45 gate.

## Consequences

- Operators can stage the planetary raster pair and attribution objects ahead
  of reader availability without changing the live dataset contract.
- Manifest publication exposes the v2 auxiliary entry atomically after the
  compatible reader release exists.
- The in-place prefix preserves the hosted HFX 0.3.0 dataset identity.
- Human verification of the ticket #45 release remains part of the final
  publication procedure.
