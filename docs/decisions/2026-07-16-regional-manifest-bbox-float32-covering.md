# HydroBASINS → HFX: Regional Manifest bbox is the float32 Covering Union

**Status:** Accepted

**Date:** 2026-07-16

**Follow-up:** Implemented by Effort ticket #64 (Program #31), a post-delivery
adapter conformance fix surfaced by the 2026-07-16 planetary build. Independent
of #65 (antimeridian coordinate-domain clamp).

## Decision

The regional (partial-fabric) path in the HydroBASINS adapter's `write_manifest`
derives `manifest.bbox` by casting the GeoDataFrame's float64 `total_bounds` to
**float32** component-wise:

```python
manifest["bbox"] = [float(np.float32(v)) for v in units.geometry.total_bounds]
```

The planetary path continues to use `manifest["bbox"] = list(PLANETARY_BBOX)`.
`PLANETARY_BBOX` remains `[-180.0, -90.0, 180.0, 90.0]`.

## Context

`hfx-cli --strict` check D4 (`values.rs::check_bbox_enclosure`) verifies that the
manifest bbox encloses the union of the per-row `bbox` covering. That covering is
a GeoParquet 1.1 struct with **float32** leaves (`build_bbox_struct`), and the
validator recomputes the union by widening each float32 leaf to f64
(`f64::from(minx)`).

Round-to-nearest float32 conversion can push a covering leaf *outward* past the
true float64 bound. The 2026-07-16 build observed `eu` with covering
`maxy = 81.85897827` against a float64 `total_bounds` `maxy = 81.85897607`. The
old float64 manifest bbox therefore sat about one float32 ulp *inside* the
covering union the validator recomputes, and D4 reported
`values.bbox_enclosure` INVALID.

Because float32 conversion is monotonic, the float32-covering union equals
`float32(total_bounds)` component-wise. Casting the manifest bbox to float32
makes it **equal** to the union the validator recomputes, so enclosure holds by
equality in every dimension. This is the tightest correct bbox and needs no
outward-rounding margin.

## Alternatives considered

- **Outward-rounded float32** (nudge mins down / maxes up by one ulp): a strict
  enclosure margin that survives future changes to how the covering is built, but
  a looser bbox and more code. The exact cast already encloses by equality, and
  the covering construction is stable in the same file.
- **Read the written covering column back** and aggregate its min/max as the
  manifest bbox: a single source of truth, but requires ordering the catchments
  write before the manifest and plumbing the covering into `write_manifest`.
  This adds unnecessary plumbing for an identical result today.
- **Leave float64, widen the validator tolerance**: the specification's
  enclosure contract is exact. The adapter held the wrong value, and the
  validator behavior remains unchanged.

## Why this is easy to get wrong later

Writing float32-rounded bounds when the precise float64 `total_bounds` is right
there reads as a loss of precision, so a future maintainer could naively "fix" it
back to float64 and silently reintroduce the enclosure failure. This document and
the `CONTEXT.md` relationship entry record why the float32 cast is deliberate.
The executable guard is
`test_regional_float32_bbox_round_up_passes_strict_conformance`, a red-first
synthetic regression with a covering leaf that rounds up under float32. It builds
through the real `write_manifest`, invokes real `hfx-cli --strict`, and asserts
`Result: VALID`. The merged-build test also uses exact `assertEqual` against the
component-wise float32-cast geometry bounds.
