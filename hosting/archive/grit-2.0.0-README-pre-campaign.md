# GRIT HFX v2 Dataset

This dataset is an HFX v0.2.1 compilation of GRIT with segment (`level=0`) and reach (`level=1`) drainage units.

## DAG `up_area_km2` semantics

Choice: partitioned (option a). Each segment's `up_area_km2` reflects the source-area share routed through that segment.

Algorithm: per-segment chain anchor. Segment rows use GRIT `drainage_area_out` directly. Reach rows are computed by anchoring each parent segment's outlet reach to the segment `drainage_area_out`, then walking upstream within that segment and subtracting each downstream reach's local `area_km2`.

Consumer caveat: in DAG split-rejoin geometry, the sum of `up_area_km2` over a flow set is not the watershed area. Consumers must use the graph plus `level=1` reaches for true watershed accumulation.

## Known data caveats

44 reach rows have `up_area_km2=NULL`, spread across 17 segments where the chain-anchor algorithm could not resolve the outlet. These are anomalies in the GRIT v1.0 source topology, not defects of the HFX encoding. `has_up_area` remains true; nulls are permitted per HFX v0.2.1 spec. They represent <0.001% of rows. See `adapters/grit-v2/build_adapter.py` for the detection rule.

Fallback segment IDs:

- AF: 140004152, 390037110, 480000538
- AS: 180020690
- EU: 230045414
- NA: 190003639, 220083647, 300005125, 360109293, 410046446
- SA: 9627, 120012564
- SI: 110020043, 110020254, 430009780, 580009350
- SP: 90000652

## Provenance

- Source: GRIT v1.0 (https://doi.org/10.5281/zenodo.17435232)
- Adapter version: 84b8c5f856cf428238889d86007f3b1c49dac3e2
- HFX spec version: 0.2.1
- Built: 2026-05-26
- Bbox: planetary [-180, -90, 180, 90]
- Row count: 22,337,300 catchments across 2 levels
