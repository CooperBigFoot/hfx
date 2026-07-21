# TDX-Hydro header-number crosswalk

`tdx_header_numbers.json` maps each NGA TDX-Hydro processing-basin identifier
to the two-digit header number used by the GEOGLOWS global river-numbering
convention. It is a numbering aid only; NGA TDX-Hydro remains the source
fabric.

- Source repository: `geoglows/tdxhydro-postprocessing`
- Source commit: `43b00d87423d47090afe0c885aa4fedc884318da`
- Source URL: `https://raw.githubusercontent.com/geoglows/tdxhydro-postprocessing/43b00d87423d47090afe0c885aa4fedc884318da/tdxhydrorapid/network_data/tdx_header_numbers.json`
- Retrieved: `2026-07-21`
- SHA-256: `8a5b7f10df614c071024bf4a91b83c31f1504a3c647aae65b0454acc81247389`

The file is committed so adapter builds remain offline-reproducible. Builds
must never refresh or fetch it at runtime. Upstream stores both processing-
basin identifiers and header numbers as JSON strings; `build_adapter.py`
normalizes header numbers to integers when loading. Native `-1` downstream
sentinels are passed through unchanged by the Global LINKNO transform.
