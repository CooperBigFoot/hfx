# TDX-Hydro source defect: two `basins` polygons carry streamID 9 in processing basin 1020018110

Status: draft for maintainer review. Nothing in this packet has been sent.

This packet documents one internal contradiction in a published TDX-Hydro data file.
It is written so that a reader with any GIS tool and the file named below can verify every statement without further context.
The sendable message is in `MESSAGE.md`.
The two features are in `features.geojson`.

## 1. Product and file

| Item | Value |
|---|---|
| Product | TDX-Hydro `basins` GeoPackage for processing basin 1020018110 |
| File name | `1020018110-basins.gpkg` |
| Layer | `basins` |
| Size | 9,772,503,040 bytes |
| SHA-256 | `4965c693b8891e0b3e4270241422567b6c1a7766099ab2324d55be08a6abec27` |
| Source | NGA earth-info TDX-Hydro download endpoint, `https://earth-info.nga.mil/php/download.php?file=1020018110-basins-gpkg` |
| First acquisition | 2026-08-09, one interrupted transfer followed by one complete transfer |
| Second acquisition | 2026-08-19, one complete transfer, identical SHA-256 |
| Coordinate reference system | EPSG:4326, longitude and latitude in decimal degrees |

Processing basin 1020018110 is one of the 62 HydroBASINS level-2 polygons that the TDX-Hydro distribution is tiled by.
It lies in central Africa.
The file was opened read-only.
No feature was edited, reprojected, or simplified.

## 2. The contradictory features

The `basins` layer contains two distinct polygons whose `streamID` attribute is 9.
Their exact vertex coordinates are in `features.geojson` in this directory, keyed by `feature_index`.
The numbers below were computed from those coordinates with geodesic (WGS84) area and length.

| Property | Feature 1 | Feature 2 |
|---|---|---|
| `streamID` | 9 | 9 |
| Rings | 1 | 1 |
| Vertices (closed ring) | 5 | 801 |
| Longitude range | 23.431055555555922 to 23.431166666667032 | 23.430944444444812 to 23.470277777778147 |
| Latitude range | 9.096388888888889 to 9.096499999999999 | 9.090388888888889 to 9.12138888888889 |
| Extent | 1/9000 degree square, about 12.2 m by 12.4 m | about 4.32 km by 3.45 km |
| Area | about 150 m2 (0.00015 km2) | about 8.40 km2 |
| Perimeter | about 49 m | about 19.87 km |
| Centroid | 23.43111 E, 9.09644 N | 23.45279 E, 9.10664 N |
| Geometry validity | valid | valid |

Feature 1 is a single raster cell of the 12 m TDX-Hydro grid.
Feature 2 is an ordinary reach catchment.
Feature 1 lies inside the bounding box of feature 2.
The two polygons do not overlap: their interiors are disjoint and they share exactly one vertex, the north-east corner of feature 1 at 23.431166666667032 E, 9.096499999999999 N, which is also a vertex of feature 2.
Feature 1 therefore sits on the western edge of feature 2, touching it at one corner.
The two records split one identifier across two separate areas.

Feature 1 coordinates, for quick reference:

```
[[23.431055555555922, 9.096499999999999],
 [23.431055555555922, 9.096388888888889],
 [23.431166666667032, 9.096388888888889],
 [23.431166666667032, 9.096499999999999],
 [23.431055555555922, 9.096499999999999]]
```

A reader can reproduce these findings by loading the layer, selecting `streamID = 9`, and inspecting the two returned rows, or by loading `features.geojson` directly.

## 3. Why this is a defect in the published data

The TDX-Hydro technical documentation (NGA, "TanDEM-X Hydro Technical Documentation", last edited 2023-01-31, approved for public release NGA-U-2023-00146, section 6.2) describes the hydrographic suite as vector stream networks together with "hydrologic catchments associated with each stream segment", produced with TauDEM, with attribution as documented by TauDEM.
TauDEM's documentation for the Stream Reach and Watershed tool defines `LINKNO` as "a unique number associated with each link (segment of channel between junctions)" and states that the watershed output identifies "each reach watershed with a unique ID number".
In the TDX-Hydro distribution the `basins` layer carries that identifier as `streamID`, and the matching `streamnet` layer carries it as `LINKNO`.
`streamID` is therefore the only key that joins a catchment polygon to its stream reach.

With two polygons under `streamID` 9, that join is ambiguous.
Any consumer that indexes catchments by `streamID`, or that expects one catchment per reach, must either pick one polygon arbitrarily, merge the two silently, or refuse the file.
None of these choices can be justified from the data alone.
The single-cell polygon has the shape of a processing artifact, one grid cell that was left outside the catchment it belongs to when the raster watershed grid was vectorized.
Only the producer can confirm that reading.

The contradiction is confined to the published file and is visible in any GIS tool.
It does not depend on any particular consumer software, tolerance, or interpretation.

## 4. The TDX-Hydro paper and its authors

Verified on 2026-09-03 against the Crossref metadata record for the DOI.

Carlson, K. A., Levin, H. K., Morris, A. L., Candela, S. G., Morales Rivera, A. M., Huening, V. G., & Fredericks, J. G. (2024). TDX-Hydro: Global High-Resolution Hydrography Derived from TanDEM-X. ESS Open Archive. https://doi.org/10.22541/essoar.171629686.65893579/v1

| Item | Value |
|---|---|
| Authors, in order | Kimberly A. Carlson; Heather K. Levin; Amy L. Morris; Salvatore G. Candela; Anieri M. Morales Rivera; Vincent G. Huening; Jaimeson G. Fredericks |
| Affiliation, all authors | National Geospatial-Intelligence Agency, Office of Geomatics |
| Venue | ESS Open Archive (AGU preprint server, Crossref publisher of record Wiley) |
| Type | posted-content (preprint) |
| Posted | 2024-05-21 |
| DOI | 10.22541/essoar.171629686.65893579/v1 |
| Crossref record | https://api.crossref.org/works/10.22541/essoar.171629686.65893579/v1 |
| Landing page | https://essopenarchive.org/users/783331/articles/939736-tdx-hydro-global-high-resolution-hydrography-derived-from-tandem-x |
| Journal version | None found. The Crossref record has an empty `relation` object and a Crossref bibliographic search for the title on 2026-09-03 returned only the preprint. |

The corresponding author and their email address are printed on the preprint landing page and in the PDF.
Automated fetches of both return HTTP 403, so the maintainer should open the landing page in a browser and read the corresponding-author line before sending.

The dataset itself is cited as:

National Geospatial-Intelligence Agency (NGA). (2023). TanDEM-X Hydro (TDX-Hydro) [Data set]. Office of Geomatics. https://earth-info.nga.mil/index.php?action=geosciences&dir=geosci

## 5. Contacts

| Contact | Role | Source |
|---|---|---|
| SFNAGGeoscienceApplications@nga.mil | NGA Geoscience Division, the address named for TDX-Hydro questions and issues | earth-info Geosciences page and the technical documentation, section 2 |
| geomatics@nga.mil | NGA Office of Geomatics, general | earth-info page footer |
| Corresponding author of the preprint | Paper author | Read from the preprint landing page by the maintainer |

Recommended routing: send to SFNAGGeoscienceApplications@nga.mil, copy geomatics@nga.mil and the corresponding author.

## 6. Sendable message

See `MESSAGE.md` in this directory.
It is self-contained and offers `features.geojson` as an attachment.

## 7. Transmission record

Not yet sent.
Fill in after the maintainer confirms transmission, through the normal reviewed path.

| Field | Value |
|---|---|
| Date sent | Not yet sent |
| Addressed recipients (To) | Not yet sent |
| Copied recipients (Cc) | Not yet sent |
| Exact sent text | Not yet sent. Reference the file or commit that holds the text as sent. |
| Attachments sent | Not yet sent |

## 8. Provenance (repository metadata, kept out of the sendable material)

- Verdict ledger: `adapters/tdx-hydro/seven-basin-verdicts.json`, entry `processing_basin_id` 1020018110, verdict `source defect`, evidence kind `acquired source geometry`, rule `duplicate-ground-equality-v1`, selected branch `different_ground`.
- Ledger flags: `spatially_equal` false, `coordinate_sequences_equal` false, `geometries_valid` [true, true].
- Adapter identity recorded in the ledger: adapter version 0.1.0, git revision `bca87d8adb0651d130bde9c7dfcf3947427cfa24`. The adjudicator at commit `bd2606c1dd268eee8f87327008411bf73a08d1b7` derived the verdict.
- `features.geojson` in this directory is extracted verbatim from the two `coordinates` arrays of that ledger entry. `test_build_adapter.py` asserts the two coordinate sequences are equal.
- Acquisition evidence: `tdx-m5-seven-acquire-evidence/salvage/reports/1020018110-basins-acquisition.json` (2026-08-09, two transfers) and `attempt21-remote/reports/1020018110-basins-acquisition.json` under the 2026-08-07 evidence root (2026-08-19, one transfer). Both record the SHA-256 above.
- Geometry numbers in section 2 were computed with shapely 2 and pyproj `Geod(ellps="WGS84")` from the ledger coordinates.
