# Draft for maintainer review. Not sent.

To: SFNAGGeoscienceApplications@nga.mil
Cc: geomatics@nga.mil; the corresponding author of the TDX-Hydro preprint (read the address from https://doi.org/10.22541/essoar.171629686.65893579/v1 before sending)
Subject: TDX-Hydro basin 1020018110: two basins polygons share streamID 9
Attachment: features.geojson (the two polygons, EPSG:4326)

Dear TDX-Hydro team,

Thank you for publishing TDX-Hydro. While working with the basins product for HydroBASINS level-2 basin 1020018110 (file 1020018110-basins.gpkg, layer "basins", SHA-256 4965c693b8891e0b3e4270241422567b6c1a7766099ab2324d55be08a6abec27, downloaded from earth-info on 2026-08-09 and again on 2026-08-19 with the same digest), I found two distinct polygons that both carry streamID 9.

The first is a single 12 m grid cell at about 23.4311 E, 9.0964 N, with an area of about 150 square metres. The second is a catchment of about 8.4 square kilometres spanning 23.4309 to 23.4703 E and 9.0904 to 9.1214 N. The two polygons do not overlap. They share exactly one vertex, the north-east corner of the small cell, which lies on the western edge of the larger catchment. The small cell sits inside the larger catchment's bounding box.

Since streamID is the key that links each basins polygon to its stream reach in the streamnet product, a duplicated value makes that relationship ambiguous. Could you confirm whether the single-cell polygon is a processing artefact that should belong to the larger catchment, and whether a corrected file is planned? The attached GeoJSON contains the exact vertex coordinates of both polygons. I am happy to provide anything else that would help.

Kind regards,
Nicolas Lazaro
