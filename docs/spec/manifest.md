# Manifest and schema

`manifest.json` is the HFX sidecar metadata file, a small JSON file that travels beside the dataset and declares dataset identity, topology class, row counts, and optional auxiliary artifacts.
In HFX, a dataset is the artifact bundle described by one manifest and its data files.
The manifest describes what the data is.
Runtime parameters live outside the manifest.

## Top-level fields

The manifest is a JSON object.
The schema sets `additionalProperties: false` at the top level, so a manifest with any field outside this table is non-conformant.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `format_version` | string | Required | HFX format version. The schema fixes this to the constant `"0.3.0"`. |
| `fabric_name` | string | Required | Source fabric identifier, the name of the source hydrofabric, a dataset describing a region's river network, this dataset was compiled from. Non-empty. Must match the pattern `^[a-z][a-z0-9_-]*$`, so it starts with a lowercase letter and uses only lowercase letters, digits, hyphens, and underscores. |
| `fabric_version` | string | Optional | Version label of the source fabric. Non-empty when present. |
| `crs` | string | Required | Coordinate reference system, the spatial coordinate system of every coordinate in the dataset. The schema fixes this to the constant `"EPSG:4326"`, the WGS84 longitude and latitude system. |
| `has_up_area` | boolean | Required | Whether the `up_area_km2` column carries computed upstream-area values. When `true`, that column carries computed values where computation is possible, and nulls where it is unavailable. When `false`, every value in that column is null. |
| `topology` | string | Required | Topology class, the shape of the drainage graph. One of `"tree"` or `"dag"`, where `dag` means a directed acyclic graph, a graph whose edges have direction and form no cycles. |
| `region` | string | Optional | Geographic or source-fabric subset label, a free-form name for the covered extent such as `"europe"`. Non-empty when present. |
| `bbox` | array of number | Required | Bounding box, the rectangle that encloses every unit, given as `[minx, miny, maxx, maxy]`. Exactly four plain numbers. |
| `unit_count` | integer | Required | Number of drainage units in the dataset, equal to the row count of `catchments.parquet`. At least `1`. |
| `created_at` | string | Required | Timestamp of the compilation run, an RFC 3339 date-time string, for example `"2026-05-14T00:00:00Z"`. |
| `adapter_version` | string | Required | Version of the adapter, the offline tool that compiled the source fabric into HFX. Non-empty. |
| `auxiliary` | array | Optional | Declared auxiliary data, a list of optional extra artifacts that ship with the dataset. Each element follows the auxiliary entry shape below. |

## Auxiliary entries

Each element of the `auxiliary` array is an object.
The schema sets `additionalProperties: false` on each auxiliary entry, so an entry with any field outside this table is non-conformant.
All three fields are required.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `schema` | string | Required | Auxiliary schema identifier, the name of the format contract this entry follows. Non-empty. |
| `artifacts` | object | Required | Mapping from an artifact key to a file path relative to the dataset root. At least one entry. Every key is a non-empty string and every value is a non-empty string. |
| `metadata` | object | Required | Schema-specific metadata block, a set of fields defined by the auxiliary schema named in `schema`. |

## Example

This minimal conformant manifest matches the tiny conformance example:

```json
{
  "format_version": "0.3.0",
  "fabric_name": "conformance-tiny",
  "crs": "EPSG:4326",
  "has_up_area": false,
  "topology": "tree",
  "bbox": [0.0, 0.0, 2.0, 2.0],
  "unit_count": 5,
  "created_at": "2026-01-01T00:00:00Z",
  "adapter_version": "conformance-fixture-v2"
}
```

This manifest shows optional source-fabric and auxiliary fields:

```json
{
  "format_version": "0.3.0",
  "fabric_name": "example-fabric",
  "fabric_version": "2026.1",
  "crs": "EPSG:4326",
  "has_up_area": true,
  "topology": "tree",
  "region": "europe",
  "bbox": [-25.0, 34.0, 45.0, 72.0],
  "unit_count": 194356,
  "created_at": "2026-05-14T00:00:00Z",
  "adapter_version": "0.2.0",
  "auxiliary": [
    {
      "schema": "hfx.aux.d8_raster.v1",
      "artifacts": {
        "flow_dir": "flow_dir.tif",
        "flow_acc": "flow_acc.tif"
      },
      "metadata": {
        "flow_dir_encoding": "esri"
      }
    }
  ]
}
```

## The machine-readable schema

The enforced machine-readable contract lives at `schemas/manifest.schema.json`.
Use that schema when you need validator behavior, exact required fields, allowed values, and object-closure rules.
For the authoritative prose rules, read section 5, `manifest.json`, of the [specification](HFX_SPEC.md).
