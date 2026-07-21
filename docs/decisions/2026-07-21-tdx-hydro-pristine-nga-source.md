# TDX-Hydro compiles from pristine NGA source, not the GEOGLOWS derivative

**Status:** Accepted

**Date:** 2026-07-21

## Context

The TDX-Hydro planetary HFX build (Program #103) needs a source distribution.
NGA publishes the pristine product on earth-info.nga.mil as per-basin
GeoPackages behind a one-file-at-a-time download endpoint. GEOGLOWS v2
redistributes a modified TDX-Hydro on AWS open data that is far easier to bulk
download, but with simplified headwater geometry, added attributes, and
renumbered identifiers.

## Decision

The adapter compiles the pristine NGA TDX-Hydro distribution. `fabric_name` /
`fabric_version` identify NGA's product.

## Why

The adapter contract is to compile a named source fabric faithfully; shipping a
derivative's altered geometry and attributes under the TDX-Hydro name would
misdescribe the fabric. Acquisition convenience was the only advantage of the
GEOGLOWS copy, and reversing this choice later means a full planetary rebuild
plus a ~2 TB re-upload. GEOGLOWS remains the documented fallback acquisition
channel if the NGA endpoint proves impractical at 62-basin scale, as a revision
of this record.

## fsn1 NGA reachability probe

The operator ran the reachability probe on 2026-07-21 from a throwaway Hetzner
VM in fsn1.

### Procedure and VM

The VM was provisioned with:

```text
hcloud server create --name hfx-nga-probe --type cx23 --image debian-12 --location fsn1 --ssh-key nicolas-workstation
```

The resulting server was `hfx-nga-probe`, server ID 153499853, IPv4
2.28.13.28, running Debian GNU/Linux 12 (bookworm) on x86_64 in fsn1. The
workstation used hcloud CLI v1.66.0 with active context `pourpoint`. It retrieved
the token non-interactively from macOS Keychain with
`security find-generic-password -s hetzner-cloud-pourpoint -a pourpoint-bootstrap -w`;
the token was never displayed. SSH key authentication as root became available
~30 s after server creation.

### Endpoint observations

The confirmed URL pattern was
`https://earth-info.nga.mil/php/download.php?file=<processing-basin-id>-<product>-gpkg`.
Observed products were `basins` and `streamnet`. Responses were `HTTP/1.1 200 OK`
with `Content-Type: application/octet-stream`. The observed remote IP from fsn1
was 214.28.196.146.

The endpoint has no HTTP range support. A `Range: bytes=0-0` request returned
200 with the full body and no `Accept-Ranges` header. Each download is
all-or-nothing. A single file cannot be resumed or transferred in segmented
aria2-style parts.

Every request from the VM returned 200 with real payload bytes. No geo-blocking
was observed from the Hetzner fsn1 network.

The endpoint reported these `Content-Length` values:

| File | Bytes | Approximate size |
|---|---:|---:|
| `7020000010-basins-gpkg` | 5907767296 | 5.9 GB |
| `1020000010-basins-gpkg` | 6979305472 | 7.0 GB |
| `7020000010-streamnet-gpkg` | 1676398592 | 1.68 GB |

### Throughput and completeness

1. A long single-stream download of `7020000010-basins-gpkg` transferred
   920,979,956 bytes in 900.0 s before the curl `--max-time 900` cutoff. It
   sustained 1.02 MB/s over 15 min and returned `http_code=200`.
2. Three concurrent 60 s streams for different files measured 5.86, 6.44, and
   5.47 MB/s per stream, for an aggregate of ~17.8 MB/s.
3. A single-stream 60 s re-test immediately afterward measured 0.99 MB/s.
4. The full-file completeness proof downloaded
   `7020000010-streamnet-gpkg` completely: 1,676,398,592 bytes in 1687.7 s,
   sustaining 0.99 MB/s over 28 min with `http_code=200`. Its on-disk size
   exactly matched `Content-Length`, and the file began with the
   `SQLite format 3` magic for a valid GeoPackage container.

Per-connection throughput was erratic, with ~1 MB/s on some connections and
~6 MB/s on others. Concurrent connections multiplied aggregate throughput to
~18 MB/s over 3 streams. At 62-basin scale, where observed
basin files were 5.9 to 7.0 GB, serial single-stream acquisition would be
impractical. Modest per-file parallelism of 3+ concurrent files puts the full
distribution within roughly a day of transfer time. Per-file parallelism is
the feasible acquisition strategy. Per-file segmentation and resume are
unavailable because the endpoint has no range support, so an interrupted
multi-GB download restarts from zero.

### Conclusion and cleanup

NGA earth-info.nga.mil is reachable and feasible from Hetzner fsn1. The probe
observed no geo-blocking, no connection resets, and successful completion of a
multi-GB file. Throughput is the operational constraint. Pristine NGA remains
the selected source; the GEOGLOWS fallback was not triggered.

The operator ran `hcloud server delete hfx-nga-probe`, which deleted server
153499853. Post-teardown, `hcloud server list` showed only the pre-existing
`pourpoint-web-1`, and `hcloud volume list` was empty. The probe left zero
Hetzner footprint.
