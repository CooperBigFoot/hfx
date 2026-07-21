# M5 lifecycle drill record

## Scope

The operator ran the full workstation-driven Hetzner campaign lifecycle on 2026-07-21 using campaign `m5-drill`. Provision and teardown used the committed scripts from `b5be486`. Bootstrap runs 2 and 3, launch, and smoke used `a9fb648`, the milestone-5 branch head containing the `polars==1.41.1` pin correction. This record captures the completed run; it is not an instruction to recreate live resources.

All timeline timestamps below are UTC on 2026-07-21.

## Campaign parameters

| Parameter | Executed value |
|---|---|
| Campaign | `m5-drill` |
| Server | `hfx-build-m5-drill` |
| Server ID | `153579047` |
| Server IPv4 | `2.28.15.249` |
| Server type | `cx23` shared vCPU |
| Image | `debian-12` |
| Location | `fsn1` |
| Volume | `hfx-build-m5-drill-data` |
| Volume ID | `106426465` |
| Volume size | `10 GB` |

The drill used `cx23` because the project dedicated-core quota was below `16` while the concurrent `ccx33` campaign `grit-d8-m3` occupied the dedicated-core budget. Shared server types bypassed that quota. The deliberately modest `10 GB` volume limited drill cost.

The S3 credential source was a transient mode-`600` environment file built on the workstation from macOS Keychain service `hetzner-object-storage-pourpoint`, using accounts `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Secret values were never displayed. Provision installed the file as `root:root` mode `600` at `/etc/pourpoint-hfx.env`. The transient workstation file was deleted after the drill.

## Timeline and results

### 1. Provision run 1

Provision ran from `13:35:50` to `13:36:36`, lasting `46 s`, and exited `0`. It created server `hfx-build-m5-drill` and volume `hfx-build-m5-drill-data`, formatted the previously unformatted `10 GB` volume with `mkfs.ext4`, mounted it at `/mnt/hfx`, and installed the credential file as `root:root` mode `600` at `/etc/pourpoint-hfx.env`. Captured log: `raw/provision-run1.log`.

### 2. Identical provision rerun

The identical provision command ran from `13:36:52` to `13:36:55`, lasting `3 s`, and exited `0`. It converged without mutation and printed the same resource summary. This proved the provision half of acceptance criterion `8`. Captured log: `raw/provision-run2.log`.

### 3. Bootstrap run 1

Bootstrap ran from `13:37:11` to `13:38:15`, lasting `64 s`, and exited `1`. It failed cleanly at the geo-package stage because `polars==1.41.0` depended on `polars-runtime-32==1.41.0`, which had been yanked from PyPI. `uv` refused resolution. The VM remained intact and the refusal directed the operator to inspect and rerun. Captured log: `raw/bootstrap-run1.log`.

The failure produced delta `M5-D1`, delivered by `PR #121`. That delta changed the pin in `bootstrap.sh` and `README.md` from `polars==1.41.0` to `polars==1.41.1`.

### 4. Bootstrap run 2 after the pin correction

Bootstrap ran from `13:47:34` to `13:51:04`, lasting `3.5 min`, and exited `0`. It converged from the failed state. The apt, `uv`, and AWS CLI stages were already installed and were skipped. It created `/opt/hfx-geo` with the pinned geo package set, installed `rustup 1.28.2` and `Rust 1.88.0`, cloned the public hfx repository anonymously, and built `hfx 0.4.0` with `cargo build --release -p hfx-cli`. It printed the complete frozen-contract summary. Captured log: `raw/bootstrap-run2.log`.

### 5. Bootstrap run 3 idempotency proof

Bootstrap ran from `13:51:23` to `13:51:28`, lasting `5 s`, and exited `0`. Every stage detected an already converged state. This proved the bootstrap half of acceptance criterion `8`. Captured log: `raw/bootstrap-run3.log`.

### 6. Toolchain verification

The source-built CLI check was:

```text
/root/hfx/target/release/hfx --version
```

It printed:

```text
hfx 0.4.0
```

The uv-managed Python geo check was:

```text
/opt/hfx-geo/bin/python -c "import geopandas, rasterio, polars, shapely, pyogrio"
```

It succeeded with `geopandas 1.1.3`, `rasterio 1.5.0`, and `polars 1.41.1`. The verification output was captured in `raw/toolchain-verify.log`. This proved acceptance criterion `4`.

### 7. Smoke launch

At `13:52:00`, this launch exited `0`:

```text
launch.sh --campaign m5-drill start --workload smoke -- /root/hfx/scripts/hetzner/smoke.sh --campaign m5-drill
```

It created detached tmux session `hfx-m5-drill-smoke`. The two volume-backed log paths were:

```text
/mnt/hfx/logs/hfx-m5-drill-smoke.log
/mnt/hfx/logs/hfx-m5-drill-smoke-20260721T135200Z.log
```

The launch output was captured in `raw/launch-start.log`.

### 8. Disconnect survival

The operator attached from the workstation with `launch.sh attach`. During the active download, the attach SSH client process with PID `9102` was killed using `SIGKILL`. An immediate `launch.sh status` showed tmux session `hfx-m5-drill-smoke` still running and `curl` still progressing at approximately `1 MB/s`.

Later, the operator closed the laptop lid completely during the download. The workload completed regardless of both workstation disconnects. This proved acceptance criterion `5` using both an explicit attach-client kill and a full laptop closure.

### 9. Smoke completion

At `14:18:53`, the workload exited `0`. The real NGA `7020000010-streamnet-gpkg` download completed on the attached volume at:

```text
/mnt/hfx/work/downloads/7020000010-streamnet-gpkg
```

The file contained exactly `1,676,398,592 bytes`. Transfer time was `26 min 52 s`. Sustained single-stream throughput was approximately `1.01 MB/s`; `curl` reported a `1014 kB/s` average. The workload verified both the pinned byte count and the SQLite magic header.

The VM uploaded a `20 bytes` test object using the provisioned S3 credentials:

```text
s3://pourpoint-hfx/smoke/m5-drill-20260721T141853030069964Z.txt
```

The completed smoke log was captured in `raw/smoke-final-log.log`. This proved acceptance criteria `2` and `3`.

The observed approximately `1.01 MB/s` single-stream rate matched the M1 probe. NGA acquisition from `fsn1` remained feasible at drill time. The GEOGLOWS fallback was not triggered, and `docs/decisions/2026-07-21-tdx-hydro-pristine-nga-source.md` remained unchanged.

### 10. Teardown

Default teardown ran from `14:51:14` to `14:51:45`, lasting `31 s`, and exited `0`. It detached volume `hfx-build-m5-drill-data`, deleted server `hfx-build-m5-drill`, and deleted the volume. The script printed exactly:

```text
campaign m5-drill has zero Hetzner footprint: server hfx-build-m5-drill absent; volume hfx-build-m5-drill-data absent
```

The output was captured in `raw/teardown.log`.

### 11. Independent zero-footprint verification

At `14:52`, an independent `hcloud server list` showed only `pourpoint-web-1` and the explicitly excluded concurrent campaign server `hfx-build-grit-d8-m3`. An independent `hcloud volume list` showed only that excluded campaign's retained `1 TB` volume, `hfx-build-grit-d8-m3-data`. No `m5-drill` resource remained.

The zero-footprint assessment excluded the concurrent `grit-d8-m3` campaign, including `hfx-build-grit-d8-m3` and `hfx-build-grit-d8-m3-data`, and the pre-existing `pourpoint-web-1` server. Those resources were unrelated to this drill and were left untouched.

### 12. Smoke object cleanup

The exact smoke object was listed at `20 bytes`, removed with `aws s3 rm`, and then the `smoke/` prefix was listed again and found empty. The removed object was:

```text
s3://pourpoint-hfx/smoke/m5-drill-20260721T141853030069964Z.txt
```

Unrelated bucket contents were untouched. This completed the bucket portion of acceptance criterion `6`. The transient credential environment file was also deleted from the workstation.

## Deviations

### Yanked Python package pin

Bootstrap run `1` encountered an external PyPI change after the original pin was chosen. `polars==1.41.0` required the yanked `polars-runtime-32==1.41.0`, so `uv` refused resolution. Delta `M5-D1` in `PR #121` advanced the pin to `polars==1.41.1`. Bootstrap runs `2` and `3` then proved recovery from the preserved failed state and idempotent convergence. The clean refusal, intact VM, and successful rerun also exercised the converge-after-failure design goal.

### Transient workstation DNS failure

After the laptop reopened, one status command encountered:

```text
lookup api.hetzner.cloud: no such host
```

The retry succeeded. VM-side operations were unaffected.

## Acceptance criteria map

| Criterion | Evidence | Result |
|---|---|---|
| `1` | The workstation drove provision, bootstrap, smoke, and teardown through the committed scripts. | Done |
| `2` | NGA file `7020000010-streamnet-gpkg` completed on the volume at `1,676,398,592 bytes` in `26 min 52 s`, approximately `1.01 MB/s` single stream. | Done |
| `3` | The VM uploaded `s3://pourpoint-hfx/smoke/m5-drill-20260721T141853030069964Z.txt` with its installed credentials. | Done |
| `4` | `/root/hfx/target/release/hfx --version` printed `hfx 0.4.0`; imports from `/opt/hfx-geo` succeeded. | Done |
| `5` | Tmux session `hfx-m5-drill-smoke` survived `SIGKILL` of attach-client PID `9102` and full laptop closure. | Done |
| `6` | Default teardown removed the campaign server and volume; independent `hcloud` checks confirmed campaign-scoped absence; the exact smoke object was removed and the `smoke/` prefix was empty. | Done |
| `7` | The scripts and README were already committed and cold-reader runnable in milestones M2 through M4. | Delivered by M2-M4 |
| `8` | The identical provision rerun converged in `3 s`; bootstrap run `3` converged in `5 s`. | Done |

## Conclusion

NGA feasibility from Hetzner `fsn1` was confirmed at drill time. The observed approximately `1.01 MB/s` single-stream throughput matched the M1 probe. The GEOGLOWS fallback was not triggered. The standing source decision in `docs/decisions/2026-07-21-tdx-hydro-pristine-nga-source.md` remains unchanged.
