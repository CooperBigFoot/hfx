# Hetzner build campaigns

## Purpose and campaign concept

A build campaign is one `provision -> bootstrap -> wrangle -> deliver -> teardown`
lifecycle. The full lifecycle is the unit of cost accounting and teardown
discipline. Nothing durable survives on the VM. Inputs are re-fetchable,
outputs are re-creatable, and delivered artifacts live outside the VM before
teardown.

Completed evidence: [TDX-Hydro processing basin 7020000010 pilot](CAMPAIGN-tdx-hydro-7020000010.md) and [three-basin TDX-Hydro assembly subset](CAMPAIGN-tdx-hydro-assembly-subset.md).

The scripts in this directory compose one lifecycle and remain independently
runnable. Campaign identifiers use 1-32 lowercase letters, digits, or hyphens
and begin with a letter or digit. The deterministic resource names are
`hfx-build-<campaign>` for the server and
`hfx-build-<campaign>-data` for the volume.

## Prerequisites

Install the `hcloud`, `jq`, `security`, and `ssh` commands on the workstation.
Configure the Hetzner CLI with the fixed `pourpoint` context. The lifecycle
scripts retrieve the project token from macOS Keychain, keep it on the
workstation, and invoke `hcloud` through that context.

Use this exact command as a private operator diagnostic:

```bash
security find-generic-password -s hetzner-cloud-pourpoint -a pourpoint-bootstrap -w
```

macOS may display a Keychain approval dialog when this command or a lifecycle
script retrieves the token. Inspect the requesting executable, approve access
for the current invocation, and return to the terminal. The diagnostic command
prints the token. Run it only in a private terminal and clear the display
afterward. Routine scripts capture the value with xtrace disabled and never
place it on the VM.

Obtain an operator-managed local S3 environment file from the operator's
password manager. It must contain shell assignments for exactly these expected
credential names:

```text
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
```

`provision.sh --s3-env-file <path>` transfers the file and installs it as
`root:root` mode `600` at `/etc/pourpoint-hfx.env`. Values stay outside the
repository and command examples. Register the `nicolas-workstation` SSH key in
the Hetzner project.

Workstation lifecycle scripts support Bash `>=3.2`. The merged portability fix
achieved this floor by replacing the Bash 4 associative-array requirement.
Remote Debian-only constructs remain on the VM.

## Script reference

All workstation lifecycle scripts accept `-h` or `--help` only as the sole
argument and exit 0 after printing usage. Infrastructure option values accept
ASCII letters, digits, dot, underscore, and hyphen. Options that take values
reject missing or option-shaped values. Duplicate options, unknown options,
and positional arguments are rejected unless a synopsis explicitly supplies a
command argv.

### `provision.sh`

```text
Usage: provision.sh --campaign <id> --s3-env-file <path> [options]
```

Options may appear in any order.

| Option | Requirement or default |
|---|---|
| `--campaign <id>` | Required |
| `--s3-env-file <path>` | Required |
| `--server-type <type>` | Optional; `ccx33` |
| `--volume-size-gb <integer>` | Optional; `100` GB |
| `--ssh-key <name>` | Optional; `nicolas-workstation` |
| `--image <name>` | Optional; `debian-12` |
| `--location <name>` | Optional; `fsn1` |
| `-h`, `--help` | Sole-argument help |

The volume size must be a positive base-10 integer. The S3 source must be a
readable, nonempty regular file and cannot be a symlink. Provisioning validates
the location and server type, selects exactly one named x86 system image, and
requires one exactly named registered SSH key. It finds resources by exact
deterministic name and validates their IDs, campaign labels, identity fields,
location, server type, image, key, size, and attachment.

The script creates an absent server and volume, waits for or powers on the
server, waits for SSH, attaches the volume, and verifies the reported device.
It creates ext4 only on a device with no recognized signatures. It reuses a
verified ext4 filesystem, creates or verifies the UUID-based fstab entry,
mounts it at `/mnt/hfx`, and verifies the expected block device backs the
mount. Hetzner volumes can grow online and cannot shrink. The provisioner grows
a smaller volume and its ext4 filesystem, retains an already larger campaign
volume, and leaves an equal volume unchanged. It reinstalls the credential file
at `/etc/pourpoint-hfx.env` on every successful run.

Success prints the campaign, server name, volume name, server IPv4 address,
retained volume size in GB, mount point, and credential path.

Safe re-run classification: **converge-or-refuse**. An absent or matching
campaign converges. A matching powered-off server is powered on, the matching
volume is attached, its verified filesystem is remounted, eligible growth is
completed, and credentials are reinstalled. The script refuses malformed or
duplicate CLI input; unsafe S3 source files; missing local commands or
credentials; invalid locations, types, images, or SSH keys; ambiguous exact
resource or key matches; identity or label conflicts; location, type, image,
key, attachment, device, filesystem, mount, or fstab conflicts; and every
shrink request implicit in cloud volume limitations by retaining the larger
volume. It also refuses malformed cloud responses, readiness failures, and
create, attach, resize, filesystem, credential, or verification failures.
Diagnostics direct the operator to inspect and rerun. Failures after resource
creation retain resources for inspection and a safe rerun.

### `bootstrap.sh`

```text
Usage: bootstrap.sh --campaign <id>
```

| Option | Requirement |
|---|---|
| `--campaign <id>` | Required; accepted once |
| `-h`, `--help` | Sole-argument help |

Bootstrap requires `ssh`, authenticates through the fixed context, finds the
exact campaign server, and validates its ID, name, management labels, role,
campaign label, and public IPv4 address. It streams remote SSH output to the
operator. The remote process verifies the mounted read-write campaign volume,
creates the root-owned workspace, download, and log directories, converges
Debian packages, pinned toolchains, and the exact Python geo environment, and
then converges an anonymous checkout to `origin/main`. It resets the checkout
to that remote branch, builds `hfx-cli` in release mode, runs its version
command, and performs final verification of the mount, directories, tools,
versions, packages, checkout, and binary. Bootstrap never touches
`/etc/pourpoint-hfx.env`.

Pinned versions are:

| Component | Version or exact set |
|---|---|
| uv | `0.8.22` |
| Python | `3.12.11` |
| rustup | `1.28.2` |
| Rust toolchain, rustc, cargo | `1.88.0` |
| Python geo packages | `geopandas==1.1.3`, `geoparquet-io==1.0.0b2`, `numpy==2.4.6`, `pandas==3.0.3`, `polars==1.41.1`, `pyarrow==22.0.0`, `pyogrio==0.12.1`, `rasterio==1.5.0`, `rio-cogeo==7.0.2`, `shapely==2.1.2` |

Success prints the frozen paths, installed tool versions, built CLI version,
and `campaign <campaign> bootstrap complete`.

Safe re-run classification: **converge-or-refuse**. Exact installed packages
and versions are reused. Missing packages and toolchains are installed;
bootstrap-owned directories are recreated or verified; the anonymous origin
URL and checkout are repaired where supported; and the release binary is
rebuilt. Bootstrap refuses an absent or incorrectly identified server, an
invalid campaign mount, unexpected preexisting paths, wrong managed versions,
failed downloads or package operations, incompatible architecture, checkout
or build failures, and failed final verification. A wrong or malformed Python
environment requires operator inspection and removal of that bootstrap-owned
path before rerun.

### `launch.sh`

The four exact forms are:

```text
launch.sh --campaign <id> start --workload <name> -- <command> [argument ...]
launch.sh --campaign <id> attach --workload <name>
launch.sh --campaign <id> tail [--log <basename>]
launch.sh --campaign <id> status --workload <name>
```

`--campaign` must precede the subcommand. `start`, `attach`, and `status`
require exactly one `--workload <name>` in the displayed position. `start`
requires the literal `--` and at least one nonempty command argument. `attach`
and `status` accept nothing after the workload. `tail` accepts either no
further arguments or one exact `--log <basename>` pair. Workload names follow
the campaign grammar: 1-32 lowercase letters, digits, or hyphens, beginning
with a letter or digit.

`start` creates detached tmux session `hfx-<campaign>-<workload>`. It tees the
command's combined output to canonical log
`/mnt/hfx/logs/hfx-<campaign>-<workload>.log` and timestamped log
`/mnt/hfx/logs/hfx-<campaign>-<workload>-YYYYMMDDTHHMMSSZ.log`. It prints the
session, canonical log, and timestamped run log paths. The logs include start
time, shell-quoted command, finish time, and command exit status.

`attach` targets the exact running session. Default `tail` selects the newest
timestamped log for the campaign and follows it from its last 50 lines. Named
tail accepts only a regular, non-symlink canonical or timestamped campaign log
whose basename matches the exact campaign. `status` reports whether the exact
session is running and prints the last 20 canonical-log lines when available.
Its exit status is 0 for running and 3 for absent or finished.

Safe re-run classification: **converge-or-refuse**. `start` refuses an existing
exact session, timestamp collision, unsafe canonical or named log path, missing
root-owned bootstrap log directory, and missing tmux. It never replaces a
running session. Identity validation, malformed command shapes, invalid names,
an absent campaign server, and SSH or remote failures also cause refusal.
`attach`, `tail`, and `status` operate only on their exact validated targets.

### `smoke.sh`

```text
Usage: smoke.sh --campaign <id>
```

The accepted invocation is exactly two arguments, `--campaign <id>`. Sole
`-h` or `--help` prints usage. Smoke requires root and the commands `curl`,
`aws`, `stat`, `od`, `tr`, `mv`, `rm`, `date`, and `chmod`.

The pinned NGA source is
`https://earth-info.nga.mil/php/download.php?file=7020000010-streamnet-gpkg`.
The expected size is `1676398592` bytes and the first 16 bytes must have SQLite
magic `53514c69746520666f726d6174203300`. Downloads use:

```text
final:   /mnt/hfx/work/downloads/7020000010-streamnet-gpkg
partial: /mnt/hfx/work/downloads/7020000010-streamnet-gpkg.partial
```

After verifying the real NGA stream-network GeoPackage, smoke loads
`/etc/pourpoint-hfx.env`, which must be a nonempty regular non-symlink owned
`root:root` with mode `600`. It requires nonempty `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`. It uploads `hfx lifecycle smoke` to endpoint
`https://fsn1.your-objectstorage.com`, region `fsn1`, bucket
`pourpoint-hfx`, with object key
`smoke/<campaign>-YYYYMMDDTHHMMSSNNNNNNNNNZ.txt`. Each successful run therefore
creates a small timestamped object under `s3://pourpoint-hfx/smoke/`.

Successful output first includes one of these file-state lines:

```text
smoke: NGA download is already complete and verified at /mnt/hfx/work/downloads/7020000010-streamnet-gpkg
smoke: NGA download completed and verified at /mnt/hfx/work/downloads/7020000010-streamnet-gpkg
```

It then prints these recordable results:

```text
smoke: verified NGA file: /mnt/hfx/work/downloads/7020000010-streamnet-gpkg
smoke: verified NGA bytes: 1676398592
smoke: uploaded: s3://pourpoint-hfx/smoke/<campaign>-<timestamp>.txt
smoke: success
```

Safe re-run classification: **converge-or-refuse**. Smoke reuses the final
download only after exact size and SQLite-header verification. It removes a
final file after a proven size or header failure, removes ordinary incomplete
partial files before a fresh complete transfer, and removes failed or invalid
fresh partials when safe. Every success uploads a new timestamped object. It
refuses unsafe download or credential paths, unverifiable files, unavailable
commands, missing root privileges, unsafe or unloadable credentials, missing
or empty required AWS variables, download or atomic-install verification
failures, and S3 upload failure.

### `tdx-hydro-campaign.sh`

The TDX-Hydro campaign runner manages one 62-basin workspace. It requires Bash
`>=3.2`. When the harness can execute `/bin/bash`, it deliberately selects that
interpreter and reports its version so a newer Bash earlier on `PATH` cannot
hide a portability failure. An older Bash exits with
`hfx: error: Bash >=3.2 is required; observed <version>`; a non-Bash
interpreter exits with
`hfx: error: Bash >=3.2 is required; observed non-Bash interpreter`.

The accepted command forms are:

```text
tdx-hydro-campaign.sh init --campaign <id> [--workspace-root <path>] --available-memory-bytes <integer> --available-disk-bytes <integer> --retained-input-bytes <integer> --retained-basin-output-bytes <integer> --assembly-memory-ceiling-bytes <integer> --assembly-scratch-ceiling-bytes <integer> --assembled-artifact-bytes <integer>
tdx-hydro-campaign.sh status --campaign <id> [--workspace-root <path>]
tdx-hydro-campaign.sh recover --campaign <id> [--workspace-root <path>]
tdx-hydro-campaign.sh acquire --campaign <id> [--workspace-root <path>] --max-parallel <integer>
tdx-hydro-campaign.sh compile --campaign <id> [--workspace-root <path>] --fabric-version <value>
tdx-hydro-campaign.sh evidence --campaign <id> [--workspace-root <path>]
tdx-hydro-campaign.sh publish --campaign <id> [--workspace-root <path>] --out <dataset-dir> --report <path> --notice <path> --citation <path> --scratch-prefix <prefix>
```

`--workspace-root` defaults to `/mnt/hfx/work`, giving campaign directory
`/mnt/hfx/work/tdx-hydro-<campaign>`. All seven `init` byte values are required,
positive base-10 integers within the signed 64-bit range. The feasibility
checks are:

```text
required_memory_bytes = assembly_memory_ceiling_bytes
required_disk_bytes = retained_input_bytes
                    + retained_basin_output_bytes
                    + assembly_scratch_ceiling_bytes
                    + assembled_artifact_bytes
available_memory_bytes >= required_memory_bytes
available_disk_bytes >= required_disk_bytes
```

Insufficient capacity is reported as
`hfx: error: insufficient memory: available <available> bytes; required <required> bytes`
or
`hfx: error: insufficient disk: available <available> bytes; required <required> bytes`.
Re-running `init` for an existing campaign requires the same normalized sizing
values and the same 62-basin inventory. Changed parameters require a new
campaign ID.

The persisted policy is `retain-all-through-publication`. Acquired final
GeoPackages, per-basin outputs, and external diagnostic reports remain on the
campaign volume. Acquisition retains an invalid final for inspection and
removes only its ordinary incomplete `.partial` file before a complete-file
retry. A resumed acquisition re-verifies each retained final's byte count,
SHA-256, SQLite identity, layer name, and persisted evidence. A resumed compile
re-validates each succeeded dataset and external report. Existing failed or
conflicting compile artifacts are retained for inspection.

The runner phases are:

1. `init` creates the fixed campaign layout and records sizing, inventory, and
   retention policy.
2. `acquire --max-parallel <1-62>` performs bounded parallel work across
   basins. Each product transfer remains a complete GET.
3. `compile --fabric-version <value>` invokes one isolated adapter build per
   basin after both products succeed. It retains
   `basin-outputs/<basin>` and `reports/<basin>-build-report.json`.
4. `evidence` writes deterministic
   `publication/evidence/acquisition.json`,
   `publication/evidence/outcomes.json`, and
   `publication/evidence/diagnostics.json`.
5. After assembly by a separate repository entrypoint, `publish` accepts the
   assembled dataset directory as opaque input. The runner inventories
   nonempty regular files and uploads the exact persisted inventory; it does
   not invoke assembly or validate dataset semantics.

`status` validates state and prints sizing plus deterministic per-stage counts.
`recover` changes interrupted `running` stages back to `pending`; `acquire` and
`compile` also perform the applicable recovery before work. Operational
recovery always re-runs the same campaign with the same sizing, inventory,
parallelism, fabric version, paths, attribution inputs, and scratch prefix.
Do not sweep resources, enumerate by name pattern or label selector, or perform
opportunistic cleanup. Inspect and retry only the exact campaign resources and
retained paths named by the diagnostic.

On the VM, compile defaults to Python `/opt/hfx-geo/bin/python`, adapter script
`/root/hfx/adapters/tdx-hydro/build_adapter.py`, and validator
`/root/hfx/target/release/hfx`. The corresponding explicit overrides are
`HFX_TDX_ADAPTER_PYTHON`, `HFX_TDX_ADAPTER_SCRIPT`, and `HFX_TDX_HFX`.
Bootstrap fetches `origin/main` and resets the whole tracked `/root/hfx`
checkout to it. Future repository files merged to `origin/main` are therefore
staged on the VM by bootstrap without maintaining an explicit adapter file
list.

Use `launch.sh` to keep each long phase in a detached session. These are
templates only; replace every angle-bracketed value with the approved campaign
parameters. They document usage and do not authorize provisioning:

```bash
./scripts/hetzner/launch.sh --campaign <campaign> start --workload tdx-init -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh init \
  --campaign <campaign> \
  --available-memory-bytes <available-memory-bytes> \
  --available-disk-bytes <available-disk-bytes> \
  --retained-input-bytes <retained-input-bytes> \
  --retained-basin-output-bytes <retained-basin-output-bytes> \
  --assembly-memory-ceiling-bytes <assembly-memory-ceiling-bytes> \
  --assembly-scratch-ceiling-bytes <assembly-scratch-ceiling-bytes> \
  --assembled-artifact-bytes <assembled-artifact-bytes>

./scripts/hetzner/launch.sh --campaign <campaign> start --workload tdx-acquire -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh acquire \
  --campaign <campaign> --max-parallel <parallelism>

./scripts/hetzner/launch.sh --campaign <campaign> start --workload tdx-compile -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh compile \
  --campaign <campaign> --fabric-version <fabric-version>

./scripts/hetzner/launch.sh --campaign <campaign> start --workload tdx-evidence -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh evidence \
  --campaign <campaign>

./scripts/hetzner/launch.sh --campaign <campaign> start --workload tdx-publish -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh publish \
  --campaign <campaign> \
  --out <absolute-assembled-dataset-dir> \
  --report <absolute-external-build-report> \
  --notice <absolute-notice-path> \
  --citation <absolute-citation-path> \
  --scratch-prefix scratch/tdx-hydro-<campaign>/<artifact>
```

Publication requires a nonempty external report, NOTICE, and CITATION file.
`--report` must resolve outside `--out`. The scratch prefix must match
`scratch/tdx-hydro-<campaign>/<artifact>`, so publication remains outside the
delivery prefix `hfx/`. A directory is a campaign directory when it contains
`state/campaign.json`. The content-based privacy guard refuses `--out` when it
is a campaign directory, is inside another campaign directory, or contains
another campaign directory at any descendant depth.

Before default teardown, copy the campaign record, the three deterministic
evidence documents, required reports, and relevant logs off the VM. The
committed campaign record must capture the successful teardown line exactly:

```text
hfx: campaign <campaign> has zero Hetzner footprint: server hfx-build-<campaign> absent; volume hfx-build-<campaign>-data absent
```

`teardown.sh` emits this line today after its exact-name absence checks. The
record must also include independent server and volume listings filtered by
exact name, with both result arrays empty:

```bash
hcloud --context pourpoint server list -o json |
  jq --arg name 'hfx-build-<campaign>' '[.[] | select(.name == $name)]'
hcloud --context pourpoint volume list -o json |
  jq --arg name 'hfx-build-<campaign>-data' '[.[] | select(.name == $name)]'
```

The expected output from each independent listing is `[]`. These listings are
evidence checks, not cleanup discovery. The unrelated `grit-d8-m3` volume is
intentionally retained and is an operator invariant: never inspect it as a
campaign target, detach it, resize it, delete it, or include it in cleanup.

### `teardown.sh`

```text
Usage: teardown.sh --campaign <id> [--keep-volume]
```

Options may appear in either order.

| Option | Requirement or effect |
|---|---|
| `--campaign <id>` | Required; accepted once |
| `--keep-volume` | Optional; retain the detached campaign volume; accepted once |
| `-h`, `--help` | Sole-argument help |

Default teardown resolves only the exact deterministic server and volume
names, validates their campaign ownership and labels, detaches the volume,
deletes the server and volume, waits for absence, and verifies zero footprint
for that exact campaign. `--keep-volume` deletes the server and verifies that
the original campaign volume remains detached. If no volume existed, the
retention result explicitly reports that state. Re-running after partial or
complete teardown converges from absent resources.

Success reports either zero footprint, a retained detached volume, or the
absence of a volume to retain.

Safe re-run classification: **converge-or-refuse**. Teardown refuses malformed
or duplicate arguments; ownership, identity, description, or label validation
failures; malformed or changed resource IDs; a volume attached to a different
server; an orphan attachment that requires manual ownership confirmation;
detach or deletion failure; timeout waiting for detach or absence; and final
zero-footprint or retained-volume verification failure. Destructive actions
are scoped to exact deterministic names and validated campaign labels. The
script revalidates identity during bounded state changes.

## Composed lifecycle walkthrough

Run this drill from the repository root:

```bash
./scripts/hetzner/provision.sh \
  --campaign m5-drill \
  --server-type cx23 \
  --s3-env-file /absolute/private/path/pourpoint-hfx.env

./scripts/hetzner/bootstrap.sh --campaign m5-drill

./scripts/hetzner/launch.sh --campaign m5-drill start --workload smoke -- \
  /root/hfx/scripts/hetzner/smoke.sh --campaign m5-drill

./scripts/hetzner/launch.sh --campaign m5-drill status --workload smoke
./scripts/hetzner/launch.sh --campaign m5-drill attach --workload smoke
./scripts/hetzner/launch.sh --campaign m5-drill tail

./scripts/hetzner/teardown.sh --campaign m5-drill
```

`cx23` is the drill recommendation because its shared vCPU avoids the project
dedicated-core quota, which is below 16. The drill establishes lifecycle
mechanics and does not profile production build performance. `attach` applies
while the session is running. `tail` follows persisted output. A status exit of
3 means the session has finished or is absent. Record or remove the uploaded
smoke object separately according to campaign procedure.

The sanctioned pause command is:

```bash
./scripts/hetzner/teardown.sh --campaign m5-drill --keep-volume
```

Resume the same campaign by provisioning with the same identity parameters.
When requesting a volume-size change, specify a size no smaller than the
retained volume. The provisioner reattaches and verifies the retained
filesystem.

## Remote conventions

The frozen remote contract is:

```text
mount:                /mnt/hfx
workspace:            /mnt/hfx/work
downloads:            /mnt/hfx/work/downloads
logs:                 /mnt/hfx/logs
checkout:             /root/hfx
built CLI:            /root/hfx/target/release/hfx
Python environment:   /opt/hfx-geo
uv:                   /usr/local/bin/uv
rustup:               /root/.cargo/bin/rustup
cargo:                /root/.cargo/bin/cargo
repository URL:       https://github.com/CooperBigFoot/hfx.git
repository branch:    origin/main
tmux:                 hfx-<campaign>-<workload>
canonical log:        /mnt/hfx/logs/hfx-<campaign>-<workload>.log
timestamped log:      /mnt/hfx/logs/hfx-<campaign>-<workload>-YYYYMMDDTHHMMSSZ.log
smoke session:        hfx-<campaign>-smoke
smoke log:            /mnt/hfx/logs/hfx-<campaign>-smoke.log
credential file:      /etc/pourpoint-hfx.env
```

The repository checkout is anonymous and converges to `origin/main`. Generic
workloads use 1-32 lowercase letters, digits, or hyphens and begin with a
letter or digit. The smoke workload uses literal workload `smoke`.

## Monitoring and disconnect safety

`launch.sh start` creates a detached tmux session and tees output to canonical
and timestamped files on the attached volume. The build survives SSH client
death. Operators can disconnect, query status, reattach to the exact live
session, or follow persisted output.

```bash
./scripts/hetzner/launch.sh --campaign m5-drill status --workload smoke
./scripts/hetzner/launch.sh --campaign m5-drill attach --workload smoke
./scripts/hetzner/launch.sh --campaign m5-drill tail
./scripts/hetzner/launch.sh --campaign m5-drill tail \
  --log hfx-m5-drill-smoke.log
```

Status 0 means the exact session is running. Status 3 means it is absent or
finished. The canonical log always names the campaign and workload and is
reinitialized by each successful start. Each start also creates a distinct UTC
timestamped log. Default tail follows the newest timestamped campaign log;
named tail can follow either approved form.

## Cost envelope

Campaign cost is the sum of server runtime, volume storage, and billable
traffic under current Hetzner terms. Verify every amount immediately before a
campaign:

```text
ccx33 server hourly price: <VERIFY IN CONSOLE>
cx23 server hourly price: <VERIFY IN CONSOLE>
volume price per GB-month: <VERIFY IN CONSOLE>
included traffic allowance: <VERIFY IN CONSOLE>
traffic overage price: <VERIFY IN CONSOLE>
```

`ccx33` is the default dedicated server component. `cx23` is the shared drill
component. Chosen volume size and retention time determine storage cost.
Operators must verify current prices in the Hetzner console immediately before
a campaign.

Default teardown is the cost-control mechanism. A completed campaign has no
server or campaign volume. `--keep-volume` pauses a campaign at volume-only
cost by deleting the server and retaining the detached volume. A retained
volume continues accruing the console-verified per-GB-month charge until a
later default teardown deletes it.

## NGA acquisition notes

The 2026-07-21 probe from Hetzner `fsn1` found no HTTP range support. A range
request returned the full body, and the response supplied no `Accept-Ranges`
header. Each file transfer is all-or-nothing. An interrupted transfer restarts
the complete file from zero. Segmented per-file downloads and resume are
unavailable.

Per-connection throughput was erratic at roughly `1-6 MB/s`. Parallel requests
for separate files multiplied aggregate throughput. At 62-basin scale, use
modest per-file parallelism across separate files. Do not segment a single
file. Observed basin GeoPackages were `5.9-7.0 GB` each. The observed
stream-network GeoPackage was `1.68 GB`, with an exact recorded size of
`1,676,398,592` bytes.

The endpoint was reachable from `fsn1`, returned real payloads, completed the
full stream-network file, and showed no observed geo-blocking during the probe.
Plan disk capacity for complete files plus active partials and retained
downloads. Expect retries to consume a complete-file transfer. Keep downloads
on the attached volume and use campaign logs to preserve transfer evidence
across disconnects. The smoke script intentionally removes an incomplete
partial and begins a complete transfer on rerun because resume is unavailable.

## Troubleshooting

| Symptom | Cause | Remedy |
|---|---|---|
| Image value is ambiguous or rejected | Hetzner exposes architecture-duplicate names for system images. The provisioner selects the unique x86 system image because campaign types and bootstrap target x86_64. Arm `cax*` support is outside this campaign contract. | Correct the image value so it has one exact named x86 system-image match. |
| Dedicated-core quota refusal | The project dedicated-core quota is below 16. | Use a shared type such as `cx23`, wait for other campaigns to release dedicated cores, or have a human raise the quota in the Hetzner console. |
| Filesystem setup refusal | The live fixes use unambiguous `wipefs --output TYPE` and trim `lsblk` device identifiers before comparison with `findmnt`; these fixes are shipped. The observed device, mount, signatures, or fstab state remains unsafe. | Retain resources. Inspect the reported block device, mount, signatures, and fstab. Correct the condition and rerun provisioning. Never erase an unrecognized signature. |
| Credential metadata or required variable failure | The installed environment file is absent, unsafe, unloadable, or incomplete. | Correct the operator-managed source, rerun provisioning to reinstall `/etc/pourpoint-hfx.env`, then rerun smoke. |
| Missing bootstrap state | Required directories, packages, or tools are absent. | Rerun the idempotent bootstrap, then retry launch or smoke. |
| Duplicate workload session | The exact tmux session already exists. | Use `attach`, `status`, or `tail`; start again after the exact session finishes. |
| NGA transfer interruption | NGA lacks range support. | Expect a complete-file restart. Use parallelism across separate files for a larger acquisition campaign. |
| Safe teardown refusal | Exact-name ownership, labels, IDs, or attachment state failed validation. | Inspect only the exact campaign resource names and labels named by the diagnostic, correct ownership or attachment state, and rerun. |

Scope zero-footprint verification to the current campaign's deterministic
server and volume names. `pourpoint-web-1` and concurrently running resources
of other campaigns are never counted in the current campaign's zero-footprint
check. This exclusion keeps unrelated, intentionally live infrastructure
outside the teardown result.
