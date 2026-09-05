# Hetzner build campaigns

## Purpose and campaign concept

A build campaign is one `provision -> bootstrap -> wrangle -> deliver -> teardown`
lifecycle. The full lifecycle is the unit of cost accounting and teardown
discipline. Nothing durable survives on the VM. Inputs are re-fetchable,
outputs are re-creatable, and delivered artifacts live outside the VM before
teardown.

Completed evidence: [TDX-Hydro processing basin 7020000010 pilot](CAMPAIGN-tdx-hydro-7020000010.md), [three-basin TDX-Hydro assembly subset](CAMPAIGN-tdx-hydro-assembly-subset.md), and [55-basin TDX-Hydro planetary campaign](CAMPAIGN-tdx-hydro-planetary.md).

Operator runbook: [acquire the seven absent TDX-Hydro basins plus the frozen control](RUNBOOK-tdx-hydro-seven-basin-acquisition.md) (acquisition only).

Compile and extension runbook: [compile the seven absent basins with the corrected adapter, prove the planetary control rebuild byte for byte and the corrected control against the maintainer-adjudicated outlet difference, extend the frozen 55-basin artifact, and preserve everything before exact-resource teardown](RUNBOOK-tdx-hydro-seven-basin-compile.md). Every campaign parameter comes from its contract block, or from `rehearsal-campaign-contract.json` for the rehearsal lifecycle that must pass before the next production lifecycle. Its helpers are `verify-compile-runbook.sh` (contract drift, rehearsal record, passing rehearsal result), `verify-campaign-inputs.sh` (evidence root, opaque credential file, hcloud context, hcloud JSON shape), `hcloud-identity.jq` (the one projection from `hcloud describe` JSON to an exact resource identity, proved read-only before provisioning and applied again after it; recorded CLI output lives under `fixtures/hcloud/`), `price-preflight.sh` (current-price cost projection and gates), `compare-dataset-trees.sh` (SHA-256 comparison of two dataset trees), `compose-campaign-driver.py` (embeds the runbook's fences verbatim into one driver with a fence proof; full, preflight, and resume modes), `campaign-dry-run.sh` (executes the whole composed driver locally under the rehearsal contract with the cloud and the VM shimmed; its record is a precondition of every lifecycle), `prepare-rehearsal-campaign.sh` (builds the rehearsal evidence root from the synthetic corpus), `seven-basin-control-adjudication.json` (the pinned adjudicated difference between the corrected and planetary control builds), `adapters/tdx-hydro/compare_unit_outlets.py` (per-unit comparison of two builds of one basin against that record, or derivation of such a record), and `adapters/tdx-hydro/synthesize_rehearsal_corpus.py` (the tiny synthetic corpus).

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
time, shell-quoted command, finish time, and command exit status. The
runner closes its output and waits for its `tee` before it exits, so the
finish record is in both logs before tmux can destroy the pane; on 2026-09-04
rehearsal run 2 lost that line when the pane died first, and the operator
driver read the workload as unfinished.

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
tdx-hydro-campaign.sh init --campaign <id> [--workspace-root <path>] [--basin <processing-basin-id>]... [--retention-policy <retain-all-through-publication|reclaim-inputs-after-terminal>] --available-memory-bytes <integer> --available-disk-bytes <integer> (--retained-input-bytes <integer> | --peak-in-flight-download-bytes 44296724480) --retained-basin-output-bytes <integer> --assembly-memory-ceiling-bytes <integer> --assembly-scratch-ceiling-bytes <integer> --assembled-artifact-bytes <integer> --active-compile-scratch-bytes <integer> --filesystem-overhead-bytes <integer>
tdx-hydro-campaign.sh status --campaign <id> [--workspace-root <path>]
tdx-hydro-campaign.sh recover --campaign <id> [--workspace-root <path>]
tdx-hydro-campaign.sh acquire --campaign <id> [--workspace-root <path>] --max-parallel <integer> [--product-attempt-ceiling <positive-integer>]
tdx-hydro-campaign.sh compile --campaign <id> [--workspace-root <path>] --fabric-version <value>
tdx-hydro-campaign.sh compile-basin --campaign <id> [--workspace-root <path>] --basin <processing-basin-id> --fabric-version <value>
tdx-hydro-campaign.sh progress --campaign <id> [--workspace-root <path>]
tdx-hydro-campaign.sh pipeline --campaign <id> [--workspace-root <path>] --max-parallel <integer> --fabric-version <value>
tdx-hydro-campaign.sh calibrate --campaign <id> [--workspace-root <path>] --max-parallel <2|4> --fabric-version <value>
tdx-hydro-campaign.sh assemble --campaign <id> [--workspace-root <path>] [--partial-fabric <dataset-root> --partial-fabric-roster <json-file> --exclude-control-basin <processing-basin-id>]
tdx-hydro-campaign.sh evidence --campaign <id> [--workspace-root <path>]
tdx-hydro-campaign.sh publish --campaign <id> [--workspace-root <path>] --out <dataset-dir> --report <path> --notice <path> --citation <path> --scratch-prefix <prefix>
```

`assemble` keeps its per-basin legacy mode when no extension options are
supplied. Supplying `--partial-fabric` selects extension mode and also requires
an absolute `--partial-fabric-roster` JSON file and one
`--exclude-control-basin` from the frozen campaign selection. The roster is a
nonempty sorted unique array of authoritative processing-basin IDs. It remains
an assembly input and is not written into the HFX manifest.

The excluded control basin must occur in the supplied roster and must have a
durable successful compile. Extension mode passes the partial fabric once,
omits that control output, and passes only successful selected basin outputs
absent from the roster as repeated adapter inputs. Assembly evidence records
the canonical fabric root, canonical roster path, complete constituent roster,
excluded control, and the IDs and campaign-relative paths of included new
basins. Resume accepts an existing result only when that complete provenance
is unchanged.

The extension command is local and performs no provisioning, NGA transfer, or
S3 operation. `publish` remains a separate explicit command.

`--workspace-root` defaults to `/mnt/hfx/work`, giving campaign directory
`/mnt/hfx/work/tdx-hydro-<campaign>`. The retention selector defaults to
`retain-all-through-publication`. Both policies require available memory and
disk, retained basin output, assembly memory and scratch ceilings, assembled
artifact, active compile scratch, and filesystem overhead. Retain-all also
requires `--retained-input-bytes`; reclaim instead requires the exact frozen
`--peak-in-flight-download-bytes 44296724480`. Supplying the other policy's
input term is an error. Every byte value is a positive base-10 integer within
the signed 64-bit range.

The common calculations are:

```text
required_memory_bytes = assembly_memory_ceiling_bytes
assembly_peak_bytes = max(assembly_scratch_ceiling_bytes, assembled_artifact_bytes)
available_memory_bytes >= required_memory_bytes
available_disk_bytes >= required_disk_bytes
```

Retain-all calculates:

```text
required_disk_bytes = retained_input_bytes
                    + retained_basin_output_bytes
                    + active_compile_scratch_bytes
                    + assembly_peak_bytes
                    + filesystem_overhead_bytes
```

Reclaim calculates:

```text
required_disk_bytes = peak_in_flight_download_bytes
                    + retained_basin_output_bytes
                    + active_compile_scratch_bytes
                    + assembly_peak_bytes
                    + filesystem_overhead_bytes
```

Assembly builds in a staging directory beside the destination and publishes
with a same-filesystem replace. Staging and final output are therefore one
artifact-sized term at a time, so the formula uses the maximum of scratch and
artifact rather than their sum.

Insufficient capacity is reported as
`hfx: error: insufficient memory: available <available> bytes; required <required> bytes`
or
`hfx: error: insufficient disk: available <available> bytes; required <required> bytes`.
Re-running `init` for an existing campaign requires the same normalized sizing
values, retention policy, and the same 62-basin inventory. Schema-1 campaign
state predates this contract and fails closed; use a new campaign ID. Changed
parameters also require a new campaign ID.

Retain-all keeps acquired final GeoPackages, resumable attributable partials,
per-basin outputs, and external diagnostic reports through publication.
It accepts acquisition `--max-parallel` from `1` through `62`; the exact common
range diagnostic remains
`option --max-parallel must be a base-10 integer from 1 through 62`.
Use `4` as the polite NGA operating value.

Direct `acquire` optionally accepts
`--product-attempt-ceiling <positive-integer>`. The value must be canonical
base 10 in the inclusive signed 64-bit range from `1` through
`9223372036854775807`. Supplying it also limits `--max-parallel` to `1..4` for
either retention policy. The option belongs only to direct `acquire` and is
never persisted in `campaign.json` or `selection.json`.

The ceiling is cumulative for each selected processing basin product. Each
`basins` product and each `streamnet` product has its own fixed count. It is
neither a per-basin ceiling nor a per-invocation ceiling. Every retry is a
fresh byte-zero GET because NGA has no range support. Acquisition reports keep
schema version 1 and atomically append one complete transfer object after each
attempt.

Supplying the ceiling converts selected basin state to schema version 5 after
a complete read-only preflight. A retain-all state has the exact outer members
`acquisition`, `processing_basin_id`, `schema_version`, and `stages`. A reclaim
state also has `retention`, with the existing exact policy and
`inputs_reclaimed` members. `acquisition` contains only
`product_attempt_ceiling`. Compile retains the schema-version-3 and
schema-version-4 shape. Acquisition stages retain `attempts`, `evidence`,
`failure_reason`, and `status`.

An inspected `succeeded` acquisition has null failure reason and complete
byte count, SHA-256, SQLite identity, and layer evidence. Its attempt count is
the number of transfers this campaign made for the product. A count of zero
means the final file was already present and passed inspection before any
transfer, so the runner adopted it without a network request. An `exhausted` acquisition has the ceiling attempt count, null
evidence, and exact reason
`product attempt ceiling exhausted; retryable acquisition did not succeed`.
The two states are mechanically distinct. A retryable failure at the ceiling
is atomically changed to `exhausted`. Inspection failures remain `failed` for
operator review, and the sibling product still runs.

The stored ceiling is fixed for the campaign's selected basins. A later direct
`acquire` may omit the option after every selected basin has schema version 5
with the same ceiling. Repeating the same explicit ceiling is also idempotent.
An interrupted mixed conversion requires the explicit stored ceiling to
finish. Changed or conflicting ceilings require a new campaign ID.

Version-5 status adds `acquire_basins_exhausted` immediately after the existing
basins failed count and `acquire_streamnet_exhausted` immediately after the
existing streamnet failed count. Compile retains its four statuses. The
terminal acquisition summary uses `status=exhausted`. Status output for basin
schema versions 1 through 4 remains unchanged.

Bounded direct acquisition performs acquisition and inspection only. It does
not compile, reclaim inputs, assemble, delete artifacts, publish, invoke AWS,
or write S3 objects. Schema versions 3 and 4 and campaign schema version 2
remain accepted unchanged. Without the option and without selected version-5
state, cumulative attempts remain unlimited and direct acquire keeps exactly
one attempt per product per dispatch and invocation. No finite default exists.
Pipeline and calibration keep their existing dispatch behavior for schema
versions 1 through 4. Both refuse selected version-5 state before transfer,
adapter resolution, compile, or reclaim.

Reclaim policy removes only the exact basins and streamnet source GeoPackages,
safe acquisition partials, and their provenance sidecars after both acquisitions
succeed and compile reaches a durable success or an actual adapter build or
validation hard failure with a positive attempt count. The runner first
atomically records acquisition evidence, the compile outcome, available
diagnostics, and the failure reason. It then reclaims the basins product, reclaims
the streamnet product, and atomically records inputs_reclaimed=true before
processing another basin. Recovery completes any interruption between those
boundaries. Missing terminal source files are treated as an interrupted or
repeated reclaim; unsafe path types fail closed without traversal.

Legacy assembly reads every successful landed
`basin-outputs/<processing-basin-id>` root. Extension assembly also reads the
supplied partial-fabric dataset and only the successful, non-excluded new basin
roots named in its persisted provenance. Landed basin outputs are never reclaim
targets.

Pipeline recovery classifies each acquisition stage from durable state.
`succeeded` is terminal. `pending`, including the interrupted-stage diagnostic,
and these three failed reasons are retryable and map to pipeline `pending`:

- `transfer interrupted; retry from byte zero`
- `download provenance or size verification failed`
- `download failed integrity verification`

These five failed reasons map to pipeline `blocked` and retain the exact reason:

- `acquisition report is unsafe or malformed; retained for inspection`
- `existing final file failed integrity verification; retained for inspection`
- `persisted evidence does not match final file; retained for inspection`
- `partial path is unsafe; retained without traversal`
- `installed final failed integrity verification; retained for inspection`

Recovery preserves a succeeded sibling, safe partial, sidecar, final,
acquisition report, attempt count, and evidence. It performs no acquisition.
Unknown failed reasons and residual `running` acquisition stages fail closed
with an acquisition-specific diagnostic.

The five durable compile failure reasons are `adapter build failed`, `adapter
validation failed`, `existing compile artifacts failed resume verification;
retained for inspection`, `compile artifact path already exists; retained for
inspection`, and `acquisition prerequisites are not both succeeded`. The two
adapter hard failures are reclaimable after a positive attempt. Both compile
inspection reasons are blocked. The prerequisite reason at zero attempts
automatically becomes `ready` once both acquisitions succeed and neither
managed output nor build-report path exists. Its artifact-present variant is
blocked with the prerequisite reason unchanged.

A blocked basin retains its source pair and permanently occupies a future pair
slot until operator remediation. Pipeline completion requires every selected
basin to become `reclaimed`, so every invocation remains nonzero while a
selected basin is blocked. Five blocked basins wedge the campaign because no
remaining pair slot is available while completion still requires every
selected basin to be reclaimed.

After a compile hard failure has been durably classified and its inputs reclaimed,
those inputs are not retryable in place. Retrying that basin requires a fresh
approximately 30-minute acquisition under a separately authorized recovery
workflow.

```text
6,979,305,472 + 1,880,039,424 = 8,859,344,896 bytes per source pair
5 * 8,859,344,896 = 44,296,724,480 peak input bytes
max_parallel <= 5 - 1
max_parallel <= 4
```

It discards a safe ordinary incomplete partial before each fresh complete-file
attempt. A resumed acquisition re-verifies every installed final's byte count, SHA-256,
SQLite identity, layer name, and persisted evidence. A resumed compile re-validates
each succeeded dataset and external report. Existing failed or conflicting compile
artifacts are retained for inspection.

`compile` remains the phase-separated whole-effective-set sweep.
`compile-basin` requires exactly one authoritative processing basin in the
frozen campaign selection and requires both acquisitions to have succeeded. It
compiles or resume-verifies only that basin. A prerequisite refusal does not
write a compile failure for the named basin and never evaluates or writes
another basin's stage state. A future pipeline must use `compile-basin`
semantics, not the whole `compile` sweep.

The compile-contract check deliberately has different placement in the two
entrypoints. `compile` establishes the contract before migration and recovery.
`compile-basin` establishes it after target-only migration, recovery, and the
prerequisite check so that a prerequisite refusal remains write-free. As a
result, a conflicting `compile-basin --fabric-version` may perform migration or
recovery writes before reporting the conflict, and an unmet prerequisite is
reported before the fabric-version conflict.

Campaign-lock re-entry is allowed only when the current process already owns
the safe campaign lock and its regular, non-symlink `owner.pid` matches that
process. Competing live owners retain the existing refusal, and stale owners
retain the existing takeover behavior. Forked workers clear inherited
ownership flags. Bash resets a parent-set `EXIT` trap in an `&` subshell, so
the ownership reset is defense in depth rather than the only protection.

The runner phases are:

1. `init` creates the fixed campaign layout and records sizing, inventory, and
   retention policy.
2. `acquire` performs bounded parallel work across basins, with products serial
   within each basin. Retain-all accepts `1..62`; reclaim accepts `1..4`.
   Use `4` as the polite NGA campaign operating policy. Product attempts are
   fresh byte-zero transfers.
3. `compile --fabric-version <value>` invokes one isolated adapter build per
   basin after both products succeed. In reclaim mode, durable compile success
   or an actual adapter build or validation hard failure immediately reclaims
   only that basin's source pair. Outputs and reports remain. Both compile
   inspection classifications remain unreclaimed.
4. `evidence` writes deterministic
   `publication/evidence/acquisition.json`,
   `publication/evidence/outcomes.json`, and
   `publication/evidence/diagnostics.json`.
5. After assembly by a separate repository entrypoint, `publish` accepts the
   assembled dataset directory as opaque input. The runner inventories
   nonempty regular files and uploads the exact persisted inventory; it does
   not invoke assembly or validate dataset semantics.

`pipeline` is available only for campaigns using
`reclaim-inputs-after-terminal`. It validates `--max-parallel` from `1..4`
and persists that JSON number with the nonempty `--fabric-version` and sorted
effective basin IDs in schema-1 `state/pipeline.json`. Fabric version and
selected basin IDs are immutable on replay. Max parallel may change within
`1..4`.

Pipeline records contain `status` and `blocked_reason` with statuses `pending`,
`acquiring`, `ready`, `compiling`, `terminal`, `reclaimed`, and `blocked`.
Only the lock-owning parent writes the document. The document is advisory:
each pass reconstructs every selected record from authoritative durable basin
state using ordered, first-match recovery rules. Already-reclaimed basins stay
reclaimed. Eligible terminal outcomes transition to `terminal` before source
removal and to `reclaimed` afterward. A succeeded compile missing only its
diagnostic is verification-only. Positive compile attempts are never rebuilt:
inspection artifacts are blocked, and an attempt without artifacts is blocked
as an interrupted attempt. Zero-attempt ready work remains `ready`. Incomplete
acquisition maps to `pending` or `blocked` as listed above. Remaining compile
contradictions fail closed. Do not hand-edit `state/pipeline.json`.

`status` retains its lock-taking validation semantics and prints sizing plus
deterministic per-stage counts. Only reclaim mode also prints a deterministic
reclaimed-input count. When pipeline state exists, status and progress then
print `pipeline_max_parallel`, `pipeline_fabric_version`, and the seven
`pipeline_<status>` counts, in that order, before the four assembly counts.
`progress` renders the same deterministic counts directly from one atomically
installed pipeline snapshot without locking, migration, recovery,
reconciliation, or writes.

Pipeline completion requires every selected advisory record and durable basin
to be reclaimed. Otherwise the command prints durable and scheduler counts and
returns nonzero after one bounded pass.

Blocked-basin remediation is explicit:

1. Stop campaign commands and confirm the exact campaign lock is absent.
2. Copy the basin `current.json`, acquisition reports, source files and
   sidecars, output directory, and build report to external inspection storage.
3. Determine whether acquisition evidence or a prior adapter invocation can be
   made canonical without repeating paid work.
4. For a valid prior compile, restore canonical output and report at the exact
   managed paths and rerun `pipeline` with the same fabric version and selected
   set. If the basin is already blocked with a compile reason, use step 6
   instead.
5. For acquisition inspection, preserve evidence and move the reason-specific
   path out before explicitly running `acquire`, then rerun `pipeline`. Move
   `reports/<id>-<product>-acquisition.json` for an unsafe report; move
   `downloads/<id>-<product>.gpkg` for existing-final, installed-final, or
   evidence-mismatch failures; move the exact `.gpkg.partial` or
   `.gpkg.partial.json` unsafe path for unsafe partial provenance. For evidence
   mismatch, the final must be moved first because leaving it permits
   `acquire` to promote a file that disagreed with persisted evidence.
6. If a second build is consciously authorized, move conflicting output and
   report paths outside managed campaign paths, run `compile-basin` with the
   same fabric version as a supervised override, then rerun `pipeline`.
7. If neither recovery is defensible, abandon the basin in this campaign and
   use a new campaign ID.

The supervised commands are operator overrides. Automatic pipeline recovery
never reacquires retained-for-inspection input and never performs a second
build. `pipeline` runs an acquisition producer at the requested
`max_parallel`, capped at four by reclaim policy. Workers launch as explicit
subshells with `( pipeline_acquisition_worker "$basin_id" ) &`. One synchronous
serial compiler runs in the parent while acquisition workers remain live.
Phase-separated `acquire`, `compile`, `compile-basin`, `status`, and `progress`
and retain-all behavior remain unchanged.

The occupancy guard counts distinct effective basin IDs. A basin occupies one
pair when any final, partial, or partial sidecar source path exists, including a
dangling symlink, or when its scheduler status is `acquiring`, `ready`,
`compiling`, or `terminal`. Physical presence wins for every status, including
`blocked`. A current count above five is corrupt state and fails with
`hfx: error: pipeline occupancy invariant exceeded: observed <actual> pairs;
maximum 5`. Projecting a proposed ID returns status 0 at five or fewer and
status 1 without output when the projection would exceed five. Status 1 is a
temporary capacity answer, not a campaign failure.

Disk credit is deliberately narrower: one full pair is credited only when both
installed finals exist. Partial-only and one-final basins receive no credit.
The guard computes
`44,296,724,480 - present_pairs * 8,859,344,896` and accepts available bytes
equal to the result. A one-byte shortfall returns status 1 and emits
`insufficient pipeline dispatch disk: available <actual> bytes; required
<required> bytes`. The sizing identities are:

```text
6,979,305,472 + 1,880,039,424 = 8,859,344,896 bytes per source pair
5 * 8,859,344,896 = 44,296,724,480 peak input bytes
max_parallel <= 5 - 1
max_parallel <= 4
```

The ephemeral completion transport is
`state/tmp/pipeline-completions.fifo`, a mode-0600 non-symlink named pipe held
read-write by the parent on literal descriptor 9. Records are
`<10-digit-basin-id><TAB><numeric-exit-status><NEWLINE>`. Durable JSON remains
the source of truth. Bash 3.2 drops a function-installed EXIT trap for a bare
`function &` invocation, so a future worker launch must use
`( pipeline_acquisition_worker "$basin_id" ) &`. Variables named by the EXIT
trap must be non-local because function locals are gone when that trap runs.
The trap makes exactly one record-write attempt and ignores write failure after
capturing the true worker status.

The scheduler performs ordered recovery before reconstruction and before every
occupancy guard. Projected-count and disk-headroom refusals are nonfatal; a
ready pair can compile and reclaim capacity before the producer retries.
Descriptor 9 uses one-second timed reads. Every nonzero read result enters the
liveness sweep because Bash 3.2 returns 1 and Bash 5 returns 142; descriptor 9
being read-write makes EOF unreachable. `kill -0` can recognize an unreaped
zombie for one additional bounded round. Both `read` and `wait` nonzero statuses
are captured under strict mode, and each tracked PID is waited exactly once.

Each basin receives at most two scheduler dispatch attempts and two serial
consumer attempts per invocation. A scheduler-state snapshot and round dispatch,
reap, and compile flags terminate a round with no durable change or action
through the bounded final-state check. A completion already accounted for by
the liveness sweep is discarded, including after that basin is redispatched.
Success requires every selected basin to be `reclaimed`. Bounded incomplete
reporting preserves `pending`, `ready`, and `blocked` states for recovery or
operator action. Fatal and signal cleanup sends TERM to tracked children, reaps
each child, and removes the FIFO before releasing the campaign lock.

The planning timing model remains 33-37 hours for acquisition, 16-18 hours for
serial compilation, and approximately 34-42 hours for the overlapped pipeline.
These values are planning estimates, not a measured planetary result.
Operational
recovery always re-runs the same campaign with the same sizing, inventory,
parallelism, fabric version, paths, attribution inputs, and scratch prefix.
Do not sweep resources, enumerate by name pattern or label selector, or perform
opportunistic cleanup. Inspect and retry only the exact campaign resources and
retained paths named by the diagnostic.

The authored 600 GB reclaim model is:

```text
44,296,724,480 peak input bytes
+ 206,220,202,290 retained basin output bytes
+ 30,000,000,000 active compile scratch bytes
+ 206,220,202,290 assembled artifact bytes
+ 5,000,000,000 filesystem overhead bytes
= 491,737,129,060 required disk bytes

560,000,000,000 - 491,737,129,060 = 68,262,870,940 bytes headroom
```

This is approximately 492 GB required against approximately 560 GB usable.
The five-pair peak and active compile scratch do not coincide with assembly,
but the conservative formula deliberately sums them. Retaining all projected
source downloads with outputs, compile scratch, assembly, and overhead
approaches a terabyte and does not fit safely within the account's 1024 GB
volume quota.

The full model is authored planning, not a measured or verified planetary
result. Term provenance is:

| Term | Provenance |
|---|---|
| `6,530,924,497`, `5,541,794,376`, `5,396,674,014` | Measured compile peak-scratch values for basins `1020000010`, `7020000010`, and `9020000010` in the completed three-basin campaign. |
| `6,979,305,472`, `1,880,039,424`, `8,859,344,896` | Measured largest source-pair components and their arithmetic sum. |
| `549,279,383,552`, `206,220,202,290`, `30,000,000,000` | Carried-forward M4-S4 capacity-disclosure terms, not measurements from the three-basin campaign. The retained-download projection is `8,859,344,896 * 62 = 549,279,383,552`. |
| `206,220,202,290` assembled artifact | Authored conservative assumption. The measured three-basin assembled aggregate is `9,037,936,480`; the authored value is `22.817x` that aggregate versus the count-linear basin multiplier of `20.667x`. |
| Five pairs, `44,296,724,480` peak, `5,000,000,000` overhead, `491,737,129,060` total | Authored planning choices derived from the stated terms. |
| `560,000,000,000` usable bytes for a 600 GB volume | Account-model convention, consistent with `140,000,000,000` usable for 150 GB; not a measured filesystem reading for a future planetary volume. |

The completed
[`RUNBOOK-tdx-hydro-assembly-subset.md`](RUNBOOK-tdx-hydro-assembly-subset.md)
calls its schema-1 subset command “complete, frozen” and refers to “all seven
required byte arguments.” Those statements predate schema 2 and remain
historical. The old command is intentionally not executable with the current
runner unless the new common fields are supplied.

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
  --assembled-artifact-bytes <assembled-artifact-bytes> \
  --active-compile-scratch-bytes <active-compile-scratch-bytes> \
  --filesystem-overhead-bytes <filesystem-overhead-bytes>

# Reclaim behavior is local and recoverable; paid execution remains gated by the planetary runbook.
./scripts/hetzner/launch.sh --campaign <campaign> start --workload tdx-init -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh init \
  --campaign <campaign> \
  --retention-policy reclaim-inputs-after-terminal \
  --available-memory-bytes <available-memory-bytes> \
  --available-disk-bytes <available-disk-bytes> \
  --peak-in-flight-download-bytes 44296724480 \
  --retained-basin-output-bytes <retained-basin-output-bytes> \
  --assembly-memory-ceiling-bytes <assembly-memory-ceiling-bytes> \
  --assembly-scratch-ceiling-bytes <assembly-scratch-ceiling-bytes> \
  --assembled-artifact-bytes <assembled-artifact-bytes> \
  --active-compile-scratch-bytes <active-compile-scratch-bytes> \
  --filesystem-overhead-bytes <filesystem-overhead-bytes>

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

The 2026-07-21 probe from Hetzner `fsn1` found no advertised HTTP range support.
The 2026-07-25 paid run showed that a range request returned the full body with
HTTP 200 and no usable `Content-Range`. Segmented per-file downloads remain
unavailable. The runner may continue an attributable partial only if a later
request returns HTTP 206 with its validated strong ETag, exact starting offset,
and consistent remote total. Curl's documented `CURLE_RANGE_ERROR` behavior
returns code 33 without writing a body when a continuation receives the measured
HTTP 200 response without `Content-Range`; the runner classifies the captured
response, discards the safe old partial, and makes one clean full GET. It never
appends that full response to the partial.
An ignored-Range call includes `--continue-at -`, so it increments `resume_count`
while recording mode `range_ignored_restart`; a changed-ETag HTTP 200 is counted
the same way even though validator mismatch, rather than an ignored Range, caused it.

Per-connection throughput was erratic. During the paid run a laptop sustained
about 13 MB/s while the campaign VM received about 1.0 MB/s on its own simultaneous
connection, showing server headroom and a per-connection constraint. Use
`--max-parallel 4` as the polite operating policy for separate product files; the
retain-all policy retains its `1..62` accepted range, while reclaim mode accepts
only `1..4`. At four connections sustaining the
measured VM rate, roughly 500 GB needs about 35 transfer-hours; allow 36-40 hours
for a 62-basin acquisition. Do not segment a single file. Observed basin
GeoPackages were `5.9-7.0 GB` each. The observed stream-network GeoPackage was
`1.68 GB`, with an exact recorded size of `1,676,398,592` bytes.

The endpoint was reachable from `fsn1`, returned real payloads, completed the
full stream-network file, and showed no observed geo-blocking during the probe.
Plan disk capacity for complete files plus active partials, sidecars, and retained
downloads. An interrupted transfer is resumable only under the validated 206
contract above; otherwise budget for one clean complete-file retry. Keep downloads
and per-product acquisition reports on the attached volume so retries and transfer
evidence survive disconnects. The smoke script's behavior is separate from the
campaign runner and remains an all-or-nothing probe.

## Bounded calibration measurement

Calibration measures two fixed, disjoint cohorts through the existing acquisition,
serial compile, terminal classification, and reclaim pipeline:

```bash
scripts/hetzner/tdx-hydro-campaign.sh calibrate \
  --campaign <id> \
  --workspace-root <path> \
  --max-parallel <2|4> \
  --fabric-version <value>
```

`parallel-2` uses max-parallel 2 and these basin IDs, in order:

```text
1020011530
3020003790
6020006540
8020008900
```

`parallel-4` uses max-parallel 4 and these basin IDs, in order:

```text
2020003440
4020006940
7020014250
9020000010
```

The campaign must use `reclaim-inputs-after-terminal`. Parallel-2 must be
measured before parallel-4 starts. Both calls retain one immutable, nonempty,
control-free fabric version. The final selection is installed once and cannot
be overwritten. A later full `pipeline` call must use that fabric version and
the selected max-parallel value.

Before every scheduler entry, calibration requires available bytes of
`44,296,724,480 + max_parallel * 8,859,344,896`. Parallel-4 therefore requires
`79,734,104,064` bytes. This reserves the five-pair reclaim guard plus all four
possible cohort downloads before any worker, attempt, trace, or basin state is
changed.

The active cohort documents are `state/calibration/parallel-2.json` and
`state/calibration/parallel-4.json`. Append-only attempt traces are named
`state/calibration/parallel-<N>-attempt-<attempt>.samples.tsv`. Each sample
combines resident product lengths with durable evidence bytes for reclaimed
products, producing a monotone acquired-work counter that survives immediate
reclaim. The running-maximum clamp defends only against retry regressions.
Elapsed time sums adjacent intervals within attempts, never gaps between
calibrate invocations, and the corrected window discloses its attempt numbers.
If interruption leaves no positive observed full-occupancy duration, retained
in-attempt raw intervals provide a nonzero fallback with zero compile
completions, making it explicitly invalid for the spend-abort threshold.
Completed scheduler snapshots are archived as
`state/calibration/parallel-<N>-pipeline.json`. A terminal, reclaimed replay
finalizes retained traces and archives the snapshot without rescheduling.

Each measurement contains `raw.start_timestamp_seconds`,
`raw.end_timestamp_seconds`, `raw.bytes`, `raw.elapsed_seconds`, `raw.retries`,
`raw.throughput_bytes_per_second`, all ten `steady_state` fields
`start_timestamp_seconds`, `end_timestamp_seconds`, `attempts`, `start_bytes`, `end_bytes`,
`bytes`, `elapsed_seconds`, `throughput_bytes_per_second`,
`compile_completions`, and `compile_wall_seconds`, plus all six
`excluded_drain_tail` fields `start_timestamp_seconds`, `end_timestamp_seconds`,
`start_bytes`, `end_bytes`, `bytes`, and `elapsed_seconds`.

Raw spans the first through last sample, excluding inter-attempt idle time from
its elapsed seconds. Corrected steady state
starts at the first configured-full-occupancy sample and ends at the sample
immediately after the last full-occupancy sample. The excluded drain tail runs
from that corrected end through raw end. Throughput is integer bytes divided by
elapsed seconds, in B/s; both denominators and both byte numerators must be
positive before division. Selection first prefers a cohort with compile
completions over a structurally incomparable zero-compile cohort. Between
comparable windows, select 2 when `small >= large`, otherwise select 2 when
`100 * small >= (100 - 5) * large`, otherwise select 4.

A corrected throughput whose steady_state.compile_completions == 0 is NOT a valid input to the M5-S4 spend-abort threshold.
M5-S4 must apply the 4,167,474 bytes/second abort test to a cohort whose corrected window contains at least one compile completion.
When only a zero-compile corrected window is available, M5-S4 must treat the threshold as UNMET rather than met.
The frozen `selected_throughput_validity` is `compile-observed` if either
candidate contains a compile, otherwise `no-compile-observed`.
When the corrected interval falls back to the gap-excluded raw interval, its
compile count is forced to zero even if compiles occurred. The binding rule
therefore reports the threshold as UNMET; inspect the disclosed attempts to
distinguish this conservative fallback from a genuinely zero-compile window.

In the pinned scheduler, parallel-4 dispatches all four in its first round; its
first reap begins permanent drain, so its corrected window contains zero compile
completions and zero compile wall seconds by construction. The production run
overlaps compile with acquisition at both settings. The parallel-4 corrected
throughput is therefore systematically optimistic, may flip a five-percent
choice, and may elevate the best value used by M5-S4's `4,167,474` bytes/second
spend threshold. M5-S4 uses parallel-2 under the pinned scheduler because its
corrected window contains compile completions.

Every successful calibration prints this final ordered disclosure block. Lines
for numeric fields appear only after the corresponding cohort is measured:

```text
calibration_fabric_version=<string>
calibration_selected_max_parallel=<pending|2|4>
calibration_selected_throughput_validity=<pending|compile-observed|no-compile-observed>
calibration_parallel_2_status=<pending|running|measured>
calibration_parallel_2_raw_start_timestamp_seconds=<integer>
calibration_parallel_2_raw_end_timestamp_seconds=<integer>
calibration_parallel_2_raw_bytes=<integer>
calibration_parallel_2_raw_elapsed_seconds=<integer>
calibration_parallel_2_raw_retries=<integer>
calibration_parallel_2_raw_throughput_bytes_per_second=<integer>
calibration_parallel_2_steady_start_timestamp_seconds=<integer>
calibration_parallel_2_steady_end_timestamp_seconds=<integer>
calibration_parallel_2_steady_attempts=<comma-separated integers>
calibration_parallel_2_steady_start_bytes=<integer>
calibration_parallel_2_steady_end_bytes=<integer>
calibration_parallel_2_steady_bytes=<integer>
calibration_parallel_2_steady_elapsed_seconds=<integer>
calibration_parallel_2_steady_throughput_bytes_per_second=<integer>
calibration_parallel_2_steady_compile_completions=<integer>
calibration_parallel_2_steady_compile_wall_seconds=<integer>
calibration_parallel_2_drain_start_timestamp_seconds=<integer>
calibration_parallel_2_drain_end_timestamp_seconds=<integer>
calibration_parallel_2_drain_start_bytes=<integer>
calibration_parallel_2_drain_end_bytes=<integer>
calibration_parallel_2_drain_bytes=<integer>
calibration_parallel_2_drain_elapsed_seconds=<integer>
calibration_parallel_4_status=<pending|running|measured>
calibration_parallel_4_raw_start_timestamp_seconds=<integer>
calibration_parallel_4_raw_end_timestamp_seconds=<integer>
calibration_parallel_4_raw_bytes=<integer>
calibration_parallel_4_raw_elapsed_seconds=<integer>
calibration_parallel_4_raw_retries=<integer>
calibration_parallel_4_raw_throughput_bytes_per_second=<integer>
calibration_parallel_4_steady_start_timestamp_seconds=<integer>
calibration_parallel_4_steady_end_timestamp_seconds=<integer>
calibration_parallel_4_steady_attempts=<comma-separated integers>
calibration_parallel_4_steady_start_bytes=<integer>
calibration_parallel_4_steady_end_bytes=<integer>
calibration_parallel_4_steady_bytes=<integer>
calibration_parallel_4_steady_elapsed_seconds=<integer>
calibration_parallel_4_steady_throughput_bytes_per_second=<integer>
calibration_parallel_4_steady_compile_completions=<integer>
calibration_parallel_4_steady_compile_wall_seconds=<integer>
calibration_parallel_4_drain_start_timestamp_seconds=<integer>
calibration_parallel_4_drain_end_timestamp_seconds=<integer>
calibration_parallel_4_drain_start_bytes=<integer>
calibration_parallel_4_drain_end_bytes=<integer>
calibration_parallel_4_drain_bytes=<integer>
calibration_parallel_4_drain_elapsed_seconds=<integer>
```

`/bin/date` supplies epoch seconds. It is part of coreutils and is assumed on
the paid box. This step does not verify that dependency because bootstrap
scripts and the runbook convergence gate are outside this step's write set.
The documentation states no real throughput result and no real selected value.

## Expected-terminal checkpoints

Checkpoint controls record cumulative expectations without changing scheduler
or basin state:

```bash
scripts/hetzner/tdx-hydro-campaign.sh checkpoint \
  --campaign <id> \
  --workspace-root <path> \
  --expected-terminal-count <1..62>

scripts/hetzner/tdx-hydro-campaign.sh checkpoint-resume \
  --campaign <id> \
  --workspace-root <path>
```

`state/checkpoints.json` is append-only history plus a running/stopped control.
Expected counts may stay equal or increase. The observed count is the number of
`reclaimed` records in one atomically installed `state/pipeline.json` snapshot.
`terminal`, `blocked`, and basin-state records do not add to that count. A met
entry remains running. A missed entry is installed with stopped state before
the command inspects the campaign-lock owner or sends TERM.

The monotonic constraint means an operator can never lower a recorded
expectation for the life of the campaign. Resume plus rerunning `pipeline`
remains available, and only checkpoint history is monotonic.

`checkpoint` does not take the campaign lock. A live pipeline receives TERM at
the exact validated owner PID and performs its existing exit-130 worker drain,
FIFO removal, and lock release. If no live owner exists, stopped state remains
durable. Repeating the same missed checkpoint retries owner adjudication and
TERM delivery without appending. Pipeline checks stopped state before lock
acquisition and again after acquisition before scheduler work.

Resume explicitly:

```bash
scripts/hetzner/tdx-hydro-campaign.sh checkpoint-resume \
  --campaign <id> \
  --workspace-root <path>
```

Resume changes only checkpoint control fields. It does not start the pipeline,
rewrite checkpoint entries, relabel a basin, alter attempts or evidence, remove
durable state, or change calibration. Rerun pipeline separately with its frozen
parameters. A missed checkpoint, a stopped campaign, an absent pipeline
snapshot, and a malformed checkpoint document are recoverable states.

If the pipeline snapshot is absent, checkpoint refuses without creating an
entry. Materialize or resume the existing pipeline, then rerun checkpoint:

```bash
scripts/hetzner/tdx-hydro-campaign.sh pipeline \
  --campaign <id> \
  --workspace-root <path> \
  --max-parallel <frozen-1-through-4> \
  --fabric-version <frozen-value>
scripts/hetzner/tdx-hydro-campaign.sh checkpoint \
  --campaign <id> \
  --workspace-root <path> \
  --expected-terminal-count <1..62>
```

If `state/checkpoints.json` is malformed or unsafe, `progress` reports the
condition without hiding the rest of campaign status. Run `checkpoint-resume`.
It moves the exact rejected directory entry without dereferencing it to the
first unused `state/checkpoint-recovery/rejected-N.json`, installs a fresh
running control, and preserves the rejected entry for inspection. Replay after
an interruption between those operations completes the fresh control install.

The lock-free read command is:

```bash
scripts/hetzner/tdx-hydro-campaign.sh progress \
  --campaign <id> \
  --workspace-root <path>
```

An absent checkpoint document prints no checkpoint lines. An empty running
document prints:

```text
checkpoint_run_state=running
checkpoint_entry_count=0
```

A nonempty valid document prints:

```text
checkpoint_run_state=<running|stopped>
checkpoint_entry_count=<integer>
checkpoint_expected_terminal_count=<integer>
checkpoint_observed_terminal_count=<integer>
checkpoint_result=<met|missed>
```

A malformed document prints:

```text
checkpoint_state=malformed
checkpoint_recovery=run checkpoint-resume
```

## Troubleshooting

| Symptom | Cause | Remedy |
|---|---|---|
| Image value is ambiguous or rejected | Hetzner exposes architecture-duplicate names for system images. The provisioner selects the unique x86 system image because campaign types and bootstrap target x86_64. Arm `cax*` support is outside this campaign contract. | Correct the image value so it has one exact named x86 system-image match. |
| Dedicated-core quota refusal | The project dedicated-core quota is below 16. | Use a shared type such as `cx23`, wait for other campaigns to release dedicated cores, or have a human raise the quota in the Hetzner console. |
| Filesystem setup refusal | The live fixes use unambiguous `wipefs --output TYPE` and trim `lsblk` device identifiers before comparison with `findmnt`; these fixes are shipped. The observed device, mount, signatures, or fstab state remains unsafe. | Retain resources. Inspect the reported block device, mount, signatures, and fstab. Correct the condition and rerun provisioning. Never erase an unrecognized signature. |
| Credential metadata or required variable failure | The installed environment file is absent, unsafe, unloadable, or incomplete. | Correct the operator-managed source, rerun provisioning to reinstall `/etc/pourpoint-hfx.env`, then rerun smoke. |
| Missing bootstrap state | Required directories, packages, or tools are absent. | Rerun the idempotent bootstrap, then retry launch or smoke. |
| Duplicate workload session | The exact tmux session already exists. | Use `attach`, `status`, or `tail`; start again after the exact session finishes. |
| NGA transfer interruption | The response may not honor Range, or the saved partial may lack safe provenance. | The campaign runner continues only a validated strong-ETag 206 response; otherwise it discards the safe partial and performs one clean GET. Use `--max-parallel 4` as operating policy across separate files, never segments of one file; retain-all accepts `1..62` and reclaim accepts `1..4`. |
| Safe teardown refusal | Exact-name ownership, labels, IDs, or attachment state failed validation. | Inspect only the exact campaign resource names and labels named by the diagnostic, correct ownership or attachment state, and rerun. |

Scope zero-footprint verification to the current campaign's deterministic
server and volume names. `pourpoint-web-1` and concurrently running resources
of other campaigns are never counted in the current campaign's zero-footprint
check. This exclusion keeps unrelated, intentionally live infrastructure
outside the teardown result.
