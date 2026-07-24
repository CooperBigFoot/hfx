# TDX-Hydro three-basin assembly campaign

## 1. Preconditions

This runbook is the paid M4-S4 execution script for campaign
`tdx-m4-subset`. Run commands from the HFX repository root on the workstation
unless a command explicitly enters the VM.

M4-S1 is committed as PASS. The committed parity record says
`Status: PASS`, defines the two reference hashes consumed by `vm-confirm`, and
requires VM-native confirmation after bootstrap and before acquisition or
compile (`adapters/tdx-hydro/GEOPANDAS-HILBERT-PARITY.md:1-11,25-30,70-74,
92-98`). Assembly wiring from PR #169 and the basin-subset contract from PR
#170 are both present at immutable ref
`264ab131e182260a4969587ca0744f1225f000db`.

Human authorization granted on 2026-07-24 covers one `ccx33` plus campaign
volume, two to three real NGA basin downloads, per-basin compile, partial
assembly, validation, evidence retention, and zero-footprint teardown. This
run uses exactly three inventory-backed basins:

- `1020000010`, crosswalk header `11`
- `7020000010`, crosswalk header `71`
- `9020000010`, crosswalk header `91`

The keys occur at
`adapters/tdx-hydro/data/tdx_header_numbers.json:2,48,63`. The different
header decades provide cross-continent world-domain contrasts. The crosswalk
does not encode basin size.

M5 is not authorized by this runbook. The full 62-basin campaign remains
separately spend-gated and must not begin from an M4 approval.

Use one Bash shell for the workstation commands so the frozen variables remain
available:

```bash
set -Eeuo pipefail
set +x

CAMPAIGN=tdx-m4-subset
SERVER_NAME=hfx-build-tdx-m4-subset
VOLUME_NAME=hfx-build-tdx-m4-subset-data
GROUND_TRUTH_REF=264ab131e182260a4969587ca0744f1225f000db
MILESTONE_BRANCH=pce/tdx-hydro-planetary-compile-and-assembly/milestone-4
VOLUME_SIZE_GB=150
RUN_UTC=$(date -u +%Y%m%dT%H%M%SZ)
LOCAL_EVIDENCE_DIR="/Users/nicolaslazaro/Desktop/work/hfx-campaign-evidence/$CAMPAIGN/$RUN_UTC"
mkdir -p "$LOCAL_EVIDENCE_DIR"
```

Abort before provisioning if the authorization, basin set, campaign ID,
resource names, immutable ref, server type, or volume size differs.

## 2. Contention preflight

This preflight is read-only. It lists every returned server and volume by exact
name, proves both exact campaign names absent, and records the unrelated
`grit-d8-m3` volume. The lifecycle prefixes the campaign ID with
`hfx-build-` and suffixes the volume name with `-data`
(`scripts/hetzner/README.md:13-17`).

```bash
test "$(hcloud context active)" = pourpoint

hcloud --context pourpoint server list -o json \
  > "$LOCAL_EVIDENCE_DIR/preflight-servers.json"
jq -r '.[] | [.id, .name] | @tsv' \
  "$LOCAL_EVIDENCE_DIR/preflight-servers.json"
jq -e --arg name "$SERVER_NAME" \
  '[.[] | select(.name == $name)] == []' \
  "$LOCAL_EVIDENCE_DIR/preflight-servers.json"

hcloud --context pourpoint volume list -o json \
  > "$LOCAL_EVIDENCE_DIR/preflight-volumes.json"
jq -r '.[] | [.id, .name] | @tsv' \
  "$LOCAL_EVIDENCE_DIR/preflight-volumes.json"
jq -e --arg name "$VOLUME_NAME" \
  '[.[] | select(.name == $name)] == []' \
  "$LOCAL_EVIDENCE_DIR/preflight-volumes.json"
jq -e '[.[] | select(.name == "grit-d8-m3")] | length == 1' \
  "$LOCAL_EVIDENCE_DIR/preflight-volumes.json"
jq -S '[.[] | select(.name == "grit-d8-m3")]' \
  "$LOCAL_EVIDENCE_DIR/preflight-volumes.json" \
  > "$LOCAL_EVIDENCE_DIR/grit-d8-m3-before.json"
cat "$LOCAL_EVIDENCE_DIR/grit-d8-m3-before.json"
```

Abort on a context mismatch, command failure, malformed response, any existing
exact campaign server or volume, or an absent or ambiguous `grit-d8-m3`
listing. Do not use pattern or label discovery for cleanup. Never detach,
resize, delete, or otherwise mutate `grit-d8-m3`. The only mutable cloud
targets in this runbook are `$SERVER_NAME` and `$VOLUME_NAME`.

Also inspect the complete listings for another live build campaign. The
dedicated-core quota allows only one campaign at a time. If another campaign
is using the available quota, abort before provisioning.

## 3. Sizing

The proven pilot retained `7,584,165,888` input bytes and emitted
`2,847,381,989` dataset bytes
(`hosting/tdx-hydro-7020000010/README.md:31-36,45-48`). M3 measured
`1,018,265,600` bytes peak process-tree RSS and `4,429,922,304` bytes peak
scratch at 16 million synthetic rows, while stating that M4 real bytes must
control volume sizing
(`adapters/tdx-hydro/ASSEMBLY-SCALE-REHEARSAL.md:5-15,99-110`).

The sizing assumption is that each unproven basin can be as large as 1.5 times
the pilot. The crosswalk gives geographic contrast but no contrary size
evidence. The weighted three-basin multiplier is therefore
`1 + 1.5 + 1.5 = 4` pilot basins.

| Category | Basis | Frozen bytes |
|---|---:|---:|
| Download retention | `4 * 7,584,165,888 = 30,336,663,552`, then 31.9 percent headroom and decimal round-up | `40,000,000,000` |
| Compile scratch reserve | More than twice the estimated largest unproven output of `1.5 * 2,847,381,989`; compiles are serial | `10,000,000,000` |
| Retained basin outputs | `4 * 2,847,381,989 = 11,389,527,956`, then 31.7 percent headroom and round-up | `15,000,000,000` |
| Assembly scratch | One full real-byte output staging allowance plus the measured M3 scratch ceiling, rounded up | `20,000,000,000` |
| Assembled output | Same weighted real dataset basis as retained outputs with 31.7 percent headroom | `15,000,000,000` |

The runner's required disk formula is retained inputs plus retained basin
outputs plus assembly scratch plus assembled artifact
(`scripts/hetzner/tdx-hydro-campaign.sh:157-167,2049-2065`). It is
`90,000,000,000` bytes. A 150 GB decimal volume is expected to expose at least
`140,000,000,000` usable bytes after filesystem overhead. The resulting
50,000,000,000-byte reserve covers the separate 10,000,000,000-byte compile
scratch allowance, filesystem overhead, logs, reports, and estimation error.

The `ccx33` has a 32 GB memory class. Freeze
`30,000,000,000` bytes as available memory after operating-system reserve and
`8,000,000,000` bytes as the assembly ceiling. The assembly ceiling is nearly
7.9 times the measured M3 RSS. The pre-init check below must prove the VM
actually exposes both frozen available capacities.

Expected phase times are 0.5 to 1 hour for provisioning and bootstrap, 1 to 2
hours for acquisition at parallelism 2, 1 to 2 hours for three serial
compiles, 0.25 to 1 hour for assembly, and 1 to 2 hours for complete
validation and evidence retention. The expected total is 4 to 8 hours.
Acquisition remains uncertain because NGA has no HTTP range support and
observed per-connection throughput is roughly 1 to 6 MB/s
(`scripts/hetzner/README.md:570-591`).

This is the complete, frozen init command. It contains all seven required byte
arguments and the three selected basins:

```bash
./scripts/hetzner/launch.sh --campaign tdx-m4-subset start --workload tdx-init -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh init \
  --campaign tdx-m4-subset \
  --workspace-root /mnt/hfx/work \
  --basin 1020000010 \
  --basin 7020000010 \
  --basin 9020000010 \
  --available-memory-bytes 30000000000 \
  --available-disk-bytes 140000000000 \
  --retained-input-bytes 40000000000 \
  --retained-basin-output-bytes 15000000000 \
  --assembly-memory-ceiling-bytes 8000000000 \
  --assembly-scratch-ceiling-bytes 20000000000 \
  --assembled-artifact-bytes 15000000000
```

Do not run it until provisioning, bootstrap, immutable-ref convergence,
capacity verification, and VM-native parity confirmation have passed.

## 4. Provision and bootstrap

The operator-managed S3 source is a readable, nonempty, non-symlink absolute
file obtained from the password manager. It contains exactly the credential
names `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Provisioning installs it
as `root:root` mode `600` at `/etc/pourpoint-hfx.env`; bootstrap does not touch
that path (`scripts/hetzner/README.md:39-51`;
`scripts/hetzner/bootstrap.sh:14,119-157`).

```bash
printf '%s' 'Absolute operator-managed S3 environment-file path: '
IFS= read -r S3_ENV_FILE
test "${S3_ENV_FILE#/}" != "$S3_ENV_FILE"
test -f "$S3_ENV_FILE"
test ! -L "$S3_ENV_FILE"
test -s "$S3_ENV_FILE"
```

Immediately before provisioning, use the Hetzner console to verify the
`ccx33` hourly price, 150 GB volume price per GB-month, included traffic, and
traffic overage price. Record them without credentials:

```bash
printf '%s' 'Console ccx33 EUR/hour: '
IFS= read -r CCX33_EUR_PER_HOUR
printf '%s' 'Console volume EUR/GB-month: '
IFS= read -r VOLUME_EUR_PER_GB_MONTH
printf '%s' 'Console included traffic: '
IFS= read -r INCLUDED_TRAFFIC
printf '%s' 'Console traffic overage EUR/TB: '
IFS= read -r TRAFFIC_OVERAGE_EUR_PER_TB
printf 'ccx33_eur_per_hour=%s\nvolume_eur_per_gb_month=%s\nincluded_traffic=%s\ntraffic_overage_eur_per_tb=%s\n' \
  "$CCX33_EUR_PER_HOUR" \
  "$VOLUME_EUR_PER_GB_MONTH" \
  "$INCLUDED_TRAFFIC" \
  "$TRAFFIC_OVERAGE_EUR_PER_TB" \
  > "$LOCAL_EVIDENCE_DIR/console-prices.txt"
```

Provision through the repository's converge-or-refuse lifecycle with every
identity parameter explicit:

```bash
./scripts/hetzner/provision.sh \
  --campaign tdx-m4-subset \
  --server-type ccx33 \
  --volume-size-gb 150 \
  --ssh-key nicolas-workstation \
  --image debian-12 \
  --location fsn1 \
  --s3-env-file "$S3_ENV_FILE" \
  2>&1 | tee "$LOCAL_EVIDENCE_DIR/provision.log"
```

This maps to the exact labeled server and volume create and attach operations
in `scripts/hetzner/provision.sh:406-456`. A rerun with identical parameters
converges. Any identity, label, location, type, image, key, size, attachment,
filesystem, or mount conflict refuses and must be inspected before retry
(`scripts/hetzner/README.md:85-117`).

Bootstrap and retain its live output:

```bash
./scripts/hetzner/bootstrap.sh --campaign tdx-m4-subset \
  2>&1 | tee "$LOCAL_EVIDENCE_DIR/bootstrap.log"
```

Bootstrap must finish with the mounted `/mnt/hfx` volume, workspace
`/mnt/hfx/work`, logs `/mnt/hfx/logs`, checkout `/root/hfx`, geo environment
`/opt/hfx-geo`, release CLI `/root/hfx/target/release/hfx`, and AWS CLI
available. Its final checks cover the mount, pinned geo packages, anonymous
checkout, release CLI, and AWS CLI
(`scripts/hetzner/bootstrap.sh:119-177,229-300,327-393`).

Resolve the exact server IPv4 address from the exact-name listing:

```bash
SERVER_IP=$(
  hcloud --context pourpoint server list -o json |
    jq -er --arg name "$SERVER_NAME" \
      '[.[] | select(.name == $name)] |
       if length == 1 then .[0].public_net.ipv4.ip else error("expected one exact server") end'
)
printf 'server_ip=%s\n' "$SERVER_IP" \
  > "$LOCAL_EVIDENCE_DIR/server-ip.txt"
```

Bootstrap converges to `origin/main`, while this runbook is grounded at the
milestone ref. Before any paid data operation, fetch the named milestone
branch, detach at the exact ref, rebuild the release CLI from that checkout,
and verify the frozen paths and capacities:

```bash
if ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
  "root@$SERVER_IP" 'bash -s' <<REMOTE_REF 2>&1 | tee "$LOCAL_EVIDENCE_DIR/converge.log"; then
set -Eeuo pipefail
git -C /root/hfx fetch origin "$MILESTONE_BRANCH"
git -C /root/hfx checkout --detach "$GROUND_TRUTH_REF"
test "\$(git -C /root/hfx rev-parse HEAD)" = "$GROUND_TRUTH_REF"
cd /root/hfx
/root/.cargo/bin/cargo build --release -p hfx-cli
test -x /root/hfx/target/release/hfx
/root/hfx/target/release/hfx --version
/opt/hfx-geo/bin/python -c 'import geopandas, numpy, pyarrow, shapely; print(geopandas.__version__, numpy.__version__, pyarrow.__version__, shapely.__version__)'
aws --version
test -f /etc/pourpoint-hfx.env
test ! -L /etc/pourpoint-hfx.env
test "\$(stat -c '%U:%G %a' /etc/pourpoint-hfx.env)" = 'root:root 600'
awk '/MemAvailable:/ {exit !(\$2 * 1024 >= 30000000000)}' /proc/meminfo
test "\$(df -B1 --output=avail /mnt/hfx | tail -n 1 | tr -d ' ')" -ge 140000000000
REMOTE_REF
  :
else
  ./scripts/hetzner/teardown.sh --campaign tdx-m4-subset \
    2>&1 | tee "$LOCAL_EVIDENCE_DIR/convergence-failure-teardown.log"
  exit 1
fi
```

This gate fails closed. Abort and run default teardown if the fetch, checkout,
build, tool, credential metadata, memory, or disk check fails. Do not inspect
or print credential values.

## 5. On-VM parity confirmation

VM-native parity is mandatory after bootstrap and exact-ref convergence and
before any acquisition. Define `HFX_REPO=/root/hfx`, then run the command
quoted verbatim from
`adapters/tdx-hydro/GEOPANDAS-HILBERT-PARITY.md:70-74`:

```bash
if ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
  "root@$SERVER_IP" 'bash -s' <<'REMOTE_PARITY' \
  2>&1 | tee "$LOCAL_EVIDENCE_DIR/vm-confirm.log"; then
set -Eeuo pipefail
HFX_REPO=/root/hfx
export HFX_REPO
/opt/hfx-geo/bin/python "$HFX_REPO/adapters/tdx-hydro/verify_geopandas_hilbert_parity.py" vm-confirm
REMOTE_PARITY
  :
else
  ./scripts/hetzner/teardown.sh --campaign tdx-m4-subset \
    2>&1 | tee "$LOCAL_EVIDENCE_DIR/parity-failure-teardown.log"
  exit 1
fi
```

An environment assertion, corpus regeneration, document parse, corpus hash,
or pair hash mismatch is a hard abort. Run default teardown immediately. Do
not initialize the campaign and do not acquire either NGA product. A mismatch
returns to M4-S1 remediation.

## 6. Campaign execution

The runner defaults on this VM resolve to `/opt/hfx-geo/bin/python`,
`/root/hfx/adapters/tdx-hydro/build_adapter.py`, and
`/root/hfx/target/release/hfx`; no `HFX_TDX_*` override is required
(`scripts/hetzner/tdx-hydro-campaign.sh:19-24,2086-2103`).

Record campaign start after parity passes:

```bash
date -u +%Y-%m-%dT%H:%M:%SZ \
  | tee "$LOCAL_EVIDENCE_DIR/campaign-start-utc.txt"
```

Run the complete init command frozen in section 3. Wait for workload
`tdx-init` to finish, inspect its canonical and timestamped logs, and require
exit status 0 before acquisition. The status output must show
`selected_basin_count=3` and `unselected_basin_count=59`
(`scripts/hetzner/tdx-hydro-campaign.sh:498-534`).

Acquire with two concurrent basin workers:

```bash
./scripts/hetzner/launch.sh --campaign tdx-m4-subset start --workload tdx-acquire -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh acquire \
  --campaign tdx-m4-subset \
  --workspace-root /mnt/hfx/work \
  --max-parallel 2
```

Parallelism 2 is modest across basins, respects the endpoint's all-or-nothing
file contract, and avoids opening all six selected product transfers
aggressively. The runner never segments one file
(`scripts/hetzner/tdx-hydro-campaign.sh:1774-1844,1884-1905`;
`scripts/hetzner/README.md:570-591`).

Use `NGA-TDX-Hydro-20230126` as the fabric version. It follows the established
`NGA-TDX-Hydro-YYYYMMDD` source-version format and matches the proven pilot's
latest source metadata (`hosting/tdx-hydro-7020000010/README.md:45-53`). If
either new basin proves a different authoritative source release, stop and
amend the runbook. Do not improvise a different fabric version during the paid
run.

```bash
./scripts/hetzner/launch.sh --campaign tdx-m4-subset start --workload tdx-compile -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh compile \
  --campaign tdx-m4-subset \
  --workspace-root /mnt/hfx/work \
  --fabric-version NGA-TDX-Hydro-20230126
```

Compile is serial across the selected basins, validates both acquired
prerequisites, retains each external report, and runs adapter validation before
recording success (`scripts/hetzner/tdx-hydro-campaign.sh:1019-1135`). Review
failed compile states before classifying them. Only a genuine basin-side
contract-fatal may become an exclusion.

Assemble only compile-succeeded selected basin outputs:

```bash
./scripts/hetzner/launch.sh --campaign tdx-m4-subset start --workload tdx-assemble -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh assemble \
  --campaign tdx-m4-subset \
  --workspace-root /mnt/hfx/work
```

Assembly records the sorted input basin IDs, invokes the landed adapter
assembly entrypoint, validates the result, writes
`reports/assembly.json`, and adopts only attributable interrupted output
(`scripts/hetzner/tdx-hydro-campaign.sh:785-959`).

Generate deterministic subset-aware evidence:

```bash
./scripts/hetzner/launch.sh --campaign tdx-m4-subset start --workload tdx-evidence -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh evidence \
  --campaign tdx-m4-subset \
  --workspace-root /mnt/hfx/work
```

It writes `publication/evidence/acquisition.json`,
`publication/evidence/outcomes.json`, and
`publication/evidence/diagnostics.json`, distinguishing the three selected
basins from the 59 unselected basins
(`scripts/hetzner/tdx-hydro-campaign.sh:1138-1431`).

Use these monitoring commands with the exact current workload:

```bash
./scripts/hetzner/launch.sh --campaign tdx-m4-subset status --workload tdx-acquire \
  || test "$?" -eq 3
./scripts/hetzner/launch.sh --campaign tdx-m4-subset tail \
  --log hfx-tdx-m4-subset-tdx-acquire.log
```

Replace only the workload suffix with `tdx-init`, `tdx-compile`,
`tdx-assemble`, or `tdx-evidence` for the corresponding phase. Status 0 means
running; status 3 means absent or finished. The canonical log's recorded exit
status decides success (`scripts/hetzner/README.md:166-204,524-543`).

After any interruption, rerun the same phase with byte-for-byte identical
argv. If state remains `running`, this explicit recovery is also available:

```bash
./scripts/hetzner/launch.sh --campaign tdx-m4-subset start --workload tdx-recover -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh recover \
  --campaign tdx-m4-subset \
  --workspace-root /mnt/hfx/work
```

Re-running init must include the same seven byte values and the same three
`--basin` values. A changed selection or sizing contract is refused and
requires a new campaign ID
(`scripts/hetzner/tdx-hydro-campaign.sh:651-690`). Resume re-verification
re-reads and re-hashes every retained successful final, including byte count,
SHA-256, SQLite identity, and layer inspection. Budget that I/O cost before
choosing resume.

## 7. Validation

Campaign-internal validation already covers each succeeded basin output and
the assembled output with the adapter wrapper. It also proves that the
assembly input list contains exactly compile-succeeded selected basins and
that its report matches those inputs
(`scripts/hetzner/tdx-hydro-campaign.sh:821-855,882-958,1042-1048,1122-1131`).

The following three exact argv blocks are VM commands. The SSH block below
executes them. Run the M3 verifier over the complete assembled files with this
exact argv:

```bash
cd /root/hfx/adapters/tdx-hydro
/opt/hfx-geo/bin/python rehearse_assembly_scale.py verify \
  /mnt/hfx/work/tdx-hydro-tdx-m4-subset/assembly/dataset \
  --batch-size 65536
```

Run the release validator in strict 100 percent mode with this exact argv:

```bash
/root/hfx/target/release/hfx \
  /mnt/hfx/work/tdx-hydro-tdx-m4-subset/assembly/dataset \
  --strict \
  --sample-pct 100
```

Run the adapter's explicit GeoParquet validation with this exact argv:

```bash
/opt/hfx-geo/bin/python \
  /root/hfx/adapters/tdx-hydro/build_adapter.py validate \
  /mnt/hfx/work/tdx-hydro-tdx-m4-subset/assembly/dataset \
  --hfx-binary /root/hfx/target/release/hfx
```

Execute and retain those validations from the VM:

```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
  "root@$SERVER_IP" 'bash -s' <<'REMOTE_VALIDATION' \
  2>&1 | tee "$LOCAL_EVIDENCE_DIR/validation.log"
set -Eeuo pipefail
cd /root/hfx/adapters/tdx-hydro
/opt/hfx-geo/bin/python rehearse_assembly_scale.py verify \
  /mnt/hfx/work/tdx-hydro-tdx-m4-subset/assembly/dataset \
  --batch-size 65536
/root/hfx/target/release/hfx \
  /mnt/hfx/work/tdx-hydro-tdx-m4-subset/assembly/dataset \
  --strict \
  --sample-pct 100
/opt/hfx-geo/bin/python \
  /root/hfx/adapters/tdx-hydro/build_adapter.py validate \
  /mnt/hfx/work/tdx-hydro-tdx-m4-subset/assembly/dataset \
  --hfx-binary /root/hfx/target/release/hfx
REMOTE_VALIDATION
```

Acceptance mapping:

| Acceptance requirement | Evidence |
|---|---|
| Every selected basin attempted and every exclusion named | subset-aware acquisition and outcomes evidence |
| Exactly landed units, no duplicates | campaign assembly input report plus strict HFX validation |
| Whole-file coherent order | M3 verifier scans non-decreasing `(hilbert, id)` |
| Snap IDs exactly `1..N` | M3 verifier |
| `hfx.aux.snap.v2` at `aux/snap_stems.parquet` and `references_levels = [0]` | M3 verifier and manifest |
| Every snap `unit_id` resolves in catchments | M3 verifier |
| Truthful partial coverage | assembly manifest and campaign input basin IDs |
| HFX validity | release `hfx --strict --sample-pct 100` |
| GeoParquet 1.1 validity | adapter `validate` |

The M3 verifier is not a surjective snap-completeness proof. It does not prove
that every catchment has a snap. Input-side campaign validation and the landed
assembly construction remain part of the evidence.

## 8. Evidence retention

M4 is a validation subset. Do not publish it to S3. Publication is omitted to
avoid treating the risk-retirement artifact as a deliverable. There will be no
`publication/current.json`; the local evidence bundle is authoritative for
M4-S5.

Generate a deterministic complete-file byte and SHA-256 inventory before
teardown:

```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
  "root@$SERVER_IP" 'bash -s' <<'REMOTE_INVENTORY'
set -Eeuo pipefail
CAMPAIGN_DIR=/mnt/hfx/work/tdx-hydro-tdx-m4-subset
cd "$CAMPAIGN_DIR/assembly/dataset"
find . -type f -print0 |
  sort -z |
  xargs -0 sha256sum \
  > "$CAMPAIGN_DIR/reports/assembled-sha256.txt"
find . -type f -printf '%s\t%P\n' |
  sort \
  > "$CAMPAIGN_DIR/reports/assembled-bytes.tsv"
test ! -e "$CAMPAIGN_DIR/publication/current.json"
REMOTE_INVENTORY
```

Copy the complete state tree, deterministic evidence documents, all external
per-basin reports, assembly report, validation inventories, assembled
manifest, and all campaign logs off the VM:

```bash
mkdir -p \
  "$LOCAL_EVIDENCE_DIR/campaign" \
  "$LOCAL_EVIDENCE_DIR/logs" \
  "$LOCAL_EVIDENCE_DIR/assembled"

scp -r "root@$SERVER_IP:/mnt/hfx/work/tdx-hydro-tdx-m4-subset/state" \
  "$LOCAL_EVIDENCE_DIR/campaign/"
scp -r "root@$SERVER_IP:/mnt/hfx/work/tdx-hydro-tdx-m4-subset/publication/evidence" \
  "$LOCAL_EVIDENCE_DIR/campaign/"
scp -r "root@$SERVER_IP:/mnt/hfx/work/tdx-hydro-tdx-m4-subset/reports" \
  "$LOCAL_EVIDENCE_DIR/campaign/"
scp "root@$SERVER_IP:/mnt/hfx/work/tdx-hydro-tdx-m4-subset/assembly/dataset/manifest.json" \
  "$LOCAL_EVIDENCE_DIR/assembled/"
scp -r "root@$SERVER_IP:/mnt/hfx/logs" \
  "$LOCAL_EVIDENCE_DIR/"
```

Verify the retained bundle locally:

```bash
test -f "$LOCAL_EVIDENCE_DIR/campaign/state/campaign.json"
test -f "$LOCAL_EVIDENCE_DIR/campaign/evidence/acquisition.json"
test -f "$LOCAL_EVIDENCE_DIR/campaign/evidence/outcomes.json"
test -f "$LOCAL_EVIDENCE_DIR/campaign/evidence/diagnostics.json"
test -f "$LOCAL_EVIDENCE_DIR/campaign/reports/assembly.json"
test -f "$LOCAL_EVIDENCE_DIR/campaign/reports/assembled-sha256.txt"
test -f "$LOCAL_EVIDENCE_DIR/campaign/reports/assembled-bytes.tsv"
test -f "$LOCAL_EVIDENCE_DIR/assembled/manifest.json"
find "$LOCAL_EVIDENCE_DIR/logs" -type f -name 'hfx-tdx-m4-subset-*.log' -print
```

Do not copy the assembled Parquet files locally. The pilot alone is about
2.85 GB and the conservative three-basin estimate is 11.39 GB before margin.
For this reproducible validation subset, retain the manifest, exact byte
inventory, exact SHA-256 inventory, reports, state, and logs. The Parquet data
is recreatable from the retained source identities and campaign contract.

## 9. Teardown

After all local evidence checks pass, run default teardown. Do not use
`--keep-volume` for successful closure.

```bash
./scripts/hetzner/teardown.sh --campaign tdx-m4-subset \
  2>&1 | tee "$LOCAL_EVIDENCE_DIR/teardown.log"
```

The teardown log must contain this exact line:

```text
hfx: campaign tdx-m4-subset has zero Hetzner footprint: server hfx-build-tdx-m4-subset absent; volume hfx-build-tdx-m4-subset-data absent
```

Default teardown resolves only the exact names, validates campaign labels,
detaches, deletes, waits for absence, and verifies zero footprint
(`scripts/hetzner/teardown.sh:247-341`).

Independently list exact names after teardown, list all returned names, prove
the campaign resources absent, and prove `grit-d8-m3` remains present and
unchanged:

```bash
hcloud --context pourpoint server list -o json \
  > "$LOCAL_EVIDENCE_DIR/final-servers.json"
jq -r '.[] | [.id, .name] | @tsv' \
  "$LOCAL_EVIDENCE_DIR/final-servers.json"
jq -e --arg name "$SERVER_NAME" \
  '[.[] | select(.name == $name)] == []' \
  "$LOCAL_EVIDENCE_DIR/final-servers.json"

hcloud --context pourpoint volume list -o json \
  > "$LOCAL_EVIDENCE_DIR/final-volumes.json"
jq -r '.[] | [.id, .name] | @tsv' \
  "$LOCAL_EVIDENCE_DIR/final-volumes.json"
jq -e --arg name "$VOLUME_NAME" \
  '[.[] | select(.name == $name)] == []' \
  "$LOCAL_EVIDENCE_DIR/final-volumes.json"
jq -e '[.[] | select(.name == "grit-d8-m3")] | length == 1' \
  "$LOCAL_EVIDENCE_DIR/final-volumes.json"
jq -S '[.[] | select(.name == "grit-d8-m3")]' \
  "$LOCAL_EVIDENCE_DIR/final-volumes.json" \
  > "$LOCAL_EVIDENCE_DIR/grit-d8-m3-after.json"
cmp "$LOCAL_EVIDENCE_DIR/grit-d8-m3-before.json" \
  "$LOCAL_EVIDENCE_DIR/grit-d8-m3-after.json"
```

Both exact campaign filters must yield empty arrays. The final complete volume
listing must still include `grit-d8-m3`.

## 10. Failure classifications and budget kill-switch

Classify before deciding whether to continue:

| Class | Required action |
|---|---|
| VM parity or parity-environment defect | Stop before acquisition, default teardown, return to M4-S1 remediation |
| Assembly-code defect, ordering defect, merged-snap defect, coverage-manifest defect, or repeatable validator defect in landed code | Stop the milestone, retain evidence, default teardown, and re-enter M2 |
| Genuine basin-side contract-fatal | Record the basin and exact failure as an exclusion, regenerate evidence, and continue assembly with the remaining compile-succeeded selected basins |
| Environmental quota, apt, Git, NGA network, SSH, or transient infrastructure failure | Resume with identical argv only while within both kill-switch limits; otherwise retain available evidence and default teardown |
| Unsafe identity, label, attachment, filesystem, state, or artifact conflict | Do not bypass the refusal. Inspect only the exact named campaign target, then resume identically or default teardown |

An arbitrary adapter failure is not automatically a basin exclusion. Reproduce
and inspect it sufficiently to distinguish a basin-side contract-fatal from an
assembly or adapter-code defect.

The hard kill-switch is 24 elapsed hours from successful provisioning or EUR
10.00 estimated campaign spend, whichever occurs first. At either limit,
default teardown is mandatory regardless of progress. `--keep-volume` is not
allowed after a kill-switch fires. The 24-hour limit is three to six times the
expected 4 to 8 hour run and bounds range-less NGA retry exposure. The EUR
10.00 ceiling covers the expected compute, prorated 150 GB volume, and small
outbound evidence copy with substantial margin.

Before every resume or new phase, compare elapsed time and estimated spend
against the retained start time and console prices:

```bash
date -u +%Y-%m-%dT%H:%M:%SZ
cat "$LOCAL_EVIDENCE_DIR/campaign-start-utc.txt"
cat "$LOCAL_EVIDENCE_DIR/console-prices.txt"
```

If a transient problem can be corrected within both limits, rerun the same
provision, bootstrap, recover, or phase argv. A longer pause may use
`teardown.sh --keep-volume` only before the kill-switch and only when the
remaining volume-only cost still fits EUR 10.00. Resume with the exact
provision argv. Successful completion and every kill-switch event use default
teardown.

## 11. Budget

The authorization supplied an approximate historical `ccx33` class of EUR
0.10 to 0.12 per hour. Treat that range as approximate and potentially stale.
The repository deliberately requires immediate console verification
(`scripts/hetzner/README.md:545-568`). Current orders may be priced higher, so
the recorded console value controls the estimate and the EUR 10.00 ceiling.

For planning, use a conservative EUR 0.27 per hour including tax, EUR 0.044 per
GB-month for volume, and eight hours:

```text
server: 8 h * EUR 0.27/h                         = EUR 2.16
volume: 150 GB * EUR 0.044/GB-month * 8/730 h   = EUR 0.07
traffic: expected within console-confirmed allowance = EUR 0.00
expected rounded total                            = EUR 3.00
hard ceiling                                      = EUR 10.00
```

NGA downloads are incoming traffic. Local evidence copy is outgoing traffic.
Confirm the included allowance and overage terms in the console before
provisioning. If current prices make either the expected total exceed EUR 3.00
materially or the 24-hour worst case approach EUR 10.00, abort before
provisioning and obtain renewed authorization.
