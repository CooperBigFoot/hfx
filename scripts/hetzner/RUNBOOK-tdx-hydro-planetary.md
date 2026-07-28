# TDX-Hydro bounded planetary campaign

This runbook governs one paid, irreversible, unattended 62-basin campaign. Execute it verbatim in one Bash shell. Any refusal after provisioning ends in evidence salvage, default teardown, and exact-name zero-footprint proof.

## 1. Authority, immutable revision, and absolute prohibitions

`83aa26c` is the original campaign source contract. `ab1e3c2` is the checkpoint implementation and complete command-surface authority used to author this document. The paid run uses one later immutable `origin/main` revision containing M5-S1, M5-S2, M5-S3, M5-S3B, M5-S3C-1, M5-S3C-2, and this M5-S4 runbook. Never guess or hard-code that future merge SHA.

Only the exact campaign server and volume named below may be mutated. No glob, prefix match, label selector, wildcard deletion, or cleanup discovery is permitted. Never touch `pourpoint-web-1`. The old `grit-d8-m3` volume was deleted on 2026-07-24 and its absence is expected; README lines 733-735 are stale when they call it intentionally retained. That resolution is prose only: no executable command may query, create, mutate, list, or filter either protected name.

The secrets environment file is accepted only as a path. Never print, `cat`, `echo`, log, archive, copy, hash, or transmit its contents. Disable tracing before reading its path and before every command that sources it. Never deliver under `hfx/`; parking is confined to the frozen scratch prefix. Default teardown is mandatory on success, failure, abort, refusal, and either hard switch. `--keep-volume` is forbidden for this campaign.

The runner surface is authoritative at `ab1e3c2:scripts/hetzner/tdx-hydro-campaign.sh:59-74`:

```text
init, status, recover, acquire, compile, compile-basin, progress, pipeline,
calibrate, checkpoint, checkpoint-resume, assemble, evidence, publish
checkpoint --campaign <id> [--workspace-root <path>] --expected-terminal-count <1..62>
checkpoint-resume --campaign <id> [--workspace-root <path>]
```

The checkpoint parser and dispatch are at lines 4711-4713, 4757-4812, and 4876-4882. Do not invent hour, deadline, or monetary flags.

## 2. Frozen campaign identity and one-shell setup

Run the workstation shell with Bash 4 or newer. This is required because Bash 3.2 may not apply `set -e` to a bare failed `[[ value =~ regex ]]`; every regex assertion must remain fail-closed.

```bash
test -n "${BASH_VERSION:-}" && test "${BASH_VERSINFO[0]}" -ge 4 || {
  printf '%s\n' 'Bash 4 or newer is required' >&2
  exit 1
}
set -Eeuo pipefail
set +x
IFS=$'\n\t'

CAMPAIGN=tdx-m5-planetary
SERVER_NAME=hfx-build-tdx-m5-planetary
VOLUME_NAME=hfx-build-tdx-m5-planetary-data
VOLUME_SIZE_GB=600
SERVER_TYPE=ccx33
WORKSPACE_ROOT=/mnt/hfx/work
CAMPAIGN_DIR=/mnt/hfx/work/tdx-hydro-tdx-m5-planetary
FABRIC_VERSION=NGA-TDX-Hydro-20230126
SCRATCH_PREFIX=scratch/tdx-hydro-tdx-m5-planetary/planetary-hfx-v0.3.0
BUDGET_CEILING_EUR=60.00
ELAPSED_CEILING_HOURS=72
LOCAL_EVIDENCE_DIR="$PWD/tdx-m5-planetary-evidence"
mkdir -p -- "$LOCAL_EVIDENCE_DIR"
chmod 700 -- "$LOCAL_EVIDENCE_DIR"
printf '%s\n' 'Enter the secrets environment FILE PATH (contents must never be displayed):' >&2
IFS= read -r S3_ENV_FILE
test -n "$S3_ENV_FILE"
test -f "$S3_ENV_FILE" && test ! -L "$S3_ENV_FILE"
```

Immutable campaign choices are one `ccx33`, one 600 GB volume, reclaim-after-terminal retention, at most five physically occupied source pairs, one serial compiler, calibration-selected and then immutable `max-parallel`, EUR 60.00 total, and 72 elapsed hours. The nominal successful-provision clock is conservatively replaced by the earlier provisioning-request origin.

## 3. Cost proof, current-price preflight, and dual kill switches

Historical planning inputs are unsourced current pricing. The subset runbook line 698 labels EUR 0.27/hour including tax, the old plan treated it VAT-exclusive, `state.json` says EUR 0.31/hour, and README lines 868-872 retain console placeholders. No console-verified price exists.

```text
ccx33: EUR 0.27/hour
volume: 600 * EUR 0.044/GB-month / 730 = EUR 0.0361643836.../hour
combined: EUR 0.3061643836.../hour
72-hour server: EUR 19.4400
72-hour volume: EUR 2.6038356164
72-hour total: EUR 22.0438
headroom: EUR 37.9562
additional affordable time: 123.97 hours
total affordable time: 195.97 hours = 2.72 * 72 hours
```

Even adding 27 percent VAT is only EUR 28.00 at 72 hours. Under this model and the mandatory current-price refusal below, the EUR 60 switch can never fire before the 72-hour switch. Time, not money, is binding.

Immediately before provisioning, obtain from the console: whether prices include VAT, applicable VAT percentage, `ccx33` hourly price, volume EUR/GB-month, included outbound quantity, outbound overage EUR per billing unit, billing-unit bytes, and a conservative billable outbound quantity covering parking and evidence. Enter decimal numbers without units; use `1` or `0` for VAT-inclusive.

```bash
read -r -p 'Prices VAT-inclusive (1 or 0): ' PRICE_VAT_INCLUDED
read -r -p 'VAT percent: ' VAT_PERCENT
read -r -p 'ccx33 EUR/hour: ' SERVER_EUR_PER_HOUR
read -r -p 'Volume EUR/GB-month: ' VOLUME_EUR_PER_GB_MONTH
read -r -p 'Included outbound bytes: ' INCLUDED_OUTBOUND_BYTES
read -r -p 'Outbound overage EUR/billing-unit: ' OUTBOUND_EUR_PER_UNIT
read -r -p 'Outbound billing-unit bytes: ' OUTBOUND_UNIT_BYTES
read -r -p 'Conservative billable outbound bytes: ' BILLABLE_OUTBOUND_BYTES

awk -v vat_in="$PRICE_VAT_INCLUDED" -v vat="$VAT_PERCENT" \
  -v server="$SERVER_EUR_PER_HOUR" -v volume="$VOLUME_EUR_PER_GB_MONTH" \
  -v included="$INCLUDED_OUTBOUND_BYTES" -v overage="$OUTBOUND_EUR_PER_UNIT" \
  -v unit="$OUTBOUND_UNIT_BYTES" -v outbound="$BILLABLE_OUTBOUND_BYTES" '
function numeric(x) { return x != "" && x ~ /^[0-9]+([.][0-9]+)?$/ && (x+0) >= 0 }
BEGIN {
  if (!numeric(vat_in) || (vat_in != 0 && vat_in != 1) || !numeric(vat) ||
      !numeric(server) || !numeric(volume) || !numeric(included) ||
      !numeric(overage) || !numeric(unit) || unit+0 <= 0 || !numeric(outbound)) exit 2
  factor = vat_in ? 1 : 1 + vat / 100
  server72 = server * factor * 72
  volume72 = 600 * volume * factor * 72 / 730
  excess = outbound > included ? outbound - included : 0
  units = excess == 0 ? 0 : int((excess + unit - 1) / unit)
  outbound_cost = units * overage * factor
  total = server72 + volume72 + outbound_cost
  printf "vat_inclusive=%s\nvat_percent=%.6f\nserver_eur_per_hour_gross=%.10f\n", vat_in, vat, server*factor
  printf "volume_eur_per_gb_month_gross=%.10f\nincluded_outbound_bytes=%.0f\n", volume*factor, included
  printf "outbound_eur_per_unit_gross=%.10f\noutbound_unit_bytes=%.0f\nbillable_outbound_bytes=%.0f\n", overage*factor, unit, outbound
  printf "server_72h_eur=%.10f\nvolume_72h_eur=%.10f\noutbound_eur=%.10f\ntotal_72h_eur=%.10f\n", server72, volume72, outbound_cost, total
  if (!(total < 60.00)) exit 3
}' > "$LOCAL_EVIDENCE_DIR/current-price-preflight.txt"
```

Blank, negative, nonnumeric, NaN, infinity, unknown VAT treatment, or unknown outbound terms refuse provisioning.

Define the reusable phase/checkpoint/resume gate after `SERVER_IP` exists. Every remaining range is supplied at its maximum, never minimum. Missing, malformed, or unbounded estimates refuse. Equality refuses.

```bash
campaign_gate() {
  test "$#" -eq 3
  local phase=$1 remaining_hours=$2 remaining_outbound_eur=$3 now origin elapsed projected_hours progress_file result total
  [[ "$remaining_hours" =~ ^[0-9]+([.][0-9]+)?$ ]]
  [[ "$remaining_outbound_eur" =~ ^[0-9]+([.][0-9]+)?$ ]]
  origin=$(cat "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt")
  [[ "$origin" =~ ^[0-9]+$ ]]
  now=$(date +%s)
  progress_file="$LOCAL_EVIDENCE_DIR/gate-${phase}-progress-$(date -u +%Y%m%dT%H%M%SZ).txt"
  ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
    "root@$SERVER_IP" /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh progress \
    --campaign tdx-m5-planetary --workspace-root /mnt/hfx/work > "$progress_file" || {
      ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
        "root@$SERVER_IP" /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh progress \
        --campaign tdx-m5-planetary --workspace-root /mnt/hfx/work > "$progress_file.retry"
      progress_file="$progress_file.retry"
    }
  read -r SERVER_GROSS VOLUME_GROSS < <(awk -F= '
    /server_eur_per_hour_gross=/{s=$2}
    /volume_eur_per_gb_month_gross=/{v=$2}
    END{print s,v}' "$LOCAL_EVIDENCE_DIR/current-price-preflight.txt")
  read -r elapsed projected_hours total < <(awk -v n="$now" -v o="$origin" -v r="$remaining_hours" \
    -v s="$SERVER_GROSS" -v v="$VOLUME_GROSS" -v out="$remaining_outbound_eur" '
    BEGIN { e=(n-o)/3600; p=e+r; t=p*(s+600*v/730)+out; printf "%.10f %.10f %.10f\n",e,p,t }')
  result=$(awk -v h="$projected_hours" -v t="$total" 'BEGIN{print (h<72 && t<60)?"PASS":"REFUSE"}')
  {
    printf 'timestamp_utc=%s\nphase=%s\nremaining_hours_max=%s\nremaining_outbound_eur_max=%s\n' "$(date -u +%FT%TZ)" "$phase" "$remaining_hours" "$remaining_outbound_eur"
    printf 'elapsed_hours=%s\nprojected_completion_hours=%s\nprojected_total_eur=%s\nresult=%s\n' "$elapsed" "$projected_hours" "$total" "$result"
    sed 's/^/progress: /' "$progress_file"
  } >> "$LOCAL_EVIDENCE_DIR/checkpoint-estimates.txt"
  test "$result" = PASS
}
```

```text
CORRECT, maxima charged: 44 + 12.6 + 2 + 12 + 2 = 72.6 -> REFUSE
FORBIDDEN, minima:       44 + 12.6 + 2 + 0  + 0 = 58.6

Whole phase: 2 + 54.6 + 2 + 12 + 2 = 72.6 -> BREACH
FORBIDDEN:   2 + 54.6 + 2 + 0  + 0 = 60.6
break-even overlap penalty: 54 / 42 - 1 = 28.57 percent
```

The stated 20-30 percent risk band contains breach. At a missed checkpoint stop dispatch first. Resume only after `campaign_gate` records PASS; otherwise salvage and default-teardown.

## 4. Read-only exact-name cloud and all-62 inventory preflights

Freeze the merged revision before cloud access:

```bash
git fetch origin main
GROUND_TRUTH_REF=$(git rev-parse origin/main)
test -n "$GROUND_TRUTH_REF"
git merge-base --is-ancestor 83aa26c "$GROUND_TRUTH_REF"
git cat-file -e "$GROUND_TRUTH_REF:scripts/hetzner/RUNBOOK-tdx-hydro-planetary.md"
git show "$GROUND_TRUTH_REF:scripts/hetzner/tdx-hydro-campaign.sh" | grep -F 'tdx-hydro-campaign.sh checkpoint --campaign <id> [--workspace-root <path>] --expected-terminal-count <1..62>'
git show "$GROUND_TRUTH_REF:scripts/hetzner/tdx-hydro-campaign.sh" | grep -F 'tdx-hydro-campaign.sh checkpoint-resume --campaign <id> [--workspace-root <path>]'
printf '%s\n' "$GROUND_TRUTH_REF" > "$LOCAL_EVIDENCE_DIR/ground-truth-ref.txt"
git show "$GROUND_TRUTH_REF:adapters/tdx-hydro/data/tdx_header_numbers.json" | jq -e '
  type == "object" and length == 62 and
  (to_entries | all((.key | test("^[0-9]{10}$")) and
    (.value | type == "string" and test("^[0-9]+$") and length > 0)))'
```

Confirm from merged history that all seven preceding steps and this runbook are present. Ambiguity stops before provisioning. In the Hetzner console separately prove quota for one dedicated-core `ccx33`, one 600 GB volume, and no other paid campaign consuming it.

```bash
test "$(hcloud context active)" = pourpoint
hcloud --context pourpoint server list -o json > "$LOCAL_EVIDENCE_DIR/preflight-servers.json"
jq -e --arg name "$SERVER_NAME" '[.[] | select(.name == $name)] == []' "$LOCAL_EVIDENCE_DIR/preflight-servers.json"
hcloud --context pourpoint volume list -o json > "$LOCAL_EVIDENCE_DIR/preflight-volumes.json"
jq -e --arg name "$VOLUME_NAME" '[.[] | select(.name == $name)] == []' "$LOCAL_EVIDENCE_DIR/preflight-volumes.json"
```

These inspect only exact campaign-name absence. They are not cleanup discovery.

## 5. Provisioning, successful-provision timestamp, and hard-teardown watchdog

Install the cleanup functions before provisioning becomes possible. Best-effort salvage must not mask teardown. The exact success line and both array proofs are required before `CLEANUP_COMPLETE=1`.

```bash
CLEANUP_RUNNING=0
CLEANUP_COMPLETE=0
salvage_evidence() { test -n "${SERVER_IP:-}" || return 0; scp -r -o BatchMode=yes "root@$SERVER_IP:$CAMPAIGN_DIR/state" "$LOCAL_EVIDENCE_DIR/" 2>>"$LOCAL_EVIDENCE_DIR/salvage-errors.log" || true; }
zero_footprint() {
  hcloud --context pourpoint server list -o json > "$LOCAL_EVIDENCE_DIR/final-servers.json"
  jq -e --arg name "$SERVER_NAME" '[.[] | select(.name == $name)] == []' "$LOCAL_EVIDENCE_DIR/final-servers.json"
  hcloud --context pourpoint volume list -o json > "$LOCAL_EVIDENCE_DIR/final-volumes.json"
  jq -e --arg name "$VOLUME_NAME" '[.[] | select(.name == $name)] == []' "$LOCAL_EVIDENCE_DIR/final-volumes.json"
}
cleanup() {
  local prior=$?
  test "$CLEANUP_COMPLETE" -eq 0 || return "$prior"
  test "$CLEANUP_RUNNING" -eq 0 || return "$prior"
  CLEANUP_RUNNING=1; set +e
  salvage_evidence
  ./scripts/hetzner/teardown.sh --campaign tdx-m5-planetary 2>&1 | tee "$LOCAL_EVIDENCE_DIR/teardown.log"
  grep -Fx 'hfx: campaign tdx-m5-planetary has zero Hetzner footprint: server hfx-build-tdx-m5-planetary absent; volume hfx-build-tdx-m5-planetary-data absent' "$LOCAL_EVIDENCE_DIR/teardown.log"
  teardown_ok=$?
  zero_footprint; proof_ok=$?
  if test "$teardown_ok" -eq 0 && test "$proof_ok" -eq 0; then CLEANUP_COMPLETE=1; fi
  CLEANUP_RUNNING=0
  test "$CLEANUP_COMPLETE" -eq 1 || return 1
  return "$prior"
}
trap 'prior=$?; trap - EXIT INT TERM HUP; cleanup || prior=1; exit "$prior"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
date +%s > "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"
./scripts/hetzner/provision.sh --campaign tdx-m5-planetary --server-type ccx33 \
  --volume-size-gb 600 --ssh-key nicolas-workstation --image debian-12 --location fsn1 \
  --s3-env-file "$S3_ENV_FILE" 2>&1 | tee "$LOCAL_EVIDENCE_DIR/provision.log"
date +%s > "$LOCAL_EVIDENCE_DIR/successful-provision-epoch.txt"
date -u +%FT%TZ > "$LOCAL_EVIDENCE_DIR/successful-provision-utc.txt"
awk '{print $1+259200}' "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt" > "$LOCAL_EVIDENCE_DIR/hard-deadline-epoch.txt"
```

The billing and campaign clocks use the request epoch. Start this watchdog; keep the workstation awake, powered, authenticated, and network-connected. Cancel it only after normal teardown and both proofs.

```bash
(
  set +e
  deadline=$(cat "$LOCAL_EVIDENCE_DIR/hard-deadline-epoch.txt")
  now=$(date +%s); delay=$((deadline-now)); test "$delay" -le 0 || sleep "$delay"
  ./scripts/hetzner/teardown.sh --campaign tdx-m5-planetary 2>&1 | tee "$LOCAL_EVIDENCE_DIR/watchdog-teardown.log"
  grep -Fx 'hfx: campaign tdx-m5-planetary has zero Hetzner footprint: server hfx-build-tdx-m5-planetary absent; volume hfx-build-tdx-m5-planetary-data absent' "$LOCAL_EVIDENCE_DIR/watchdog-teardown.log"
  hcloud --context pourpoint server list -o json > "$LOCAL_EVIDENCE_DIR/watchdog-final-servers.json"
  jq -e --arg name "$SERVER_NAME" '[.[] | select(.name == $name)] == []' "$LOCAL_EVIDENCE_DIR/watchdog-final-servers.json"
  hcloud --context pourpoint volume list -o json > "$LOCAL_EVIDENCE_DIR/watchdog-final-volumes.json"
  jq -e --arg name "$VOLUME_NAME" '[.[] | select(.name == $name)] == []' "$LOCAL_EVIDENCE_DIR/watchdog-final-volumes.json"
) > "$LOCAL_EVIDENCE_DIR/watchdog.log" 2>&1 &
WATCHDOG_PID=$!
printf '%s\n' "$WATCHDOG_PID" > "$LOCAL_EVIDENCE_DIR/watchdog.pid"
```

The watchdog is idempotent after completed teardown. It is not permission to retain resources until hour 72; hour 60 is the normal target.

## 6. Bootstrap, revision convergence, dependency and capacity preflights

```bash
./scripts/hetzner/bootstrap.sh --campaign tdx-m5-planetary 2>&1 | tee "$LOCAL_EVIDENCE_DIR/bootstrap.log"
SERVER_IP=$(jq -er --arg name "$SERVER_NAME" '[.[]|select(.name==$name)] | if length==1 then .[0].public_net.ipv4.ip else error("not exactly one") end' "$LOCAL_EVIDENCE_DIR/preflight-servers.json" 2>/dev/null || hcloud --context pourpoint server list -o json | jq -er --arg name "$SERVER_NAME" '[.[]|select(.name==$name)] | if length==1 then .[0].public_net.ipv4.ip else error("not exactly one") end')
ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "root@$SERVER_IP" 'bash -s' <<REMOTE_REF 2>&1 | tee "$LOCAL_EVIDENCE_DIR/converge.log"
set -Eeuo pipefail
set +x
git -C /root/hfx fetch origin main
test "\$(git -C /root/hfx rev-parse FETCH_HEAD)" = "$GROUND_TRUTH_REF"
git -C /root/hfx checkout --detach "$GROUND_TRUTH_REF"
test "\$(git -C /root/hfx rev-parse HEAD)" = "$GROUND_TRUTH_REF"
cd /root/hfx
/root/.cargo/bin/cargo build --release -p hfx-cli
test -x /root/hfx/target/release/hfx
for command_name in aws jq mv mkdir rm chmod find wc tr ps curl sha256sum od ogrinfo sort grep; do command -v -- "\$command_name" >/dev/null; done
HFX_TDX_ADAPTER_PYTHON=/opt/hfx-geo/bin/python
HFX_TDX_HFX=/root/hfx/target/release/hfx
test -x "\$HFX_TDX_ADAPTER_PYTHON"; test -x "\$HFX_TDX_HFX"
test -f /root/hfx/adapters/tdx-hydro/build_adapter.py; test ! -L /root/hfx/adapters/tdx-hydro/build_adapter.py
test -f /etc/pourpoint-hfx.env; test ! -L /etc/pourpoint-hfx.env
test "\$(stat -c '%U:%G %a' /etc/pourpoint-hfx.env)" = 'root:root 600'
awk '/MemAvailable:/ {exit !(\$2 * 1024 >= 30000000000)}' /proc/meminfo
OBSERVED_AVAILABLE_DISK_BYTES=\$(df -B1 --output=avail /mnt/hfx | tail -n 1 | tr -d ' ')
[[ "\$OBSERVED_AVAILABLE_DISK_BYTES" =~ ^[0-9]+$ ]]
test "\$OBSERVED_AVAILABLE_DISK_BYTES" -ge 496737129060
REMOTE_REF
```

The bootstrap is `scripts/hetzner/bootstrap.sh`, not a TDX-specific filename. Its lines 195-212 omit defaults chosen by `HFX_TDX_ADAPTER_PYTHON` and `HFX_TDX_HFX`; therefore the separate Python, release HFX, and adapter checks are mandatory. The adapter bypasses `resolve_command`; a missing script becomes opaque `adapter build failed`.

```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "root@$SERVER_IP" 'bash -s' <<'REMOTE_PARITY' 2>&1 | tee "$LOCAL_EVIDENCE_DIR/vm-confirm.log"
set -Eeuo pipefail
HFX_REPO=/root/hfx; export HFX_REPO
/opt/hfx-geo/bin/python "$HFX_REPO/adapters/tdx-hydro/verify_geopandas_hilbert_parity.py" vm-confirm
REMOTE_PARITY
```

Any checkout, dependency, credential-metadata, parity, memory, disk, corpus, or hash failure triggers default teardown before acquisition.

## 7. Campaign initialization and fail-closed preflight checkpoint at hour 2

The constants at `ab1e3c2:scripts/hetzner/tdx-hydro-campaign.sh:25-35` are:

```bash
readonly HFX_TDX_RECLAIM_PAIR_COUNT=5
readonly HFX_TDX_RECLAIM_MAX_PARALLEL=4
readonly HFX_TDX_RECLAIM_PAIR_BYTES=8859344896
readonly HFX_TDX_RECLAIM_PEAK_BYTES=44296724480
readonly HFX_TDX_PIPELINE_MAX_DISPATCH_ATTEMPTS=2
readonly HFX_TDX_PIPELINE_MAX_CONSUME_ATTEMPTS=2
readonly HFX_TDX_CALIBRATION_MARGIN_PERCENT=5
readonly HFX_TDX_CALIBRATION_PARALLEL_2_IDS="1020011530 3020003790 6020006540 8020008900"
readonly HFX_TDX_CALIBRATION_PARALLEL_4_IDS="2020003440 4020006940 7020014250 9020000010"
```

Line 49 runtime-asserts the five-pair identity. README lines 602-632 contain the hashed model.

```text
44,296,724,480 peak inputs
+ 206,220,202,290 retained basin output
+ 30,000,000,000 active compile scratch
+ max(206,220,202,290 assembly scratch, 206,220,202,290 artifact)
+ 5,000,000,000 filesystem overhead
= 491,737,129,060 required
560,000,000,000 - required = 68,262,870,940 headroom
pair = 6,979,305,472 + 1,880,039,424 = 8,859,344,896
peak = 5 * pair = 44,296,724,480
headroom = 12.19 percent = 7.70 largest-pair equivalents
```

The 206,220,202,290-byte allowance is unsourced governing policy, asserted without derivation at `COMPILE-SCALE-REHEARSAL.md:105`. Used twice, it is 83.9 percent (84 percent rounded) of the requirement and may be underestimated by at most 16.55 percent before overflow. It is not a measured fact.

Runner lines 4923-4942 implement the exact preflight predicate:

```bash
if [[ "$retention_policy" == reclaim-inputs-after-terminal ]] &&
    [[ "$peak_in_flight_download_bytes" != "$HFX_TDX_RECLAIM_PEAK_BYTES" ]]; then
    usage_error "option --peak-in-flight-download-bytes must equal $HFX_TDX_RECLAIM_PEAK_BYTES for retention policy reclaim-inputs-after-terminal"
fi
required_memory_bytes=$assembly_memory_ceiling_bytes
assembly_peak_bytes=$assembly_scratch_ceiling_bytes
if ((assembled_artifact_bytes > assembly_peak_bytes)); then
    assembly_peak_bytes=$assembled_artifact_bytes
fi
required_disk_bytes=0
eval "policy_input_bytes=\${$policy_input_name}"
required_disk_bytes=$(checked_add "$required_disk_bytes" "$policy_input_bytes")
required_disk_bytes=$(checked_add "$required_disk_bytes" "$retained_basin_output_bytes")
required_disk_bytes=$(checked_add "$required_disk_bytes" "$active_compile_scratch_bytes")
required_disk_bytes=$(checked_add "$required_disk_bytes" "$assembly_peak_bytes")
required_disk_bytes=$(checked_add "$required_disk_bytes" "$filesystem_overhead_bytes")
((available_memory_bytes >= required_memory_bytes)) ||
    hfx_die "insufficient memory: available $available_memory_bytes bytes; required $required_memory_bytes bytes"
((available_disk_bytes >= required_disk_bytes)) ||
    hfx_die "insufficient disk: available $available_disk_bytes bytes; required $required_disk_bytes bytes"
```

Capture workstation-side disk with exactly one identical retry for transport failure or empty output. The cleanup trap is armed, so a persistent failure is a deliberate refusal-to-teardown, not an accident. Assertions are unchanged; real shortfall tears down.

```bash
OBSERVED_AVAILABLE_DISK_BYTES=
for probe_attempt in 1 2; do
  OBSERVED_AVAILABLE_DISK_BYTES=$(ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "root@$SERVER_IP" "df -B1 --output=avail /mnt/hfx | tail -n 1 | tr -d ' '") && test -n "$OBSERVED_AVAILABLE_DISK_BYTES" && break
  OBSERVED_AVAILABLE_DISK_BYTES=
done
[[ "$OBSERVED_AVAILABLE_DISK_BYTES" =~ ^[0-9]+$ ]]
test "$OBSERVED_AVAILABLE_DISK_BYTES" -ge 496737129060
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary start --workload tdx-init -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh init --campaign tdx-m5-planetary \
  --workspace-root /mnt/hfx/work --retention-policy reclaim-inputs-after-terminal \
  --available-memory-bytes 30000000000 --available-disk-bytes "$OBSERVED_AVAILABLE_DISK_BYTES" \
  --peak-in-flight-download-bytes 44296724480 --retained-basin-output-bytes 206220202290 \
  --assembly-memory-ceiling-bytes 8000000000 --assembly-scratch-ceiling-bytes 206220202290 \
  --assembled-artifact-bytes 206220202290 --active-compile-scratch-bytes 30000000000 \
  --filesystem-overhead-bytes 5000000000
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary status --workload tdx-init
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary tail --log hfx-tdx-m5-planetary-tdx-init.log
```

The 5,000,000,000 operational margin is above the true requirement. The passed disk value is observed `df`, not the 560-billion accounting convention, which README line 632 says is not a future measurement.

```bash
ssh -o BatchMode=yes "root@$SERVER_IP" 'bash -s' <<'REMOTE_INIT_CHECK'
set -Eeuo pipefail
CAMPAIGN_DIR=/mnt/hfx/work/tdx-hydro-tdx-m5-planetary
test ! -e "$CAMPAIGN_DIR/state/selection.json"
jq -e 'length == 62' "$CAMPAIGN_DIR/state/inventory.json"
jq -e -S --slurp '.[0] == .[1]' /root/hfx/adapters/tdx-hydro/data/tdx_header_numbers.json "$CAMPAIGN_DIR/state/inventory.json"
test "$(find "$CAMPAIGN_DIR/state/basins" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')" = 62
test "$(find "$CAMPAIGN_DIR/state/basins" -mindepth 2 -maxdepth 2 -type f -name current.json | wc -l | tr -d ' ')" = 62
/root/hfx/scripts/hetzner/tdx-hydro-campaign.sh progress --campaign tdx-m5-planetary --workspace-root /mnt/hfx/work | tee /tmp/tdx-init-progress
grep -Fx inventory_count=62 /tmp/tdx-init-progress
! grep -F subset /tmp/tdx-init-progress
rm -- /tmp/tdx-init-progress
REMOTE_INIT_CHECK
campaign_gate hour-2 58 0
```

Progress is lock-free because `ab1e3c2:4954-4957` is exactly:

```bash
elif [[ "$subcommand" == progress ]]; then
    [[ -d "$campaign_dir" && ! -L "$campaign_dir" ]] || hfx_die "campaign does not exist safely: $campaign_dir"
    validate_workspace_state
    print_status
```

Never use `status` while another command may own it.

## 8. Calibration cohorts, disclosure, selection, and spend-abort threshold

Run sequentially, with no overlapping campaign command:

```bash
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary start --workload tdx-calibrate-2 -- /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh calibrate --campaign tdx-m5-planetary --workspace-root /mnt/hfx/work --max-parallel 2 --fabric-version NGA-TDX-Hydro-20230126
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary status --workload tdx-calibrate-2
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary tail --log hfx-tdx-m5-planetary-tdx-calibrate-2.log
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary start --workload tdx-calibrate-4 -- /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh calibrate --campaign tdx-m5-planetary --workspace-root /mnt/hfx/work --max-parallel 4 --fabric-version NGA-TDX-Hydro-20230126
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary status --workload tdx-calibrate-4
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary tail --log hfx-tdx-m5-planetary-tdx-calibrate-4.log
```

Status exit 0 means running; 3 means absent or finished. The canonical log's recorded command exit determines success. Cohort two must compile, terminal-classify, and reclaim before cohort four starts. The synchronous serial consumer at runner lines 2030-2052 selects one ready basin and calls `compile_basin_locked`. `calibrate` releases its lock at 4691-4692 before nested pipeline reacquires it at 1972; start nothing in that window. A premature pipeline produces the misleading selection-mismatch diagnostic; finish calibration instead.

Retain calibration JSON, cohorts, attempt traces, archived snapshots, logs, ordered disclosure, and raw/corrected values. Raw is bytes over gap-excluded whole span; resume inflates it; it is disclosure-only. Corrected is `.measurement.steady_state.throughput_bytes_per_second` and is threshold-valid only with `steady_state.compile_completions > 0`. Parallel four has zero by construction. Degenerate corrected intervals force completions to zero, conservatively causing abort; distinguish fallback only using disclosed attempts and traces.

The selection function is exactly `ab1e3c2:scripts/hetzner/tdx-hydro-campaign.sh:759-782`; obsolete comments at 751-758 are excluded and superseded by the binding 2,825,511 B/s runbook rule:

```bash
calibration_select() {
    local small=$1
    local large=$2
    local small_completions=$3
    local large_completions=$4
    local selected_value
    if ((small_completions > 0 && large_completions == 0)); then
        selected_value=2
    elif ((small_completions == 0 && large_completions > 0)); then
        selected_value=4
    elif ((small >= large)); then
        selected_value=2
    elif ((100 * small >= (100 - HFX_TDX_CALIBRATION_MARGIN_PERCENT) * large)); then
        selected_value=2
    else
        selected_value=4
    fi
    calibration_selection=$selected_value
    if ((small_completions > 0 || large_completions > 0)); then
        calibration_selection_validity=compile-observed
    else
        calibration_selection_validity=no-compile-observed
    fi
}
```

It chooses the only compile-observed setting, otherwise higher comparable corrected throughput, preferring two within five percent. When parallel four has no compile completions, use the persisted runner selection; within margin it must be two.

```bash
FROZEN_MAX_PARALLEL=$(ssh -o BatchMode=yes "root@$SERVER_IP" jq -er '.selected_max_parallel | select(. == 2 or . == 4)' "$CAMPAIGN_DIR/state/calibration.json")
BEST_VALID_CORRECTED=$(ssh -o BatchMode=yes "root@$SERVER_IP" jq -er '[.cohorts[] | select(.measurement.steady_state.compile_completions > 0) | .measurement.steady_state.throughput_bytes_per_second] | if length>0 then max else error("UNMET") end' "$CAMPAIGN_DIR/state/calibration.json") || BEST_VALID_CORRECTED=UNMET
test "$BEST_VALID_CORRECTED" != UNMET
awk -v rate="$BEST_VALID_CORRECTED" 'BEGIN { exit !(rate >= 2825511) }'
```

Comparability precedes ranking. If no compile-observed corrected window exists, record `UNMET`. If best valid is below 2,825,511 B/s, retain both measurements and reason, export evidence, and teardown. The threshold is manual runbook policy, not runner enforcement:

```text
549,279,383,552 / 2,825,511 = 194,400.016 seconds
+ 2 assembly + 12 verification + 2 provisioning + 2 teardown = 72.000004 hours
exact break-even = 2,825,511.23 B/s
```

The threshold admits the rounded edge, but the downstream strict dual gate refuses equality. It supersedes 4,167,474 B/s, a raw compile-free three-stream max-4 aggregate incorrectly applied to compile-contended max-2 corrected data and 47.5 percent too strict. Correct history: `549,279,383,552 / 4,167,474 = 131,801.51 seconds = 36h36m42s`, not 37 hours.

## 9. Overlapping acquisition and serial-compile pipeline

Repeat memory and disk probes immediately before pipeline:

```bash
ssh -o BatchMode=yes "root@$SERVER_IP" "awk '/MemAvailable:/ {exit !(\$2*1024>=30000000000)}' /proc/meminfo && test \"\$(df -B1 --output=avail /mnt/hfx | tail -n1 | tr -d ' ')\" -ge 491737129060"
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary start --workload tdx-pipeline -- /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh pipeline --campaign tdx-m5-planetary --workspace-root /mnt/hfx/work --max-parallel "$FROZEN_MAX_PARALLEL" --fabric-version NGA-TDX-Hydro-20230126
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary status --workload tdx-pipeline
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary tail --log hfx-tdx-m5-planetary-tdx-pipeline.log
```

The acquisition projection is 33-37 hours; serial compilation 16-18 hours; overlapping pipeline approximately 34-42 hours. The overlap figure is unsourced prose at README 592-593 and the largest budget term. M4 instead supplies phase-separated values, 1h48m-2h assembly, and 51-57h serial orientation. CPU saturation and spill writes have an unmeasured 20-30 percent overlap penalty.

## 10. Cumulative checkpoints, worst-case estimate gate, progress, and recovery

| Elapsed from provisioning request | Required state |
|---:|---|
| 0-2h | provision, bootstrap, convergence, price/quota/capacity/inventory/parity, init |
| hour 2 | temporal preflight gate |
| hour 24 | live `pipeline_reclaimed >= 26`; persisted checkpoint |
| 34-42h | calibration and pipeline finish |
| hour 44 | `pipeline_reclaimed=62`; persisted checkpoint |
| 1h48m-2h | assembly |
| hour 46 | temporal assembly gate |
| 0-12h after | verify, park, retain evidence |
| hour 58 | temporal parked-artifact/evidence gate |
| 0-2h | default teardown |
| hour 60 | zero footprint target |
| hour 72 | watchdog default teardown regardless of state |

Hours 2, 44, and 46 have zero internal slack; all 12 reserve hours lie after 46 and must always be charged. Hour-24 threshold derivation:

```text
72 - 2 provision - 2 assembly - 12 verification - 2 teardown = 54 pipeline hours
hour 24 is pipeline hour 22
62 * 22 / 54 = 25.259...; ceil = 26 reclaimed
```

The old 20 rule implied 68.2 pipeline hours. Paces 40.8, 44.2, 50.4, 54.6 would show about 33,31,27,25 and all pass 20. The 26 gate rejects breach. It is deliberately tighter than a 54-hour linear pace because about seven calibration hours front-load eight reclaimed: `8 + 54*15/47 = 25.2`. A miss is a durable stop, not immediate failure, though restart and re-verification I/O must be charged.

At every observation use the exact lock-free `progress`; preserve a failure, wait for the writer transition, retry identical argv once, then stop dispatch and classify. It may transiently see atomic writes. `expected 62 basin directories` always compares the full inventory even during a four-basin cohort. The stored checkpoint count is defined exactly at `ab1e3c2:1389-1398` as:

```bash
checkpoint_observed_terminal_count=$("$JQ" -r '[.basins[].status | select(. == "reclaimed")] | length' "$pipeline_state")
```

Only `reclaimed` counts; an advisory `terminal` does not. Read live `pipeline_reclaimed`, not the stored-as-of-stop count. `status` locks and may expose malformed calibration that progress does not.

```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "root@$SERVER_IP" /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh progress --campaign tdx-m5-planetary --workspace-root /mnt/hfx/work
checkpoint_status=0
ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "root@$SERVER_IP" /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh checkpoint --campaign tdx-m5-planetary --workspace-root /mnt/hfx/work --expected-terminal-count 26 2>&1 | tee "$LOCAL_EVIDENCE_DIR/checkpoint-hour-24.log" || checkpoint_status=$?
```

At hour 44 repeat progress, then:

```bash
checkpoint_status=0
ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "root@$SERVER_IP" /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh checkpoint --campaign tdx-m5-planetary --workspace-root /mnt/hfx/work --expected-terminal-count 62 2>&1 | tee "$LOCAL_EVIDENCE_DIR/checkpoint-hour-44.log" || checkpoint_status=$?
```

With pipefail, the status is SSH's. Every invocation resets `checkpoint_status=0`. A met result exits 0 and says `not-required`; a missed result prints five complete lines and exits 1; stopped replay prints missed and exits 1; every refusal exits 1. Status 1 is recoverable only if retained output is a complete expected missed contract:

```text
checkpoint_expected_terminal_count=<integer>
checkpoint_observed_terminal_count=<integer>
checkpoint_result=<met|missed>
checkpoint_run_state=<running|stopped>
checkpoint_signal=<not-required|sent|no-live-owner>
```

The command signals the campaign-lock owner, not specifically the scheduler (lines 1425-1502). `checkpoint` and resume do not lock; dispatch lines 4961-4972 only validate structure. A miss may interrupt any lock owner, but planned checkpoints occur during pipeline. All state writes are atomic and identical argv is recoverable after gate PASS.

```bash
campaign_gate checkpoint-resume 28 0
ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "root@$SERVER_IP" /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh checkpoint-resume --campaign tdx-m5-planetary --workspace-root /mnt/hfx/work
```

Resume does not start pipeline; rerun the exact frozen pipeline argv. Absent snapshot says exactly `hfx: error: pipeline snapshot is absent; run pipeline with the frozen max-parallel and fabric version, then rerun checkpoint`: resume checkpoint control if stopped, rerun pipeline, repeat checkpoint. Malformed output must be retained as `checkpoint_state=malformed` and `checkpoint_recovery=run checkpoint-resume`; resume archives rejection. Hours 2,46,58,60 use `campaign_gate`, never expected count zero.

Ordinary interruption: progress, ensure no live owner, then:

```bash
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary start --workload tdx-recover -- /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh recover --campaign tdx-m5-planetary --workspace-root /mnt/hfx/work
```

Then gate and rerun pipeline. Resume re-hashes succeeded finals, so charge I/O. Reclaimed failed basins cannot retry in place; separately authorized reacquisition costs about 30 minutes.

```bash
jq -e '(.basin_ids|length)==62 and (.basin_ids==(.basin_ids|sort|unique)) and (.basins|keys)==.basin_ids and ([.basins[].status|select(.=="reclaimed")]|length)==62' "$CAMPAIGN_DIR/state/pipeline.json"
test "$(find "$CAMPAIGN_DIR/state/basins" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')" = 62
test "$(find "$CAMPAIGN_DIR/state/basins" -mindepth 2 -maxdepth 2 -type f -name current.json | wc -l | tr -d ' ')" = 62
```

Every authoritative basin needs a durable attempted outcome. Named exclusions are terminal attempts; do not claim complete coverage without 62 compile successes.

## 11. Failure classification and named exclusions

| Failure class | Required action |
|---|---|
| Reproducible named basin source/geometry/topology contract fatal | Name basin and exact fatal; continue under partial-fabric rules |
| Transient NGA, SSH, Git, apt, quota, infrastructure | Stop dispatch; identical argv only after gate PASS |
| Retained acquisition/compile conflict | Retain exact paths; runner named-path remedy; budget 30m reacquisition |
| Pipeline/FIFO/lock/liveness/state-transition/orchestration defect | No artifact; evidence, teardown, return M5-S3B |
| Adapter/assembly/order/merged-snap/coverage/validator landed defect | Stop, no artifact, teardown, re-enter M2 |
| Parity/ref/dependency/inventory/capacity/price/identity preflight | Do not acquire; teardown if provisioned |
| Missed cumulative checkpoint | Stop; gate PASS then resume, otherwise teardown |
| stopped checkpoint expects N | resume, then equal-or-higher checkpoint |
| expected count cannot decrease below N | rerun N or higher |
| unsafe checkpoint lock/owner/contents/recovery path | inspect and move only exact named entry, rerun named command |
| TERM delivery failure or indeterminate PID | rerun same expectation, or resolve exact PID/ps ambiguity |
| recovery archive full | move preserved rejected entries to operator storage, resume |
| invalid observed count | preserve, export, teardown; no in-place source remedy |
| either switch reached/projected | no resume, no retention; salvage, teardown |
| parking/export unproven | no success; retain only below switches, else teardown |
| selection mismatch while calibration pending | finish active calibration; do not alter selection |
| 62-directory diagnostic during cohort | retry progress after writer; never expect four |
| publish nonzero without `hfx: error:` | assume unguarded `aws s3 cp`; preserve stderr/current.json; convergent retry only below switches |
| opaque adapter build failure | verify script, Python, and HFX exact paths |
| signal during lock-taking command | exact stale-lock procedure, then gated identical argv |
| takeover/destination already exists | exact manual recovery below after dead-owner proof |
| source-undetermined diagnostic | escalation-only, preserve, export, teardown |

Arbitrary adapter failures are not exclusions. A genuine basin fatal surfaces as adapter build/validation failure, is reclaim-eligible, finishes `reclaimed`, and counts (lines 2158-2164,2369-2383). `blocked` only represents retained inspection conflicts, is uncounted, and prevents completion. Remediate each blocked basin; hour-44 expectation remains 62 and expectations never decrease. Assemble only succeeded basins after every exclusion is proven basin-side.

Manual lock recovery uses no glob and no recursive removal. After proving no live campaign process and retaining the exact listing:

```bash
TAKEOVER_PATH="$CAMPAIGN_DIR/state/locks/campaign.lock.takeover"
test -d "$TAKEOVER_PATH" && test ! -L "$TAKEOVER_PATH"
test -f "$TAKEOVER_PATH/owner.pid" && test ! -L "$TAKEOVER_PATH/owner.pid"
test "$(find "$TAKEOVER_PATH" -mindepth 1 -maxdepth 1 | wc -l | tr -d ' ')" = 1
TAKEOVER_PID=$(cat "$TAKEOVER_PATH/owner.pid"); [[ "$TAKEOVER_PID" =~ ^[0-9]+$ ]]
! ps -p "$TAKEOVER_PID" >/dev/null 2>&1
rm -- "$TAKEOVER_PATH/owner.pid"; rmdir -- "$TAKEOVER_PATH"
```

For `.campaign.lock.stale.<pid>`, manually copy only the exact diagnostic path:

```bash
read -r -p 'Exact stale lock path from diagnostic: ' STALE_LOCK_PATH
case "$STALE_LOCK_PATH" in "$CAMPAIGN_DIR/state/locks/".campaign.lock.stale.[0-9]*) ;; *) exit 1;; esac
test -d "$STALE_LOCK_PATH" && test ! -L "$STALE_LOCK_PATH"
test -f "$STALE_LOCK_PATH/owner.pid" && test ! -L "$STALE_LOCK_PATH/owner.pid"
test "$(find "$STALE_LOCK_PATH" -mindepth 1 -maxdepth 1 | wc -l | tr -d ' ')" = 1
STALE_PID=$(cat "$STALE_LOCK_PATH/owner.pid"); [[ "$STALE_PID" =~ ^[0-9]+$ ]]; ! ps -p "$STALE_PID" >/dev/null 2>&1
rm -- "$STALE_LOCK_PATH/owner.pid"; rmdir -- "$STALE_LOCK_PATH"
```

Ambiguity means remove nothing and teardown. Escalation-only diagnostics include indeterminate preserved owner/PID; unsafe renamed stale lock; nonregular temporary state; prior durable calibration work; missing/unsafe/malformed/regressed/empty attempt traces; and nonpositive measurement intervals/bytes. Never invent state surgery.

## 12. Assembly and the hour-46 gate

Require pipeline stopped; 62 attempted; every failure named basin-side; no pending/acquiring/ready/compiling/terminal/blocked status; succeeded plus named exclusions equals 62; 8GB memory; persisted assembly disk requirement.

```bash
ssh -o BatchMode=yes "root@$SERVER_IP" 'bash -s' <<'REMOTE_ASSEMBLY_PREFLIGHT'
set -Eeuo pipefail
C=/mnt/hfx/work/tdx-hydro-tdx-m5-planetary
awk '/MemAvailable:/ {exit !($2*1024>=8000000000)}' /proc/meminfo
required=$(jq -er '[.sizing.assembly_scratch_ceiling_bytes, .sizing.assembled_artifact_bytes] | max | numbers' "$C/state/campaign.json")
available=$(df -B1 --output=avail /mnt/hfx | tail -n1 | tr -d ' ')
[[ "$available" =~ ^[0-9]+$ ]]; test "$available" -ge "$required"
REMOTE_ASSEMBLY_PREFLIGHT
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary start --workload tdx-assemble -- /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh assemble --campaign tdx-m5-planetary --workspace-root /mnt/hfx/work
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary status --workload tdx-assemble
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary tail --log hfx-tdx-m5-planetary-tdx-assemble.log
campaign_gate hour-46 14 0
```

Allow 1h48m-2h. A gate miss precedes all validation or parking.

## 13. Verification and validation

```bash
ssh -o BatchMode=yes "root@$SERVER_IP" 'bash -s' <<'REMOTE_VALIDATE' 2>&1 | tee "$LOCAL_EVIDENCE_DIR/validation.log"
set -Eeuo pipefail
cd /root/hfx/adapters/tdx-hydro
/opt/hfx-geo/bin/python rehearse_assembly_scale.py verify /mnt/hfx/work/tdx-hydro-tdx-m5-planetary/assembly/dataset --batch-size 65536
/root/hfx/target/release/hfx /mnt/hfx/work/tdx-hydro-tdx-m5-planetary/assembly/dataset --strict --sample-pct 100 --format text
/opt/hfx-geo/bin/python /root/hfx/adapters/tdx-hydro/build_adapter.py validate /mnt/hfx/work/tdx-hydro-tdx-m5-planetary/assembly/dataset --hfx-binary /root/hfx/target/release/hfx
REMOTE_VALIDATE
```

The adapter wrapper at `build_adapter.py:5003-5050` invokes release HFX with strict/100/text, validates catchments and snap stems as GeoParquet 1.1, and requires graph to be non-GeoParquet. Its parser is 5053-5081; M3 verifier parser is `rehearse_assembly_scale.py:1093-1100`. M3 must prove whole-file nondecreasing order, snap IDs 1..N, `hfx.aux.snap.v2`, `references_levels=[0]`, and all snap unit IDs resolve. It does not prove every catchment has a snap; per-basin evidence and assembly construction remain required.

Complete coverage requires 62 successes. Otherwise use ADR partial-fabric `region` and float32 covering-union bbox. Any landed basin/manifest/report/validator mismatch is a landed-code defect and re-enters M2.

## 14. Scratch-prefix-only parking and attribution identity

Generate deterministic evidence before publication and require its acquisition, outcomes, and diagnostics to cover exactly all 62 authoritative IDs, every attempted basin, and every named exclusion:

```bash
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary start --workload tdx-evidence -- /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh evidence --campaign tdx-m5-planetary --workspace-root /mnt/hfx/work
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary status --workload tdx-evidence
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary tail --log hfx-tdx-m5-planetary-tdx-evidence.log
```

```bash
ssh -o BatchMode=yes "root@$SERVER_IP" 'bash -s' <<'REMOTE_ATTR'
set -Eeuo pipefail
C=/mnt/hfx/work/tdx-hydro-tdx-m5-planetary
tracked=$(mktemp -d)
trap 'rm -r -- "$tracked"' EXIT
git -C /root/hfx show HEAD:adapters/tdx-hydro/NOTICE > "$tracked/NOTICE"
git -C /root/hfx show HEAD:adapters/tdx-hydro/CITATION.txt > "$tracked/CITATION.txt"
cmp "$tracked/NOTICE" /root/hfx/adapters/tdx-hydro/NOTICE
cmp "$tracked/CITATION.txt" /root/hfx/adapters/tdx-hydro/CITATION.txt
sha256sum /root/hfx/adapters/tdx-hydro/NOTICE /root/hfx/adapters/tdx-hydro/CITATION.txt > "$C/reports/attribution.sha256"
REMOTE_ATTR
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary start --workload tdx-publish -- /bin/bash -c 'set -Eeuo pipefail; set +x; source /etc/pourpoint-hfx.env; exec /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh publish --campaign tdx-m5-planetary --workspace-root /mnt/hfx/work --out /mnt/hfx/work/tdx-hydro-tdx-m5-planetary/assembly/dataset --report /mnt/hfx/work/tdx-hydro-tdx-m5-planetary/reports/assembly.json --notice /root/hfx/adapters/tdx-hydro/NOTICE --citation /root/hfx/adapters/tdx-hydro/CITATION.txt --scratch-prefix scratch/tdx-hydro-tdx-m5-planetary/planetary-hfx-v0.3.0'
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary status --workload tdx-publish
./scripts/hetzner/launch.sh --campaign tdx-m5-planetary tail --log hfx-tdx-m5-planetary-tdx-publish.log
```

Publication predicates at `ab1e3c2:scripts/hetzner/tdx-hydro-campaign.sh:3836-3851` are exact:

```bash
acquire_campaign_lock
validate_publication_campaign_state
[[ "$scratch_prefix" =~ ^scratch/tdx-hydro-$campaign/[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] ||
    hfx_die 'scratch prefix must match the campaign-scoped scratch grammar'
for entry in "$publication_out" "$publication_report" "$publication_notice" "$publication_citation"; do
    [[ "$entry" == /* && ! "$entry" =~ [[:cntrl:]] ]] ||
        hfx_die "publication paths must be absolute and contain no control characters: $entry"
done
[[ -d "$publication_out" && ! -L "$publication_out" ]] ||
    hfx_die "publication output is not a regular non-symlink directory: $publication_out"
[[ -f "$publication_report" && ! -L "$publication_report" && -s "$publication_report" ]] ||
    hfx_die "publication report is not a nonempty regular non-symlink file: $publication_report"
[[ -f "$publication_notice" && ! -L "$publication_notice" && -s "$publication_notice" ]] ||
    hfx_die "NOTICE is not a nonempty regular non-symlink file: $publication_notice"
[[ -f "$publication_citation" && ! -L "$publication_citation" && -s "$publication_citation" ]] ||
    hfx_die "CITATION.txt is not a nonempty regular non-symlink file: $publication_citation"
```

Dataset root must contain no competing attribution.

After parking, silently source only `/etc/pourpoint-hfx.env`, download exact keys, compare them, and inventory only the frozen prefix:

```bash
ssh -o BatchMode=yes "root@$SERVER_IP" 'bash -s' <<'REMOTE_PARK_PROOF' 2>&1 | tee "$LOCAL_EVIDENCE_DIR/parking-proof.log"
set -Eeuo pipefail
set +x
source /etc/pourpoint-hfx.env
tmp=$(mktemp -d)
trap 'rm -r -- "$tmp"' EXIT
aws s3 cp s3://pourpoint-hfx/scratch/tdx-hydro-tdx-m5-planetary/planetary-hfx-v0.3.0/NOTICE "$tmp/NOTICE" --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --only-show-errors
aws s3 cp s3://pourpoint-hfx/scratch/tdx-hydro-tdx-m5-planetary/planetary-hfx-v0.3.0/CITATION.txt "$tmp/CITATION.txt" --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --only-show-errors
cmp "$tmp/NOTICE" /root/hfx/adapters/tdx-hydro/NOTICE
cmp "$tmp/CITATION.txt" /root/hfx/adapters/tdx-hydro/CITATION.txt
sha256sum "$tmp/NOTICE" "$tmp/CITATION.txt"
wc -c "$tmp/NOTICE" "$tmp/CITATION.txt"
aws s3api list-objects-v2 --bucket pourpoint-hfx --prefix scratch/tdx-hydro-tdx-m5-planetary/planetary-hfx-v0.3.0/ --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --output json --no-paginate > /mnt/hfx/work/tdx-hydro-tdx-m5-planetary/reports/remote-scratch-inventory.json
jq -e --slurpfile publication /mnt/hfx/work/tdx-hydro-tdx-m5-planetary/publication/current.json '
  ([.Contents[] | {key:.Key,bytes:.Size}] | sort_by(.key)) ==
  ($publication[0].objects | map({key:.key,bytes:.bytes}) | sort_by(.key))
' /mnt/hfx/work/tdx-hydro-tdx-m5-planetary/reports/remote-scratch-inventory.json
REMOTE_PARK_PROOF
```

Never list, write, or delete an `hfx/` key. The temporary directory is removed by exact path only.

## 15. Evidence export and the hour-58 gate

```bash
ssh -o BatchMode=yes "root@$SERVER_IP" "find '$CAMPAIGN_DIR/assembly/dataset' -type f -print0 | sort -z | xargs -0 wc -c" > "$LOCAL_EVIDENCE_DIR/dataset-bytes.txt"
ssh -o BatchMode=yes "root@$SERVER_IP" "find '$CAMPAIGN_DIR/assembly/dataset' -type f -print0 | sort -z | xargs -0 sha256sum" > "$LOCAL_EVIDENCE_DIR/dataset-sha256.txt"
scp -r -o BatchMode=yes "root@$SERVER_IP:$CAMPAIGN_DIR/state" "$LOCAL_EVIDENCE_DIR/state"
scp -r -o BatchMode=yes "root@$SERVER_IP:$CAMPAIGN_DIR/publication/evidence" "$LOCAL_EVIDENCE_DIR/publication-evidence"
scp -r -o BatchMode=yes "root@$SERVER_IP:$CAMPAIGN_DIR/reports" "$LOCAL_EVIDENCE_DIR/reports"
scp -o BatchMode=yes "root@$SERVER_IP:$CAMPAIGN_DIR/assembly/dataset/manifest.json" "$LOCAL_EVIDENCE_DIR/manifest.json"
campaign_gate hour-58 2 0
```

Use further explicit source paths to export all workload/validation logs, scratch inventory, attribution proof, 62 basin records, acquisition/diagnostic/assembly reports, checkpoint history/recovery, calibration traces/cohorts/snapshots, pipeline/compile/assembly/publication contracts, and off-VM timestamps, price, gates, watchdog, revision, bootstrap, parity, capacity, and inventory evidence. Never copy a broad parent containing credentials. Verify every required local file. Evidence documents must cover exactly the 62 IDs, every attempt, and named exclusions.

## 16. Mandatory default teardown, hour-60 target, hour-72 hard ceiling, and exact-name zero-footprint proof

The explicit success path invokes the same cleanup:

```bash
cleanup
```

It runs exactly:

```bash
./scripts/hetzner/teardown.sh --campaign tdx-m5-planetary 2>&1 | tee "$LOCAL_EVIDENCE_DIR/teardown.log"
```

and must retain verbatim:

```text
hfx: campaign tdx-m5-planetary has zero Hetzner footprint: server hfx-build-tdx-m5-planetary absent; volume hfx-build-tdx-m5-planetary-data absent
```

Then it independently retains exact-name server and volume lists and proves both filtered arrays empty. These three proofs apply on success, abort, failure, refusal, signal, watchdog, and every exit. README's sanctioned-pause keep-volume example is stale for this node. Teardown's exact mutation and proof are at `ab1e3c2:scripts/hetzner/teardown.sh:247-330`.

Target zero footprint by hour 60. At hour 72 default teardown runs regardless of state. On teardown failure preserve diagnostics, inspect only exact campaign names, rerun default teardown, and keep watchdog armed. After normal proof:

```bash
kill "$WATCHDOG_PID" 2>/dev/null || true
wait "$WATCHDOG_PID" 2>/dev/null || true
CLEANUP_COMPLETE=1
trap - EXIT INT TERM HUP
```

## 17. Local zero-cloud verification and landing gates

These are documentation-author checks, not paid-operation commands. At `ab1e3c2`, inspect parser gates, checkpoint schema/diagnostics/status block, lifecycle parsers, adapter parser, and M3 parser. Then run only fake-client fixtures:

```bash
/bin/bash scripts/hetzner/tdx-hydro-campaign.sh --help
HFX_TEST_FOCUS=checkpoint-schema /bin/bash scripts/hetzner/test-tdx-hydro-campaign.sh
HFX_TEST_FOCUS=checkpoint-stop /bin/bash scripts/hetzner/test-tdx-hydro-campaign.sh
HFX_TEST_FOCUS=checkpoint-progress /bin/bash scripts/hetzner/test-tdx-hydro-campaign.sh
HFX_TEST_FOCUS=cohort /bin/bash scripts/hetzner/test-tdx-hydro-campaign.sh
HFX_TEST_FOCUS=measurement /bin/bash scripts/hetzner/test-tdx-hydro-campaign.sh
HFX_TEST_FOCUS=calibration-replay /bin/bash scripts/hetzner/test-tdx-hydro-campaign.sh
HFX_TEST_FOCUS=disclosure /bin/bash scripts/hetzner/test-tdx-hydro-campaign.sh
HFX_TEST_FOCUS=scheduler-shape /bin/bash scripts/hetzner/test-tdx-hydro-campaign.sh
/bin/bash scripts/hetzner/test-tdx-hydro-campaign.sh
```

Focus labels are at harness lines 1166-1176. Fake clients shadow cloud, network, SSH, AWS, and curl. Static-audit every code block against the pinned parsers: runner/lifecycle/adapter/verifier forms; workload grammar; positional HFX dataset; no executable keep-volume; no secret read/print/hash/copy; no wildcard/selector cleanup; no protected-resource command; concrete paid values.

Repository gates:

```bash
test -f scripts/hetzner/RUNBOOK-tdx-hydro-planetary.md
test -f scripts/hetzner/RUNBOOK-tdx-hydro-assembly-subset.md
git diff --exit-code -- scripts/hetzner/RUNBOOK-tdx-hydro-assembly-subset.md
git add --intent-to-add scripts/hetzner/RUNBOOK-tdx-hydro-planetary.md
git diff --name-only -- | diff -u - <(printf '%s\n' scripts/hetzner/RUNBOOK-tdx-hydro-planetary.md)
git diff --check
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build -p hfx-cli
for d in conformance/valid/*/ examples/tiny/; do ./target/debug/hfx "$d"; done
for d in conformance/invalid/*/; do rc=0; ./target/debug/hfx "$d" || rc=$?; test "$rc" -eq 1; done
spec_v=$(grep -m1 -oE 'Version [0-9]+\.[0-9]+\.[0-9]+' spec/HFX_SPEC.md | awk '{print $2}')
schema_v=$(jq -r '.properties.format_version.const' schemas/manifest.schema.json)
test "$spec_v" = "$schema_v"
```

Also run `check-jsonschema --schemafile schemas/manifest.schema.json ...` and `make docs` where those tools are available. Inspect the diff, confirm only this new runbook, log completion with `clog`, and commit with a conventional documentation message. Do not bump versions, edit CHANGELOG, tag, push, provision, or acquire.
