# TDX-Hydro seven-basin acquisition campaign

This runbook is an instruction document. Authoring and reviewing it provisions nothing, contacts neither NGA nor S3, performs no acquisition or teardown, and writes nothing to object storage.

This campaign performs bounded acquisition and inspection only. It does not compile, adjudicate source or adapter behavior, assemble, publish, delete data, or write S3 objects. It halts after off-VM preservation and requires separate human approval before any S3 publication or capacity decision.

ENVIRONMENT HAZARD: GDAL must be installed and discoverable via gdal-config.

version_policy is NONE: NO version bump, NO tag.

## 1. Authority, purpose, and absolute prohibitions

The immutable authoring authority is `266df3755754a679c60e5f2c3c8e2c8e8db4b663`. The exact command surfaces are runner usage at `266df3755754a679c60e5f2c3c8e2c8e8db4b663:scripts/hetzner/tdx-hydro-campaign.sh:59-74`; bounded option dispatch and validation at lines `4973-5024,5104-5112`; init retention and sizing validation at lines `5168-5211`; immutable selection and parameter comparison at lines `2780-2804`; bounded-state conversion at lines `2928-3004`; bounded product behavior at lines `4671-4718`; and low-concurrency dispatch at lines `4762-4792`. Lifecycle authority is provision usage/parser at `scripts/hetzner/provision.sh:14-27,323-375`, bootstrap usage at `scripts/hetzner/bootstrap.sh:16-20`, launch usage at `scripts/hetzner/launch.sh:14-25`, and teardown usage, parser, and zero-footprint result at `scripts/hetzner/teardown.sh:14-22,222-241,247-330`.

Only server `hfx-build-tdx-m5-seven-acquire` and volume `hfx-build-tdx-m5-seven-acquire-data` may be mutated. Globs, wildcards, prefix matches, label selectors, label mutation, S3 calls, AWS CLI calls, compilation, adapter invocation, assembly, publication, deletion of data, `--keep-volume`, and every contact with `pourpoint-web-1` are forbidden. Mandatory exact-resource teardown is the sole permitted destructive operation. Read-only project-wide quota listings are permitted only in the named preflight and post-teardown audit; they never authorize mutation.

This remains off-campaign authoring until a maintainer gives explicit human approval at the console. No unattended process may approve the run.

## 2. Frozen identity, selection, retention, and shell setup

The campaign is `tdx-m5-seven-acquire`, using one `ccx33` in `fsn1`, one 600 GB volume, workspace `/mnt/hfx/work`, and campaign directory `/mnt/hfx/work/tdx-hydro-tdx-m5-seven-acquire`. The seven absent basins are `1020018110`, `2020003440`, `2020065840`, `2020071190`, `4020050470`, `5020049720`, and `6020000010`. The control is `7020000010`; the operator may not replace it.

The control occurs in the authoritative 62-basin inventory at `adapters/tdx-hydro/data/tdx_header_numbers.json:48`. The historical record at `scripts/hetzner/CAMPAIGN-tdx-hydro-7020000010.md:44-57,66-98,163-165` records successful acquisition, compile, strict validation, adapter validation, and scratch verification. The preserved 55-basin roster is exactly the authoritative 62 IDs less the seven absent IDs, so `7020000010` is in that roster.

Retention is `retain-all-through-publication`. `reclaim-inputs-after-terminal` deletes both acquired GeoPackages after a basin reaches a terminal compile state. The three previous compile-failure basins therefore have no source data, and roughly 85 GB must now be acquired again. Using reclaim again could pay the full transfer cost, succeed, and then destroy the geometry needed for later data-backed adjudication. Only an explicit maintainer decision recorded in this tracked runbook before merge may choose another policy. No operator-time deviation is allowed.

Init freezes the sorted selection in `state/selection.json`. A changed selection emits `hfx: error: basin selection changed; use a new campaign ID`. Init also compares canonical `campaign.json`; changed retention or sizing emits `hfx: error: campaign parameters changed; use a new campaign ID`. Any later change requires a new campaign ID.

Tracing is disabled before the path is read. The file is accepted only as an opaque path. Never display, cat, echo, source, log, archive, copy, hash, or otherwise transmit the file or its contents, and never include it in evidence.

Run in one Bash 4-or-newer shell:

```bash
test -n "${BASH_VERSION:-}" && test "${BASH_VERSINFO[0]}" -ge 4 || {
  printf '%s\n' 'Bash 4 or newer is required' >&2
  exit 1
}
set -Eeuo pipefail
set +x
IFS=$'\n\t'

CAMPAIGN=tdx-m5-seven-acquire
SERVER_NAME=hfx-build-tdx-m5-seven-acquire
VOLUME_NAME=hfx-build-tdx-m5-seven-acquire-data
SERVER_TYPE=ccx33
VOLUME_SIZE_GB=600
LOCATION=fsn1
WORKSPACE_ROOT=/mnt/hfx/work
CAMPAIGN_DIR=/mnt/hfx/work/tdx-hydro-tdx-m5-seven-acquire
LOCAL_EVIDENCE_DIR="$PWD/tdx-m5-seven-acquire-evidence"
BUDGET_CEILING_EUR=25.00
ELAPSED_CEILING_HOURS=54
mkdir -p -- "$LOCAL_EVIDENCE_DIR"
chmod 700 -- "$LOCAL_EVIDENCE_DIR"

printf '%s\n' 'Enter the secrets environment FILE PATH (contents must never be displayed):' >&2
IFS= read -r S3_ENV_FILE
test -n "$S3_ENV_FILE"
test -f "$S3_ENV_FILE"
test ! -L "$S3_ENV_FILE"
test -r "$S3_ENV_FILE"
test -s "$S3_ENV_FILE"

printf '%s\n' 'Type APPROVE to authorize one ccx33 in fsn1, one 600 GB volume, a 54-hour ceiling, and a EUR 25.00 gross ceiling:' >&2
IFS= read -r HUMAN_APPROVAL
test "$HUMAN_APPROVAL" = APPROVE
```

The tracked `provision.sh` installs the supplied file on the VM. This conflicts with the preceding no-copy/no-transmit rule. Do not call `provision.sh` unless the maintainer separately confirms that passing this file to the exact tracked provisioner is the intended, narrowly authorized exception. Record approval locally without the path or contents. If the exception is denied or uncertain, stop with zero cloud footprint. Do not invent an alternate provisioner.

```bash
printf '%s\n' 'Type APPROVE-TRACKED-PROVISIONER-TRANSFER to authorize the tracked provisioner exception:' >&2
IFS= read -r TRANSFER_APPROVAL
test "$TRANSFER_APPROVAL" = APPROVE-TRACKED-PROVISIONER-TRANSFER
printf '%s\n' 'maintainer approved the tracked provisioner credential transfer exception' \
  > "$LOCAL_EVIDENCE_DIR/provisioner-transfer-approval.txt"
```

## 3. Current price inputs, arithmetic, and kill switches

Immediately before provisioning, obtain from the Hetzner console whether prices include VAT, VAT percentage, gross-or-net `ccx33` EUR/hour, volume EUR/GB-month, included outbound bytes, outbound overage EUR per billing unit, billing-unit bytes, and a conservative billable outbound byte count for copying up to 120 GB of inputs plus state, reports, and logs off the VM. Blank, malformed, negative, NaN, infinite, or unknown values refuse provisioning.

```text
8 selected basins / 2 concurrently dispatched basins = 4 waves
4 waves * 2 serial products per basin * 3 attempts per product * 92 minutes per attempt
= 2,208 minutes
= 36.8 hours bounded-acquisition budget

4.00 hours provisioning + bootstrap + release-build allowance
+ 120,000,000,000 bytes / 5,000,000 bytes/second / 3,600 seconds/hour
  = 6.67 hours section-10 off-VM-copy allowance at a conservative 5 MB/s downlink
+ 1.00 hour teardown allowance
+ 36.80 hours bounded-acquisition budget
= 48.47 hours derived worst-case elapsed budget

48.47 hours derived worst-case elapsed budget < 54 hours elapsed ceiling
```

`--max-parallel 2` dispatches at most two basins concurrently. Each basin's two products are serial, and each product has at most three 92-minute attempts. The 54-hour ceiling contains the frozen bounded work plus the named setup, off-VM copy, and teardown allowances. At 54 hours from the provisioning request, stop every dispatch and salvage, then run mandatory default teardown.

Use the planetary runbook's numeric validation and VAT normalization pattern, with the full 54-hour ceiling and a strict total below EUR 25.00:

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
  server54 = server * factor * 54
  volume54 = 600 * volume * factor * 54 / 730
  excess = outbound > included ? outbound - included : 0
  units = excess == 0 ? 0 : int((excess + unit - 1) / unit)
  outbound_cost = units * overage * factor
  total = server54 + volume54 + outbound_cost
  printf "vat_inclusive=%s\nvat_percent_entered=%s\nserver_eur_per_hour_entered=%s\nvolume_eur_per_gb_month_entered=%s\n", vat_in, vat, server, volume
  printf "outbound_eur_per_unit_entered=%s\nserver_eur_per_hour_gross=%.10f\n", overage, server*factor
  printf "volume_eur_per_gb_month_gross=%.10f\nincluded_outbound_bytes=%.0f\n", volume*factor, included
  printf "outbound_eur_per_unit_gross=%.10f\noutbound_unit_bytes=%.0f\nbillable_outbound_bytes=%.0f\n", overage*factor, unit, outbound
  printf "server_54h_eur=%.10f\nvolume_54h_eur=%.10f\noutbound_eur=%.10f\nprojected_gross_total=%.10f\n", server54, volume54, outbound_cost, total
  if (!(total < 25.00)) exit 3
}' > "$LOCAL_EVIDENCE_DIR/current-price-preflight.txt"
```

Retain this calculation with the immediately preceding console inputs substituted as numbers:

```text
gross_server_ceiling_hours = gross_ccx33_eur_per_hour * 54
gross_volume_ceiling_hours = 600 * gross_volume_eur_per_gb_month * 54 / 730
gross_outbound = ceil(max(0, billable_outbound_bytes - included_outbound_bytes) / outbound_billing_unit_bytes) * gross_outbound_eur_per_unit
projected_gross_total = gross_server_ceiling_hours + gross_volume_ceiling_hours + gross_outbound
required result: projected_gross_total < EUR 25.00
```

The hard switches are 54 elapsed hours from the provisioning request or EUR 25.00 projected/actual gross spend, whichever comes first. Equality refuses. Before every new acquisition or recovery dispatch, recalculate current elapsed time and projected and actual gross spend from the recorded epoch and current console values. Proceed only when elapsed and projected completion are strictly below 54 and projected and actual gross spend are strictly below EUR 25.00. If preflight cannot prove the full 54-hour campaign strictly below EUR 25.00, refuse provisioning. If projected or actual gross spend reaches EUR 25.00 after provisioning, stop every dispatch, salvage, and run mandatory default teardown.

Install this gate after provisioning. Enter the current actual gross campaign spend from the console on every call. It rejects malformed values, equality, elapsed time at the ceiling, projected completion at the ceiling, and projected or actual gross cost at the ceiling.

```bash
campaign_gate() {
  test "$#" -eq 2
  local phase=$1 remaining_hours=$2 origin now actual_gross elapsed projected_hours projected_gross
  local current_server_gross current_volume_gross current_included current_outbound_gross current_unit current_billable
  [[ "$phase" =~ ^[a-z0-9-]+$ ]]
  [[ "$remaining_hours" =~ ^[0-9]+([.][0-9]+)?$ ]]
  origin=$(<"$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt")
  [[ "$origin" =~ ^[0-9]+$ ]]
  read -r -p 'Current gross ccx33 EUR/hour: ' current_server_gross
  read -r -p 'Current gross volume EUR/GB-month: ' current_volume_gross
  read -r -p 'Current included outbound bytes: ' current_included
  read -r -p 'Current gross outbound EUR/billing-unit: ' current_outbound_gross
  read -r -p 'Current outbound billing-unit bytes: ' current_unit
  read -r -p 'Current conservative billable outbound bytes: ' current_billable
  read -r -p 'Current actual gross campaign spend EUR: ' actual_gross
  for value in "$current_server_gross" "$current_volume_gross" "$current_included" \
    "$current_outbound_gross" "$current_unit" "$current_billable" "$actual_gross"; do
    [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]]
  done
  awk -v unit="$current_unit" 'BEGIN{exit !(unit > 0)}'
  now=$(date +%s)
  IFS=' ' read -r elapsed projected_hours projected_gross < <(
    awk -v now="$now" -v origin="$origin" -v remaining="$remaining_hours" \
      -v actual="$actual_gross" \
      -v server="$current_server_gross" -v volume="$current_volume_gross" \
      -v included="$current_included" -v overage="$current_outbound_gross" \
      -v unit="$current_unit" -v outbound="$current_billable" '
      BEGIN {
        elapsed=(now-origin)/3600; projected=elapsed+remaining;
        excess=outbound>included?outbound-included:0;
        units=excess==0?0:int((excess+unit-1)/unit);
        projected_cost=actual+remaining*(server+600*volume/730)+units*overage;
        printf "%.10f %.10f %.10f\n",elapsed,projected,projected_cost
      }')
  if ! awk -v elapsed="$elapsed" -v projected="$projected_hours" \
    -v actual="$actual_gross" -v cost="$projected_gross" \
    'BEGIN{exit !(elapsed < 54 && projected < 54 && actual < 25.00 && cost < 25.00)}'; then
    : > "$LOCAL_EVIDENCE_DIR/hard-ceiling-reached"
    return 1
  fi
  printf 'phase=%s\nserver_eur_per_hour_gross=%s\nvolume_eur_per_gb_month_gross=%s\nincluded_outbound_bytes=%s\noutbound_eur_per_unit_gross=%s\noutbound_unit_bytes=%s\nbillable_outbound_bytes=%s\nelapsed_hours=%s\nprojected_completion_hours=%s\nactual_gross_eur=%s\nprojected_gross_eur=%s\n' \
    "$phase" "$current_server_gross" "$current_volume_gross" "$current_included" \
    "$current_outbound_gross" "$current_unit" "$current_billable" \
    "$elapsed" "$projected_hours" "$actual_gross" "$projected_gross" \
    > "$LOCAL_EVIDENCE_DIR/gate-$phase-$(date -u +%Y%m%dT%H%M%SZ).txt"
}
```

## 4. Read-only quota, exact-name, ref, and capacity preflights

The authoring-time observations are:

```text
pourpoint-web-1 is a shared cx33 and consumes no dedicated-core quota.
No ccx server exists.
The volume list is empty.
```

The human must confirm these facts still hold and that quota permits one `ccx33` and one 600 GB volume. The preflight may list all servers and volumes read-only. It must never turn a list result into cleanup.

```bash
git fetch origin main
GROUND_TRUTH_REF=$(git rev-parse origin/main)
test -n "$GROUND_TRUTH_REF"
git merge-base --is-ancestor 266df3755754a679c60e5f2c3c8e2c8e8db4b663 "$GROUND_TRUTH_REF"
git cat-file -e "$GROUND_TRUTH_REF:scripts/hetzner/RUNBOOK-tdx-hydro-seven-basin-acquisition.md"
git show "$GROUND_TRUTH_REF:scripts/hetzner/tdx-hydro-campaign.sh" | grep -F -- '--product-attempt-ceiling <positive-integer>'
printf '%s\n' "$GROUND_TRUTH_REF" > "$LOCAL_EVIDENCE_DIR/ground-truth-ref.txt"

test "$(hcloud context active)" = pourpoint
hcloud --context pourpoint server list -o json > "$LOCAL_EVIDENCE_DIR/preflight-servers.json"
hcloud --context pourpoint volume list -o json > "$LOCAL_EVIDENCE_DIR/preflight-volumes.json"
jq -e --arg name "$SERVER_NAME" '[.[] | select(.name == $name)] == []' "$LOCAL_EVIDENCE_DIR/preflight-servers.json"
jq -e --arg name "$VOLUME_NAME" '[.[] | select(.name == $name)] == []' "$LOCAL_EVIDENCE_DIR/preflight-volumes.json"
jq -e '[.[] | select(.server_type.name | startswith("ccx"))] == []' "$LOCAL_EVIDENCE_DIR/preflight-servers.json"
jq -e 'length == 0' "$LOCAL_EVIDENCE_DIR/preflight-volumes.json"
```

## 5. Fail-closed salvage and mandatory teardown trap

Install this handler before provisioning. It disables tracing, copies only the four exact roots when the exact server is addressable, records copy failures without masking teardown, invokes exact-name default teardown, requires the exact zero-footprint line, and independently proves both exact names absent. It runs on success, failure, abort, refusal, and both kill switches.

```bash
cleanup_running=0
cleanup_complete=0
run_bounded() {
  local seconds=$1 command_pid timer_pid command_status=0 remaining step
  shift
  "$@" &
  command_pid=$!
  (
    remaining=$seconds
    while test "$remaining" -gt 0; do
      step=$remaining
      if test "$step" -gt 5; then step=5; fi
      sleep "$step"
      remaining=$((remaining - step))
    done
    kill "$command_pid" 2>/dev/null || true
  ) &
  timer_pid=$!
  wait "$command_pid" || command_status=1
  kill "$timer_pid" 2>/dev/null || true
  return "$command_status"
}
stop_dispatches() {
  set +x
  if test -z "${SERVER_IP:-}"; then
    SERVER_IP=$(hcloud --context pourpoint server describe "$SERVER_NAME" -o json 2>/dev/null | jq -er '.public_net.ipv4.ip') || SERVER_IP=
  fi
  if test -n "${SERVER_IP:-}"; then
    ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "root@$SERVER_IP" \
      "tmux kill-session -t '=hfx-tdx-m5-seven-acquire-tdx-init' 2>/dev/null || true; tmux kill-session -t '=hfx-tdx-m5-seven-acquire-tdx-acquire' 2>/dev/null || true; tmux kill-session -t '=hfx-tdx-m5-seven-acquire-tdx-recover' 2>/dev/null || true" || true
  fi
}
copy_remote_root() {
  local source=$1 destination=$2 copy_pid timer_pid copy_status=0 origin now remaining step
  if test -f "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"; then
    if test -f "$LOCAL_EVIDENCE_DIR/hard-ceiling-reached"; then
      remaining=30
    else
      origin=$(<"$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt")
      now=$(date +%s)
      remaining=$((origin + 194400 - now))
      if test "$remaining" -le 0; then
        remaining=1
        : > "$LOCAL_EVIDENCE_DIR/hard-ceiling-reached"
      fi
    fi
    scp -r -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
      "$source" "$destination" &
    copy_pid=$!
    (
      while test "$remaining" -gt 0; do
        step=$remaining
        if test "$step" -gt 30; then step=30; fi
        sleep "$step"
        remaining=$((remaining - step))
      done
      kill "$copy_pid" 2>/dev/null || true
    ) &
    timer_pid=$!
    wait "$copy_pid" || copy_status=1
    kill "$timer_pid" 2>/dev/null || true
  else
    scp -r -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
      "$source" "$destination" || copy_status=1
  fi
  return "$copy_status"
}
salvage_evidence() {
  local salvage_status=0
  set +x
  if test -z "${SERVER_IP:-}"; then
    SERVER_IP=$(hcloud --context pourpoint server describe "$SERVER_NAME" -o json 2>/dev/null | jq -er '.public_net.ipv4.ip') || SERVER_IP=
  fi
  if test -z "${SERVER_IP:-}" && test -f "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"; then
    salvage_status=1
  fi
  if test -n "${SERVER_IP:-}"; then
    mkdir -p -- "$LOCAL_EVIDENCE_DIR/salvage"
    copy_remote_root \
      "root@$SERVER_IP:/mnt/hfx/work/tdx-hydro-tdx-m5-seven-acquire/downloads" \
      "$LOCAL_EVIDENCE_DIR/salvage/" || salvage_status=1
    copy_remote_root \
      "root@$SERVER_IP:/mnt/hfx/work/tdx-hydro-tdx-m5-seven-acquire/reports" \
      "$LOCAL_EVIDENCE_DIR/salvage/" || salvage_status=1
    copy_remote_root \
      "root@$SERVER_IP:/mnt/hfx/work/tdx-hydro-tdx-m5-seven-acquire/state" \
      "$LOCAL_EVIDENCE_DIR/salvage/" || salvage_status=1
    copy_remote_root \
      "root@$SERVER_IP:/mnt/hfx/logs" \
      "$LOCAL_EVIDENCE_DIR/salvage/" || salvage_status=1
  fi
  printf 'salvage_status=%s\n' "$salvage_status" > "$LOCAL_EVIDENCE_DIR/salvage-status.txt"
  return "$salvage_status"
}
default_teardown() {
  local teardown_status=0
  set +x
  if test -n "${WATCHDOG_PID:-}"; then
    kill "$WATCHDOG_PID" 2>/dev/null || true
  fi
  ./scripts/hetzner/teardown.sh --campaign tdx-m5-seven-acquire \
    2>&1 | tee "$LOCAL_EVIDENCE_DIR/teardown.log" || teardown_status=1
  grep -Fx -- 'hfx: campaign tdx-m5-seven-acquire has zero Hetzner footprint: server hfx-build-tdx-m5-seven-acquire absent; volume hfx-build-tdx-m5-seven-acquire-data absent' \
    "$LOCAL_EVIDENCE_DIR/teardown.log" || teardown_status=1
  hcloud --context pourpoint server list -o json > "$LOCAL_EVIDENCE_DIR/final-servers.json" || teardown_status=1
  jq -e --arg name "$SERVER_NAME" '[.[] | select(.name == $name)] == []' "$LOCAL_EVIDENCE_DIR/final-servers.json" || teardown_status=1
  hcloud --context pourpoint volume list -o json > "$LOCAL_EVIDENCE_DIR/final-volumes.json" || teardown_status=1
  jq -e --arg name "$VOLUME_NAME" '[.[] | select(.name == $name)] == []' "$LOCAL_EVIDENCE_DIR/final-volumes.json" || teardown_status=1
  return "$teardown_status"
}
campaign_cleanup() {
  local prior_status=$? salvage_status=0 teardown_status=0 origin now
  set +x
  if test "$cleanup_complete" -eq 1; then return "$prior_status"; fi
  if test "$cleanup_running" -eq 1; then return "$prior_status"; fi
  cleanup_running=1
  if test -n "${WATCHDOG_PID:-}"; then kill "$WATCHDOG_PID" 2>/dev/null || true; fi
  trap - EXIT INT TERM HUP
  set +e
  if test -f "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"; then run_bounded 20 stop_dispatches || true; fi
  salvage_evidence || salvage_status=1
  if test "$salvage_status" -ne 0 && test ! -f "$LOCAL_EVIDENCE_DIR/hard-ceiling-reached" && test -f "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"; then
    origin=$(<"$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt")
    while :; do
      now=$(date +%s)
      if test $((now - origin)) -ge 194400; then break; fi
      printf '%s\n' 'Salvage failed below the hard ceiling; obtain maintainer approval before teardown.' >&2
      wait_seconds=$((194400 - now + origin))
      if test "$wait_seconds" -gt 60; then wait_seconds=60; fi
      SALVAGE_FAILURE_APPROVAL=
      IFS= read -r -t "$wait_seconds" SALVAGE_FAILURE_APPROVAL || true
      if test "$SALVAGE_FAILURE_APPROVAL" = TEARDOWN-WITH-SALVAGE-FAILURE; then break; fi
      sleep 1
    done
  fi
  default_teardown || teardown_status=1
  test "$salvage_status" -eq 0 || prior_status=1
  test "$teardown_status" -eq 0 || prior_status=1
  return "$prior_status"
}
trap campaign_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
```

The recursive salvage roots are exactly:

```text
/mnt/hfx/work/tdx-hydro-tdx-m5-seven-acquire/downloads
/mnt/hfx/work/tdx-hydro-tdx-m5-seven-acquire/reports
/mnt/hfx/work/tdx-hydro-tdx-m5-seven-acquire/state
/mnt/hfx/logs
```

Teardown is destructive only to the exact campaign server and volume and is permitted only after off-VM preservation. The handler does not delete local evidence or remote data paths. If salvage cannot be proven below the hard ceiling, pause for a maintainer while the ceiling remains in force. At the hard ceiling, default teardown remains mandatory and the salvage failure must be recorded.

## 6. Provision, bootstrap, and converge the immutable revision

Only after the section 2 exception approval, record the request epoch immediately before the sole provisioning command:

```bash
test ! -e "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"
test ! -L "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"
test ! -e "$LOCAL_EVIDENCE_DIR/hard-ceiling-reached"
test ! -L "$LOCAL_EVIDENCE_DIR/hard-ceiling-reached"
(
  while test ! -f "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"; do sleep 1; done
  origin=$(<"$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt")
  deadline=$((origin + 194400))
  while :; do
    now=$(date +%s)
    remaining=$((deadline - now))
    if test "$remaining" -le 0; then break; fi
    step=$remaining
    if test "$step" -gt 30; then step=30; fi
    sleep "$step"
  done
  : > "$LOCAL_EVIDENCE_DIR/hard-ceiling-reached"
  kill -TERM "$$"
  pkill -TERM -P "$$" 2>/dev/null || true
) &
WATCHDOG_PID=$!
date +%s > "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"
./scripts/hetzner/provision.sh \
  --campaign tdx-m5-seven-acquire \
  --s3-env-file "$S3_ENV_FILE" \
  --server-type ccx33 \
  --volume-size-gb 600 \
  --location fsn1 \
  2>&1 | tee "$LOCAL_EVIDENCE_DIR/provision.log"
./scripts/hetzner/bootstrap.sh --campaign tdx-m5-seven-acquire \
  2>&1 | tee "$LOCAL_EVIDENCE_DIR/bootstrap.log"
SERVER_IP=$(hcloud --context pourpoint server describe "$SERVER_NAME" -o json | jq -er '.public_net.ipv4.ip')
```

Converge the checkout, rebuild `hfx-cli`, and verify dependencies and capacity without reading credentials:

```bash
ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "root@$SERVER_IP" \
  bash -s -- "$GROUND_TRUTH_REF" <<'REMOTE'
set -Eeuo pipefail
set +x
ground_truth_ref=$1
cd /root/hfx
git fetch origin main
git checkout --detach "$ground_truth_ref"
test "$(git rev-parse HEAD)" = "$ground_truth_ref"
gdal-config --version
ogrinfo --version
jq --version
curl --version
/root/.cargo/bin/cargo build --release -p hfx-cli
available_bytes=$(df -B1 --output=avail /mnt/hfx | tail -n 1 | tr -d ' ')
test "$available_bytes" -ge 560000000000
available_memory_bytes=$(awk '/MemAvailable:/{printf "%.0f\n", $2 * 1024}' /proc/meminfo)
test "$available_memory_bytes" -ge 30000000000
REMOTE
```

## 7. Frozen campaign initialization

Run this as one detached workload. Re-running init must use byte-for-byte identical argv.

```bash
./scripts/hetzner/launch.sh --campaign tdx-m5-seven-acquire start --workload tdx-init -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh init \
  --campaign tdx-m5-seven-acquire \
  --workspace-root /mnt/hfx/work \
  --basin 1020018110 \
  --basin 2020003440 \
  --basin 2020065840 \
  --basin 2020071190 \
  --basin 4020050470 \
  --basin 5020049720 \
  --basin 6020000010 \
  --basin 7020000010 \
  --retention-policy retain-all-through-publication \
  --available-memory-bytes 30000000000 \
  --available-disk-bytes 560000000000 \
  --retained-input-bytes 120000000000 \
  --retained-basin-output-bytes 1 \
  --assembly-memory-ceiling-bytes 1 \
  --assembly-scratch-ceiling-bytes 1 \
  --assembled-artifact-bytes 1 \
  --active-compile-scratch-bytes 20000000000 \
  --filesystem-overhead-bytes 20000000000
```

The workload is detached. Repeat the accepted status form below until it reports that the session is no longer running. Require the canonical log shown by status to end with the runner-generated finish record carrying exit `0` before executing the state checks below. Any other exit enters salvage and teardown.

```bash
./scripts/hetzner/launch.sh --campaign tdx-m5-seven-acquire status --workload tdx-init \
  || test "$?" -eq 3
```

The sizing arithmetic is:

```text
120,000,000,000 retained input bytes
+ 1 retained basin output byte
+ 20,000,000,000 active compile scratch bytes
+ max(1 assembly scratch byte, 1 assembled artifact byte)
+ 20,000,000,000 filesystem overhead bytes
= 160,000,000,002 required disk bytes

560,000,000,000 available bytes - 160,000,000,002 required bytes
= 399,999,999,998 bytes headroom
```

The 120 GB retained-input term conservatively covers roughly 85 GB of seven reacquisitions, the additional proven control pair, failed-attempt partials, and reporting overhead. It is a campaign sizing allowance. The transferred byte count may be lower.

After the workload finishes, require the exact canonical state and status:

```bash
ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "root@$SERVER_IP" bash -s <<'REMOTE'
set -Eeuo pipefail
campaign=/mnt/hfx/work/tdx-hydro-tdx-m5-seven-acquire/state/campaign.json
selection=/mnt/hfx/work/tdx-hydro-tdx-m5-seven-acquire/state/selection.json
jq -e '.campaign == "tdx-m5-seven-acquire" and
  .retention == {policy:"retain-all-through-publication",reclaim_inputs:false,retain_acquired_inputs:true,retain_basin_outputs:true,retain_external_reports:true} and
  .sizing.available_memory_bytes == 30000000000 and .sizing.available_disk_bytes == 560000000000 and
  .sizing.retained_input_bytes == 120000000000 and .sizing.retained_basin_output_bytes == 1 and
  .sizing.assembly_memory_ceiling_bytes == 1 and .sizing.assembly_scratch_ceiling_bytes == 1 and
  .sizing.assembled_artifact_bytes == 1 and .sizing.active_compile_scratch_bytes == 20000000000 and
  .sizing.filesystem_overhead_bytes == 20000000000 and .sizing.required_disk_bytes == 160000000002' "$campaign"
jq -e '.schema_version == 1 and .basin_ids == ["1020018110","2020003440","2020065840","2020071190","4020050470","5020049720","6020000010","7020000010"]' "$selection"
/root/hfx/scripts/hetzner/tdx-hydro-campaign.sh status --campaign tdx-m5-seven-acquire --workspace-root /mnt/hfx/work |
  grep -Fx 'selected_basin_count=8'
/root/hfx/scripts/hetzner/tdx-hydro-campaign.sh status --campaign tdx-m5-seven-acquire --workspace-root /mnt/hfx/work |
  grep -Fx 'unselected_basin_count=54'
REMOTE
```

## 8. Bounded low-concurrency acquisition

The spelling is `--product-attempt-ceiling`. It is valid only for direct `acquire` and requires `--max-parallel` in `1..4`. Each selected basin product has its own cumulative counter spanning invocations. Each retry starts a fresh byte-zero GET.

Verified against `prepare_bounded_acquisition` at `266df3755754a679c60e5f2c3c8e2c8e8db4b663:scripts/hetzner/tdx-hydro-campaign.sh:2938-2965`, `--product-attempt-ceiling 3` is MANDATORY on the first `acquire`. Without it, no selected basin is yet at schema version 5, the runner leaves `bounded_acquisition_active=0`, `acquire_campaign` dispatches unbounded `acquire_basin`, and no product can ever reach `exhausted`. Omitting it would cause endless retries against an endpoint that drops every 85 to 92 minutes.

The first call converts all selected records to schema version 5 and stores ceiling `3`. The option is optional subsequently because that ceiling is reused; repeating `3` is idempotent. A different value emits `hfx: error: product attempt ceiling changed; use a new campaign ID`. Partial conversion emits `hfx: error: bounded acquisition conversion is incomplete; rerun acquire with --product-attempt-ceiling 3`. The trailing `3` is the stored ceiling. These full, byte-exact messages distinguish refusals during an hourly billed campaign. Rerun the identical acquire argv with ceiling `3` only after the current time and cost gate passes.

NGA serves no range requests; resume was removed; connections drop at 85 to 92 minutes; throughput is erratic at roughly 1 to 6 MB/s per connection; and the largest product is about 9.8 GB, which cannot complete in one connection below about 1.9 MB/s. Two workers bound pressure while allowing independent products to progress. Products within one basin remain serial.

Recalculate the time and cost gate, then run exactly:

```bash
campaign_gate pre-acquire 44.47
```

```bash
./scripts/hetzner/launch.sh --campaign tdx-m5-seven-acquire start --workload tdx-acquire -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh acquire \
  --campaign tdx-m5-seven-acquire \
  --workspace-root /mnt/hfx/work \
  --max-parallel 2 \
  --product-attempt-ceiling 3
```

The only monitoring commands are:

```bash
./scripts/hetzner/launch.sh --campaign tdx-m5-seven-acquire status --workload tdx-acquire \
  || test "$?" -eq 3
./scripts/hetzner/launch.sh --campaign tdx-m5-seven-acquire tail \
  --log hfx-tdx-m5-seven-acquire-tdx-acquire.log
```

Run `tail` in a separate read-only shell without the campaign trap. In the campaign shell, repeat the status command until it exits `3`. Before every status check, run `campaign_gate acquisition-monitor 7.67`; this reserves the frozen 6.67-hour off-VM-copy allowance and 1-hour teardown allowance. A refusal stops dispatch and enters cleanup. When status reports that the session is no longer running, require its displayed canonical log to end with the runner-generated finish record carrying exit `0` before continuing.

```bash
campaign_gate acquisition-monitor 7.67
```

If interruption leaves a running state, recalculate the current time and cost gate, run only this recovery, then rerun the identical acquisition argv above after another gate check:

```bash
campaign_gate pre-recovery 44.47
RECOVERY_RAN=1
```

```bash
./scripts/hetzner/launch.sh --campaign tdx-m5-seven-acquire start --workload tdx-recover -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh recover \
  --campaign tdx-m5-seven-acquire \
  --workspace-root /mnt/hfx/work
```

Repeat the accepted recovery status form until the session is no longer running and require the canonical finish record to carry exit `0`. Then run `campaign_gate pre-acquire 44.47` and repeat the identical acquisition argv, including ceiling `3`.

```bash
./scripts/hetzner/launch.sh --campaign tdx-m5-seven-acquire status --workload tdx-recover \
  || test "$?" -eq 3
```

Terminal bounded form is status `exhausted`, attempts equal to `3`, evidence `null`, and failure reason `product attempt ceiling exhausted; retryable acquisition did not succeed`. Inspection failures remain `failed` for human review and do not masquerade as exhaustion. The sibling product is still attempted.

## 9. Terminal checks and acquisition-only campaign record

Every absent basin must have both `basins` and `streamnet` in `succeeded` or `exhausted`. A succeeded stage carries the runner-inspected positive byte count, SHA-256, SQLite identity, and layer name; its attempt count is the number of transfers this campaign made, and zero means the runner adopted a final file that was already present and verified (the runner change merged as `0ffa2d048ce5d748c0ab4c71fbe6f5862478107d` made that state installable). An exhausted stage carries attempts `3`, null evidence, and the exact failure reason. Any other shape refuses and enters salvage and teardown.

Generate the record atomically from the eight explicit states:

```bash
ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "root@$SERVER_IP" bash -s <<'REMOTE'
set -Eeuo pipefail
campaign_dir=/mnt/hfx/work/tdx-hydro-tdx-m5-seven-acquire
temporary=$campaign_dir/reports/.seven-basin-acquisition-record.json.tmp.$$
test ! -e "$temporary" && test ! -L "$temporary"
jq -s '
  def product($name): .stages["acquire_" + $name];
  def valid_stage:
    (.status == "succeeded" and (.attempts | type == "number" and . >= 0) and .failure_reason == null and
      (.evidence.bytes | type == "number" and . > 0) and
      (.evidence.sha256 | test("^[0-9a-f]{64}$")) and .evidence.sqlite_identity == "53514c69746520666f726d6174203300" and
      (.evidence.layer_name | type == "string" and length > 0)) or
    (.status == "exhausted" and .attempts == 3 and .evidence == null and
      .failure_reason == "product attempt ceiling exhausted; retryable acquisition did not succeed");
  sort_by(.processing_basin_id) as $records |
  [ $records[].processing_basin_id ] as $ids |
  if $ids != ["1020018110","2020003440","2020065840","2020071190","4020050470","5020049720","6020000010","7020000010"] then error("unexpected processing basin IDs") else . end |
  if all($records[]; (product("basins") | valid_stage) and (product("streamnet") | valid_stage)) | not then error("nonterminal product state") else . end |
  {campaign:"tdx-m5-seven-acquire", control_id:"7020000010",
   absent_ids:["1020018110","2020003440","2020065840","2020071190","4020050470","5020049720","6020000010"],
   records:[$records[] | . as $record | {processing_basin_id,
     products:{basins:product("basins"),streamnet:product("streamnet")},
     outcome:(if .processing_basin_id == "7020000010" then
       if product("basins").status == "succeeded" and product("streamnet").status == "succeeded" then "control-acquired" else "control-transfer-failure" end
       elif product("basins").status == "exhausted" or product("streamnet").status == "exhausted" then "transfer-failure"
       else "acquired-awaiting-data-backed-adjudication" end)}]}' \
  "$campaign_dir/state/basins/1020018110/current.json" \
  "$campaign_dir/state/basins/2020003440/current.json" \
  "$campaign_dir/state/basins/2020065840/current.json" \
  "$campaign_dir/state/basins/2020071190/current.json" \
  "$campaign_dir/state/basins/4020050470/current.json" \
  "$campaign_dir/state/basins/5020049720/current.json" \
  "$campaign_dir/state/basins/6020000010/current.json" \
  "$campaign_dir/state/basins/7020000010/current.json" > "$temporary"
jq -e '.campaign == "tdx-m5-seven-acquire" and .control_id == "7020000010" and
  .absent_ids == ["1020018110","2020003440","2020065840","2020071190","4020050470","5020049720","6020000010"] and
  ([.records[].processing_basin_id] | sort) == ["1020018110","2020003440","2020065840","2020071190","4020050470","5020049720","6020000010","7020000010"] and
  (.records | length == 8) and
  all(.records[] | select(.processing_basin_id != "7020000010"); .outcome == "transfer-failure" or .outcome == "acquired-awaiting-data-backed-adjudication") and
  all(.records[] | select(.processing_basin_id == "7020000010"); .outcome == "control-acquired" or .outcome == "control-transfer-failure")' "$temporary"
mv -- "$temporary" "$campaign_dir/reports/seven-basin-acquisition-record.json"
REMOTE
```

Coverage below 62/62 is expected and acceptable. An exhausted basin does not block closure. This record must never emit `source-defect` or `adapter-strictness`.

## 10. Off-VM preservation and local verification

Invoke the common salvage function before teardown on every path. It recursively copies the four exact roots without a glob, preserving succeeded GeoPackages, safe partials, reports, state, the campaign record, and canonical and timestamped logs. The secret path and `/etc/pourpoint-hfx.env` are outside those roots and must never be copied.

After copying, loop over the literal IDs and products and verify local evidence:

```bash
salvage_evidence
for basin_id in 1020018110 2020003440 2020065840 2020071190 4020050470 5020049720 6020000010 7020000010; do
  state="$LOCAL_EVIDENCE_DIR/salvage/state/basins/$basin_id/current.json"
  test -f "$state"
  for product in basins streamnet; do
    stage="acquire_$product"
    status=$(jq -er --arg stage "$stage" '.stages[$stage].status' "$state")
    final="$LOCAL_EVIDENCE_DIR/salvage/downloads/$basin_id-$product.gpkg"
    if test "$status" = succeeded; then
      test -f "$final" && test ! -L "$final"
      expected_bytes=$(jq -er --arg stage "$stage" '.stages[$stage].evidence.bytes' "$state")
      test "$(stat -f '%z' "$final")" -eq "$expected_bytes"
      expected_sha=$(jq -er --arg stage "$stage" '.stages[$stage].evidence.sha256' "$state")
      test "$(shasum -a 256 "$final" | awk '{print $1}')" = "$expected_sha"
      test "$(dd if="$final" bs=16 count=1 2>/dev/null | xxd -p)" = 53514c69746520666f726d6174203300
      jq -e --arg stage "$stage" '(.stages[$stage].attempts | type == "number" and . >= 0) and .stages[$stage].failure_reason == null and .stages[$stage].evidence.sqlite_identity == "53514c69746520666f726d6174203300" and (.stages[$stage].evidence.layer_name | type == "string" and length > 0)' "$state"
    elif test "$status" = exhausted; then
      test ! -e "$final" && test ! -L "$final"
      jq -e --arg stage "$stage" '.stages[$stage].attempts == 3 and .stages[$stage].evidence == null and .stages[$stage].failure_reason == "product attempt ceiling exhausted; retryable acquisition did not succeed"' "$state"
    else
      exit 1
    fi
    report="$LOCAL_EVIDENCE_DIR/salvage/reports/$basin_id-$product-acquisition.json"
    test -f "$report" && test ! -L "$report"
  done
done
test -f "$LOCAL_EVIDENCE_DIR/salvage/state/campaign.json"
test -f "$LOCAL_EVIDENCE_DIR/salvage/state/selection.json"
test -f "$LOCAL_EVIDENCE_DIR/salvage/reports/seven-basin-acquisition-record.json"
test -f "$LOCAL_EVIDENCE_DIR/salvage/logs/hfx-tdx-m5-seven-acquire-tdx-init.log"
test -f "$LOCAL_EVIDENCE_DIR/salvage/logs/hfx-tdx-m5-seven-acquire-tdx-acquire.log"
if test "${RECOVERY_RAN:-0}" -eq 1; then
  test -f "$LOCAL_EVIDENCE_DIR/salvage/logs/hfx-tdx-m5-seven-acquire-tdx-recover.log"
fi
```

No source file may be removed after preservation. No remote or local data cleanup command belongs here.

## 11. Mandatory default teardown and halt

After successful local verification, invoke the same default teardown used by every trap path and mark cleanup complete only after it passes:

```bash
default_teardown
cleanup_complete=1
printf '%s\n' 'Acquisition is complete; no adjudication was performed; no S3 write occurred; separate human approval is required before publication or any capacity decision.'
```

The common path runs:

```bash
./scripts/hetzner/teardown.sh --campaign tdx-m5-seven-acquire \
  2>&1 | tee "$LOCAL_EVIDENCE_DIR/teardown.log"
```

It must contain this exact stderr line:

```text
hfx: campaign tdx-m5-seven-acquire has zero Hetzner footprint: server hfx-build-tdx-m5-seven-acquire absent; volume hfx-build-tdx-m5-seven-acquire-data absent
```

Then it performs these exact independent read-only checks:

```bash
hcloud --context pourpoint server list -o json > "$LOCAL_EVIDENCE_DIR/final-servers.json"
jq -e --arg name "$SERVER_NAME" '[.[] | select(.name == $name)] == []' "$LOCAL_EVIDENCE_DIR/final-servers.json"
hcloud --context pourpoint volume list -o json > "$LOCAL_EVIDENCE_DIR/final-volumes.json"
jq -e --arg name "$VOLUME_NAME" '[.[] | select(.name == $name)] == []' "$LOCAL_EVIDENCE_DIR/final-volumes.json"
```

Acquisition is complete. No adjudication was performed, no S3 write occurred, and separate human approval is required before publication or any capacity decision.

## 12. Failure classifications

| Class | Required action |
|---|---|
| Retryable NGA transfer reaching attempt 3 | Preserve `exhausted`, assign transfer-failure, continue other basins |
| Inspection failure or unsafe/malformed state/path | Refuse interpretation, salvage, default teardown, maintainer review |
| Quota, price, identity, revision, dependency, memory, or disk preflight | Stop before acquisition; salvage if anything exists; default teardown if provisioned |
| Time or cost ceiling | Stop dispatch, salvage, mandatory default teardown |
| SSH or transient infrastructure interruption below both ceilings | Preserve evidence, run exact recovery, rerun identical acquire argv |
| Any request to compile, adjudicate, assemble, publish, delete, use S3, use `--keep-volume`, or touch another resource | Refuse |
| Salvage failure | Record the failure, seek maintainer help while below the hard ceiling, and still default-teardown at the hard ceiling |

## 13. Author-only review and landing gates

This section is local documentation-author work completed before merge. It must never execute the campaign. The exact tracked write set is `scripts/hetzner/README.md` and `scripts/hetzner/RUNBOOK-tdx-hydro-seven-basin-acquisition.md`. The README gains one index link only; runner behavior, calculations, option spelling, existing runbook text, and shell assertions remain unchanged.

Audit every executable line against the pinned parser ranges cited in section 1. Allowed runner subcommands are `init`, `status`, `recover`, and `acquire`. Search prose and code for forbidden terms and distinguish prohibitions from executable use. Review every Bash fence for strict-mode behavior, quoting, arrays, reads, pipelines, traps, heredoc expansion, secret-path nondisclosure, exact resource identity, absence of globs, and salvage-before-teardown ordering. If installed, concatenate the ordered Bash fences and run `shellcheck --shell=bash -`; justify any suppression in `pr-body.md`.

Run the fake-client harness and the repository gates exactly as specified by the repository contract. Run no adapter Python suite and do not use `unittest discover`.

```bash
test -f scripts/hetzner/RUNBOOK-tdx-hydro-seven-basin-acquisition.md
git diff --check
git diff --name-only -- | diff -u - <(printf '%s\n' scripts/hetzner/README.md scripts/hetzner/RUNBOOK-tdx-hydro-seven-basin-acquisition.md)
git diff --exit-code -- scripts/hetzner/tdx-hydro-campaign.sh scripts/hetzner/provision.sh scripts/hetzner/bootstrap.sh scripts/hetzner/launch.sh scripts/hetzner/teardown.sh .pce/repository-contract.json
/bin/bash scripts/hetzner/test-tdx-hydro-campaign.sh
```

With GDAL discoverable through `gdal-config`, run these five gates in this exact order with no intervening command:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo check --workspace --all-targets
cargo test --workspace
cargo build --workspace
```
