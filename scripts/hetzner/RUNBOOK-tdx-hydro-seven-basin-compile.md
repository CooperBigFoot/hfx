# TDX-Hydro seven-basin compile and extension campaign

This runbook is an instruction document. Authoring, reviewing, and verifying it provisions nothing, contacts neither NGA nor S3, performs no compile or teardown, and writes nothing to object storage.

This campaign consumes the one remaining approved bounded Hetzner lifecycle for Effort #195. It builds two control outputs for basin `7020000010`, compares them byte for byte, compiles the seven absent basins with the corrected adapter, extends the frozen 55-basin artifact with every newly compiled basin, attempts strict whole-dataset validation, preserves everything off the VM with digests, and tears down by exact resource identity.

version_policy is NONE: NO version bump, NO tag.

<!-- BEGIN COMPILE CAMPAIGN CONTRACT
{
  "schema": 2,
  "campaign": "seven-basin-extension",
  "authority_ref": "69747055bcb1876d9d1fad48c60f5cae6a24ea60",
  "authority_document": "planning/visions/2026-09-03-close-the-seven-basin-coverage-gap.md",
  "authority_section": "Compute and preservation constraints",
  "lifecycles_authorized": 1,
  "server_name": "hfx-build-seven-basin-extension",
  "volume_name": "hfx-build-seven-basin-extension-data",
  "server_type": "ccx33",
  "location": "fsn1",
  "volume_size_gb": 600,
  "wall_clock_ceiling_hours": 72,
  "gross_cost_ceiling_eur": 40.0,
  "price_source": "https://api.hetzner.cloud/v1/pricing",
  "decision_points_hours": [24, 48, 66],
  "retention_policy": "retain-all-produced-output-off-vm-before-exact-resource-teardown",
  "approval_record": "provisioner-transfer-approval.txt",
  "out_of_scope_resources": ["pourpoint-web-1"],
  "absent_basins": ["1020018110", "2020003440", "2020065840", "2020071190", "4020050470", "5020049720", "6020000010"],
  "control_basin": "7020000010",
  "source_corpus": {"file_count": 16, "total_bytes": 84101885952},
  "control_builds": ["corrected-adapter", "planetary-revision-43a98aff8c15a1a196f47b10217ad2f5553b6611-with-recorded-ARG_MAX-hotpatch"],
  "control_hotpatch": {
    "commit": "bde61149d3fefc5e3f30435bf7ed3d0bb32a519c",
    "path": "scripts/hetzner/tdx-hydro-campaign.sh",
    "pre_patch_blob": "41d6df3f10030a481b2227a878837c7f23f3e658",
    "post_patch_blob": "d227920a7ac0ab98ffcc80aac2c72a5dfc9c2429"
  },
  "control_digests": {
    "aux/snap_stems.parquet": "caf2eec3d1930f25d932b559ca943c7710a9a6cf18b4a9f84d3d94cbf379d9b2",
    "catchments.parquet": "d2019ba08fd39c873eb0bac22946d25c91e785ef3345538447d30fc15ccf3be4",
    "graph.parquet": "fb64ce0fa941f244841ffc5eeed4f2057ea65262a0183a1ac1e81c67380e6cc5",
    "manifest.json": "8ca5b2135d19c18a4b8fba6c93c63ffb7a784a2749867483ea6c0c49c46560c4"
  },
  "baseline": {
    "prefix": "s3://pourpoint-hfx/scratch/tdx-hydro-tdx-m5-planetary/planetary-hfx-v0-3-0/dataset/",
    "region": "tdx-hydro-partial-4dbff0d6ec31",
    "basin_count": 55,
    "unit_count": 12748154,
    "exported_bytes": 114063230627,
    "roster_digest_prefix": "4dbff0d6ec31"
  },
  "extension_scratch_prefix": "scratch/tdx-hydro-seven-basin-extension/",
  "permitted_acts": ["transfer-preserved-source-corpus", "reacquire-selected-source-on-integrity-failure", "compile-both-control-builds", "compare-control-outputs", "compile-selected-basins", "pull-baseline-read-only", "assemble-extension", "attempt-strict-validation", "preserve-all-produced-output-off-vm", "read-only-audit", "exact-resource-teardown"],
  "sole_destructive_act": "exact-resource-teardown"
}
END COMPILE CAMPAIGN CONTRACT -->

## 1. Authority and scope

The authority for this campaign is the merged vision at `planning/visions/2026-09-03-close-the-seven-basin-coverage-gap.md` on `main`, section "Compute and preservation constraints", first merged at `69747055bcb1876d9d1fad48c60f5cae6a24ea60`. That section carries forward exactly one previously approved bounded lifecycle. This runbook spends that lifecycle. Any further lifecycle needs new maintainer authority before provisioning. A repository change cannot manufacture that authority.

The campaign identity is `seven-basin-extension`. It is distinct from the two earlier `tdx-m5-seven-compile` lifecycles and from `tdx-m5-seven-acquire`, which all reached exact-resource teardown. Records of those lifecycles remain under the evidence root and are never modified by this campaign.

Only the named campaign server and volume may be mutated. Those are server `hfx-build-seven-basin-extension` and volume `hfx-build-seven-basin-extension-data`. `pourpoint-web-1` is outside scope and must remain untouched. Globs, prefixes, label selectors, and project-wide mutation are forbidden. Read-only project listings are allowed for preflight and the final zero-footprint audit.

Mandatory exact-resource teardown of the named server and volume is the sole permitted destructive operation. This campaign deletes no source data, no output, no evidence, no S3 object, and no other server or volume. The baseline prefix is read-only; nothing under it is modified or deleted. No produced output may remain unique to the VM or volume.

The permitted acts are the transfer of the preserved source corpus, reacquisition of a selected product only when its integrity check fails, both control builds and their comparison, the per-basin compiles, a read-only pull of the baseline, one extension assembly, one strict validation attempt, off-VM preservation, read-only audits, and exact-resource teardown. Adjudication, defect-report transmission, publication under `hfx/`, and adapter changes are outside this runbook.

## 2. Fixed ceilings and retention

These limits exist before any provisioning step and cannot be raised while the campaign is running:

| Limit | Fixed value |
| --- | ---: |
| Server | one `ccx33` |
| Location | `fsn1` |
| Volume | one 600 GB volume |
| Wall-clock time from provisioning request | strictly less than 72 hours |
| Projected and actual gross campaign cost | strictly less than EUR 40.00 |
| Price source | `https://api.hetzner.cloud/v1/pricing`, queried immediately before provisioning and before every gate |
| Decision points | 24, 48, and 66 hours after the provisioning request |

The retention policy is `retain-all-produced-output-off-vm-before-exact-resource-teardown`. Every per-basin output, both control outputs, the extended artifact, campaign state, reports, logs, and refusal diagnostics are retained until a digest-matching off-VM copy exists. Failure does not relax retention. Capacity pressure never permits deletion.

Equality with a ceiling refuses the act. At either ceiling, or whenever the next bounded act cannot be proved to finish below both ceilings, stop dispatch, preserve completed work, then perform exact-resource teardown. Teardown targets the two exact names above and is mandatory on success, failure, refusal, interruption, and timeout. `--keep-volume` is forbidden.

## 3. Carried approval is a precondition

Nicolas Lazaro's 2026-08-19 authorization of the tracked provisioner's credential-transfer exception is carried only by the existing file `$HFX_CAMPAIGN_EVIDENCE/provisioner-transfer-approval.txt`. It is an existing external input. This campaign may not create the approval record.

The campaign may test this record for existence, regular-file-ness, and non-emptiness. Its contents are opaque. No automated process may create, edit, replace, regenerate, copy, upload, print, log, hash, or display the record or its contents. If it is absent, empty, not a regular file, or a symlink, refuse provisioning with zero cloud mutation. Human approval cannot be recreated from a prompt, a date, repository text, or prior campaign state.

The S3 environment file is a separate opaque input. Keep tracing disabled before its path is read. Never display or record its path or contents. Only the already approved tracked provisioner transfer may place it on the exact named server.

## 4. Operator shell, inputs, and local verification

Run every operator-side command from the repository root in one shell:

```bash
set -Eeuo pipefail
set +x
IFS=$'\n\t'

CAMPAIGN=seven-basin-extension
SERVER_NAME=hfx-build-seven-basin-extension
VOLUME_NAME=hfx-build-seven-basin-extension-data
CONTROL_ID=7020000010
ABSENT_IDS='1020018110 2020003440 2020065840 2020071190 4020050470 5020049720 6020000010'
WORKSPACE_ROOT=/mnt/hfx/work
CAMPAIGN_DIR=/mnt/hfx/work/tdx-hydro-seven-basin-extension
CONTROL_ROOT=/mnt/hfx/work/control-builds
BASELINE_ROOT=/mnt/hfx/work/baseline
BASELINE_PREFIX=s3://pourpoint-hfx/scratch/tdx-hydro-tdx-m5-planetary/planetary-hfx-v0-3-0
EXTENSION_PREFIX=s3://pourpoint-hfx/scratch/tdx-hydro-seven-basin-extension
S3_ENDPOINT=https://fsn1.your-objectstorage.com
BUDGET_CEILING_EUR=40.00
ELAPSED_CEILING_HOURS=72
HARD_CEILING_SECONDS=259200

test -n "${HFX_CAMPAIGN_EVIDENCE:-}"
case "$HFX_CAMPAIGN_EVIDENCE" in /*) ;; *) exit 1 ;; esac
test -d "$HFX_CAMPAIGN_EVIDENCE" && test ! -L "$HFX_CAMPAIGN_EVIDENCE"
LOCAL_EVIDENCE_DIR="$HFX_CAMPAIGN_EVIDENCE/seven-basin-extension"
CORPUS_DIR="$HFX_CAMPAIGN_EVIDENCE/off-vm/acquired-source"
CORPUS_MANIFEST="$HFX_CAMPAIGN_EVIDENCE/attempt21-source-remote-sha256.txt"
PLANETARY_MIRROR="$PWD/tdx-m5-planetary-evidence/mirror"
PRESERVED_CONTROL="$HFX_CAMPAIGN_EVIDENCE/off-vm/control-builds/planetary/$CONTROL_ID"
mkdir -p -- "$LOCAL_EVIDENCE_DIR"
chmod 700 -- "$LOCAL_EVIDENCE_DIR"

printf '%s\n' 'Enter the secrets environment FILE PATH (contents must never be displayed):' >&2
IFS= read -r S3_ENV_FILE
test -n "$S3_ENV_FILE" && test -f "$S3_ENV_FILE" && test ! -L "$S3_ENV_FILE" && test -s "$S3_ENV_FILE"
```

`HFX_CAMPAIGN_EVIDENCE` is the existing evidence root that holds the approval record, the preserved corpus, and the preserved planetary control output. New records go under `$LOCAL_EVIDENCE_DIR` so the earlier lifecycles' records stay intact.

Verify the tracked contract, the operator inputs, and the preserved inputs before any cloud action:

```bash
for check in scope-permits-compilation ceilings-and-kill-switches control-hotpatch-is-pinned control-digests-are-pinned baseline-is-pinned authority-is-current; do
  ./scripts/hetzner/verify-compile-runbook.sh --check "$check"
done
./scripts/hetzner/verify-compile-runbook.sh --evidence-root "$HFX_CAMPAIGN_EVIDENCE" --check approval-is-a-precondition
./scripts/hetzner/verify-campaign-inputs.sh --evidence-root "$LOCAL_EVIDENCE_DIR" --check evidence-root-writable
./scripts/hetzner/verify-campaign-inputs.sh --s3-env-file "$S3_ENV_FILE" --check credential-file-authenticates
./scripts/hetzner/verify-campaign-inputs.sh --check hcloud-context-resolves

test -f "$CORPUS_MANIFEST" && test "$(grep -c . "$CORPUS_MANIFEST")" -eq 16
(cd "$CORPUS_DIR" && shasum -a 256 -c "$CORPUS_MANIFEST") | tee "$LOCAL_EVIDENCE_DIR/corpus-local-verification.txt"
test "$(grep -c ': OK$' "$LOCAL_EVIDENCE_DIR/corpus-local-verification.txt")" -eq 16
test "$(find "$CORPUS_DIR" -type f -name '*.gpkg' -exec stat -f '%z' {} + | awk '{s+=$1} END {print s}')" -eq 84101885952

jq -n --argjson digests "$(sed -n '/^<!-- BEGIN COMPILE CAMPAIGN CONTRACT$/,/^END COMPILE CAMPAIGN CONTRACT -->$/p' scripts/hetzner/RUNBOOK-tdx-hydro-seven-basin-compile.md | sed '1d;$d' | jq '.control_digests')" '$digests' > "$LOCAL_EVIDENCE_DIR/expected-control-sha256.json"
(cd "$PRESERVED_CONTROL" && jq -r 'to_entries[] | "\(.value)  ./\(.key)"' "$LOCAL_EVIDENCE_DIR/expected-control-sha256.json" | shasum -a 256 -c) | tee "$LOCAL_EVIDENCE_DIR/preserved-control-verification.txt"
test "$(grep -c ': OK$' "$LOCAL_EVIDENCE_DIR/preserved-control-verification.txt")" -eq 4

jq -c '.input_basin_ids | sort | unique' "$PLANETARY_MIRROR/state/assembly.json" > "$LOCAL_EVIDENCE_DIR/baseline-roster.json"
test "$(jq 'length' "$LOCAL_EVIDENCE_DIR/baseline-roster.json")" -eq 55
test "$(jq -r 'join(",")' "$LOCAL_EVIDENCE_DIR/baseline-roster.json" | tr -d '\n' | shasum -a 256 | cut -c1-12)" = 4dbff0d6ec31
jq -e --arg id "$CONTROL_ID" 'index($id) != null' "$LOCAL_EVIDENCE_DIR/baseline-roster.json"
jq -e --slurpfile roster "$LOCAL_EVIDENCE_DIR/baseline-roster.json" '(keys - $roster[0]) == ["1020018110","2020003440","2020065840","2020071190","4020050470","5020049720","6020000010"]' adapters/tdx-hydro/data/tdx_header_numbers.json
```

The corpus is the preserved 16-GeoPackage set, 84,101,885,952 bytes, whose SHA-256 manifest was recorded on both sides of the earlier transfer. Its local integrity proof takes roughly 10 to 30 minutes. A corpus that fails this proof is not transferred; section 9 names the reacquisition fallback.

The baseline roster is derived from the local planetary mirror's assembly state, which lists the 55 basins the frozen artifact was assembled from. Its sorted comma-joined SHA-256 prefix must equal the partial-region suffix `4dbff0d6ec31` recorded in `CAMPAIGN-tdx-hydro-planetary.md`, and its complement in the authoritative inventory must be exactly the seven absent basins. Section 12 rechecks the roster against the pulled baseline on the VM.

## 5. Current-price preflight and gates

Immediately before provisioning, obtain current prices from the Hetzner pricing API and record the numeric inputs, the retrieval timestamp, and the arithmetic:

```bash
./scripts/hetzner/price-preflight.sh \
  --server-type ccx33 --location fsn1 --volume-size-gb 600 \
  --hours 72 --billable-outbound-bytes 400000000000 \
  --ceiling-eur "$BUDGET_CEILING_EUR" \
  --out "$LOCAL_EVIDENCE_DIR/current-price-preflight.json"
```

The helper reads the project token from the Keychain with tracing off, passes it to `curl` through a stdin config so it stays out of argv, and refuses when any price is missing or malformed. Its arithmetic is:

```text
gross_server = gross_ccx33_eur_per_hour * 72
gross_volume = 600 * gross_volume_eur_per_gb_month * 72 / 730
gross_ipv4 = gross_primary_ipv4_eur_per_hour * 72
overage_units = ceil(max(0, billable_outbound_bytes - included_outbound_bytes) / 1e12)
projected_gross_total = gross_server + gross_volume + gross_ipv4 + overage_units * gross_outbound_eur_per_unit
required: projected_gross_total < 40.00
```

The 400,000,000,000 billable outbound estimate covers one full off-VM copy of every produced output, both control outputs, the extended artifact, and the evidence tree, plus a second copy of the extended artifact to the extension scratch prefix. Included traffic on `ccx33` is far above that figure, so the overage term is expected to be zero; the helper still records it.

Install this gate after provisioning and run it before every workload dispatch and before every status poll. It refreshes prices, computes elapsed time from the recorded request epoch, computes a conservative actual spend as billed hours times the sum of hourly rates, and refuses on equality with any ceiling:

```bash
campaign_gate() {
  test "$#" -eq 2
  local phase=$1 remaining_hours=$2 origin now elapsed record
  [[ "$phase" =~ ^[a-z0-9-]+$ ]]
  [[ "$remaining_hours" =~ ^[0-9]+([.][0-9]+)?$ ]]
  origin=$(<"$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt")
  [[ "$origin" =~ ^[0-9]+$ ]]
  now=$(date +%s)
  elapsed=$(awk -v now="$now" -v origin="$origin" 'BEGIN { printf "%.10f\n", (now - origin) / 3600 }')
  record="$LOCAL_EVIDENCE_DIR/gate-$phase-$(date -u +%Y%m%dT%H%M%SZ).json"
  if ! awk -v elapsed="$elapsed" -v remaining="$remaining_hours" -v ceiling="$ELAPSED_CEILING_HOURS" \
      'BEGIN { exit !(elapsed + remaining < ceiling) }'; then
    : > "$LOCAL_EVIDENCE_DIR/hard-ceiling-reached"
    return 1
  fi
  if ! ./scripts/hetzner/price-preflight.sh \
      --server-type ccx33 --location fsn1 --volume-size-gb 600 \
      --hours 72 --billable-outbound-bytes 400000000000 \
      --ceiling-eur "$BUDGET_CEILING_EUR" \
      --provisioning-request-epoch "$origin" \
      --out "$record"; then
    : > "$LOCAL_EVIDENCE_DIR/hard-ceiling-reached"
    return 1
  fi
  printf 'phase=%s\nelapsed_hours=%s\nremaining_hours=%s\nrecord=%s\n' \
    "$phase" "$elapsed" "$remaining_hours" "$record" >> "$LOCAL_EVIDENCE_DIR/gates.log"
}
```

Hetzner exposes no invoice total through the API. The conservative actual spend in each gate record is an upper bound computed from current rates and billed hours, the same approach the earlier lifecycles recorded. A refusal stops dispatch and enters preservation and teardown.

## 6. Read-only quota, exact-name, and ref preflight

Confirm the checked ref, require the authority ref to be its ancestor, and list servers and volumes read-only. Refuse if either exact resource name already exists, because an existing resource has unknown ownership. The preflight never turns a listing into cleanup.

```bash
git fetch origin main
GROUND_TRUTH_REF=$(git rev-parse origin/main)
git merge-base --is-ancestor 69747055bcb1876d9d1fad48c60f5cae6a24ea60 "$GROUND_TRUTH_REF"
git cat-file -e "$GROUND_TRUTH_REF:scripts/hetzner/RUNBOOK-tdx-hydro-seven-basin-compile.md"
git show "$GROUND_TRUTH_REF:scripts/hetzner/tdx-hydro-campaign.sh" | grep -F -- '--partial-fabric <dataset-root> --partial-fabric-roster <json-file> --exclude-control-basin <processing-basin-id>'
printf '%s\n' "$GROUND_TRUTH_REF" > "$LOCAL_EVIDENCE_DIR/ground-truth-ref.txt"

test "$(hcloud context active)" = pourpoint
hcloud --context pourpoint server list -o json > "$LOCAL_EVIDENCE_DIR/preflight-servers.json"
hcloud --context pourpoint volume list -o json > "$LOCAL_EVIDENCE_DIR/preflight-volumes.json"
jq -e --arg name "$SERVER_NAME" '[.[] | select(.name == $name)] == []' "$LOCAL_EVIDENCE_DIR/preflight-servers.json"
jq -e --arg name "$VOLUME_NAME" '[.[] | select(.name == $name)] == []' "$LOCAL_EVIDENCE_DIR/preflight-volumes.json"
jq -e '[.[] | select(.server_type.name | startswith("ccx"))] == []' "$LOCAL_EVIDENCE_DIR/preflight-servers.json"
jq -e 'length == 0' "$LOCAL_EVIDENCE_DIR/preflight-volumes.json"
```

The project quota is 8 dedicated cores, so one `ccx33` is the only dedicated server that fits. `pourpoint-web-1` is a shared `cx33` and consumes no dedicated quota; it must appear unchanged in every listing.

## 7. Fail-closed preservation and mandatory teardown trap

Install this handler before provisioning. It disables tracing, stops the campaign workloads, copies the exact preservation roots when the server is addressable, records copy failures without masking teardown, invokes exact-name default teardown, and independently proves both exact names absent. It runs on success, failure, refusal, interruption, and both kill switches.

```bash
cleanup_running=0
cleanup_complete=0
remote_ip() {
  if test -z "${SERVER_IP:-}"; then
    SERVER_IP=$(hcloud --context pourpoint server describe "$SERVER_NAME" -o json 2>/dev/null | jq -er '.public_net.ipv4.ip') || SERVER_IP=
  fi
  test -n "${SERVER_IP:-}"
}
stop_dispatches() {
  set +x
  remote_ip || return 0
  ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "root@$SERVER_IP" \
    "for w in tdx-init tdx-acquire tdx-compile tdx-assemble; do tmux kill-session -t \"=hfx-$CAMPAIGN-\$w\" 2>/dev/null || true; done" || true
}
copy_remote_root() {
  local source=$1 destination=$2
  mkdir -p -- "$destination"
  rsync -a --partial --timeout=120 -e 'ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new' \
    "root@$SERVER_IP:$source" "$destination/"
}
salvage_evidence() {
  local salvage_status=0
  set +x
  remote_ip || { test ! -f "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"; return; }
  mkdir -p -- "$LOCAL_EVIDENCE_DIR/salvage"
  copy_remote_root "$CAMPAIGN_DIR/state" "$LOCAL_EVIDENCE_DIR/salvage/campaign" || salvage_status=1
  copy_remote_root "$CAMPAIGN_DIR/reports" "$LOCAL_EVIDENCE_DIR/salvage/campaign" || salvage_status=1
  copy_remote_root /mnt/hfx/logs "$LOCAL_EVIDENCE_DIR/salvage" || salvage_status=1
  copy_remote_root "$CONTROL_ROOT" "$LOCAL_EVIDENCE_DIR/salvage" || salvage_status=1
  copy_remote_root "$CAMPAIGN_DIR/basin-outputs" "$LOCAL_EVIDENCE_DIR/salvage/campaign" || salvage_status=1
  copy_remote_root "$CAMPAIGN_DIR/assembly/dataset" "$LOCAL_EVIDENCE_DIR/salvage/extension" || salvage_status=1
  copy_remote_root /mnt/hfx/work/sha256 "$LOCAL_EVIDENCE_DIR/salvage" || salvage_status=1
  printf 'salvage_status=%s\n' "$salvage_status" > "$LOCAL_EVIDENCE_DIR/salvage-status.txt"
  return "$salvage_status"
}
default_teardown() {
  local teardown_status=0
  set +x
  if test -n "${WATCHDOG_PID:-}"; then kill "$WATCHDOG_PID" 2>/dev/null || true; fi
  ./scripts/hetzner/teardown.sh --campaign "$CAMPAIGN" \
    2>&1 | tee "$LOCAL_EVIDENCE_DIR/teardown.log" || teardown_status=1
  grep -Fx -- "hfx: campaign $CAMPAIGN has zero Hetzner footprint: server $SERVER_NAME absent; volume $VOLUME_NAME absent" \
    "$LOCAL_EVIDENCE_DIR/teardown.log" || teardown_status=1
  hcloud --context pourpoint server list -o json > "$LOCAL_EVIDENCE_DIR/final-servers.json" || teardown_status=1
  jq -e --arg name "$SERVER_NAME" '[.[] | select(.name == $name)] == []' "$LOCAL_EVIDENCE_DIR/final-servers.json" || teardown_status=1
  hcloud --context pourpoint volume list -o json > "$LOCAL_EVIDENCE_DIR/final-volumes.json" || teardown_status=1
  jq -e --arg name "$VOLUME_NAME" '[.[] | select(.name == $name)] == []' "$LOCAL_EVIDENCE_DIR/final-volumes.json" || teardown_status=1
  jq -e '[.[] | select(.name == "pourpoint-web-1")] | length == 1' "$LOCAL_EVIDENCE_DIR/final-servers.json" || teardown_status=1
  return "$teardown_status"
}
campaign_cleanup() {
  local prior_status=$? salvage_status=0 teardown_status=0
  set +x
  if test "$cleanup_complete" -eq 1 || test "$cleanup_running" -eq 1; then return "$prior_status"; fi
  cleanup_running=1
  if test -n "${WATCHDOG_PID:-}"; then kill "$WATCHDOG_PID" 2>/dev/null || true; fi
  trap - EXIT INT TERM HUP
  set +e
  if test -f "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"; then stop_dispatches || true; fi
  if test -f "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt" && test ! -f "$LOCAL_EVIDENCE_DIR/preservation-complete"; then
    salvage_evidence || salvage_status=1
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

The salvage roots are ordered by value per byte: campaign state and reports, logs, control builds, per-basin outputs, the extended artifact, and the VM-side digest manifests. Section 14's ordered preservation writes `preservation-complete` only after every root has a digest-verified off-VM copy, and the handler skips salvage only when that marker exists. Teardown is destructive only to the exact campaign server and volume. A salvage failure is recorded and never blocks teardown at the hard ceiling.

## 8. Provision, bootstrap, converge, and swap

Record the request epoch immediately before the sole provisioning command, then start the 72-hour watchdog:

```bash
test ! -e "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"
test ! -e "$LOCAL_EVIDENCE_DIR/hard-ceiling-reached"
(
  while test ! -f "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"; do sleep 1; done
  origin=$(<"$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt")
  deadline=$((origin + HARD_CEILING_SECONDS))
  while :; do
    now=$(date +%s)
    remaining=$((deadline - now))
    if test "$remaining" -le 0; then break; fi
    step=$remaining; if test "$step" -gt 30; then step=30; fi
    sleep "$step"
  done
  : > "$LOCAL_EVIDENCE_DIR/hard-ceiling-reached"
  kill -TERM "$$"
) &
WATCHDOG_PID=$!
date +%s > "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"
./scripts/hetzner/provision.sh \
  --campaign "$CAMPAIGN" \
  --s3-env-file "$S3_ENV_FILE" \
  --server-type ccx33 \
  --volume-size-gb 600 \
  --location fsn1 \
  2>&1 | tee "$LOCAL_EVIDENCE_DIR/provision.log"
./scripts/hetzner/bootstrap.sh --campaign "$CAMPAIGN" \
  2>&1 | tee "$LOCAL_EVIDENCE_DIR/bootstrap.log"
remote_ip
hcloud --context pourpoint server describe "$SERVER_NAME" -o json | jq '{id,name,server_type:.server_type.name,location:.datacenter.location.name,volumes}' > "$LOCAL_EVIDENCE_DIR/provisioned-server.json"
hcloud --context pourpoint volume describe "$VOLUME_NAME" -o json | jq '{id,name,size,location:.location.name,server}' > "$LOCAL_EVIDENCE_DIR/provisioned-volume.json"
jq -e '.server_type == "ccx33" and .location == "fsn1"' "$LOCAL_EVIDENCE_DIR/provisioned-server.json"
jq -e '.size == 600 and .location == "fsn1"' "$LOCAL_EVIDENCE_DIR/provisioned-volume.json"
```

Both earlier compile lifecycles needed a second `provision.sh` run after SSH readiness timed out, and one hit a `known_hosts` mismatch on a reused address. Rerunning `provision.sh` with identical arguments is the documented remedy; it reuses the exact existing resources by name and ID. The two `describe` records above are the exact identities that teardown must later match.

Converge two checkouts on the VM, build `hfx` from the corrected revision, apply the recorded ARG_MAX hotpatch to the planetary worktree, and enable swap. The hotpatch touches only the campaign runner, so the planetary adapter's bytes come from revision `43a98aff8c15a1a196f47b10217ad2f5553b6611` itself; the hotpatch is applied so the provenance matches the recorded control build exactly.

Require a clean detached worktree at planetary revision `43a98aff8c15a1a196f47b10217ad2f5553b6611`. Apply only the path-scoped diff from pinned hotpatch commit `bde61149d3fefc5e3f30435bf7ed3d0bb32a519c`. Refuse unless the pre-patch blob is `41d6df3f10030a481b2227a878837c7f23f3e658`. Refuse unless the post-patch blob is `d227920a7ac0ab98ffcc80aac2c72a5dfc9c2429`.

```bash
ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "root@$SERVER_IP" \
  bash -s -- "$GROUND_TRUTH_REF" <<'REMOTE' 2>&1 | tee "$LOCAL_EVIDENCE_DIR/converge.log"
set -Eeuo pipefail
set +x
ground_truth_ref=$1
planetary_ref=43a98aff8c15a1a196f47b10217ad2f5553b6611
hotpatch_ref=bde61149d3fefc5e3f30435bf7ed3d0bb32a519c
hotpatch_path=scripts/hetzner/tdx-hydro-campaign.sh
pre_patch_blob=41d6df3f10030a481b2227a878837c7f23f3e658
post_patch_blob=d227920a7ac0ab98ffcc80aac2c72a5dfc9c2429

cd /root/hfx
git fetch origin main
git checkout --detach "$ground_truth_ref"
test "$(git rev-parse HEAD)" = "$ground_truth_ref"
test -z "$(git status --porcelain)"
/root/.cargo/bin/cargo build --release -p hfx-cli
test -x /root/hfx/target/release/hfx

test ! -e /root/hfx-planetary
git worktree add --detach /root/hfx-planetary "$planetary_ref"
cd /root/hfx-planetary
test "$(git rev-parse HEAD)" = "$planetary_ref"
test -z "$(git status --porcelain)"
test "$(git rev-parse "HEAD:$hotpatch_path")" = "$pre_patch_blob"
test "$(git rev-parse "$hotpatch_ref^:$hotpatch_path")" = "$pre_patch_blob"
git diff --binary "$hotpatch_ref^" "$hotpatch_ref" -- "$hotpatch_path" | git apply --index
test "$(git diff --cached --name-only)" = "$hotpatch_path"
test "$(git rev-parse ":$hotpatch_path")" = "$post_patch_blob"
printf 'planetary_ref=%s\nhotpatch_ref=%s\nhotpatch_path=%s\npre_patch_blob=%s\npost_patch_blob=%s\n' \
  "$planetary_ref" "$hotpatch_ref" "$hotpatch_path" "$pre_patch_blob" "$post_patch_blob" > /root/hfx-planetary-provenance.txt

gdal-config --version
ogrinfo --version
jq --version
aws --version
rsync --version | head -n 1
/opt/hfx-geo/bin/python -c 'import geopandas, pyarrow, shapely'

root_avail=$(df -B1 --output=avail / | tail -n 1 | tr -d ' ')
root_swap_bytes=$((root_avail - 20000000000))
if test "$root_swap_bytes" -gt 200000000000; then root_swap_bytes=200000000000; fi
test "$root_swap_bytes" -gt 0
fallocate -l "$root_swap_bytes" /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
mkdir -p /mnt/hfx/swap
fallocate -l 150000000000 /mnt/hfx/swap/swapfile && chmod 600 /mnt/hfx/swap/swapfile && mkswap /mnt/hfx/swap/swapfile && swapon /mnt/hfx/swap/swapfile
swapon --show --bytes
free -b
df -B1 --output=avail /mnt/hfx | tail -n 1 | tr -d ' ' > /root/observed-available-disk-bytes.txt
test "$(cat /root/observed-available-disk-bytes.txt)" -ge 420000000000
awk '/MemAvailable:/ {exit !($2 * 1024 >= 30000000000)}' /proc/meminfo
mkdir -p /mnt/hfx/work/sha256 "$HOME/.ssh"
REMOTE
scp -o BatchMode=yes "root@$SERVER_IP:/root/hfx-planetary-provenance.txt" "$LOCAL_EVIDENCE_DIR/planetary-provenance.txt"
scp -o BatchMode=yes "root@$SERVER_IP:/root/observed-available-disk-bytes.txt" "$LOCAL_EVIDENCE_DIR/observed-available-disk-bytes.txt"
```

Swap sizing: the 55-basin strict validation needed about 265 GB resident and finished only on 287 GB of swap. A `ccx33` has 32 GB of memory and a 240 GB local disk, so the root swap file takes up to 200 GB and the volume swap file 150 GB, about 350 GB in total. The volume then holds the corpus (84.1 GB), the baseline (114.1 GB), the extended artifact (about 120 GB), per-basin outputs (about 25 GB), control builds (about 6 GB), and 150 GB of swap, about 500 GB of 600 GB. Section 13 states the expected validation outcome.

## 9. Corpus transfer and remote integrity

Initialize the campaign with the eight selected basins so the runner owns the `downloads` directory, then transfer the corpus into it and prove integrity on the VM against the recorded manifest:

```bash
campaign_gate pre-init 70
./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" start --workload tdx-init -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh init \
  --campaign "$CAMPAIGN" \
  --workspace-root "$WORKSPACE_ROOT" \
  --basin 1020018110 --basin 2020003440 --basin 2020065840 --basin 2020071190 \
  --basin 4020050470 --basin 5020049720 --basin 6020000010 --basin 7020000010 \
  --retention-policy retain-all-through-publication \
  --available-memory-bytes 32000000000 \
  --available-disk-bytes "$(cat "$LOCAL_EVIDENCE_DIR/observed-available-disk-bytes.txt")" \
  --retained-input-bytes 90000000000 \
  --retained-basin-output-bytes 30000000000 \
  --assembly-memory-ceiling-bytes 30000000000 \
  --assembly-scratch-ceiling-bytes 130000000000 \
  --assembled-artifact-bytes 130000000000 \
  --active-compile-scratch-bytes 20000000000 \
  --filesystem-overhead-bytes 20000000000
./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" status --workload tdx-init || test "$?" -eq 3
```

The sizing arithmetic the runner records is 90 GB retained input, 30 GB retained basin outputs, 20 GB active compile scratch, 130 GB assembly scratch or artifact, and 20 GB filesystem overhead, 290 GB in total. The baseline (114.1 GB) and the volume swap (150 GB) are outside the runner's model and are covered by the 420 GB availability check in section 8.

```bash
rsync -a --partial --info=progress2 -e 'ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new' \
  "$CORPUS_DIR/" "root@$SERVER_IP:$CAMPAIGN_DIR/downloads/" 2>&1 | tee "$LOCAL_EVIDENCE_DIR/corpus-transfer.log"
scp -o BatchMode=yes "$CORPUS_MANIFEST" "root@$SERVER_IP:/mnt/hfx/work/sha256/source-expected-sha256.txt"
ssh -o BatchMode=yes "root@$SERVER_IP" \
  "cd '$CAMPAIGN_DIR/downloads' && sha256sum -c /mnt/hfx/work/sha256/source-expected-sha256.txt" \
  | tee "$LOCAL_EVIDENCE_DIR/corpus-remote-verification.txt"
test "$(grep -c ': OK$' "$LOCAL_EVIDENCE_DIR/corpus-remote-verification.txt")" -eq 16
```

Measure throughput over the first 30 minutes. If the projected completion of the transfer exceeds the 24-hour decision point, stop the transfer, keep the completed files, and let the bounded acquisition below fetch the remainder from NGA. Adoption and reacquisition use the same command; a present file that passes inspection is reused without any network request, and an absent file is acquired with at most three attempts per product:

```bash
campaign_gate pre-acquire 60
./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" start --workload tdx-acquire -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh acquire \
  --campaign "$CAMPAIGN" --workspace-root "$WORKSPACE_ROOT" \
  --max-parallel 2 --product-attempt-ceiling 3
./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" status --workload tdx-acquire || test "$?" -eq 3
```

A transferred file that fails the remote SHA-256 check is removed from the VM's `downloads` directory only, never from the preserved corpus, and the acquisition rerun fetches it from NGA. Reacquisition from NGA is a fallback for an integrity failure or a stalled transfer. After the workload finishes, require every selected basin to have both products `succeeded`; an `exhausted` product excludes that basin from compilation and is recorded in section 15.

## 10. Two control builds and byte comparison

Transfer the preserved planetary control output to the VM as the byte-for-byte reference and reverify it there:

```bash
rsync -a -e 'ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new' \
  "$PRESERVED_CONTROL/" "root@$SERVER_IP:$CONTROL_ROOT/preserved/$CONTROL_ID/"
scp -o BatchMode=yes "$LOCAL_EVIDENCE_DIR/expected-control-sha256.json" "root@$SERVER_IP:/mnt/hfx/work/sha256/expected-control-sha256.json"
```

Build the control with the corrected adapter first, because its result decides whether any per-basin output can be trusted. Then rebuild the control with the planetary adapter so the VM's reproduction of the recorded digests is on record. Each build writes to its own output root; no tree is overwritten. Pass the preserved manifest's `created_at` when the corrected adapter's `build` accepts `--created-at`; the manifest embeds that timestamp, and only a pinned value can make `manifest.json` byte-identical. When the flag is absent, the comparison tolerates a manifest that differs only in `created_at` and records that tolerance.

```bash
campaign_gate pre-control-builds 56
ssh -o BatchMode=yes "root@$SERVER_IP" bash -s -- "$CONTROL_ID" "$CAMPAIGN_DIR" "$CONTROL_ROOT" <<'REMOTE' 2>&1 | tee "$LOCAL_EVIDENCE_DIR/control-builds.log"
set -Eeuo pipefail
set +x
control_id=$1; campaign_dir=$2; control_root=$3
python=/opt/hfx-geo/bin/python
hfx=/root/hfx/target/release/hfx
preserved_created_at=$(jq -r '.created_at' "$control_root/preserved/$control_id/manifest.json")
(cd "$control_root/preserved/$control_id" && jq -r 'to_entries[] | "\(.value)  ./\(.key)"' /mnt/hfx/work/sha256/expected-control-sha256.json | sha256sum -c)

mkdir -p "$control_root/corrected" "$control_root/planetary"
created_at_args=()
if "$python" /root/hfx/adapters/tdx-hydro/build_adapter.py build --help 2>/dev/null | grep -q -- '--created-at'; then
  created_at_args=(--created-at "$preserved_created_at")
fi
"$python" /root/hfx/adapters/tdx-hydro/build_adapter.py build \
  --basins "$campaign_dir/downloads/$control_id-basins.gpkg" \
  --streamnet "$campaign_dir/downloads/$control_id-streamnet.gpkg" \
  --out "$control_root/corrected/$control_id" \
  --report "$control_root/corrected/$control_id-build-report.json" \
  --processing-basin-id "$control_id" --fabric-version 0.3.0 \
  ${created_at_args[@]+"${created_at_args[@]}"}
"$python" /root/hfx/adapters/tdx-hydro/build_adapter.py validate "$control_root/corrected/$control_id" --hfx-binary "$hfx"

"$python" /root/hfx-planetary/adapters/tdx-hydro/build_adapter.py build \
  --basins "$campaign_dir/downloads/$control_id-basins.gpkg" \
  --streamnet "$campaign_dir/downloads/$control_id-streamnet.gpkg" \
  --out "$control_root/planetary/$control_id" \
  --report "$control_root/planetary/$control_id-build-report.json" \
  --processing-basin-id "$control_id" --fabric-version 0.3.0
"$python" /root/hfx/adapters/tdx-hydro/build_adapter.py validate "$control_root/planetary/$control_id" --hfx-binary "$hfx"

/root/hfx/scripts/hetzner/compare-dataset-trees.sh \
  --left "$control_root/preserved/$control_id" --right "$control_root/corrected/$control_id" \
  --expected-sha256 /mnt/hfx/work/sha256/expected-control-sha256.json \
  --allow-created-at-difference > "$control_root/compare-corrected.json"
/root/hfx/scripts/hetzner/compare-dataset-trees.sh \
  --left "$control_root/preserved/$control_id" --right "$control_root/planetary/$control_id" \
  --expected-sha256 /mnt/hfx/work/sha256/expected-control-sha256.json \
  --allow-created-at-difference > "$control_root/compare-planetary.json"
/root/hfx/scripts/hetzner/compare-dataset-trees.sh \
  --left "$control_root/corrected/$control_id" --right "$control_root/planetary/$control_id" \
  --allow-created-at-difference > "$control_root/compare-corrected-planetary.json"
jq -r '.verdict' "$control_root/compare-corrected.json" "$control_root/compare-planetary.json" "$control_root/compare-corrected-planetary.json"
REMOTE
scp -o BatchMode=yes "root@$SERVER_IP:$CONTROL_ROOT/compare-*.json" "$LOCAL_EVIDENCE_DIR/"
jq -e '.verdict == "identical" or (.verdict == "created-at-only" and (.files | map(select(.path != "manifest.json")) | all(.verdict == "identical")))' "$LOCAL_EVIDENCE_DIR/compare-corrected.json"
```

The three data files, `catchments.parquet`, `graph.parquet`, and `aux/snap_stems.parquet`, must equal the preserved digests exactly in both builds. `manifest.json` must equal the preserved digest when `--created-at` was available, and must differ only in `created_at` otherwise; the comparison record states which case occurred. Any other difference stops the work for adjudication. Do not explain a difference away, do not rerun with different arguments, and do not continue to per-basin compilation. Preserve both trees and both build reports, then go to section 14 and section 16.

A corrected build that refuses is the same stop: the refusal, its build report if any, and the log are preserved as the campaign's terminal evidence, and no per-basin compile starts.

## 11. Per-basin compile with the corrected adapter

Compile every selected basin through the campaign runner. The fabric version must equal the baseline's `NGA-TDX-Hydro-20230126` so the extension assembly accepts the new inputs. The control compiles here too, because extension assembly requires the excluded control's campaign compile to have succeeded.

```bash
campaign_gate pre-compile 40
./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" start --workload tdx-compile -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh compile \
  --campaign "$CAMPAIGN" --workspace-root "$WORKSPACE_ROOT" \
  --fabric-version NGA-TDX-Hydro-20230126
```

Poll with the gate before every status check and repeat the pair until status exits 3. The reserve of 20 hours covers assembly, preservation, and teardown. When status reports the session finished, require the canonical log to end with the runner finish record carrying exit `0`; any other exit is preserved as evidence and the campaign continues at section 14:

```bash
campaign_gate compile-monitor 20
./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" status --workload tdx-compile || test "$?" -eq 3
```

Every refusal is terminal evidence. The runner records `failed` with the adapter's message in `state/basins/<id>/current.json`, keeps any build report, and the workload log holds the traceback. Nothing is guessed around and nothing is retried with different inputs. Preserve the per-basin results before assembly:

```bash
ssh -o BatchMode=yes "root@$SERVER_IP" bash -s -- "$CAMPAIGN_DIR" <<'REMOTE'
set -Eeuo pipefail
cd "$1"
find basin-outputs -type f -print0 | sort -z | xargs -0 -r sha256sum > /mnt/hfx/work/sha256/basin-outputs-sha256.txt
REMOTE
mkdir -p "$LOCAL_EVIDENCE_DIR/off-vm/campaign"
copy_remote_root "$CAMPAIGN_DIR/basin-outputs" "$LOCAL_EVIDENCE_DIR/off-vm/campaign"
copy_remote_root "$CAMPAIGN_DIR/reports" "$LOCAL_EVIDENCE_DIR/off-vm/campaign"
copy_remote_root "$CAMPAIGN_DIR/state" "$LOCAL_EVIDENCE_DIR/off-vm/campaign"
copy_remote_root /mnt/hfx/logs "$LOCAL_EVIDENCE_DIR/off-vm"
copy_remote_root /mnt/hfx/work/sha256 "$LOCAL_EVIDENCE_DIR/off-vm"
(cd "$LOCAL_EVIDENCE_DIR/off-vm/campaign" && shasum -a 256 -c "$LOCAL_EVIDENCE_DIR/off-vm/sha256/basin-outputs-sha256.txt") | tee "$LOCAL_EVIDENCE_DIR/basin-outputs-verification.txt"
! grep -v ': OK$' "$LOCAL_EVIDENCE_DIR/basin-outputs-verification.txt"
compiled_absent=$(for id in $ABSENT_IDS; do jq -r --arg id "$id" 'select(.stages.compile.status == "succeeded") | $id' "$LOCAL_EVIDENCE_DIR/off-vm/campaign/state/basins/$id/current.json"; done)
printf '%s\n' "$compiled_absent" | sed '/^$/d' > "$LOCAL_EVIDENCE_DIR/compiled-absent-basins.txt"
```

If `compiled-absent-basins.txt` is empty, no extension is attempted. The strict-validated 55-basin artifact stays the final fabric, section 15 records the disposition of every basin, and the campaign continues at section 14.

## 12. Baseline pull, roster verification, and extension assembly

Pull the frozen artifact from the baseline prefix onto the volume. The prefix is read from S3 only; nothing under it is written, overwritten, or deleted. Verify the pulled manifest against the merged campaign record before it is used:

```bash
campaign_gate pre-baseline 16
ssh -o BatchMode=yes "root@$SERVER_IP" bash -s -- "$BASELINE_ROOT" "$BASELINE_PREFIX" "$S3_ENDPOINT" <<'REMOTE' 2>&1 | tee "$LOCAL_EVIDENCE_DIR/baseline-pull.log"
set -Eeuo pipefail
set +x
baseline_root=$1; prefix=$2; endpoint=$3
set -a; source /etc/pourpoint-hfx.env; set +a
mkdir -p "$baseline_root"
aws s3 ls "$prefix/dataset/" --recursive --endpoint-url "$endpoint" --region fsn1 > "$baseline_root/remote-listing.txt"
test "$(grep -c . "$baseline_root/remote-listing.txt")" -eq 6
aws s3 cp "$prefix/dataset/" "$baseline_root/dataset/" --recursive --endpoint-url "$endpoint" --region fsn1 --only-show-errors
aws s3 cp "$prefix/evidence/state/assembly.json" "$baseline_root/evidence-assembly.json" --endpoint-url "$endpoint" --region fsn1 --only-show-errors || true
jq -e '.region == "tdx-hydro-partial-4dbff0d6ec31" and .unit_count == 12748154 and .format_version == "0.3.0" and .fabric_version == "NGA-TDX-Hydro-20230126" and .fabric_name == "tdx_hydro"' "$baseline_root/dataset/manifest.json"
test "$(find "$baseline_root/dataset" -type f -exec stat -c '%s' {} + | awk '{s+=$1} END {print s}')" -eq 114063230627
(cd "$baseline_root/dataset" && find . -type f -print0 | sort -z | xargs -0 -r sha256sum) > /mnt/hfx/work/sha256/baseline-sha256.txt
REMOTE
scp -o BatchMode=yes "$LOCAL_EVIDENCE_DIR/baseline-roster.json" "root@$SERVER_IP:$BASELINE_ROOT/roster.json"
ssh -o BatchMode=yes "root@$SERVER_IP" bash -s -- "$BASELINE_ROOT" <<'REMOTE'
set -Eeuo pipefail
baseline_root=$1
test "$(jq -r 'join(",")' "$baseline_root/roster.json" | tr -d '\n' | sha256sum | cut -c1-12)" = 4dbff0d6ec31
if test -s "$baseline_root/evidence-assembly.json"; then
  test "$(jq -c '.input_basin_ids | sort | unique' "$baseline_root/evidence-assembly.json")" = "$(jq -c '.' "$baseline_root/roster.json")"
fi
REMOTE
```

The roster is the 55-basin assembly input list. Its digest prefix must equal the partial-region suffix of the pulled manifest. When the baseline's own evidence prefix holds the campaign assembly state, the two rosters must agree.

Assemble the extension. The runner passes the fabric once, omits the excluded control output, adds only successful selected basins absent from the roster, refuses a compiled basin already present in the roster, and requires at least one new basin. It derives the new roster, region digest, bbox, and unit count from the actual inputs. It then runs the adapter's strict validation on the assembled artifact inside the same workload.

```bash
test -s "$LOCAL_EVIDENCE_DIR/compiled-absent-basins.txt"
campaign_gate pre-assemble 14
./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" start --workload tdx-assemble -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh assemble \
  --campaign "$CAMPAIGN" --workspace-root "$WORKSPACE_ROOT" \
  --partial-fabric "$BASELINE_ROOT/dataset" \
  --partial-fabric-roster "$BASELINE_ROOT/roster.json" \
  --exclude-control-basin "$CONTROL_ID"
```

The adapter stages the artifact and renames it into `assembly/dataset` in one step, so the artifact is final the moment that directory exists. Preserve it as soon as it appears, while validation continues (section 14), so a later interruption of validation cannot cost the artifact:

```bash
until ssh -o BatchMode=yes "root@$SERVER_IP" test -d "$CAMPAIGN_DIR/assembly/dataset"; do
  campaign_gate assemble-monitor 12
  if ./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" status --workload tdx-assemble; then sleep 300; else test "$?" -eq 3; break; fi
done
```

## 13. Strict validation attempt and its truthful outcome

Validation of the extended artifact runs `hfx <dataset> --strict --sample-pct 100`. The validator materializes every geometry before checking, so the 55-basin artifact needed about 265 GB resident and 12.7 hours on this server class with 287 GB of swap. The extended artifact is larger. With the roughly 350 GB of swap from section 8 the expected outcome is a run of 13 to 30 hours that may finish, may be OOM-killed, or may reach the 66-hour decision point first. All three outcomes are acceptable to record; only one is a pass.

Classify from the runner's assembly state and the workload log:

| Evidence | Recorded outcome |
| --- | --- |
| `state/assembly.json` status `succeeded` | strict validation `passed` |
| status `failed`, reason `assembled dataset validation failed; retained for inspection`, log shows `hfx` exit status 1 with validator diagnostics | strict validation `failed`, diagnostics preserved |
| status `failed`, same reason, log shows a negative or 137 exit status, `dmesg` shows the OOM killer ending `hfx`, or the operator stopped the workload at a decision point | strict validation `incomplete` |
| status `failed`, reason `adapter assembly failed` | assembly `failed`; no extended artifact exists |

An interrupted or OOM-killed validation is recorded as `incomplete`, never as passing. Only a completed pass is called strict-validated. Capture the evidence when the workload ends:

```bash
./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" status --workload tdx-assemble || test "$?" -eq 3
ssh -o BatchMode=yes "root@$SERVER_IP" bash -s -- "$CAMPAIGN_DIR" <<'REMOTE' > "$LOCAL_EVIDENCE_DIR/validation-evidence.txt"
set -Eeuo pipefail
cat "$1/state/assembly.json"
dmesg -T 2>/dev/null | grep -i -E 'out of memory|killed process' || true
grep -E 'return code|hfx: error|validation' /mnt/hfx/logs/hfx-seven-basin-extension-tdx-assemble.log | tail -n 40 || true
REMOTE
```

The extended artifact and the complete validation record are preserved whatever the outcome. The strict-validated 55-basin artifact remains the known-good baseline and the final artifact when validation of the extension did not pass.

## 14. Preservation before teardown

Preserve in this order, each root with a SHA-256 manifest computed on the VM and recomputed after transfer. A copy counts only when relative paths and digests match on both sides. The order follows value per byte so an interruption loses the least:

1. campaign `state`, `reports`, and `/mnt/hfx/logs`
2. both control builds, their reports, and the three comparison records
3. every per-basin output
4. the extended artifact, first to the extension scratch prefix, then to the operator
5. the VM-side digest manifests

```bash
campaign_gate pre-preservation 6
ssh -o BatchMode=yes "root@$SERVER_IP" bash -s -- "$CAMPAIGN_DIR" "$CONTROL_ROOT" <<'REMOTE'
set -Eeuo pipefail
cd "$1"
for root in state reports basin-outputs assembly/dataset; do
  test -d "$root" || continue
  find "$root" -type f -print0 | sort -z | xargs -0 -r sha256sum > "/mnt/hfx/work/sha256/campaign-$(printf '%s' "$root" | tr / -)-sha256.txt"
done
(cd /mnt/hfx && find logs -type f -print0 | sort -z | xargs -0 -r sha256sum) > /mnt/hfx/work/sha256/logs-sha256.txt
(cd "$2" && find . -type f -print0 | sort -z | xargs -0 -r sha256sum) > /mnt/hfx/work/sha256/control-builds-sha256.txt
REMOTE
for root in state reports basin-outputs; do copy_remote_root "$CAMPAIGN_DIR/$root" "$LOCAL_EVIDENCE_DIR/off-vm/campaign"; done
copy_remote_root /mnt/hfx/logs "$LOCAL_EVIDENCE_DIR/off-vm"
copy_remote_root "$CONTROL_ROOT" "$LOCAL_EVIDENCE_DIR/off-vm"
copy_remote_root /mnt/hfx/work/sha256 "$LOCAL_EVIDENCE_DIR/off-vm"
for manifest in campaign-state campaign-reports campaign-basin-outputs; do
  (cd "$LOCAL_EVIDENCE_DIR/off-vm/campaign" && shasum -a 256 -c "$LOCAL_EVIDENCE_DIR/off-vm/sha256/$manifest-sha256.txt") | tee "$LOCAL_EVIDENCE_DIR/verify-$manifest.txt"
  ! grep -v ': OK$' "$LOCAL_EVIDENCE_DIR/verify-$manifest.txt"
done
(cd "$LOCAL_EVIDENCE_DIR/off-vm" && shasum -a 256 -c "$LOCAL_EVIDENCE_DIR/off-vm/sha256/logs-sha256.txt") | tee "$LOCAL_EVIDENCE_DIR/verify-logs.txt"
! grep -v ': OK$' "$LOCAL_EVIDENCE_DIR/verify-logs.txt"
(cd "$LOCAL_EVIDENCE_DIR/off-vm/control-builds" && shasum -a 256 -c "$LOCAL_EVIDENCE_DIR/off-vm/sha256/control-builds-sha256.txt") | tee "$LOCAL_EVIDENCE_DIR/verify-control-builds.txt"
! grep -v ': OK$' "$LOCAL_EVIDENCE_DIR/verify-control-builds.txt"
```

Copy the extended artifact, when it exists, to a new prefix under the campaign's scratch namespace and prove the S3 copy by listing sizes and by downloading it back to a scratch directory on the VM and recomputing digests. The prefix is named by content. The baseline prefix is never written.

```bash
if ssh -o BatchMode=yes "root@$SERVER_IP" test -d "$CAMPAIGN_DIR/assembly/dataset"; then
  ssh -o BatchMode=yes "root@$SERVER_IP" bash -s -- "$CAMPAIGN_DIR" "$EXTENSION_PREFIX" "$S3_ENDPOINT" "$CONTROL_ROOT" <<'REMOTE' 2>&1 | tee "$LOCAL_EVIDENCE_DIR/extension-s3-preservation.log"
set -Eeuo pipefail
set +x
campaign_dir=$1; prefix=$2; endpoint=$3; control_root=$4
set -a; source /etc/pourpoint-hfx.env; set +a
test -z "$(aws s3 ls "$prefix/extension-hfx-v0-3-0/" --endpoint-url "$endpoint" --region fsn1)"
aws s3 cp "$campaign_dir/assembly/dataset/" "$prefix/extension-hfx-v0-3-0/dataset/" --recursive --endpoint-url "$endpoint" --region fsn1 --only-show-errors
aws s3 cp /mnt/hfx/work/sha256/campaign-assembly-dataset-sha256.txt "$prefix/extension-hfx-v0-3-0/dataset-sha256.txt" --endpoint-url "$endpoint" --region fsn1 --only-show-errors
aws s3 cp "$campaign_dir/state/assembly.json" "$prefix/extension-hfx-v0-3-0/evidence/state/assembly.json" --endpoint-url "$endpoint" --region fsn1 --only-show-errors
aws s3 cp "$control_root/" "$prefix/control-builds/" --recursive --endpoint-url "$endpoint" --region fsn1 --only-show-errors
aws s3 cp "$campaign_dir/basin-outputs/" "$prefix/basin-outputs/" --recursive --endpoint-url "$endpoint" --region fsn1 --only-show-errors
aws s3 ls "$prefix/" --recursive --endpoint-url "$endpoint" --region fsn1 > /mnt/hfx/work/sha256/extension-remote-listing.txt
rm -rf /mnt/hfx/work/s3-readback && mkdir -p /mnt/hfx/work/s3-readback
aws s3 cp "$prefix/extension-hfx-v0-3-0/dataset/" /mnt/hfx/work/s3-readback/dataset/ --recursive --endpoint-url "$endpoint" --region fsn1 --only-show-errors
(cd /mnt/hfx/work/s3-readback && sed 's#^\([0-9a-f]\{64\}\)  assembly/dataset/#\1  dataset/#' /mnt/hfx/work/sha256/campaign-assembly-dataset-sha256.txt | sha256sum -c) > /mnt/hfx/work/sha256/extension-s3-readback-verification.txt
! grep -v ': OK$' /mnt/hfx/work/sha256/extension-s3-readback-verification.txt
rm -rf /mnt/hfx/work/s3-readback
REMOTE
  copy_remote_root "$CAMPAIGN_DIR/assembly/dataset" "$LOCAL_EVIDENCE_DIR/off-vm/extension"
  copy_remote_root /mnt/hfx/work/sha256 "$LOCAL_EVIDENCE_DIR/off-vm"
  (cd "$LOCAL_EVIDENCE_DIR/off-vm/extension" && sed 's#^\([0-9a-f]\{64\}\)  assembly/dataset/#\1  dataset/#' "$LOCAL_EVIDENCE_DIR/off-vm/sha256/campaign-assembly-dataset-sha256.txt" | shasum -a 256 -c) | tee "$LOCAL_EVIDENCE_DIR/verify-extension.txt"
  ! grep -v ': OK$' "$LOCAL_EVIDENCE_DIR/verify-extension.txt"
fi
: > "$LOCAL_EVIDENCE_DIR/preservation-complete"
```

The read-back scratch directory is the only VM path this campaign removes, and it holds nothing that was not first proved present under the extension prefix. The S3 copies of the control builds and per-basin outputs give the extension prefix a self-contained provenance; the operator copies remain the primary evidence root.

## 15. Campaign record

Generate one record naming every selected basin and its disposition, both control builds with their comparison verdicts, the extension outcome, and the validation classification. The record is derived from state files:

```bash
record_or_empty() { if test -f "$LOCAL_EVIDENCE_DIR/$1"; then printf '%s\n' "$LOCAL_EVIDENCE_DIR/$1"; else printf '%s\n' /dev/null; fi; }
jq -n \
  --arg campaign "$CAMPAIGN" \
  --arg ground_truth_ref "$(cat "$LOCAL_EVIDENCE_DIR/ground-truth-ref.txt")" \
  --slurpfile corrected "$(record_or_empty compare-corrected.json)" \
  --slurpfile planetary "$(record_or_empty compare-planetary.json)" \
  --slurpfile assembly "$(record_or_empty off-vm/campaign/state/assembly.json)" \
  --arg validation_outcome "$VALIDATION_OUTCOME" \
  --argjson basins "$(for id in $ABSENT_IDS $CONTROL_ID; do jq -c --arg id "$id" '{processing_basin_id:$id, acquire_basins:.stages.acquire_basins.status, acquire_streamnet:.stages.acquire_streamnet.status, compile:.stages.compile.status, failure_reason:.stages.compile.failure_reason, diagnostic_report:.stages.compile.diagnostic_report}' "$LOCAL_EVIDENCE_DIR/off-vm/campaign/state/basins/$id/current.json"; done | jq -s '.')" '{
    schema_version: 1,
    campaign: $campaign,
    ground_truth_ref: $ground_truth_ref,
    control_builds: {
      corrected_versus_preserved: ($corrected[0].verdict // "not-attempted"),
      planetary_versus_preserved: ($planetary[0].verdict // "not-attempted"),
      preserved_matches_pinned_digests: ($corrected[0].left_matches_expected_sha256 // null)
    },
    basins: $basins,
    extension: (if ($assembly | length) == 0 then null else {
      status: $assembly[0].status,
      failure_reason: $assembly[0].failure_reason,
      fabric_basin_ids: $assembly[0].fabric_basin_ids,
      included_basin_ids: $assembly[0].included_basin_ids,
      excluded_control_basin_id: $assembly[0].excluded_control_basin_id
    } end),
    strict_validation: $validation_outcome,
    final_fabric: (if $validation_outcome == "passed" then "extended artifact" else "strict-validated 55-basin baseline" end)
  }' > "$LOCAL_EVIDENCE_DIR/campaign-record.json"
```

Set `VALIDATION_OUTCOME` to `passed`, `failed`, `incomplete`, or `not-attempted` from the section 13 table before generating the record. The record classifies dispositions only from evidence: a basin with compile `succeeded` has a preserved output and digest, and a basin with compile `failed` carries the adapter's exact refusal. This record never emits a source-defect or adapter-strictness verdict; adjudication belongs to the merged ledger at `adapters/tdx-hydro/seven-basin-verdicts.json` through its own reviewed path.

## 16. Mandatory teardown and zero-footprint audit

After preservation is proved, run the same default teardown every trap path uses, then mark cleanup complete:

```bash
test -f "$LOCAL_EVIDENCE_DIR/preservation-complete"
default_teardown
cleanup_complete=1
```

The teardown command resolves both resources by exact name, validates their labels, compares the IDs against those captured at provisioning on every poll, refuses globs, and never accepts `--keep-volume`. Its stderr must contain this exact line:

```text
hfx: campaign seven-basin-extension has zero Hetzner footprint: server hfx-build-seven-basin-extension absent; volume hfx-build-seven-basin-extension-data absent
```

The independent read-only audit then requires both exact names absent and `pourpoint-web-1` present and unchanged. Compare the IDs in `final-servers.json` and `final-volumes.json` with `provisioned-server.json` and `provisioned-volume.json`: neither captured ID may appear. A failed teardown is a campaign failure requiring maintainer attention. It never permits deleting a different resource.

## 17. Time-boxed decision points

The provisioning request epoch is hour 0. At each decision point the operator compares progress against the stage plan and acts without discretion:

| Hour | Required progress | Action when not met |
| ---: | --- | --- |
| 24 | corpus on the VM and verified; both control builds compared; per-basin compile started | stop the running workload, preserve everything that exists (section 14), record the incomplete stage, tear down |
| 48 | per-basin compile finished and preserved; baseline pulled; extension assembled or refused; validation started or recorded as not attempted | stop the running workload, preserve, record, tear down |
| 66 | validation passed, failed, or still running | stop validation with `launch.sh` and record it `incomplete`; preserve remaining roots; teardown must complete before hour 72 |

The stage plan behind those points is approximately: provisioning and convergence 2 hours; corpus transfer 3 to 12 hours; control builds and comparison 3 hours; seven per-basin compiles 7 to 14 hours; baseline pull 1 hour; assembly 2 to 6 hours; validation 13 to 30 hours; preservation 4 to 8 hours; teardown 1 hour. Preservation of small roots is quick and runs at every stop. A decision point never authorizes a new act, a retry with different inputs, or a second lifecycle.

## 18. Failure classifications

| Class | Required action |
| --- | --- |
| Corpus integrity failure on either side | Remove only the VM copy of the failed file, reacquire from NGA with the bounded acquisition, record the digest mismatch |
| Corrected control build refusal or any digest difference beyond `created_at` | Preserve both trees and reports, stop all compilation, tear down, hand to adjudication |
| Per-basin compile refusal | Preserve the refusal, its report, and the log; continue with other basins |
| No absent basin compiles | Skip assembly; record the baseline as the final fabric; preserve and tear down |
| Extension assembly refusal | Preserve the runner diagnostic and state; record no extended artifact; tear down |
| Validation OOM, interruption, or decision point | Record `incomplete`; preserve the artifact and the record; never claim a pass |
| Quota, price, identity, ref, dependency, memory, or disk preflight failure | Stop before the act; salvage if anything exists; tear down if provisioned |
| Time or cost ceiling | Stop dispatch, preserve, mandatory teardown |
| SSH or transient interruption below both ceilings | Preserve evidence, rerun the identical command after another gate |
| Any request to adjudicate, publish under `hfx/`, delete preserved data, write the baseline prefix, use `--keep-volume`, or touch another resource | Refuse |
| Preservation failure | Record the failure, seek maintainer help while below the hard ceiling, and still tear down at the hard ceiling |

## 19. Author-only review and landing gates

This section is documentation-author work completed before merge. It never executes the campaign. The tracked write set is this runbook, `verify-compile-runbook.sh`, `verify-campaign-inputs.sh`, `price-preflight.sh`, `compare-dataset-trees.sh`, their tests, and one README index entry.

```bash
bash scripts/hetzner/test-verify-compile-runbook.sh
bash scripts/hetzner/test-verify-campaign-inputs.sh
bash scripts/hetzner/test-price-preflight.sh
bash scripts/hetzner/test-compare-dataset-trees.sh
bash scripts/hetzner/test-tdx-hydro-campaign.sh
for check in scope-permits-compilation ceilings-and-kill-switches control-hotpatch-is-pinned control-digests-are-pinned baseline-is-pinned authority-is-current; do
  bash scripts/hetzner/verify-compile-runbook.sh --check "$check"
done
git diff --check
```

Review every Bash fence for strict-mode behavior, quoting, secret-path nondisclosure, exact resource identity, absence of globs in mutation, and preservation-before-teardown ordering. Run the repository gates that apply to `scripts/hetzner` and record their output in the PR body.
