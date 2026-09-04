# TDX-Hydro seven-basin compile and extension campaign

This runbook is an instruction document. Authoring, reviewing, and verifying it provisions nothing, contacts neither NGA nor S3, performs no compile or teardown, and writes nothing to object storage.

This campaign consumes the one remaining approved bounded Hetzner lifecycle for Effort #195. It builds two control outputs for basin `7020000010`, proves the planetary rebuild byte for byte against the preserved control, proves the corrected build against the maintainer-adjudicated difference record, compiles the seven absent basins with the corrected adapter, extends the frozen 55-basin artifact with every newly compiled basin, attempts strict whole-dataset validation, preserves everything off the VM with digests, and tears down by exact resource identity.

version_policy is NONE: NO version bump, NO tag.

<!-- BEGIN COMPILE CAMPAIGN CONTRACT
{
  "schema": 5,
  "campaign": "seven-basin-extension",
  "authority_ref": "69747055bcb1876d9d1fad48c60f5cae6a24ea60",
  "authority_document": "planning/visions/2026-09-03-close-the-seven-basin-coverage-gap.md",
  "authority_section": "Compute and preservation constraints",
  "lifecycles_authorized": 1,
  "lifecycle_ledger": {
    "consumed": [
      {"date": "2026-09-04", "provisioning_request": "2026-09-04T10:46:18Z", "zero_footprint": "2026-09-04T10:50:58Z", "server_id": 164550505, "volume_id": 106790870, "cause": "hcloud-json-shape-mismatch", "workload_dispatched": false},
      {"date": "2026-09-04", "provisioning_request": "2026-09-04T12:51:01Z", "zero_footprint": "2026-09-04T16:00:57Z", "server_id": 164562279, "volume_id": 106791727, "cause": "control-transfer-parent-directory-absent", "workload_dispatched": true}
    ],
    "rehearsal_authority": {
      "maintainer": "Nicolas Lazaro", "date": "2026-09-04", "record": "https://github.com/CooperBigFoot/hfx/pull/234", "campaign": "campaign-rehearsal", "contract": "scripts/hetzner/rehearsal-campaign-contract.json",
      "limits": "one cx23 in fsn1, one 10 GB volume, under 6 hours from the provisioning request, under EUR 1.00 projected and actual per run, exact-resource teardown",
      "reruns": "permitted after each merged, reviewed fix while the cumulative estimated rehearsal spend stays below the cumulative ceiling",
      "cumulative_ceiling_eur": 1.0,
      "runs": [
        {"date": "2026-09-04", "provisioning_request": "2026-09-04T20:51:00Z", "zero_footprint": "2026-09-04T21:04:28Z", "server_id": 164601847, "volume_id": 106794141, "cause": "ssh-remote-argument-flattening", "workload_dispatched": false, "estimated_cost_eur": 0.01}
      ]
    },
    "current_authority": {"maintainer": "Nicolas Lazaro", "date": "2026-09-04", "lifecycles": 1, "record": "https://github.com/CooperBigFoot/hfx/pull/234", "precondition": "a lifecycle-result.json with result passed under the rehearsal evidence root for campaign-rehearsal", "limits": "one ccx33 in fsn1, one 600 GB volume, under 72 hours from the provisioning request, under EUR 40.00 projected and actual, exact-resource teardown"}
  },
  "requires_passing_rehearsal": true,
  "server_name": "hfx-build-seven-basin-extension",
  "volume_name": "hfx-build-seven-basin-extension-data",
  "server_type": "ccx33",
  "location": "fsn1",
  "volume_size_gb": 600,
  "wall_clock_ceiling_hours": 72,
  "gross_cost_ceiling_eur": 40.0,
  "billable_outbound_bytes": 400000000000,
  "price_source": "https://api.hetzner.cloud/v1/pricing",
  "decision_points_hours": [24, 48, 66],
  "gate_reserve_hours": {"pre-init": 70, "pre-acquire": 60, "pre-control-builds": 56, "pre-compile": 40, "compile-monitor": 20, "pre-baseline": 16, "pre-assemble": 14, "assemble-monitor": 12, "pre-preservation": 6},
  "retention_policy": "retain-all-produced-output-off-vm-before-exact-resource-teardown",
  "approval_record": "provisioner-transfer-approval.txt",
  "out_of_scope_resources": ["pourpoint-web-1"],
  "absent_basins": ["1020018110", "2020003440", "2020065840", "2020071190", "4020050470", "5020049720", "6020000010"],
  "control_basin": "7020000010",
  "control_unit_count": 331263,
  "fabric_version": "NGA-TDX-Hydro-20230126",
  "control_fabric_version": "0.3.0",
  "source_corpus": {"file_count": 16, "total_bytes": 84101885952, "manifest": "attempt21-source-remote-sha256.txt"},
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
  "control_adjudication_record": "scripts/hetzner/seven-basin-control-adjudication.json",
  "control_reference": "preserved-off-vm",
  "baseline": {
    "prefix": "s3://pourpoint-hfx/scratch/tdx-hydro-tdx-m5-planetary/planetary-hfx-v0-3-0/dataset/",
    "region": "tdx-hydro-partial-4dbff0d6ec31",
    "basin_count": 55,
    "unit_count": 12748154,
    "exported_bytes": 114063230627,
    "object_count": 6,
    "roster_digest_prefix": "4dbff0d6ec31",
    "basin_ids": [
      "1020000010", "1020011530", "1020021940", "1020027430", "1020034170", "1020035180", "1020040190", "2020000010",
      "2020018240", "2020024230", "2020033490", "2020041390", "2020057170", "3020000010", "3020003790", "3020005240",
      "3020008670", "3020009320", "3020024310", "4020000010", "4020006940", "4020015090", "4020024190", "4020034510",
      "4020050210", "4020050220", "4020050290", "5020000010", "5020015660", "5020037270", "5020054880", "5020055870",
      "5020082270", "6020006540", "6020008320", "6020014330", "6020017370", "6020021870", "6020029280", "7020000010",
      "7020014250", "7020021430", "7020024600", "7020038340", "7020046750", "7020047840", "7020065090", "8020000010",
      "8020008900", "8020010700", "8020020760", "8020022890", "8020032840", "8020044560", "9020000010"
    ]
  },
  "extension_scratch_prefix": "scratch/tdx-hydro-seven-basin-extension/",
  "workload_sizing": {
    "available_memory_bytes": 32000000000,
    "retained_input_bytes": 90000000000,
    "retained_basin_output_bytes": 30000000000,
    "assembly_memory_ceiling_bytes": 30000000000,
    "assembly_scratch_ceiling_bytes": 130000000000,
    "assembled_artifact_bytes": 130000000000,
    "active_compile_scratch_bytes": 20000000000,
    "filesystem_overhead_bytes": 20000000000,
    "root_disk_reserve_bytes": 20000000000,
    "root_swap_bytes_max": 200000000000,
    "volume_swap_bytes": 150000000000,
    "required_available_disk_bytes": 420000000000,
    "required_memory_bytes": 30000000000
  },
  "permitted_acts": ["transfer-preserved-source-corpus", "reacquire-selected-source-on-integrity-failure", "compile-both-control-builds", "compare-control-outputs", "compile-selected-basins", "pull-baseline-read-only", "assemble-extension", "attempt-strict-validation", "preserve-all-produced-output-off-vm", "read-only-audit", "exact-resource-teardown"],
  "sole_destructive_act": "exact-resource-teardown"
}
END COMPILE CAMPAIGN CONTRACT -->

## 1. Authority and scope

The authority for this campaign is the merged vision at `planning/visions/2026-09-03-close-the-seven-basin-coverage-gap.md` on `main`, section "Compute and preservation constraints", first merged at `69747055bcb1876d9d1fad48c60f5cae6a24ea60`. That section carries forward exactly one previously approved bounded lifecycle. This runbook spends that lifecycle. Any further lifecycle needs new maintainer authority before provisioning. A repository change cannot manufacture that authority.

The campaign identity is `seven-basin-extension`. It is distinct from the two earlier `tdx-m5-seven-compile` lifecycles and from `tdx-m5-seven-acquire`, which all reached exact-resource teardown. Records of those lifecycles remain under the evidence root and are never modified by this campaign.

Only the named campaign server and volume may be mutated. Those are server `hfx-build-seven-basin-extension` and volume `hfx-build-seven-basin-extension-data`. `pourpoint-web-1` is outside scope and must remain untouched. Globs, prefixes, label selectors, and project-wide mutation are forbidden. Read-only project listings are allowed for preflight and the final zero-footprint audit.

Mandatory exact-resource teardown of the named server and volume is the sole permitted destructive operation. This campaign deletes no source data, no output, no evidence, no S3 object, and no other server or volume. The baseline prefix is read-only; nothing under it is modified or deleted. No produced output may remain unique to the VM or volume.

The permitted acts are the transfer of the preserved source corpus, reacquisition of a selected product only when its integrity check fails, both control builds and their comparisons, the per-basin compiles, a read-only pull of the baseline, one extension assembly, one strict validation attempt, off-VM preservation, read-only audits, and exact-resource teardown. Adjudication, defect-report transmission, publication under `hfx/`, and adapter changes are outside this runbook.

### Lifecycle consumed on 2026-09-04

The one lifecycle this contract authorizes was spent on 2026-09-04. Provisioning and bootstrap succeeded (provisioning request 10:46:18Z, bootstrap complete 10:50:23Z). The section 8 identity gate then read the server location through `.datacenter.location.name`, a path that hcloud v1.66.0 leaves null, so the recorded projection carried `"location": null`, the value gate returned false, and the strict-mode driver entered the cleanup path. No workload was dispatched. Exact-resource teardown removed server `164550505` and volume `106790870`, zero footprint was proven at 10:50:58Z, and `pourpoint-web-1` was untouched. The billed interval was under five minutes, so the gross cost is negligible. The operator record is `seven-basin-extension/OPERATOR-LOG.md` under the evidence root.

The gate now projects every identity through the tracked `scripts/hetzner/hcloud-identity.jq`, and section 6 proves that projection against the installed CLI before any provisioning request. This consumed-lifecycle record grants nothing by itself; the authority for the next lifecycle is the maintainer decision recorded below.

### Second lifecycle authorized on 2026-09-04

After that lifecycle was consumed, Nicolas Lazaro authorized on 2026-09-04 exactly one further bounded lifecycle under the same limits: one `ccx33` in `fsn1`, one 600 GB volume, strictly under 72 hours from the provisioning request, strictly under EUR 40.00 projected and actual, and exact-resource teardown. That second lifecycle was consumed the same day (below). The contract's `lifecycles_authorized` stays at one because each authority grants one lifecycle and the ledger records which one is current. Each 2026-09-04 authorization covers one lifecycle only. When it is spent, the ledger must record it as consumed, and any further lifecycle again requires new maintainer authority before provisioning.

### Second lifecycle consumed on 2026-09-04

The second lifecycle ran from the provisioning request at 12:51:01Z (server `164562279`, volume `106791727`) to zero footprint at 16:00:57Z. Provisioning, bootstrap, convergence with the hotpatch, campaign initialization, the 84 GB corpus transfer, and the adopting acquisition all completed, in three driver starts: the first converge died at 12:59:17Z on the remote `rsync --version | head -n 1` SIGPIPE; the first acquisition failed at 15:11:35Z on the runner's adopted-product state defect, fixed by the change merged as `0ffa2d048ce5d748c0ab4c71fbe6f5862478107d`; the resumed acquisition succeeded for all eight selected basins. The section 10 transfer of the preserved control then failed at 16:00:31Z because no fence had created `/mnt/hfx/work/control-builds`, and the runbook's own cleanup performed exact-resource teardown. No control build, per-basin output, or artifact was produced. The operator record is `seven-basin-extension/OPERATOR-LOG.md` under the evidence root. Every one of those defects is repaired in the fences below, and section 20 requires the repaired fences to run end to end on a rehearsal lifecycle before the next production lifecycle.

### Rehearsal and third lifecycle authorized on 2026-09-04

After the second lifecycle was consumed, Nicolas Lazaro authorized on 2026-09-04 a rehearsal lifecycle and, conditional on it, exactly one further production lifecycle. The rehearsal runs the same fences on a synthetic corpus under the limits pinned in `lifecycle_ledger.rehearsal_authority`: one `cx23` in `fsn1`, one 10 GB volume, strictly under 6 hours, strictly under EUR 1.00 per run, and exact-resource teardown; its parameters are the tracked `scripts/hetzner/rehearsal-campaign-contract.json`. Later on 2026-09-04 the maintainer made the rehearsal repeatable: after each merged, reviewed fix a rehearsal may run again under the same limits as long as the cumulative estimated rehearsal spend recorded in `rehearsal_authority.runs` stays below `cumulative_ceiling_eur`; the verifier refuses a rehearsal once the estimates reach that ceiling. The production lifecycle keeps the limits above and is recorded in `lifecycle_ledger.current_authority`; its `precondition` names the passing rehearsal record that the production preflight requires. Both authorities are the maintainer decision recorded in the pull request named by their `record` fields, together with the vision on `main`. The 2026-09-04 authorization covers one lifecycle only. A rehearsal that fails is recorded in `rehearsal_authority.runs` with its cause and estimated cost; the next rehearsal waits for a merged, reviewed fix and for the cumulative estimate to remain below the ceiling.

### Rehearsal run 1 consumed on 2026-09-04

Rehearsal run 1 ran from the provisioning request at 20:51:00Z (server `164601847`, volume `106794141`) to zero footprint at 21:04:28Z, about 13 minutes and an estimated EUR 0.01. Provisioning (with the reused-address `known_hosts` rerun of section 8), bootstrap on the `cx23`, and the identity gate all passed. The converge fence then sent the five `workload_sizing` values to the VM as one space-joined argument; `ssh` joins its remote command arguments into a single string that the remote login shell splits again, so the VM received five positional parameters, the remote `read` filled one variable, and the numeric guard ended the shell before any output. The runbook's own cleanup performed exact-resource teardown. The repair is the `remote_tokens` rule stated in section 4 and enforced by the composer in section 20.

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

test -n "${HFX_CAMPAIGN_EVIDENCE:-}"
case "$HFX_CAMPAIGN_EVIDENCE" in /*) ;; *) exit 1 ;; esac
test -d "$HFX_CAMPAIGN_EVIDENCE" && test ! -L "$HFX_CAMPAIGN_EVIDENCE"
RUNBOOK=scripts/hetzner/RUNBOOK-tdx-hydro-seven-basin-compile.md
if test -n "${HFX_CAMPAIGN_CONTRACT:-}"; then
  case "$HFX_CAMPAIGN_CONTRACT" in /*) ;; *) exit 1 ;; esac
  test -f "$HFX_CAMPAIGN_CONTRACT" && test ! -L "$HFX_CAMPAIGN_CONTRACT"
  CAMPAIGN_CONTRACT_JSON=$(cat -- "$HFX_CAMPAIGN_CONTRACT")
else
  CAMPAIGN_CONTRACT_JSON=$(sed -n '/^<!-- BEGIN COMPILE CAMPAIGN CONTRACT$/,/^END COMPILE CAMPAIGN CONTRACT -->$/p' "$RUNBOOK" | sed '1d;$d')
fi
contract_value() { jq -er "$1" <<<"$CAMPAIGN_CONTRACT_JSON"; }
remote_tokens() { local token; for token in "$@"; do printf '%q ' "$token"; done; }
test "$(contract_value '.schema')" -eq 5
CAMPAIGN=$(contract_value '.campaign')
[[ "$CAMPAIGN" =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]]
SERVER_NAME=$(contract_value '.server_name')
VOLUME_NAME=$(contract_value '.volume_name')
test "$SERVER_NAME" = "hfx-build-$CAMPAIGN" && test "$VOLUME_NAME" = "hfx-build-$CAMPAIGN-data"
SERVER_TYPE=$(contract_value '.server_type')
LOCATION=$(contract_value '.location')
VOLUME_SIZE_GB=$(contract_value '.volume_size_gb')
CONTROL_ID=$(contract_value '.control_basin')
CONTROL_UNIT_COUNT=$(contract_value '.control_unit_count')
ABSENT_IDS=()
while IFS= read -r absent_id; do ABSENT_IDS+=("$absent_id"); done < <(contract_value '.absent_basins[]')
test "${#ABSENT_IDS[@]}" -eq "$(contract_value '.absent_basins | length')" && test "${#ABSENT_IDS[@]}" -ge 1
FABRIC_VERSION=$(contract_value '.fabric_version')
CONTROL_FABRIC_VERSION=$(contract_value '.control_fabric_version')
WORKSPACE_ROOT=/mnt/hfx/work
CAMPAIGN_DIR="/mnt/hfx/work/tdx-hydro-$CAMPAIGN"
CONTROL_ROOT=/mnt/hfx/work/control-builds
BASELINE_ROOT=/mnt/hfx/work/baseline
BASELINE_DATASET_PREFIX=$(contract_value '.baseline.prefix')
BASELINE_PREFIX=${BASELINE_DATASET_PREFIX%/dataset/}
test "$BASELINE_PREFIX/dataset/" = "$BASELINE_DATASET_PREFIX"
EXTENSION_SCRATCH_PREFIX=$(contract_value '.extension_scratch_prefix')
EXTENSION_PREFIX="s3://pourpoint-hfx/${EXTENSION_SCRATCH_PREFIX%/}"
S3_ENDPOINT=https://fsn1.your-objectstorage.com
BUDGET_CEILING_EUR=$(contract_value '.gross_cost_ceiling_eur')
ELAPSED_CEILING_HOURS=$(contract_value '.wall_clock_ceiling_hours')
[[ "$ELAPSED_CEILING_HOURS" =~ ^[0-9]+$ ]]
HARD_CEILING_SECONDS=$((ELAPSED_CEILING_HOURS * 3600))
BILLABLE_OUTBOUND_BYTES=$(contract_value '.billable_outbound_bytes')
CORPUS_FILE_COUNT=$(contract_value '.source_corpus.file_count')
CORPUS_TOTAL_BYTES=$(contract_value '.source_corpus.total_bytes')
CORPUS_DIR="$HFX_CAMPAIGN_EVIDENCE/off-vm/acquired-source"
CORPUS_MANIFEST="$HFX_CAMPAIGN_EVIDENCE/$(contract_value '.source_corpus.manifest')"
PRESERVED_CONTROL="$HFX_CAMPAIGN_EVIDENCE/off-vm/control-builds/planetary/$CONTROL_ID"
CONTROL_ADJUDICATION_SOURCE=$(contract_value '.control_adjudication_record')
CONTROL_REFERENCE=$(contract_value '.control_reference')
test "$CONTROL_REFERENCE" = preserved-off-vm || test "$CONTROL_REFERENCE" = vm-planetary-build
LOCAL_EVIDENCE_DIR="$HFX_CAMPAIGN_EVIDENCE/$CAMPAIGN"
if test -e "$LOCAL_EVIDENCE_DIR" && test "${HFX_CAMPAIGN_RESUME:-0}" != 1; then
  test -d "$LOCAL_EVIDENCE_DIR" && test ! -L "$LOCAL_EVIDENCE_DIR"
  mv -- "$LOCAL_EVIDENCE_DIR" "$LOCAL_EVIDENCE_DIR-superseded-$(date -u +%Y%m%dT%H%M%SZ)"
fi
if test "${HFX_CAMPAIGN_RESUME:-0}" = 1; then test -f "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"; fi
mkdir -p -- "$LOCAL_EVIDENCE_DIR"
chmod 700 "$LOCAL_EVIDENCE_DIR"
printf '%s\n' "$CAMPAIGN_CONTRACT_JSON" > "$LOCAL_EVIDENCE_DIR/campaign-contract.json"
rsync --version | sed -n 1p | grep -E '^rsync +version 3\.[1-9]'

printf '%s\n' 'Enter the secrets environment FILE PATH (contents must never be displayed):' >&2
IFS= read -r S3_ENV_FILE
test -n "$S3_ENV_FILE" && test -f "$S3_ENV_FILE" && test ! -L "$S3_ENV_FILE" && test -s "$S3_ENV_FILE"
```

`remote_tokens` quotes every argument a fence sends to the VM with `printf '%q'`. `ssh` joins its remote command arguments into one string that the remote login shell splits again, so an unquoted argument holding a space arrives as several positional parameters; on 2026-09-04 at 21:03:54Z the rehearsal's converge fence sent five sizing values as one space-joined argument, the VM saw five parameters, the remote `read` filled one variable, and the numeric guard ended the lifecycle before any output. Every `bash -s --` fence therefore builds a `remote_args` array, sends exactly `"$(remote_tokens "${remote_args[@]}")"`, and the remote script assigns and validates each positional it expects before doing anything else; section 20's composer refuses any other form.

Every campaign parameter comes from one campaign contract: the JSON block at the top of this runbook for the production campaign, or the file named by `HFX_CAMPAIGN_CONTRACT` for the rehearsal campaign of section 20. The shell copies the contract it used into `$LOCAL_EVIDENCE_DIR/campaign-contract.json`, so the evidence records which parameters ran. No fence carries a campaign-specific literal; the same fences run both campaigns.

The workstation is macOS. Its BSD `chmod` reads `--` after the mode as a file name, so the mode line above carries no `--`; the first 2026-09-04 launch died on that line before any cloud call. Its bundled `/usr/bin/rsync` is openrsync, which rejects the `--info=progress2` flag the section 9 transfer fence uses, so GNU rsync 3.1 or later must be first on `PATH` (Homebrew installs it as `/opt/homebrew/bin/rsync`); the version line above refuses otherwise. That version line reads the banner through `sed -n 1p`: rsync writes its banner in several writes, so `head -n 1` closed the pipe early, rsync died of SIGPIPE, and under `set -o pipefail` the line returned 141 and ended the first preflight attempt of the second 2026-09-04 lifecycle. The workstation-side digest commands use `shasum -a 256` and `stat -f`, which are the BSD forms; the `sha256sum` and `stat -c` forms appear only inside `ssh` heredocs that run on the Debian VM.

The shell sets `IFS` to newline and tab, so the absent basins are held in a Bash array and every loop iterates `"${ABSENT_IDS[@]}"`; a space-separated string would be one word under that `IFS`. The count check above proves the array holds exactly the contract's entries.

`HFX_CAMPAIGN_EVIDENCE` is the existing evidence root that holds the approval record, the preserved corpus, and the preserved planetary control output. New records go under `$LOCAL_EVIDENCE_DIR`, named by the campaign. A fresh start finds that directory already holding an earlier lifecycle's records (the consumed 2026-09-04 lifecycles left theirs there) and moves it aside by rename to `<name>-superseded-<UTC timestamp>` before it writes anything, so earlier records stay intact and a stale `provisioning-request-epoch.txt` cannot refuse the provisioning fence after the trap is armed. Only a resume (`HFX_CAMPAIGN_RESUME=1`, set by the composed resume driver) keeps the directory, and it then requires the recorded epoch to be present.

Verify the tracked contract, the operator inputs, and the preserved inputs before any cloud action:

```bash
for check in scope-permits-compilation ceilings-and-kill-switches control-hotpatch-is-pinned control-digests-are-pinned control-adjudication-is-pinned baseline-is-pinned authority-is-current rehearsal-record-is-pinned; do
  ./scripts/hetzner/verify-compile-runbook.sh --check "$check"
done
./scripts/hetzner/verify-compile-runbook.sh --evidence-root "$HFX_CAMPAIGN_EVIDENCE" --check approval-is-a-precondition
if test "$(contract_value '.requires_passing_rehearsal')" = true; then
  test -n "${HFX_REHEARSAL_EVIDENCE:-}"
  ./scripts/hetzner/verify-compile-runbook.sh --evidence-root "$HFX_REHEARSAL_EVIDENCE" --check rehearsal-passed
fi
if test "$CONTROL_REFERENCE" = preserved-off-vm; then
  cp -- "$CONTROL_ADJUDICATION_SOURCE" "$LOCAL_EVIDENCE_DIR/control-adjudication.json"
  jq -e --arg id "$CONTROL_ID" --argjson units "$CONTROL_UNIT_COUNT" '.processing_basin_id == $id and .unit_count == $units' "$LOCAL_EVIDENCE_DIR/control-adjudication.json"
fi
./scripts/hetzner/verify-campaign-inputs.sh --evidence-root "$LOCAL_EVIDENCE_DIR" --check evidence-root-writable
./scripts/hetzner/verify-campaign-inputs.sh --s3-env-file "$S3_ENV_FILE" --check credential-file-authenticates
./scripts/hetzner/verify-campaign-inputs.sh --check hcloud-context-resolves

test -f "$CORPUS_MANIFEST" && test "$(grep -c . "$CORPUS_MANIFEST")" -eq "$CORPUS_FILE_COUNT"
(cd "$CORPUS_DIR" && shasum -a 256 -c "$CORPUS_MANIFEST") | tee "$LOCAL_EVIDENCE_DIR/corpus-local-verification.txt"
test "$(grep -c ': OK$' "$LOCAL_EVIDENCE_DIR/corpus-local-verification.txt")" -eq "$CORPUS_FILE_COUNT"
test "$(find "$CORPUS_DIR" -type f -name '*.gpkg' -exec stat -f '%z' {} + | awk '{s+=$1} END {print s}')" -eq "$CORPUS_TOTAL_BYTES"

if test "$CONTROL_REFERENCE" = preserved-off-vm; then
  contract_value '.control_digests' > "$LOCAL_EVIDENCE_DIR/expected-control-sha256.json"
  (cd "$PRESERVED_CONTROL" && jq -r 'to_entries[] | "\(.value)  ./\(.key)"' "$LOCAL_EVIDENCE_DIR/expected-control-sha256.json" | shasum -a 256 -c) | tee "$LOCAL_EVIDENCE_DIR/preserved-control-verification.txt"
  test "$(grep -c ': OK$' "$LOCAL_EVIDENCE_DIR/preserved-control-verification.txt")" -eq "$(jq 'length' "$LOCAL_EVIDENCE_DIR/expected-control-sha256.json")"
else
  test ! -e "$PRESERVED_CONTROL"
  test "$(contract_value '.control_digests')" = DERIVED-ON-VM && test "$CONTROL_ADJUDICATION_SOURCE" = DERIVED-ON-VM
fi

contract_value '.baseline.basin_ids | sort | unique' | jq -c . > "$LOCAL_EVIDENCE_DIR/baseline-roster.json"
test "$(jq 'length' "$LOCAL_EVIDENCE_DIR/baseline-roster.json")" -eq "$(contract_value '.baseline.basin_count')"
test "$(jq -r 'join(",")' "$LOCAL_EVIDENCE_DIR/baseline-roster.json" | tr -d '\n' | shasum -a 256 | cut -c1-12)" = "$(contract_value '.baseline.roster_digest_prefix')"
jq -e --arg id "$CONTROL_ID" 'index($id) != null' "$LOCAL_EVIDENCE_DIR/baseline-roster.json"
jq -e --slurpfile roster "$LOCAL_EVIDENCE_DIR/baseline-roster.json" --argjson absent "$(contract_value '.absent_basins')" \
  '(($roster[0] + $absent) - keys) == [] and ($absent - ($absent - $roster[0])) == []' adapters/tdx-hydro/data/tdx_header_numbers.json
```

The production corpus is the preserved 16-GeoPackage set, 84,101,885,952 bytes, whose SHA-256 manifest was recorded on both sides of the earlier transfer; the contract pins the file count, the byte total, and the manifest name. Its local integrity proof takes roughly 10 to 30 minutes. A corpus that fails this proof is not transferred; section 9 names the reacquisition fallback.

The production run also requires `rehearsal-passed`: `HFX_REHEARSAL_EVIDENCE` names the rehearsal evidence root, and the verifier requires the `lifecycle-result.json` that section 16 writes there to carry `result` `passed` for `campaign-rehearsal`. The rehearsal contract sets `requires_passing_rehearsal` to `false`, so the rehearsal itself skips that check. The maintainer's third-lifecycle authority in the contract is conditional on that record.

The contract's `control_reference` says where the reference control comes from. `preserved-off-vm` (production) verifies the preserved planetary control and the tracked adjudication record here; `vm-planetary-build` (rehearsal) has no reference yet, requires the digests and record fields to read `DERIVED-ON-VM`, and lets section 10 build the reference and derive the record on the VM, so the byte-for-byte planetary gate never depends on float identity between the workstation and the server. The control adjudication record is the tracked file `scripts/hetzner/seven-basin-control-adjudication.json` for the production campaign and the record derived on the VM for the rehearsal. It pins the corrected control's orientation digest, the exact set of units whose outlet is allowed to differ from the planetary control, the maximum shift, and the decision. Section 10 consumes the copy made above. The verifier refuses a production record that is missing, untracked, malformed, internally inconsistent, or still carrying a placeholder.

The baseline roster is the contract's `baseline.basin_ids`, the 55 basins the frozen artifact was assembled from, pinned inline so no untracked mirror is needed. Its sorted comma-joined SHA-256 prefix must equal the partial-region suffix `4dbff0d6ec31` recorded in `CAMPAIGN-tdx-hydro-planetary.md`, every roster and absent basin must exist in the authoritative inventory, and no absent basin may sit in the roster; the verifier's `baseline-is-pinned` check additionally requires the production roster's complement in the inventory to be exactly the seven absent basins. Section 12 rechecks the roster against the pulled baseline on the VM.

## 5. Current-price preflight and gates

Immediately before provisioning, obtain current prices from the Hetzner pricing API and record the numeric inputs, the retrieval timestamp, and the arithmetic:

```bash
./scripts/hetzner/price-preflight.sh \
  --server-type "$SERVER_TYPE" --location "$LOCATION" --volume-size-gb "$VOLUME_SIZE_GB" \
  --hours "$ELAPSED_CEILING_HOURS" --billable-outbound-bytes "$BILLABLE_OUTBOUND_BYTES" \
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

The production contract's 400,000,000,000 billable outbound estimate covers one full off-VM copy of every produced output, both control outputs, the extended artifact, and the evidence tree, plus a second copy of the extended artifact to the extension scratch prefix. Included traffic on `ccx33` is far above that figure, so the overage term is expected to be zero; the helper still records it.

Install this gate after provisioning and run it before every workload dispatch and before every status poll. It refreshes prices, computes elapsed time from the recorded request epoch, computes a conservative actual spend as billed hours times the sum of hourly rates, and refuses on equality with any ceiling. A price-list retrieval that fails for a transport reason (helper exit 1) is retried twice with a fresh record path before it counts as a refusal; a refusal (helper exit 3) stops dispatch at once. Without that distinction one transient API failure among the hundreds of polls of a long campaign would have ended it:

```bash
campaign_gate() {
  test "$#" -eq 2
  local phase=$1 remaining_hours=$2 origin now elapsed record gate_attempt gate_status
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
  gate_status=1
  for gate_attempt in 1 2 3; do
    gate_status=0
    ./scripts/hetzner/price-preflight.sh \
      --server-type "$SERVER_TYPE" --location "$LOCATION" --volume-size-gb "$VOLUME_SIZE_GB" \
      --hours "$ELAPSED_CEILING_HOURS" --billable-outbound-bytes "$BILLABLE_OUTBOUND_BYTES" \
      --ceiling-eur "$BUDGET_CEILING_EUR" \
      --provisioning-request-epoch "$origin" \
      --out "$record" || gate_status=$?
    if test "$gate_status" -eq 0 || test "$gate_status" -eq 3; then break; fi
    printf 'phase=%s\nattempt=%s\ntransport_status=%s\n' "$phase" "$gate_attempt" "$gate_status" >> "$LOCAL_EVIDENCE_DIR/gate-transport-failures.log"
    sleep 30
    record="$LOCAL_EVIDENCE_DIR/gate-$phase-$(date -u +%Y%m%dT%H%M%SZ).json"
  done
  if test "$gate_status" -ne 0; then
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
git merge-base --is-ancestor 0ffa2d048ce5d748c0ab4c71fbe6f5862478107d "$GROUND_TRUTH_REF"
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

./scripts/hetzner/verify-campaign-inputs.sh --check hcloud-json-shape | tee "$LOCAL_EVIDENCE_DIR/preflight-hcloud-shape.txt"
hcloud --context pourpoint server describe pourpoint-web-1 -o json | jq --arg kind server -f scripts/hetzner/hcloud-identity.jq > "$LOCAL_EVIDENCE_DIR/preflight-shape-witness.json"
jq -e '.name == "pourpoint-web-1" and .location == "fsn1"' "$LOCAL_EVIDENCE_DIR/preflight-shape-witness.json"
```

The project quota is 8 dedicated cores, so one `ccx33` is the only dedicated server that fits. `pourpoint-web-1` is a shared `cx33` and consumes no dedicated quota; it must appear unchanged in every listing.

The shape check is the last preflight before the trap and the provisioning request. It describes `pourpoint-web-1`, the `ccx33` server type, and the `fsn1` location read-only and projects each through `scripts/hetzner/hcloud-identity.jq`, the same file the section 8 gate applies to the provisioned server and volume. The projection raises an error for any field that is absent, null, or of the wrong type, so a CLI whose JSON shape has moved refuses here with zero cloud mutation. The witness record is the proof that `.location.name` resolved on the installed CLI. No volume exists at this point, because the quota preflight above requires an empty volume listing; the volume projection reads the same `.location.name`, `.size`, and `.server` paths that `provision.sh` validates on the live volume description, and the tracked test proves it against the recorded attached-volume shape.

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
provision_status=1
for provision_attempt in 1 2 3; do
  if ./scripts/hetzner/provision.sh \
    --campaign "$CAMPAIGN" \
    --s3-env-file "$S3_ENV_FILE" \
    --server-type "$SERVER_TYPE" \
    --volume-size-gb "$VOLUME_SIZE_GB" \
    --location "$LOCATION" \
    2>&1 | tee "$LOCAL_EVIDENCE_DIR/provision.log"; then
    provision_status=0
    break
  fi
  cp -- "$LOCAL_EVIDENCE_DIR/provision.log" "$LOCAL_EVIDENCE_DIR/provision-attempt$provision_attempt.log"
  if grep -q -E 'REMOTE HOST IDENTIFICATION HAS CHANGED|Host key verification failed' "$LOCAL_EVIDENCE_DIR/provision.log"; then
    stale_ip=$(hcloud --context pourpoint server describe "$SERVER_NAME" -o json 2>/dev/null | jq -er '.public_net.ipv4.ip') || stale_ip=
    if test -n "$stale_ip"; then ssh-keygen -R "$stale_ip" >/dev/null 2>&1 || true; fi
  fi
  sleep 30
done
test "$provision_status" -eq 0
./scripts/hetzner/bootstrap.sh --campaign "$CAMPAIGN" \
  2>&1 | tee "$LOCAL_EVIDENCE_DIR/bootstrap.log"
remote_ip
hcloud --context pourpoint server describe "$SERVER_NAME" -o json | jq --arg kind server -f scripts/hetzner/hcloud-identity.jq > "$LOCAL_EVIDENCE_DIR/provisioned-server.json"
hcloud --context pourpoint volume describe "$VOLUME_NAME" -o json | jq --arg kind volume -f scripts/hetzner/hcloud-identity.jq > "$LOCAL_EVIDENCE_DIR/provisioned-volume.json"
jq -e --arg name "$SERVER_NAME" --arg type "$SERVER_TYPE" --arg location "$LOCATION" '.name == $name and .server_type == $type and .location == $location' "$LOCAL_EVIDENCE_DIR/provisioned-server.json"
jq -e --arg name "$VOLUME_NAME" --argjson size "$VOLUME_SIZE_GB" --arg location "$LOCATION" '.name == $name and .size == $size and .location == $location' "$LOCAL_EVIDENCE_DIR/provisioned-volume.json"
jq -e --slurpfile server "$LOCAL_EVIDENCE_DIR/provisioned-server.json" '.server == $server[0].id and $server[0].volumes == [.id]' "$LOCAL_EVIDENCE_DIR/provisioned-volume.json"
```

Both earlier compile lifecycles needed a second `provision.sh` run after SSH readiness timed out, and one hit a `known_hosts` mismatch on a reused address; the first attempt of the second 2026-09-04 lifecycle failed the same way and succeeded on its rerun. Rerunning `provision.sh` with identical arguments is the documented remedy; it reuses the exact existing resources by name and ID, so the fence makes up to three identical attempts, keeps every failed attempt's log, and removes only a stale `known_hosts` entry for the reused address between attempts. The two `describe` records above are the exact identities that teardown must later match. Both come from `scripts/hetzner/hcloud-identity.jq`, the projection section 6 proved against the installed CLI, so every path this gate reads has already resolved to a non-null value of the expected type before the provisioning request. The final line proves the volume is attached to this server and that the server carries this volume alone.

Converge two checkouts on the VM, build `hfx` from the corrected revision, apply the recorded ARG_MAX hotpatch to the planetary worktree, and enable swap. The hotpatch touches only the campaign runner, so the planetary adapter's bytes come from revision `43a98aff8c15a1a196f47b10217ad2f5553b6611` itself; the hotpatch is applied so the provenance matches the recorded control build exactly.

Require a clean detached worktree at planetary revision `43a98aff8c15a1a196f47b10217ad2f5553b6611`. Apply only the path-scoped diff from pinned hotpatch commit `bde61149d3fefc5e3f30435bf7ed3d0bb32a519c`. Refuse unless the pre-patch blob is `41d6df3f10030a481b2227a878837c7f23f3e658`. Refuse unless the post-patch blob is `d227920a7ac0ab98ffcc80aac2c72a5dfc9c2429`.

```bash
remote_args=("$GROUND_TRUTH_REF" "$(contract_value '.workload_sizing.root_disk_reserve_bytes')" "$(contract_value '.workload_sizing.root_swap_bytes_max')" "$(contract_value '.workload_sizing.volume_swap_bytes')" "$(contract_value '.workload_sizing.required_available_disk_bytes')" "$(contract_value '.workload_sizing.required_memory_bytes')")
ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "root@$SERVER_IP" \
  bash -s -- "$(remote_tokens "${remote_args[@]}")" <<'REMOTE' 2>&1 | tee "$LOCAL_EVIDENCE_DIR/converge.log"
set -Eeuo pipefail
set +x
ground_truth_ref=$1; root_disk_reserve_bytes=$2; root_swap_bytes_max=$3; volume_swap_bytes=$4; required_available_disk_bytes=$5; required_memory_bytes=$6
[[ "$ground_truth_ref" =~ ^[0-9a-f]{40}$ ]]
for sizing in "$root_disk_reserve_bytes" "$root_swap_bytes_max" "$volume_swap_bytes" "$required_available_disk_bytes" "$required_memory_bytes"; do [[ "$sizing" =~ ^[0-9]+$ ]]; done
test "$#" -eq 6
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
rsync --version | sed -n 1p
/opt/hfx-geo/bin/python -c 'import geopandas, pyarrow, shapely'

root_avail=$(df -B1 --output=avail / | tail -n 1 | tr -d ' ')
root_swap_bytes=$((root_avail - root_disk_reserve_bytes))
if test "$root_swap_bytes" -gt "$root_swap_bytes_max"; then root_swap_bytes=$root_swap_bytes_max; fi
test "$root_swap_bytes" -gt 0
fallocate -l "$root_swap_bytes" /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
mkdir -p /mnt/hfx/swap
fallocate -l "$volume_swap_bytes" /mnt/hfx/swap/swapfile && chmod 600 /mnt/hfx/swap/swapfile && mkswap /mnt/hfx/swap/swapfile && swapon /mnt/hfx/swap/swapfile
swapon --show --bytes
free -b
df -B1 --output=avail /mnt/hfx | tail -n 1 | tr -d ' ' > /root/observed-available-disk-bytes.txt
test "$(cat /root/observed-available-disk-bytes.txt)" -ge "$required_available_disk_bytes"
awk -v required="$required_memory_bytes" '/MemAvailable:/ {exit !($2 * 1024 >= required)}' /proc/meminfo
mkdir -p /mnt/hfx/work/sha256 /mnt/hfx/work/control-builds/preserved "$HOME/.ssh"
REMOTE
scp -o BatchMode=yes "root@$SERVER_IP:/root/hfx-planetary-provenance.txt" "$LOCAL_EVIDENCE_DIR/planetary-provenance.txt"
scp -o BatchMode=yes "root@$SERVER_IP:/root/observed-available-disk-bytes.txt" "$LOCAL_EVIDENCE_DIR/observed-available-disk-bytes.txt"
```

The remote `rsync --version` line reads the banner through `sed -n 1p` for the same reason as the workstation line in section 4: the remote rsync 3.2.7 wrote its banner in several writes, `head -n 1` closed the pipe, and the resulting SIGPIPE exit 141 under the remote `pipefail` ended the first converge of the second 2026-09-04 lifecycle at 12:59:17Z. The converge also creates `/mnt/hfx/work/control-builds/preserved`, the parent directory the section 10 transfer needs; on 2026-09-04 at 16:00:31Z the transfer ran before any command had created it, remote rsync failed with `mkdir ... No such file or directory`, and that lifecycle ended there.

Swap and capacity sizing come from the contract's `workload_sizing`. For production: the 55-basin strict validation needed about 265 GB resident and finished only on 287 GB of swap. A `ccx33` has 32 GB of memory and a 240 GB local disk, so the root swap file takes up to 200 GB after a 20 GB root reserve and the volume swap file 150 GB, about 350 GB in total. The volume then holds the corpus (84.1 GB), the baseline (114.1 GB), the extended artifact (about 130 GB), per-basin outputs (about 25 GB), control builds (about 9 GB), and 150 GB of swap, about 510 GB of 600 GB; the roughly 74 GB that remain cannot hold a second copy of the extended artifact, which is why section 14 streams the S3 read-back when the volume lacks room. Section 13 states the expected validation outcome.

## 9. Corpus transfer and remote integrity

Initialize the campaign with the eight selected basins so the runner owns the `downloads` directory, then transfer the corpus into it and prove integrity on the VM against the recorded manifest:

```bash
campaign_gate pre-init "$(contract_value '.gate_reserve_hours["pre-init"]')"
selected_basin_args=()
for selected_id in "${ABSENT_IDS[@]}" "$CONTROL_ID"; do selected_basin_args+=(--basin "$selected_id"); done
./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" start --workload tdx-init -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh init \
  --campaign "$CAMPAIGN" \
  --workspace-root "$WORKSPACE_ROOT" \
  "${selected_basin_args[@]}" \
  --retention-policy retain-all-through-publication \
  --available-memory-bytes "$(contract_value '.workload_sizing.available_memory_bytes')" \
  --available-disk-bytes "$(cat "$LOCAL_EVIDENCE_DIR/observed-available-disk-bytes.txt")" \
  --retained-input-bytes "$(contract_value '.workload_sizing.retained_input_bytes')" \
  --retained-basin-output-bytes "$(contract_value '.workload_sizing.retained_basin_output_bytes')" \
  --assembly-memory-ceiling-bytes "$(contract_value '.workload_sizing.assembly_memory_ceiling_bytes')" \
  --assembly-scratch-ceiling-bytes "$(contract_value '.workload_sizing.assembly_scratch_ceiling_bytes')" \
  --assembled-artifact-bytes "$(contract_value '.workload_sizing.assembled_artifact_bytes')" \
  --active-compile-scratch-bytes "$(contract_value '.workload_sizing.active_compile_scratch_bytes')" \
  --filesystem-overhead-bytes "$(contract_value '.workload_sizing.filesystem_overhead_bytes')"
./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" status --workload tdx-init || test "$?" -eq 3
```

Every gate's reserve is the contract's `gate_reserve_hours` entry for its phase, the hours the remaining stages need below the ceiling; the production values are 70 here and descend to 6 before preservation. The production sizing the runner records is 90 GB retained input, 30 GB retained basin outputs, 20 GB active compile scratch, 130 GB assembly scratch or artifact, and 20 GB filesystem overhead, 290 GB in total. The baseline (114.1 GB) and the volume swap (150 GB) are outside the runner's model and are covered by the 420 GB availability check in section 8.

```bash
transfer_start=$(date +%s)
rsync -a --partial --info=progress2 -e 'ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new' \
  "$CORPUS_DIR/" "root@$SERVER_IP:$CAMPAIGN_DIR/downloads/" > "$LOCAL_EVIDENCE_DIR/corpus-transfer.log" 2>&1 &
transfer_pid=$!
transfer_stopped=0
transfer_slept=0
while kill -0 "$transfer_pid" 2>/dev/null && test "$transfer_slept" -lt 1800; do sleep 30; transfer_slept=$((transfer_slept + 30)); done
if kill -0 "$transfer_pid" 2>/dev/null; then
  transferred=$(ssh -o BatchMode=yes "root@$SERVER_IP" "du -sb '$CAMPAIGN_DIR/downloads' | cut -f1" 2>/dev/null) || transferred=0
  transfer_now=$(date +%s)
  decision_point_epoch=$(( $(<"$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt") + $(contract_value '.decision_points_hours[0]') * 3600 ))
  projected_end=$(awk -v t="$transferred" -v total="$CORPUS_TOTAL_BYTES" -v start="$transfer_start" -v now="$transfer_now" \
    'BEGIN { rate = t / (now - start); if (rate <= 0) { print 9999999999 } else { printf "%.0f\n", now + (total - t) / rate } }')
  printf 'transferred_bytes_at_30min=%s\nprojected_completion_epoch=%s\ndecision_point_epoch=%s\n' \
    "$transferred" "$projected_end" "$decision_point_epoch" > "$LOCAL_EVIDENCE_DIR/corpus-transfer-projection.txt"
  if test "$projected_end" -gt "$decision_point_epoch"; then
    kill "$transfer_pid" 2>/dev/null || true
    transfer_stopped=1
  fi
fi
wait "$transfer_pid" || test "$transfer_stopped" -eq 1
printf 'transfer_stopped=%s\ntransfer_seconds=%s\n' "$transfer_stopped" "$(( $(date +%s) - transfer_start ))" >> "$LOCAL_EVIDENCE_DIR/corpus-transfer-projection.txt"
scp -o BatchMode=yes "$CORPUS_MANIFEST" "root@$SERVER_IP:/mnt/hfx/work/sha256/source-expected-sha256.txt"
ssh -o BatchMode=yes "root@$SERVER_IP" \
  "cd '$CAMPAIGN_DIR/downloads' && sha256sum -c /mnt/hfx/work/sha256/source-expected-sha256.txt" \
  | tee "$LOCAL_EVIDENCE_DIR/corpus-remote-verification.txt" || true
remote_ok_count=$(grep -c ': OK$' "$LOCAL_EVIDENCE_DIR/corpus-remote-verification.txt") || remote_ok_count=0
if test "$remote_ok_count" -ne "$CORPUS_FILE_COUNT"; then
  grep -v ': OK$' "$LOCAL_EVIDENCE_DIR/corpus-remote-verification.txt" | sed -n 's/^\.\/\([^:]*\): .*$/\1/p' > "$LOCAL_EVIDENCE_DIR/corpus-remote-failed-files.txt"
  while IFS= read -r failed_file; do
    test -n "$failed_file" || continue
    [[ "$failed_file" =~ ^[0-9]{10}-(basins|streamnet)\.gpkg$ ]]
    ssh -o BatchMode=yes "root@$SERVER_IP" "rm -f -- '$CAMPAIGN_DIR/downloads/$failed_file'"
  done < "$LOCAL_EVIDENCE_DIR/corpus-remote-failed-files.txt"
fi
```

The transfer runs in the background so the fence can measure throughput over the first 30 minutes. If the projected completion of the transfer exceeds the first decision point, the fence stops the transfer, keeps the completed files, and lets the bounded acquisition below fetch the remainder from NGA. A transferred file that fails the remote SHA-256 check is removed from the VM's `downloads` directory only, never from the preserved corpus, so the acquisition fetches it again. On 2026-09-04 the 84 GB transfer took 6,387 seconds and every file verified. Adoption and reacquisition use the same command; a present file that passes inspection is reused without any network request and is recorded with zero attempts, and an absent file is acquired with at most three attempts per product:

```bash
campaign_gate pre-acquire "$(contract_value '.gate_reserve_hours["pre-acquire"]')"
./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" start --workload tdx-acquire -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh acquire \
  --campaign "$CAMPAIGN" --workspace-root "$WORKSPACE_ROOT" \
  --max-parallel 2 --product-attempt-ceiling 3
./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" status --workload tdx-acquire || test "$?" -eq 3
```

Reacquisition from NGA is a fallback for an integrity failure or a stalled transfer. After the workload finishes, require every selected basin to have both products `succeeded`; an `exhausted` product excludes that basin from compilation and is recorded in section 15. The runner's adopted-product state defect of 2026-09-04 (a reused product recorded as `succeeded` with zero attempts was refused by its own validator and left `.current.json.tmp.*` files in every selected basin's state directory) is fixed on `main` by the runner change merged as `0ffa2d048ce5d748c0ab4c71fbe6f5862478107d`; section 6 requires that fix to be an ancestor of the checked ref.

## 10. Two control builds and the adjudicated comparison

Transfer the preserved planetary control output to the VM. It is the byte-for-byte reference for the planetary rebuild and the per-unit reference for the corrected build. The fence creates the destination's parent first: on 2026-09-04 at 16:00:31Z this transfer was the first command to name `/mnt/hfx/work/control-builds`, no earlier fence had created it, remote rsync refused with `mkdir ... failed: No such file or directory` (exit 11), and the second authorized lifecycle ended with nothing compiled. Reverify the control there and place the control adjudication record beside the expected digests. When the contract's `control_reference` is `vm-planetary-build`, the fence instead builds the reference on the VM with the planetary adapter worktree that section 8 converged, records its digests as `expected-control-sha256.json`, copies the reference tree off the VM as the campaign's preserved control, and writes the digests into the evidence copy of the contract; the planetary rebuild in the next fence then compares two builds of the same adapter on the same machine, so no workstation float result is ever the byte reference:

```bash
ssh -o BatchMode=yes "root@$SERVER_IP" mkdir -p "$CONTROL_ROOT/preserved"
if test "$CONTROL_REFERENCE" = preserved-off-vm; then
  rsync -a -e 'ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new' \
    "$PRESERVED_CONTROL/" "root@$SERVER_IP:$CONTROL_ROOT/preserved/$CONTROL_ID/"
  scp -o BatchMode=yes "$LOCAL_EVIDENCE_DIR/expected-control-sha256.json" "root@$SERVER_IP:/mnt/hfx/work/sha256/expected-control-sha256.json"
  scp -o BatchMode=yes "$LOCAL_EVIDENCE_DIR/control-adjudication.json" "root@$SERVER_IP:/mnt/hfx/work/sha256/control-adjudication.json"
else
  remote_args=("$CONTROL_ID" "$CAMPAIGN_DIR" "$CONTROL_ROOT" "$CONTROL_FABRIC_VERSION")
  ssh -o BatchMode=yes "root@$SERVER_IP" bash -s -- "$(remote_tokens "${remote_args[@]}")" <<'REMOTE' 2>&1 | tee "$LOCAL_EVIDENCE_DIR/control-reference-build.log"
set -Eeuo pipefail
set +x
control_id=$1; campaign_dir=$2; control_root=$3; control_fabric_version=$4
[[ "$control_id" =~ ^[0-9]{10}$ && "$campaign_dir" == /* && "$control_root" == /* && -n "$control_fabric_version" ]]
test "$#" -eq 4
test ! -e "$control_root/preserved/$control_id"
/opt/hfx-geo/bin/python /root/hfx-planetary/adapters/tdx-hydro/build_adapter.py build \
  --basins "$campaign_dir/downloads/$control_id-basins.gpkg" \
  --streamnet "$campaign_dir/downloads/$control_id-streamnet.gpkg" \
  --out "$control_root/preserved/$control_id" \
  --report "$control_root/preserved/$control_id-build-report.json" \
  --processing-basin-id "$control_id" --fabric-version "$control_fabric_version"
/opt/hfx-geo/bin/python /root/hfx/adapters/tdx-hydro/build_adapter.py validate "$control_root/preserved/$control_id" --hfx-binary /root/hfx/target/release/hfx
(cd "$control_root/preserved/$control_id" && find . -type f | sort | sed 's#^\./##' | while IFS= read -r file; do printf '%s %s\n' "$file" "$(sha256sum -- "$file" | cut -c1-64)"; done) \
  | jq -R -n '[inputs | split(" ") | {key: .[0], value: .[1]}] | from_entries' > /mnt/hfx/work/sha256/expected-control-sha256.json
REMOTE
  mkdir -p -- "$PRESERVED_CONTROL"
  rsync -a -e 'ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new' \
    "root@$SERVER_IP:$CONTROL_ROOT/preserved/$CONTROL_ID/" "$PRESERVED_CONTROL/"
  scp -o BatchMode=yes "root@$SERVER_IP:/mnt/hfx/work/sha256/expected-control-sha256.json" "$LOCAL_EVIDENCE_DIR/expected-control-sha256.json"
  (cd "$PRESERVED_CONTROL" && jq -r 'to_entries[] | "\(.value)  ./\(.key)"' "$LOCAL_EVIDENCE_DIR/expected-control-sha256.json" | shasum -a 256 -c) | tee "$LOCAL_EVIDENCE_DIR/preserved-control-verification.txt"
  test "$(grep -c ': OK$' "$LOCAL_EVIDENCE_DIR/preserved-control-verification.txt")" -eq "$(jq 'length' "$LOCAL_EVIDENCE_DIR/expected-control-sha256.json")"
  jq --slurpfile digests "$LOCAL_EVIDENCE_DIR/expected-control-sha256.json" '.control_digests = $digests[0]' "$LOCAL_EVIDENCE_DIR/campaign-contract.json" > "$LOCAL_EVIDENCE_DIR/campaign-contract.json.tmp"
  mv -- "$LOCAL_EVIDENCE_DIR/campaign-contract.json.tmp" "$LOCAL_EVIDENCE_DIR/campaign-contract.json"
fi
```

### Maintainer adjudication of 2026-09-04

On 2026-09-04 Nicolas Lazaro adjudicated the difference between the corrected control build and the preserved planetary control. TDX-Hydro reaches are digitized outlet first: on the preserved corpus every reach proven by endpoint coincidence has its first vertex at the outlet. Planetary revision `43a98aff8c15a1a196f47b10217ad2f5553b6611` trusted the final source vertex as the outlet of near-degenerate reaches and of isolated roots. The corrected adapter orients every connected reach from endpoint coincidence evidence and every isolated root from basin-wide polarity, and it refuses when that evidence is absent, contradictory, or tied. The maintainer accepted the consequence: the corrected control keeps every same-level graph edge, every polygon, and every non-outlet attribute identical to the planetary control, and the outlets of an enumerated set of units move by a bounded amount. That set, its size, the maximum shift, the orientation digest of the corrected build, and the decision text are pinned in the tracked record `scripts/hetzner/seven-basin-control-adjudication.json`. The frozen 55-basin fabric keeps the earlier final-vertex convention for near-degenerate reaches and isolated roots, and the extension states this at delivery under Effort #108.

The adjudicated comparison replaces byte identity as the gate for the corrected build. The planetary rebuild keeps the byte-for-byte gate, because its reproduction of the pinned digests proves the VM toolchain. Every other gate in this runbook stands unchanged.

### Orientation preflight, builds, and comparisons

Run the read-only `orient` preflight against the control's source inputs before either build. Its orientation digest must equal the pinned corrected digest; a different digest means the checked-out adapter is not the adjudicated one, and the campaign stops before spending build time. Then build the control with the corrected adapter, because its result decides whether any per-basin output can be trusted, and rebuild the control with the planetary adapter so the VM's reproduction of the recorded digests is on record. Each build writes to its own output root; no tree is overwritten. Pass the preserved manifest's `created_at` when the corrected adapter's `build` accepts `--created-at`; the manifest embeds that timestamp, and only a pinned value can make `manifest.json` byte-identical. The remote fence records which branch it took in `created-at-record.json` under the control root. When the flag was passed, the corrected build's `manifest.json` must be byte-identical to the preserved one. Only when the flag was unavailable may the manifest differ solely in `created_at`, and the record states that this tolerance applied. The planetary rebuild has no such flag at its revision, so its comparison always carries the allowance and its `manifest.json` is expected to differ only in `created_at`.

Both control builds pass the contract's `control_fabric_version` (`0.3.0` in production) because the preserved control was built with that value and the manifest embeds it; the per-basin compiles in section 11 use the contract's `fabric_version` (`NGA-TDX-Hydro-20230126` in production) because the baseline artifact carries that value and extension assembly requires the inputs to agree with it. The manifest also embeds `adapter_version`, which is `0.1.0` at both revisions today; a later bump would surface as a `manifest.json` difference and stops the work like any other difference.

```bash
campaign_gate pre-control-builds "$(contract_value '.gate_reserve_hours["pre-control-builds"]')"
remote_args=("$CONTROL_ID" "$CAMPAIGN_DIR" "$CONTROL_ROOT" "$CONTROL_FABRIC_VERSION" "$CONTROL_REFERENCE")
ssh -o BatchMode=yes "root@$SERVER_IP" bash -s -- "$(remote_tokens "${remote_args[@]}")" <<'REMOTE' 2>&1 | tee "$LOCAL_EVIDENCE_DIR/control-builds.log"
set -Eeuo pipefail
set +x
control_id=$1; campaign_dir=$2; control_root=$3; control_fabric_version=$4; control_reference=$5
[[ "$control_id" =~ ^[0-9]{10}$ && "$campaign_dir" == /* && "$control_root" == /* && -n "$control_fabric_version" ]]
[[ "$control_reference" == preserved-off-vm || "$control_reference" == vm-planetary-build ]]
test "$#" -eq 5
python=/opt/hfx-geo/bin/python
hfx=/root/hfx/target/release/hfx
adjudication=/mnt/hfx/work/sha256/control-adjudication.json
planetary_ref=43a98aff8c15a1a196f47b10217ad2f5553b6611
preserved_created_at=$(jq -r '.created_at' "$control_root/preserved/$control_id/manifest.json")
(cd "$control_root/preserved/$control_id" && jq -r 'to_entries[] | "\(.value)  ./\(.key)"' /mnt/hfx/work/sha256/expected-control-sha256.json | sha256sum -c)
if [ "$control_reference" = preserved-off-vm ]; then jq -e --arg id "$control_id" '.processing_basin_id == $id' "$adjudication" >/dev/null; else test ! -e "$adjudication"; fi

mkdir -p "$control_root/corrected" "$control_root/planetary"
"$python" /root/hfx/adapters/tdx-hydro/build_adapter.py orient \
  --basins "$campaign_dir/downloads/$control_id-basins.gpkg" \
  --streamnet "$campaign_dir/downloads/$control_id-streamnet.gpkg" \
  --processing-basin-id "$control_id" \
  --report "$control_root/corrected/$control_id-orient.json" >/dev/null
test "$(jq -r '.orientation_digest' "$control_root/corrected/$control_id-orient.json")" = "$(jq -r '.corrected_orientation_digest' "$adjudication")"

created_at_args=()
created_at_flag_used=false
if "$python" /root/hfx/adapters/tdx-hydro/build_adapter.py build --help 2>/dev/null | grep -c -- '--created-at' >/dev/null; then
  created_at_args=(--created-at "$preserved_created_at")
  created_at_flag_used=true
fi
jq -n --argjson used "$created_at_flag_used" --arg value "$preserved_created_at" \
  '{schema_version: 1, corrected_build_created_at_flag_used: $used, preserved_created_at: $value,
    manifest_rule: (if $used then "manifest.json must be byte-identical" else "manifest.json may differ only in created_at" end)}' \
  > "$control_root/created-at-record.json"
"$python" /root/hfx/adapters/tdx-hydro/build_adapter.py build \
  --basins "$campaign_dir/downloads/$control_id-basins.gpkg" \
  --streamnet "$campaign_dir/downloads/$control_id-streamnet.gpkg" \
  --out "$control_root/corrected/$control_id" \
  --report "$control_root/corrected/$control_id-build-report.json" \
  --processing-basin-id "$control_id" --fabric-version "$control_fabric_version" \
  ${created_at_args[@]+"${created_at_args[@]}"}
"$python" /root/hfx/adapters/tdx-hydro/build_adapter.py validate "$control_root/corrected/$control_id" --hfx-binary "$hfx"

"$python" /root/hfx-planetary/adapters/tdx-hydro/build_adapter.py build \
  --basins "$campaign_dir/downloads/$control_id-basins.gpkg" \
  --streamnet "$campaign_dir/downloads/$control_id-streamnet.gpkg" \
  --out "$control_root/planetary/$control_id" \
  --report "$control_root/planetary/$control_id-build-report.json" \
  --processing-basin-id "$control_id" --fabric-version "$control_fabric_version"
"$python" /root/hfx/adapters/tdx-hydro/build_adapter.py validate "$control_root/planetary/$control_id" --hfx-binary "$hfx"

corrected_allowance=()
if [ "$created_at_flag_used" = false ]; then
  corrected_allowance=(--allow-created-at-difference)
fi
/root/hfx/scripts/hetzner/compare-dataset-trees.sh \
  --left "$control_root/preserved/$control_id" --right "$control_root/corrected/$control_id" \
  --expected-sha256 /mnt/hfx/work/sha256/expected-control-sha256.json \
  ${corrected_allowance[@]+"${corrected_allowance[@]}"} > "$control_root/compare-corrected.json" || true
/root/hfx/scripts/hetzner/compare-dataset-trees.sh \
  --left "$control_root/preserved/$control_id" --right "$control_root/planetary/$control_id" \
  --expected-sha256 /mnt/hfx/work/sha256/expected-control-sha256.json \
  --allow-created-at-difference > "$control_root/compare-planetary.json"
/root/hfx/scripts/hetzner/compare-dataset-trees.sh \
  --left "$control_root/corrected/$control_id" --right "$control_root/planetary/$control_id" \
  --allow-created-at-difference > "$control_root/compare-corrected-planetary.json" || true
if [ "$control_reference" = vm-planetary-build ]; then
  "$python" /root/hfx/adapters/tdx-hydro/compare_unit_outlets.py \
    --reference "$control_root/preserved/$control_id" \
    --candidate "$control_root/corrected/$control_id" \
    --derive-expected "$adjudication" --planetary-revision "$planetary_ref" --processing-basin-id "$control_id" \
    --orient-report "$control_root/corrected/$control_id-orient.json" \
    --report "$control_root/derive-adjudicated-outlets.json" >/dev/null
fi
"$python" /root/hfx/adapters/tdx-hydro/compare_unit_outlets.py \
  --reference "$control_root/preserved/$control_id" \
  --candidate "$control_root/corrected/$control_id" \
  --expected "$adjudication" \
  --orient-report "$control_root/corrected/$control_id-orient.json" \
  --report "$control_root/compare-adjudicated-outlets.json" >/dev/null
jq -r '.verdict' "$control_root/compare-corrected.json" "$control_root/compare-planetary.json" "$control_root/compare-corrected-planetary.json" "$control_root/compare-adjudicated-outlets.json"
REMOTE
scp -o BatchMode=yes "root@$SERVER_IP:$CONTROL_ROOT/compare-*.json" "root@$SERVER_IP:$CONTROL_ROOT/created-at-record.json" "root@$SERVER_IP:$CONTROL_ROOT/corrected/$CONTROL_ID-orient.json" "$LOCAL_EVIDENCE_DIR/"
if test "$CONTROL_REFERENCE" = vm-planetary-build; then
  scp -o BatchMode=yes "root@$SERVER_IP:/mnt/hfx/work/sha256/control-adjudication.json" "$LOCAL_EVIDENCE_DIR/control-adjudication.json"
  jq -e --arg id "$CONTROL_ID" --argjson units "$CONTROL_UNIT_COUNT" '.processing_basin_id == $id and .unit_count == $units' "$LOCAL_EVIDENCE_DIR/control-adjudication.json"
fi
jq -e '.corrected_build_created_at_flag_used | type == "boolean"' "$LOCAL_EVIDENCE_DIR/created-at-record.json"
jq -e '.verdict == "identical" or (.verdict == "created-at-only" and (.files | map(select(.path != "manifest.json")) | all(.verdict == "identical")))' "$LOCAL_EVIDENCE_DIR/compare-planetary.json"
jq -e '.left_matches_expected_sha256 == true' "$LOCAL_EVIDENCE_DIR/compare-corrected.json"
if jq -e '.corrected_build_created_at_flag_used' "$LOCAL_EVIDENCE_DIR/created-at-record.json" >/dev/null; then
  jq -e '.files[] | select(.path == "manifest.json") | .verdict == "identical"' "$LOCAL_EVIDENCE_DIR/compare-corrected.json"
else
  jq -e '.files[] | select(.path == "manifest.json") | .verdict == "identical" or .verdict == "created-at-only"' "$LOCAL_EVIDENCE_DIR/compare-corrected.json"
fi
jq -e '(.files | map(.path)) == ["aux/snap_stems.parquet","catchments.parquet","graph.parquet","manifest.json"] and all(.files[]; .verdict != "missing")' "$LOCAL_EVIDENCE_DIR/compare-corrected.json"
jq -e --slurpfile expected "$LOCAL_EVIDENCE_DIR/control-adjudication.json" '
  .verdict == "accepted"
  and (.refusals | length == 0)
  and .unit_count == $expected[0].unit_count
  and .downstream_differences == $expected[0].downstream_differences
  and .outlet_differences == $expected[0].outlet_differences
  and .outlet_differences_outside_adjudicated_set == 0
  and .adjudicated_units_without_difference == 0
  and .snap_stem_differences_outside_adjudicated_set == 0
  and .catchment_geometry_differences == 0
  and (.catchment_attribute_differences | all(.[]; . == 0))
  and (.graph_differences | all(.[]; . == 0))
  and .max_shift_deg <= $expected[0].max_shift_deg
  and .reference_orientation_digest == $expected[0].planetary_orientation_digest
  and .candidate_orientation_digest == $expected[0].corrected_orientation_digest
  and .orient_report_digest == $expected[0].corrected_orientation_digest' "$LOCAL_EVIDENCE_DIR/compare-adjudicated-outlets.json"
test "$(jq -r '.orientation_digest' "$LOCAL_EVIDENCE_DIR/$CONTROL_ID-orient.json")" = "$(jq -r '.corrected_orientation_digest' "$LOCAL_EVIDENCE_DIR/control-adjudication.json")"
```

The planetary rebuild must reproduce the preserved digests: its three data files must equal the pinned digests exactly and its `manifest.json` may differ only in `created_at`. That reproduction proves the VM toolchain against the recorded build.

Under `vm-planetary-build` the fence first derives the adjudication record on the VM from the two builds (`compare_unit_outlets.py --derive-expected`), then runs the same pinned comparison against that record and copies it off the VM; the record is therefore accepted by construction on the outlet set, while every polygon, attribute, graph, manifest, and unit-set difference still refuses. The corrected control build is accepted only when the adjudicated comparison reports `accepted`. The comparison reads every row of `catchments.parquet`, `graph.parquet`, and `aux/snap_stems.parquet` of both builds. Every same-level graph edge, every polygon, and every non-outlet attribute must be identical between the two builds. The set of units whose outlet differs must equal exactly the adjudicated set pinned in the tracked record. Every outlet shift must lie within the pinned maximum. Snap stems may differ only for units inside that set; the report counts them. The orientation digest recomputed from the preserved control's graph and outlet columns must equal the pinned planetary digest, the digest recomputed from the corrected build must equal the pinned corrected digest, and the VM `orient` report must carry that same digest. The corrected build's `manifest.json` follows the `created-at-record.json` rule above, and its `catchments.parquet` is expected to differ in bytes because outlet columns moved; the byte verdicts of `graph.parquet` and `aux/snap_stems.parquet` are recorded in `compare-corrected.json` for the campaign record. Any other difference stops the work for adjudication. Do not explain a difference away, do not rerun with different arguments, and do not continue to per-basin compilation. Preserve both trees, both build reports, the orient report, and the four comparison records, then go to section 14 and section 16.

A corrected build that refuses, or an `orient` preflight whose digest differs from the pinned one, is the same stop: the refusal, its report if any, and the log are preserved as the campaign's terminal evidence, and no per-basin compile starts.

## 11. Per-basin compile with the corrected adapter

Compile every selected basin through the campaign runner. The fabric version must equal the baseline's `NGA-TDX-Hydro-20230126` so the extension assembly accepts the new inputs. The control compiles here too, because extension assembly requires the excluded control's campaign compile to have succeeded.

```bash
campaign_gate pre-compile "$(contract_value '.gate_reserve_hours["pre-compile"]')"
./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" start --workload tdx-compile -- \
  /root/hfx/scripts/hetzner/tdx-hydro-campaign.sh compile \
  --campaign "$CAMPAIGN" --workspace-root "$WORKSPACE_ROOT" \
  --fabric-version "$FABRIC_VERSION"
```

Poll with the gate before every status check and repeat the pair until status exits 3. The reserve (20 hours in production) covers assembly, preservation, and teardown. When status reports the session finished, require the canonical log to end with the runner finish record carrying exit `0`; any other exit sets `compile_exit_zero` to `0`, is preserved as evidence, and the campaign continues at section 14 without assembly:

```bash
campaign_gate compile-monitor "$(contract_value '.gate_reserve_hours["compile-monitor"]')"
./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" status --workload tdx-compile || test "$?" -eq 3
```

```bash
ssh -o BatchMode=yes "root@$SERVER_IP" tail -n 1 "/mnt/hfx/logs/hfx-$CAMPAIGN-tdx-compile.log" \
  | tee "$LOCAL_EVIDENCE_DIR/compile-finish-record.txt"
compile_exit_zero=1
grep -E -- '^launch: finished at [0-9T:Z-]+ with exit 0$' "$LOCAL_EVIDENCE_DIR/compile-finish-record.txt" || compile_exit_zero=0
```

Every refusal is terminal evidence. The runner records `failed` with the adapter's message in `state/basins/<id>/current.json`, keeps any build report, and the workload log holds the traceback. Nothing is guessed around and nothing is retried with different inputs. Preserve the per-basin results before assembly:

```bash
remote_args=("$CAMPAIGN_DIR")
ssh -o BatchMode=yes "root@$SERVER_IP" bash -s -- "$(remote_tokens "${remote_args[@]}")" <<'REMOTE'
set -Eeuo pipefail
campaign_dir=$1
[[ "$campaign_dir" == /* ]]
test "$#" -eq 1
cd "$campaign_dir"
find basin-outputs -type f -print0 | sort -z | xargs -0 -r sha256sum > /mnt/hfx/work/sha256/basin-outputs-sha256.txt
REMOTE
mkdir -p "$LOCAL_EVIDENCE_DIR/off-vm/campaign"
copy_remote_root "$CAMPAIGN_DIR/basin-outputs" "$LOCAL_EVIDENCE_DIR/off-vm/campaign"
copy_remote_root "$CAMPAIGN_DIR/reports" "$LOCAL_EVIDENCE_DIR/off-vm/campaign"
copy_remote_root "$CAMPAIGN_DIR/state" "$LOCAL_EVIDENCE_DIR/off-vm/campaign"
copy_remote_root /mnt/hfx/logs "$LOCAL_EVIDENCE_DIR/off-vm"
copy_remote_root /mnt/hfx/work/sha256 "$LOCAL_EVIDENCE_DIR/off-vm"
if test -s "$LOCAL_EVIDENCE_DIR/off-vm/sha256/basin-outputs-sha256.txt"; then
  (cd "$LOCAL_EVIDENCE_DIR/off-vm/campaign" && shasum -a 256 -c "$LOCAL_EVIDENCE_DIR/off-vm/sha256/basin-outputs-sha256.txt") | tee "$LOCAL_EVIDENCE_DIR/basin-outputs-verification.txt"
  ! grep -v ': OK$' "$LOCAL_EVIDENCE_DIR/basin-outputs-verification.txt"
else
  printf 'no basin output was produced; nothing to verify\n' | tee "$LOCAL_EVIDENCE_DIR/basin-outputs-verification.txt"
fi
compiled_absent=$(for id in "${ABSENT_IDS[@]}"; do jq -r --arg id "$id" 'select(.stages.compile.status == "succeeded") | $id' "$LOCAL_EVIDENCE_DIR/off-vm/campaign/state/basins/$id/current.json"; done)
printf '%s\n' "$compiled_absent" | sed '/^$/d' > "$LOCAL_EVIDENCE_DIR/compiled-absent-basins.txt"
test "$(for id in "${ABSENT_IDS[@]}"; do test -f "$LOCAL_EVIDENCE_DIR/off-vm/campaign/state/basins/$id/current.json" && printf '%s\n' "$id"; done | wc -l | tr -d ' ')" -eq "${#ABSENT_IDS[@]}"
```

If `compiled-absent-basins.txt` is empty or `compile_exit_zero` is `0`, no extension is attempted. The strict-validated baseline artifact stays the final fabric, section 15 records the disposition of every basin, and the campaign continues at section 14.

## 12. Baseline pull, roster verification, and extension assembly

Section 12 runs only when `compile_exit_zero` is `1` and `compiled-absent-basins.txt` is nonempty; the composed driver of section 20 wraps sections 12 and 13 in that condition. Pull the frozen artifact from the baseline prefix onto the volume. The prefix is read from S3 only; nothing under it is written, overwritten, or deleted. Verify the pulled manifest against the contract before it is used:

```bash
campaign_gate pre-baseline "$(contract_value '.gate_reserve_hours["pre-baseline"]')"
remote_args=("$BASELINE_ROOT" "$BASELINE_PREFIX" "$S3_ENDPOINT" "$(contract_value '.baseline.region')" "$(contract_value '.baseline.unit_count')" "$(contract_value '.baseline.exported_bytes')" "$(contract_value '.baseline.object_count')" "$FABRIC_VERSION")
ssh -o BatchMode=yes "root@$SERVER_IP" bash -s -- "$(remote_tokens "${remote_args[@]}")" <<'REMOTE' 2>&1 | tee "$LOCAL_EVIDENCE_DIR/baseline-pull.log"
set -Eeuo pipefail
set +x
baseline_root=$1; prefix=$2; endpoint=$3; baseline_region=$4; baseline_unit_count=$5; baseline_exported_bytes=$6; baseline_object_count=$7; fabric_version=$8
[[ "$baseline_root" == /* && "$prefix" == s3://* && "$endpoint" == https://* && -n "$baseline_region" && -n "$fabric_version" ]]
for count in "$baseline_unit_count" "$baseline_exported_bytes" "$baseline_object_count"; do [[ "$count" =~ ^[0-9]+$ ]]; done
test "$#" -eq 8
set -a; source /etc/pourpoint-hfx.env; set +a
mkdir -p "$baseline_root"
aws s3 ls "$prefix/dataset/" --recursive --endpoint-url "$endpoint" --region fsn1 > "$baseline_root/remote-listing.txt"
test "$(grep -c . "$baseline_root/remote-listing.txt")" -eq "$baseline_object_count"
aws s3 cp "$prefix/dataset/" "$baseline_root/dataset/" --recursive --endpoint-url "$endpoint" --region fsn1 --only-show-errors
aws s3 cp "$prefix/evidence/state/assembly.json" "$baseline_root/evidence-assembly.json" --endpoint-url "$endpoint" --region fsn1 --only-show-errors || true
jq -e --arg region "$baseline_region" --argjson units "$baseline_unit_count" --arg fabric "$fabric_version" '.region == $region and .unit_count == $units and .format_version == "0.3.0" and .fabric_version == $fabric and .fabric_name == "tdx_hydro"' "$baseline_root/dataset/manifest.json"
test "$(find "$baseline_root/dataset" -type f -exec stat -c '%s' {} + | awk '{s+=$1} END {print s}')" -eq "$baseline_exported_bytes"
(cd "$baseline_root/dataset" && find . -type f -print0 | sort -z | xargs -0 -r sha256sum) > /mnt/hfx/work/sha256/baseline-sha256.txt
REMOTE
scp -o BatchMode=yes "$LOCAL_EVIDENCE_DIR/baseline-roster.json" "root@$SERVER_IP:$BASELINE_ROOT/roster.json"
remote_args=("$BASELINE_ROOT" "$(contract_value '.baseline.roster_digest_prefix')")
ssh -o BatchMode=yes "root@$SERVER_IP" bash -s -- "$(remote_tokens "${remote_args[@]}")" <<'REMOTE'
set -Eeuo pipefail
baseline_root=$1; roster_digest_prefix=$2
[[ "$baseline_root" == /* && "$roster_digest_prefix" =~ ^[0-9a-f]{12}$ ]]
test "$#" -eq 2
test "$(jq -r 'join(",")' "$baseline_root/roster.json" | tr -d '\n' | sha256sum | cut -c1-12)" = "$roster_digest_prefix"
if test -s "$baseline_root/evidence-assembly.json"; then
  test "$(jq -c '.input_basin_ids | sort | unique' "$baseline_root/evidence-assembly.json")" = "$(jq -c '.' "$baseline_root/roster.json")"
fi
REMOTE
```

The roster is the baseline's assembly input list, 55 basins in production. Its digest prefix must equal the partial-region suffix of the pulled manifest. When the baseline's own evidence prefix holds the campaign assembly state, the two rosters must agree.

Assemble the extension. The runner passes the fabric once, omits the excluded control output, adds only successful selected basins absent from the roster, refuses a compiled basin already present in the roster, and requires at least one new basin. It derives the new roster, region digest, bbox, and unit count from the actual inputs. It then runs the adapter's strict validation on the assembled artifact inside the same workload.

```bash
test -s "$LOCAL_EVIDENCE_DIR/compiled-absent-basins.txt"
campaign_gate pre-assemble "$(contract_value '.gate_reserve_hours["pre-assemble"]')"
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
  campaign_gate assemble-monitor "$(contract_value '.gate_reserve_hours["assemble-monitor"]')"
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
| status `failed`, reason `adapter assembly failed and left an artifact; retained for inspection` | assembly `failed`; the partial tree under `assembly/dataset` is preserved as a diagnostic and is no artifact |

An interrupted or OOM-killed validation is recorded as `incomplete`, never as passing. Only a completed pass is called strict-validated. Capture the evidence when the workload ends:

```bash
./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" status --workload tdx-assemble || test "$?" -eq 3
remote_args=("$CAMPAIGN_DIR" "$CAMPAIGN")
ssh -o BatchMode=yes "root@$SERVER_IP" bash -s -- "$(remote_tokens "${remote_args[@]}")" <<'REMOTE' > "$LOCAL_EVIDENCE_DIR/validation-evidence.txt"
set -Eeuo pipefail
campaign_dir=$1; campaign=$2
[[ "$campaign_dir" == /* && "$campaign" =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]]
test "$#" -eq 2
cat "$campaign_dir/state/assembly.json"
dmesg -T 2>/dev/null | grep -i -E 'out of memory|killed process' || true
grep -E 'return code|hfx: error|validation' "/mnt/hfx/logs/hfx-$campaign-tdx-assemble.log" | tail -n 40 || true
REMOTE
```

The extended artifact and the complete validation record are preserved whatever the outcome. The strict-validated 55-basin artifact remains the known-good baseline and the final artifact when validation of the extension did not pass.

## 14. Preservation before teardown

Preserve in this order, each root with a SHA-256 manifest computed on the VM and recomputed after transfer. A copy counts only when relative paths and digests match on both sides. The first pass runs while strict validation may still be writing the canonical log and `state/assembly.json`, so the fence repeats the digest, copy, and verification up to three times until every root verifies unchanged; a root with no files (no basin output at all) verifies as empty instead of aborting, because an empty outcome is a truthful outcome. The order follows value per byte so an interruption loses the least:

1. campaign `state`, `reports`, and `/mnt/hfx/logs`
2. both control builds, their reports, the orient report, and the four comparison records
3. every per-basin output
4. the extended artifact, first to the extension scratch prefix, then to the operator
5. the VM-side digest manifests

```bash
campaign_gate pre-preservation "$(contract_value '.gate_reserve_hours["pre-preservation"]')"
preservation_status=1
for preservation_attempt in 1 2 3; do
  remote_args=("$CAMPAIGN_DIR" "$CONTROL_ROOT")
  ssh -o BatchMode=yes "root@$SERVER_IP" bash -s -- "$(remote_tokens "${remote_args[@]}")" <<'REMOTE'
set -Eeuo pipefail
campaign_dir=$1; control_root=$2
[[ "$campaign_dir" == /* && "$control_root" == /* ]]
test "$#" -eq 2
cd "$campaign_dir"
for root in state reports basin-outputs assembly/dataset; do
  test -d "$root" || continue
  find "$root" -type f -print0 | sort -z | xargs -0 -r sha256sum > "/mnt/hfx/work/sha256/campaign-$(printf '%s' "$root" | tr / -)-sha256.txt"
done
(cd /mnt/hfx && find logs -type f -print0 | sort -z | xargs -0 -r sha256sum) > /mnt/hfx/work/sha256/logs-sha256.txt
(cd "$control_root" && find . -type f -print0 | sort -z | xargs -0 -r sha256sum) > /mnt/hfx/work/sha256/control-builds-sha256.txt
REMOTE
  for root in state reports basin-outputs; do copy_remote_root "$CAMPAIGN_DIR/$root" "$LOCAL_EVIDENCE_DIR/off-vm/campaign"; done
  copy_remote_root /mnt/hfx/logs "$LOCAL_EVIDENCE_DIR/off-vm"
  copy_remote_root "$CONTROL_ROOT" "$LOCAL_EVIDENCE_DIR/off-vm"
  copy_remote_root /mnt/hfx/work/sha256 "$LOCAL_EVIDENCE_DIR/off-vm"
  preservation_status=0
  for manifest in campaign-state campaign-reports campaign-basin-outputs; do
    if test -s "$LOCAL_EVIDENCE_DIR/off-vm/sha256/$manifest-sha256.txt"; then
      (cd "$LOCAL_EVIDENCE_DIR/off-vm/campaign" && shasum -a 256 -c "$LOCAL_EVIDENCE_DIR/off-vm/sha256/$manifest-sha256.txt") > "$LOCAL_EVIDENCE_DIR/verify-$manifest.txt" || preservation_status=1
      grep -v ': OK$' "$LOCAL_EVIDENCE_DIR/verify-$manifest.txt" >/dev/null && preservation_status=1
    else
      printf 'no files under this root; nothing to verify\n' > "$LOCAL_EVIDENCE_DIR/verify-$manifest.txt"
    fi
  done
  (cd "$LOCAL_EVIDENCE_DIR/off-vm" && shasum -a 256 -c "$LOCAL_EVIDENCE_DIR/off-vm/sha256/logs-sha256.txt") > "$LOCAL_EVIDENCE_DIR/verify-logs.txt" || preservation_status=1
  grep -v ': OK$' "$LOCAL_EVIDENCE_DIR/verify-logs.txt" >/dev/null && preservation_status=1
  (cd "$LOCAL_EVIDENCE_DIR/off-vm/control-builds" && shasum -a 256 -c "$LOCAL_EVIDENCE_DIR/off-vm/sha256/control-builds-sha256.txt") > "$LOCAL_EVIDENCE_DIR/verify-control-builds.txt" || preservation_status=1
  grep -v ': OK$' "$LOCAL_EVIDENCE_DIR/verify-control-builds.txt" >/dev/null && preservation_status=1
  if test "$preservation_status" -eq 0; then break; fi
  printf 'preservation attempt %s found a root that changed between digest and copy; repeating\n' "$preservation_attempt" >> "$LOCAL_EVIDENCE_DIR/preservation-retries.log"
  sleep 30
done
test "$preservation_status" -eq 0
```

Copy the extended artifact, when it exists, to a new prefix under the campaign's scratch namespace and prove the S3 copy by listing sizes and by reading every object back and recomputing its digest. When the volume has room for a second copy of the artifact plus 10 GB, the read-back lands in a scratch directory on the VM exactly as before; otherwise each object is streamed from S3 through `sha256sum` without touching the disk, and the record line `readback_mode=streamed` in `extension-s3-preservation.log` says so. The production volume cannot hold that second copy (section 8), so the streamed mode is the expected production path. The prefix is named by content. The baseline prefix is never written.

```bash
if ssh -o BatchMode=yes "root@$SERVER_IP" test -d "$CAMPAIGN_DIR/assembly/dataset"; then
  remote_args=("$CAMPAIGN_DIR" "$EXTENSION_PREFIX" "$S3_ENDPOINT" "$CONTROL_ROOT")
  ssh -o BatchMode=yes "root@$SERVER_IP" bash -s -- "$(remote_tokens "${remote_args[@]}")" <<'REMOTE' 2>&1 | tee "$LOCAL_EVIDENCE_DIR/extension-s3-preservation.log"
set -Eeuo pipefail
set +x
campaign_dir=$1; prefix=$2; endpoint=$3; control_root=$4
[[ "$campaign_dir" == /* && "$prefix" == s3://* && "$endpoint" == https://* && "$control_root" == /* ]]
test "$#" -eq 4
set -a; source /etc/pourpoint-hfx.env; set +a
test -z "$(aws s3 ls "$prefix/extension-hfx-v0-3-0/" --endpoint-url "$endpoint" --region fsn1)"
aws s3 cp "$campaign_dir/assembly/dataset/" "$prefix/extension-hfx-v0-3-0/dataset/" --recursive --endpoint-url "$endpoint" --region fsn1 --only-show-errors
aws s3 cp /mnt/hfx/work/sha256/campaign-assembly-dataset-sha256.txt "$prefix/extension-hfx-v0-3-0/dataset-sha256.txt" --endpoint-url "$endpoint" --region fsn1 --only-show-errors
aws s3 cp "$campaign_dir/state/assembly.json" "$prefix/extension-hfx-v0-3-0/evidence/state/assembly.json" --endpoint-url "$endpoint" --region fsn1 --only-show-errors
aws s3 cp "$control_root/" "$prefix/control-builds/" --recursive --endpoint-url "$endpoint" --region fsn1 --only-show-errors
aws s3 cp "$campaign_dir/basin-outputs/" "$prefix/basin-outputs/" --recursive --endpoint-url "$endpoint" --region fsn1 --only-show-errors
aws s3 ls "$prefix/" --recursive --endpoint-url "$endpoint" --region fsn1 > /mnt/hfx/work/sha256/extension-remote-listing.txt
artifact_bytes=$(find "$campaign_dir/assembly/dataset" -type f -exec stat -c '%s' {} + | awk '{s+=$1} END {print s}')
available_bytes=$(df -B1 --output=avail /mnt/hfx | tail -n 1 | tr -d ' ')
if test "$available_bytes" -ge $((artifact_bytes + 10000000000)); then
  printf 'readback_mode=on-disk artifact_bytes=%s available_bytes=%s\n' "$artifact_bytes" "$available_bytes"
  rm -rf /mnt/hfx/work/s3-readback && mkdir -p /mnt/hfx/work/s3-readback
  aws s3 cp "$prefix/extension-hfx-v0-3-0/dataset/" /mnt/hfx/work/s3-readback/dataset/ --recursive --endpoint-url "$endpoint" --region fsn1 --only-show-errors
  (cd /mnt/hfx/work/s3-readback && sed 's#^\([0-9a-f]\{64\}\)  assembly/dataset/#\1  dataset/#' /mnt/hfx/work/sha256/campaign-assembly-dataset-sha256.txt | sha256sum -c) > /mnt/hfx/work/sha256/extension-s3-readback-verification.txt
  ! grep -v ': OK$' /mnt/hfx/work/sha256/extension-s3-readback-verification.txt
  rm -rf /mnt/hfx/work/s3-readback
else
  printf 'readback_mode=streamed artifact_bytes=%s available_bytes=%s\n' "$artifact_bytes" "$available_bytes"
  : > /mnt/hfx/work/sha256/extension-s3-readback-verification.txt
  while IFS= read -r manifest_line; do
    expected_digest=${manifest_line%%  *}; relative=${manifest_line#*  }; relative=${relative#assembly/dataset/}
    observed_digest=$(aws s3 cp "$prefix/extension-hfx-v0-3-0/dataset/$relative" - --endpoint-url "$endpoint" --region fsn1 | sha256sum | cut -c1-64)
    if test "$observed_digest" = "$expected_digest"; then printf 'dataset/%s: OK\n' "$relative"; else printf 'dataset/%s: FAILED\n' "$relative"; fi
  done < /mnt/hfx/work/sha256/campaign-assembly-dataset-sha256.txt >> /mnt/hfx/work/sha256/extension-s3-readback-verification.txt
  ! grep -v ': OK$' /mnt/hfx/work/sha256/extension-s3-readback-verification.txt
fi
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
  --slurpfile adjudicated "$(record_or_empty compare-adjudicated-outlets.json)" \
  --slurpfile created_at_record "$(record_or_empty created-at-record.json)" \
  --slurpfile assembly "$(record_or_empty off-vm/campaign/state/assembly.json)" \
  --arg validation_outcome "$VALIDATION_OUTCOME" \
  --argjson basins "$(for id in "${ABSENT_IDS[@]}" "$CONTROL_ID"; do jq -c --arg id "$id" '{processing_basin_id:$id, acquire_basins:.stages.acquire_basins.status, acquire_streamnet:.stages.acquire_streamnet.status, compile:.stages.compile.status, failure_reason:.stages.compile.failure_reason, diagnostic_report:.stages.compile.diagnostic_report}' "$LOCAL_EVIDENCE_DIR/off-vm/campaign/state/basins/$id/current.json"; done | jq -s '.')" '{
    schema_version: 1,
    campaign: $campaign,
    ground_truth_ref: $ground_truth_ref,
    control_builds: {
      corrected_versus_preserved: ($corrected[0].verdict // "not-attempted"),
      planetary_versus_preserved: ($planetary[0].verdict // "not-attempted"),
      corrected_adjudicated_comparison: ($adjudicated[0].verdict // "not-attempted"),
      corrected_outlet_differences: ($adjudicated[0].outlet_differences // null),
      preserved_matches_pinned_digests: ($corrected[0].left_matches_expected_sha256 // null),
      corrected_build_created_at_flag_used: ($created_at_record[0].corrected_build_created_at_flag_used // null)
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
    final_fabric: (if $validation_outcome == "passed" then "extended artifact" else "strict-validated baseline" end)
  }' > "$LOCAL_EVIDENCE_DIR/campaign-record.json"
```

Set `VALIDATION_OUTCOME` to `passed`, `failed`, `incomplete`, or `not-attempted` from the section 13 table before generating the record. The record classifies dispositions only from evidence: a basin with compile `succeeded` has a preserved output and digest, and a basin with compile `failed` carries the adapter's exact refusal. This record never emits a source-defect or adapter-strictness verdict; adjudication belongs to the merged ledger at `adapters/tdx-hydro/seven-basin-verdicts.json` through its own reviewed path.

## 16. Mandatory teardown and zero-footprint audit

After preservation is proved, run the same default teardown every trap path uses, then mark cleanup complete:

```bash
test -f "$LOCAL_EVIDENCE_DIR/preservation-complete"
default_teardown
cleanup_complete=1
jq -e --argjson id "$(jq -r .id "$LOCAL_EVIDENCE_DIR/provisioned-server.json")" '[.[] | select(.id == $id)] == []' "$LOCAL_EVIDENCE_DIR/final-servers.json"
jq -e --argjson id "$(jq -r .id "$LOCAL_EVIDENCE_DIR/provisioned-volume.json")" '[.[] | select(.id == $id)] == []' "$LOCAL_EVIDENCE_DIR/final-volumes.json"
jq -n --arg campaign "$CAMPAIGN" --arg ground_truth_ref "$(cat "$LOCAL_EVIDENCE_DIR/ground-truth-ref.txt")" \
  --arg validation "$VALIDATION_OUTCOME" --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg epoch "$(cat "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt")" \
  --slurpfile record "$LOCAL_EVIDENCE_DIR/campaign-record.json" '{
    schema_version: 1,
    campaign: $campaign,
    ground_truth_ref: $ground_truth_ref,
    provisioning_request_epoch: ($epoch | tonumber),
    finished_at: $finished_at,
    strict_validation: $validation,
    zero_footprint: true,
    control_gates: $record[0].control_builds,
    result: (if $validation == "passed" then "passed" else "not-passed" end)
  }' > "$LOCAL_EVIDENCE_DIR/lifecycle-result.json"
```

The teardown command resolves both resources by exact name, validates their labels, compares the IDs against those captured at provisioning on every poll, refuses globs, and never accepts `--keep-volume`. Its stderr must contain this exact line:

```text
hfx: campaign seven-basin-extension has zero Hetzner footprint: server hfx-build-seven-basin-extension absent; volume hfx-build-seven-basin-extension-data absent
```

The independent read-only audit then requires both exact names absent and `pourpoint-web-1` present and unchanged. The fence compares the IDs in `final-servers.json` and `final-volumes.json` with `provisioned-server.json` and `provisioned-volume.json`: neither captured ID may appear. A failed teardown is a campaign failure requiring maintainer attention. It never permits deleting a different resource.

`lifecycle-result.json` is written only on this path, after teardown has proved zero footprint, and carries `result` `passed` only when strict validation passed. For the rehearsal campaign this file is the record the production preflight's `rehearsal-passed` check reads.

## 17. Time-boxed decision points

The provisioning request epoch is hour 0. The decision points are the contract's `decision_points_hours`; the production values appear below. At each decision point the operator compares progress against the stage plan and acts without discretion:

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
| Corrected control build refusal, an `orient` digest that differs from the pinned one, or an adjudicated comparison that is not `accepted` | Preserve both trees, reports, the orient report, and the comparison records, stop all compilation, tear down, hand to adjudication |
| Planetary rebuild with any digest difference beyond `created_at` | Preserve both trees and reports, stop all compilation, tear down, hand to adjudication |
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

This section is documentation-author work completed before merge. It never executes the campaign. The tracked write set is this runbook, `seven-basin-control-adjudication.json`, `rehearsal-campaign-contract.json`, `verify-compile-runbook.sh`, `verify-campaign-inputs.sh`, `hcloud-identity.jq` with its recorded fixtures under `fixtures/hcloud/`, `price-preflight.sh`, `compare-dataset-trees.sh`, `compose-campaign-driver.py`, `prepare-rehearsal-campaign.sh`, `adapters/tdx-hydro/compare_unit_outlets.py`, `adapters/tdx-hydro/synthesize_rehearsal_corpus.py`, their tests, and one README index entry.

```bash
bash scripts/hetzner/test-verify-compile-runbook.sh
bash scripts/hetzner/test-verify-campaign-inputs.sh
bash scripts/hetzner/test-price-preflight.sh
bash scripts/hetzner/test-compare-dataset-trees.sh
bash scripts/hetzner/test-compose-campaign-driver.sh
bash scripts/hetzner/test-tdx-hydro-campaign.sh
uv run --frozen --project adapters/tdx-hydro python adapters/tdx-hydro/test_compare_unit_outlets.py
uv run --frozen --project adapters/tdx-hydro python adapters/tdx-hydro/test_synthesize_rehearsal_corpus.py
for check in scope-permits-compilation ceilings-and-kill-switches control-hotpatch-is-pinned control-digests-are-pinned control-adjudication-is-pinned baseline-is-pinned authority-is-current rehearsal-record-is-pinned; do
  bash scripts/hetzner/verify-compile-runbook.sh --check "$check"
done
git diff --check
```

Review every Bash fence for strict-mode behavior, quoting, secret-path nondisclosure, exact resource identity, absence of globs in mutation, and preservation-before-teardown ordering. Run the repository gates that apply to `scripts/hetzner` and record their output in the PR body.

## 20. Composed driver and rehearsal lifecycle

The operator does not retype the fences. `scripts/hetzner/compose-campaign-driver.py` extracts every Bash fence of this runbook, embeds each one verbatim into one driver script, and writes a fence proof (`fence-proof/runbook-NN.sh`, `fence-proof/driver-NN.sh`, and `fence-diff-proof.txt`) that must show every fence identical. The composer adds only what the prose already requires and no fence can carry: the operator log and milestone markers, the poll loops that repeat a gate-and-status pair until the workload finishes, the decision-point checks from the contract's `decision_points_hours`, and an exit-trap wrapper that turns errexit off before it calls `campaign_cleanup`, because errexit stays in force inside an `EXIT` trap and on 2026-09-04 the first wrapper ended the shell at `(exit "$rc")` before cleanup ran. Modes: `--mode full` runs sections 4 to 16; `--mode preflight` runs sections 4 to 6 and exits before the trap; `--mode resume --resume-at <stage>` reuses a recorded provisioning epoch and both provisioned identities, re-arms the watchdog, re-proves the live identities through `hcloud-identity.jq`, and continues from the named stage (`converge`, `acquire`, `controls`, `compile`, `baseline`, `preserve`). The composed driver is placed in the evidence directory and executed from the repository root with the credential path on standard input, exactly as section 4 reads it.

The rehearsal lifecycle runs the same driver under the rehearsal contract. `scripts/hetzner/prepare-rehearsal-campaign.sh` builds the rehearsal evidence root on the workstation from tiny data: it synthesizes the control basin, the two absent basins, and the second roster basin with `adapters/tdx-hydro/synthesize_rehearsal_corpus.py` (the adapter test fixture shifted per basin, eight GeoPackages), writes their SHA-256 manifest, compiles both roster basins with the checked-out adapter and assembles them into the tiny baseline (the control sits in the rehearsal roster exactly as it sits in the production roster, because extension assembly excludes the control from the roster), uploads that baseline read-only-thereafter to the rehearsal baseline prefix under `scratch/`, and writes the resolved rehearsal contract (the tracked record plus the byte totals and counts) that `HFX_CAMPAIGN_CONTRACT` names. The reference control and the adjudication record are not prepared here; section 10 builds and derives them on the VM. The maintainer places the approval record in the rehearsal evidence root by hand; the preparation refuses without it and never creates it. Every later stage then runs for real on the `cx23`: transfer and remote SHA-256, convergence with the hotpatch, both control builds and both gates, the `orient` digest gate, the per-basin compiles, extension assembly, strict validation, preservation to the rehearsal scratch prefix with digest read-back, and exact-resource teardown with the zero-footprint audit. The rehearsal never names the production baseline prefix.

```bash
python3 scripts/hetzner/compose-campaign-driver.py --mode full --out "$HFX_CAMPAIGN_EVIDENCE/composed"
diff -q "$HFX_CAMPAIGN_EVIDENCE/composed/fence-diff-proof.txt" <(python3 scripts/hetzner/compose-campaign-driver.py --print-identical-proof)
printf '%s\n' "$S3_ENV_FILE_PATH" | caffeinate -i -s bash "$HFX_CAMPAIGN_EVIDENCE/composed/campaign-driver.sh"
```

The rehearsal's reference control is built on the VM (contract `control_reference` `vm-planetary-build`, section 10), never on the workstation, because `area_km2` and `up_area_km2` come from libm trigonometry and a last-ulp difference between macOS arm64 and Debian x86 would fail the byte-for-byte planetary gate on a non-defect. The composer also refuses any `bash -s --` line whose arguments are not exactly `"$(remote_tokens "${remote_args[@]}")"` preceded by a `remote_args=(...)` assignment, and its test suite runs every such fence's argument construction through a fake `ssh` that joins and re-splits the command string the way the real one does, proving on the workstation that each remote script receives every positional it validates. Other remote invocations (`mkdir -p`, `tail -n 1`, the `du`, `sha256sum -c`, and `rm -f` command strings of section 9, the `tmux kill-session` loop of the trap, and every `rsync` remote path) carry only constant paths, validated identifiers, or names matched by a fixed pattern, so they contain no character the remote shell could split. The rehearsal succeeds only when section 16 writes `lifecycle-result.json` with `result` `passed`, which requires strict validation of the tiny extended artifact to pass and teardown to prove zero footprint. The production preflight in section 4 refuses without that record.
