#!/usr/bin/env python3
"""compose : (CompileRunbook, Mode, ResumeStage?) -> CampaignDriver + FenceProof

Extracts every Bash fence of sections 4 to 16 of the compile campaign runbook
and embeds each one verbatim into one operator driver script. The composer adds
only what the runbook prose requires and no fence can carry: the operator log,
milestone markers, the poll loops that repeat a gate-and-status pair until a
workload finishes, the decision-point checks, the validation classification of
section 13, and an exit-trap wrapper that turns errexit off before it calls the
runbook's `campaign_cleanup`. The fence proof beside the driver lets a reviewer
diff every embedded fence against the runbook; the composer itself refuses when
a fence's first line is not the one this composer was written for.

Runs on the standard library only, so the workstation needs no Python project.
"""

from __future__ import annotations

import argparse
import difflib
import pathlib
import re
import sys

RUNBOOK_RELATIVE = pathlib.Path("scripts/hetzner/RUNBOOK-tdx-hydro-seven-basin-compile.md")
FIRST_SECTION = 4
LAST_SECTION = 16
FENCE_COUNT = 25

# (fence number, stage label, exact first line). A moved or edited fence
# changes its first line and the composer refuses instead of composing the
# wrong step under a stage label.
FENCES: tuple[tuple[int, str, str], ...] = (
    (1, 'operator-shell', 'set -Eeuo pipefail'),
    (2, 'input-verification', 'for check in scope-permits-compilation ceilings-and-kill-switches control-hotpatch-is-pinned control-digests-are-pinned control-adjudication-is-pinned baseline-is-pinned authority-is-current rehearsal-record-is-pinned; do'),
    (3, 'price-preflight', './scripts/hetzner/price-preflight.sh \\'),
    (4, 'campaign-gate', 'campaign_gate() {'),
    (5, 'read-only-preflight', 'git fetch origin main'),
    (6, 'cleanup-trap', 'cleanup_running=0'),
    (7, 'provision', 'test ! -e "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"'),
    (8, 'converge', 'remote_args=("$GROUND_TRUTH_REF" "$(contract_value \'.workload_sizing.root_disk_reserve_bytes\')" "$(contract_value \'.workload_sizing.root_swap_bytes_max\')" "$(contract_value \'.workload_sizing.volume_swap_bytes\')" "$(contract_value \'.workload_sizing.required_available_disk_bytes\')" "$(contract_value \'.workload_sizing.required_memory_bytes\')")'),
    (9, 'init', 'campaign_gate pre-init "$(contract_value \'.gate_reserve_hours["pre-init"]\')"'),
    (10, 'corpus-transfer', 'transfer_start=$(date +%s)'),
    (11, 'acquire', 'campaign_gate pre-acquire "$(contract_value \'.gate_reserve_hours["pre-acquire"]\')"'),
    (12, 'control-transfer', 'ssh -o BatchMode=yes "root@$SERVER_IP" mkdir -p "$CONTROL_ROOT/preserved"'),
    (13, 'control-builds', 'campaign_gate pre-control-builds "$(contract_value \'.gate_reserve_hours["pre-control-builds"]\')"'),
    (14, 'compile-start', 'campaign_gate pre-compile "$(contract_value \'.gate_reserve_hours["pre-compile"]\')"'),
    (15, 'compile-monitor', 'campaign_gate compile-monitor "$(contract_value \'.gate_reserve_hours["compile-monitor"]\')"'),
    (16, 'compile-finish', 'for finish_attempt in 1 2 3 4 5 6 7 8 9 10 11 12; do'),
    (17, 'basin-preserve', 'remote_args=("$CAMPAIGN_DIR")'),
    (18, 'baseline-pull', 'campaign_gate pre-baseline "$(contract_value \'.gate_reserve_hours["pre-baseline"]\')"'),
    (19, 'assemble-start', 'test -s "$LOCAL_EVIDENCE_DIR/compiled-absent-basins.txt"'),
    (20, 'assemble-wait', 'until ssh -o BatchMode=yes "root@$SERVER_IP" test -d "$CAMPAIGN_DIR/assembly/dataset"; do'),
    (21, 'validation-evidence', './scripts/hetzner/launch.sh --campaign "$CAMPAIGN" status --workload tdx-assemble || test "$?" -eq 3'),
    (22, 'preserve', 'campaign_gate pre-preservation "$(contract_value \'.gate_reserve_hours["pre-preservation"]\')"'),
    (23, 'extension-preserve', 'if ssh -o BatchMode=yes "root@$SERVER_IP" test -d "$CAMPAIGN_DIR/assembly/dataset"; then'),
    (24, 'campaign-record', 'record_or_empty() { if test -f "$LOCAL_EVIDENCE_DIR/$1"; then printf \'%s\\n\' "$LOCAL_EVIDENCE_DIR/$1"; else printf \'%s\\n\' /dev/null; fi; }'),
    (25, 'teardown', 'test -f "$LOCAL_EVIDENCE_DIR/preservation-complete"'),
)
STAGE_FIRST_FENCE = {
    "converge": 8,
    "acquire": 11,
    "controls": 12,
    "compile": 14,
    "baseline": 18,
    "preserve": 22,
}
MODES = ("full", "preflight", "resume")


class ComposeError(ValueError):
    """Raised when the runbook does not carry the fences this composer expects."""


def section_number(line: str) -> int | None:
    match = re.match(r"^## (\d+)\. ", line)
    return int(match.group(1)) if match else None


def extract_fences(text: str) -> list[str]:
    """Return the Bash fences of sections 4 to 16 in order, each with a trailing newline."""
    lines = text.split("\n")
    fences: list[str] = []
    current_section: int | None = None
    index = 0
    while index < len(lines):
        line = lines[index]
        number = section_number(line)
        if number is not None:
            current_section = number
        if line.strip() == "```bash":
            end = index + 1
            while end < len(lines) and lines[end].strip() != "```":
                end += 1
            if end >= len(lines):
                raise ComposeError(f"unterminated bash fence starting at line {index + 1}")
            if current_section is not None and FIRST_SECTION <= current_section <= LAST_SECTION:
                fences.append("\n".join(lines[index + 1:end]) + "\n")
            index = end
        index += 1
    if len(fences) != FENCE_COUNT:
        raise ComposeError(f"expected {FENCE_COUNT} fences in sections {FIRST_SECTION} to {LAST_SECTION}, found {len(fences)}")
    for number, label, first_line in FENCES:
        actual = fences[number - 1].split("\n", 1)[0]
        if actual != first_line:
            raise ComposeError(f"fence {number} ({label}) starts with {actual!r}, expected {first_line!r}")
        check_remote_arguments(number, label, fences[number - 1])
    return fences


REMOTE_ARGUMENT_FORM = 'bash -s -- "$(remote_tokens "${remote_args[@]}")"'
REMOTE_ARGS_ASSIGNMENT = re.compile(r"^\s*remote_args=\(.*\)\s*$")


def check_remote_arguments(number: int, label: str, fence: str) -> None:
    """Refuse a `bash -s --` line whose arguments are not the quoted remote_args array.

    ssh joins its remote command arguments into one string that the remote shell
    splits again, so only `printf '%q'`-quoted tokens survive; the 2026-09-04
    rehearsal died on a space-joined sizing argument at the converge fence.
    """
    lines = fence.split("\n")
    for index, line in enumerate(lines):
        if "bash -s --" not in line:
            continue
        expected_tail = REMOTE_ARGUMENT_FORM
        position = line.find("bash -s --")
        if not line[position:].startswith(expected_tail):
            raise ComposeError(
                f"fence {number} ({label}) line {index + 1} sends remote arguments that are not the quoted remote_args array: {line.strip()!r}"
            )
        rest = line[position + len(expected_tail):]
        if not (rest == "" or rest.startswith(" <<'REMOTE'")):
            raise ComposeError(f"fence {number} ({label}) line {index + 1} carries extra remote arguments: {line.strip()!r}")
        preceding = [candidate for candidate in lines[:index] if candidate.strip()]
        # The ssh line may continue a `ssh ... \\` line; the assignment sits just before that.
        assignment_index = index - 1
        if assignment_index >= 0 and lines[assignment_index].rstrip().endswith("\\"):
            assignment_index -= 1
        if assignment_index < 0 or not REMOTE_ARGS_ASSIGNMENT.match(lines[assignment_index]):
            raise ComposeError(f"fence {number} ({label}) line {index + 1} has no remote_args assignment immediately before its ssh line")
        del preceding


def fence_block(number: int, body: str) -> str:
    label = FENCES[number - 1][1]
    return f"# >>> runbook fence {number:02d} ({label}) >>>\n{body}# <<< runbook fence {number:02d} <<<\n"


OPERATOR_HELPERS = r'''# OPERATOR: log, milestones, elapsed time, and the poll loop the runbook prose describes.
mkdir -p -- "$LOCAL_EVIDENCE_DIR/milestones"
POLL_SECONDS=${POLL_SECONDS:-300}
now_utc() { date -u +%Y-%m-%dT%H:%M:%SZ; }
oplog() { printf -- '- %s %s\n' "$(now_utc)" "$*" >> "$LOCAL_EVIDENCE_DIR/OPERATOR-LOG.md"; printf 'driver: %s %s\n' "$(now_utc)" "$*" >&2; }
milestone() { local name=$1; shift; printf '%s %s\n' "$(now_utc)" "$*" > "$LOCAL_EVIDENCE_DIR/milestones/$name"; oplog "MILESTONE $name: $*"; }
elapsed_hours() {
  local origin now
  origin=$(<"$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"); now=$(date +%s)
  awk -v now="$now" -v origin="$origin" 'BEGIN { printf "%.4f\n", (now - origin) / 3600 }'
}
before_hour() { awk -v e="$(elapsed_hours)" -v h="$1" 'BEGIN { exit !(e < h) }'; }
decision_point() { contract_value ".decision_points_hours[$1]"; }
gate_reserve() { contract_value ".gate_reserve_hours[\"$1\"]"; }
wait_workload() {
  # workload phase reserve_hours decision_hour: gate before every status poll (runbook section 5).
  # Returns 0 finished, 2 decision point reached, 1 gate refusal or persistent transient failure.
  local w=$1 phase=$2 reserve=$3 dp=$4 rc transient=0
  while :; do
    campaign_gate "$phase" "$reserve" || { oplog "gate $phase refused while monitoring $w"; return 1; }
    if ! before_hour "$dp"; then
      oplog "decision point ${dp}h reached while $w still running; stopping dispatch"
      return 2
    fi
    rc=0
    ./scripts/hetzner/launch.sh --campaign "$CAMPAIGN" status --workload "$w" > "$LOCAL_EVIDENCE_DIR/status-$w-latest.txt" 2>&1 || rc=$?
    case $rc in
      0) transient=0; sleep "$POLL_SECONDS" ;;
      3) return 0 ;;
      *) transient=$((transient + 1)); oplog "status for $w returned $rc (transient $transient/10)"; test "$transient" -lt 10 || return 1; sleep 60 ;;
    esac
  done
}
finish_record() {
  # The runner writes the finish line before its session ends; poll briefly so a late line is not read as a failure.
  local attempt
  for attempt in 1 2 3 4 5 6 7 8 9 10 11 12; do
    ssh -o BatchMode=yes "root@$SERVER_IP" tail -n 1 "/mnt/hfx/logs/hfx-$CAMPAIGN-$1.log" > "$LOCAL_EVIDENCE_DIR/$1-finish-record.txt" || true
    grep -q -E -- '^launch: finished at ' "$LOCAL_EVIDENCE_DIR/$1-finish-record.txt" && break
    sleep 5
  done
  cat "$LOCAL_EVIDENCE_DIR/$1-finish-record.txt"
}
require_finish_exit_zero() { grep -E -- '^launch: finished at [0-9T:Z-]+ with exit 0$' "$LOCAL_EVIDENCE_DIR/$1-finish-record.txt"; }
{
  printf '\n- %s driver start; mode %s; worktree %s; HEAD %s\n' "$(now_utc)" "__MODE__" "$PWD" "$(git rev-parse HEAD)"
} >> "$LOCAL_EVIDENCE_DIR/OPERATOR-LOG.md"
'''

TRAP_WRAPPER = r'''# OPERATOR: exit-trap wrapper. Errexit stays in force inside an EXIT trap, so the wrapper turns it
# off first; on 2026-09-04 the first wrapper ended the shell at `(exit "$rc")` before campaign_cleanup
# ran. After preservation-complete exists the small roots are refreshed once more on the way out.
refresh_small_roots_on_exit() {
  set +x
  test -f "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt" || return 0
  test -f "$LOCAL_EVIDENCE_DIR/preservation-complete" || return 0
  remote_ip || return 0
  copy_remote_root "$CAMPAIGN_DIR/state" "$LOCAL_EVIDENCE_DIR/off-vm/campaign" || true
  copy_remote_root "$CAMPAIGN_DIR/reports" "$LOCAL_EVIDENCE_DIR/off-vm/campaign" || true
  copy_remote_root /mnt/hfx/logs "$LOCAL_EVIDENCE_DIR/off-vm" || true
  copy_remote_root /mnt/hfx/work/sha256 "$LOCAL_EVIDENCE_DIR/off-vm" || true
}
operator_exit_trap() {
  local rc=$?
  set +e
  oplog "exit trap entered with status $rc"
  refresh_small_roots_on_exit
  (exit "$rc"); campaign_cleanup; rc=$?
  oplog "campaign_cleanup returned $rc"
  if test "$cleanup_running" -eq 1 && grep -qF -- 'zero Hetzner footprint' "$LOCAL_EVIDENCE_DIR/teardown.log" 2>/dev/null; then
    milestone 99-torn-down "cleanup path; exit status $rc; $(tail -n 1 "$LOCAL_EVIDENCE_DIR/teardown.log")"
  fi
  exit "$rc"
}
trap operator_exit_trap EXIT
'''

RESUME_PRELUDE = r'''# OPERATOR (resume): the provisioning request epoch and both identities were recorded by an earlier
# start; provision.sh and bootstrap.sh are not rerun. The watchdog below is the fence 7 watchdog verbatim.
test -f "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"
test ! -e "$LOCAL_EVIDENCE_DIR/hard-ceiling-reached"
[[ "$(<"$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt")" =~ ^[0-9]+$ ]]
GROUND_TRUTH_REF=$(<"$LOCAL_EVIDENCE_DIR/ground-truth-ref.txt")
test "$(git rev-parse HEAD)" = "${HFX_RESUME_REF:-$GROUND_TRUTH_REF}"
if test -n "${HFX_RESUME_REF:-}"; then
  git merge-base --is-ancestor "$GROUND_TRUTH_REF" "$HFX_RESUME_REF"
  test -z "$(git diff --stat "$GROUND_TRUTH_REF" "$HFX_RESUME_REF" -- adapters/)"
  printf '%s\n' "$HFX_RESUME_REF" > "$LOCAL_EVIDENCE_DIR/ground-truth-ref-resume.txt"
  GROUND_TRUTH_REF=$HFX_RESUME_REF
fi
oplog "resume at __STAGE__: epoch $(<"$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"); elapsed $(elapsed_hours) h; ref $GROUND_TRUTH_REF"
__WATCHDOG__remote_ip
hcloud --context pourpoint server describe "$SERVER_NAME" -o json | jq --arg kind server -f scripts/hetzner/hcloud-identity.jq > "$LOCAL_EVIDENCE_DIR/resume-server.json"
hcloud --context pourpoint volume describe "$VOLUME_NAME" -o json | jq --arg kind volume -f scripts/hetzner/hcloud-identity.jq > "$LOCAL_EVIDENCE_DIR/resume-volume.json"
cmp -- "$LOCAL_EVIDENCE_DIR/provisioned-server.json" "$LOCAL_EVIDENCE_DIR/resume-server.json"
cmp -- "$LOCAL_EVIDENCE_DIR/provisioned-volume.json" "$LOCAL_EVIDENCE_DIR/resume-volume.json"
ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "root@$SERVER_IP" bash -s -- "$GROUND_TRUTH_REF" <<'REMOTE' 2>&1 | tee "$LOCAL_EVIDENCE_DIR/resume-vm-state.txt"
set -Eeuo pipefail
set +x
findmnt -rn -M /mnt/hfx -o SOURCE,OPTIONS | grep -E ' rw,|,rw,'
stat -c 'credential %U:%G %a' /etc/pourpoint-hfx.env
cd /root/hfx && git fetch origin main && git checkout --detach "$1" && test "$(git rev-parse HEAD)" = "$1" && test -z "$(git status --porcelain)"
printf 'hfx HEAD %s\n' "$(git -C /root/hfx rev-parse HEAD)"
test -x /root/hfx/target/release/hfx
printf 'swap: %s\n' "$(swapon --show --noheadings | tr '\n' ';')"
! tmux ls >/dev/null 2>&1
printf 'avail /mnt/hfx: %s\n' "$(df -B1 --output=avail /mnt/hfx | tail -n 1 | tr -d ' ')"
REMOTE
oplog "resume: live identities equal the recorded provisioned-server.json and provisioned-volume.json; ip=$SERVER_IP"
'''


def compose(fences: list[str], mode: str, resume_at: str | None) -> str:
    parts: list[str] = []
    add = parts.append
    add("#!/usr/bin/env bash\n")
    add("# Composed by scripts/hetzner/compose-campaign-driver.py from RUNBOOK-tdx-hydro-seven-basin-compile.md.\n")
    add(f"# Mode: {mode}{' at ' + resume_at if resume_at else ''}. Every runbook fence is embedded verbatim between its markers.\n")
    add("# Invocation: printf '%s\\n' <s3-env-path> | caffeinate -i -s bash <this file>   (from the repository root)\n")
    if mode == "resume":
        add("export HFX_CAMPAIGN_RESUME=1   # OPERATOR (resume): keep the evidence directory that holds the recorded epoch\n")
    add(fence_block(1, fences[0]))
    add(OPERATOR_HELPERS.replace("__MODE__", mode + (" at " + resume_at if resume_at else "")))

    if mode in ("full", "preflight"):
        for number in (2, 3, 4, 5):
            add(fence_block(number, fences[number - 1]))
        add(r'''test "$(git rev-parse HEAD)" = "$GROUND_TRUTH_REF"   # OPERATOR: the driver runs from a detached checkout of the ground truth ref
milestone 01-preflight-passed "sections 4-6 passed; ground_truth_ref=$GROUND_TRUTH_REF; projected_gross_total_eur=$(jq -r .projected_gross_total_eur "$LOCAL_EVIDENCE_DIR/current-price-preflight.json")"
''')
        if mode == "preflight":
            add('oplog "preflight mode complete; no trap installed, no cloud mutation"\nexit 0\n')
            return "".join(parts)
    else:
        add(fence_block(4, fences[3]))

    add(fence_block(6, fences[5]))
    add(TRAP_WRAPPER)

    first = 1 if mode == "full" else STAGE_FIRST_FENCE[resume_at]
    if mode == "resume":
        watchdog_source = fences[6]
        begin = watchdog_source.index("(\n  while test ! -f")
        end = watchdog_source.index("WATCHDOG_PID=$!\n") + len("WATCHDOG_PID=$!\n")
        add(RESUME_PRELUDE.replace("__STAGE__", resume_at).replace("__WATCHDOG__", watchdog_source[begin:end]))
        add(f'milestone 02-provisioned "resumed at {resume_at}; server $(jq -c . "$LOCAL_EVIDENCE_DIR/provisioned-server.json"); volume $(jq -c . "$LOCAL_EVIDENCE_DIR/provisioned-volume.json"); ip=$SERVER_IP"\n')
    else:
        add(fence_block(7, fences[6]))
        add(r'''milestone 02-provisioned "server $(jq -c . "$LOCAL_EVIDENCE_DIR/provisioned-server.json"); volume $(jq -c . "$LOCAL_EVIDENCE_DIR/provisioned-volume.json"); ip=$SERVER_IP; epoch=$(cat "$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt")"
''')

    if first <= 8:
        add(fence_block(8, fences[7]))
        add('oplog "converged; observed-available-disk-bytes=$(cat "$LOCAL_EVIDENCE_DIR/observed-available-disk-bytes.txt")"\n')
    if first <= 9:
        add(fence_block(9, fences[8]))
        add(r'''POLL_SECONDS=20 wait_workload tdx-init init-monitor "$(gate_reserve pre-acquire)" "$(decision_point 0)"
finish_record tdx-init
require_finish_exit_zero tdx-init
''')
    if first <= 10:
        add(fence_block(10, fences[9]))
    if first <= 11:
        add(fence_block(11, fences[10]))
        add(r'''POLL_SECONDS=60 wait_workload tdx-acquire acquire-monitor "$(gate_reserve pre-control-builds)" "$(decision_point 0)"
finish_record tdx-acquire
require_finish_exit_zero tdx-acquire
mkdir -p "$LOCAL_EVIDENCE_DIR/acquire-state"
for id in "${ABSENT_IDS[@]}" "$CONTROL_ID"; do
  scp -q -o BatchMode=yes "root@$SERVER_IP:$CAMPAIGN_DIR/state/basins/$id/current.json" "$LOCAL_EVIDENCE_DIR/acquire-state/$id.json"
  oplog "acquire $id: basins=$(jq -r '.stages.acquire_basins.status' "$LOCAL_EVIDENCE_DIR/acquire-state/$id.json") streamnet=$(jq -r '.stages.acquire_streamnet.status' "$LOCAL_EVIDENCE_DIR/acquire-state/$id.json")"
done
jq -e '.stages.acquire_basins.status == "succeeded" and .stages.acquire_streamnet.status == "succeeded"' "$LOCAL_EVIDENCE_DIR/acquire-state/$CONTROL_ID.json" >/dev/null
ssh -o BatchMode=yes "root@$SERVER_IP" "cd '$CAMPAIGN_DIR/downloads' && sha256sum -c /mnt/hfx/work/sha256/source-expected-sha256.txt" \
  | tee "$LOCAL_EVIDENCE_DIR/corpus-remote-verification-final.txt" || true
milestone 03-corpus-verified-on-vm "remote sha256 after acquisition: $(grep -c ': OK$' "$LOCAL_EVIDENCE_DIR/corpus-remote-verification-final.txt" || true) of $CORPUS_FILE_COUNT OK"
''')
    if first <= 12:
        add(fence_block(12, fences[11]))
    if first <= 13:
        add(fence_block(13, fences[12]))
        add(r'''milestone 04-control-gates-passed "planetary=$(jq -r .verdict "$LOCAL_EVIDENCE_DIR/compare-planetary.json") corrected-vs-preserved=$(jq -r .verdict "$LOCAL_EVIDENCE_DIR/compare-corrected.json") adjudicated=$(jq -r .verdict "$LOCAL_EVIDENCE_DIR/compare-adjudicated-outlets.json") outlet_differences=$(jq -r .outlet_differences "$LOCAL_EVIDENCE_DIR/compare-adjudicated-outlets.json") created_at_flag_used=$(jq -r .corrected_build_created_at_flag_used "$LOCAL_EVIDENCE_DIR/created-at-record.json")"
''')
    if first <= 14:
        add(fence_block(14, fences[13]))
    if first <= 17:
        add(r'''# OPERATOR: one verbatim compile-monitor pair, then the same pair repeated by wait_workload with its
# ten-transient tolerance until status exits 3 (runbook sections 11 and 18); second decision point.
compile_stopped_at_decision_point=0
''')
        add(fence_block(15, fences[14]))
        add(r'''if wait_workload tdx-compile compile-monitor "$(gate_reserve compile-monitor)" "$(decision_point 1)"; then :; else
  wait_rc=$?; test "$wait_rc" -eq 2
  oplog "decision point $(decision_point 1)h reached with compile running; stopping"
  stop_dispatches || true; compile_stopped_at_decision_point=1; sleep 10
fi
''')
        add(fence_block(16, fences[15]))
        add('if test "$compile_stopped_at_decision_point" -eq 1; then compile_exit_zero=0; fi\n')
        add(fence_block(17, fences[16]))
        add(r'''for id in "${ABSENT_IDS[@]}" "$CONTROL_ID"; do
  oplog "compile $id: $(jq -c '{compile:.stages.compile.status, reason:.stages.compile.failure_reason}' "$LOCAL_EVIDENCE_DIR/off-vm/campaign/state/basins/$id/current.json")"
done
milestone 05-compiles-done "exit_zero=$compile_exit_zero; compiled absent basins: $(tr '\n' ' ' < "$LOCAL_EVIDENCE_DIR/compiled-absent-basins.txt"); per-basin: $(for id in "${ABSENT_IDS[@]}" "$CONTROL_ID"; do printf '%s=%s ' "$id" "$(jq -r '.stages.compile.status' "$LOCAL_EVIDENCE_DIR/off-vm/campaign/state/basins/$id/current.json")"; done)"
''')
    else:
        add('compile_exit_zero=1   # OPERATOR (resume): the compile stage was recorded by an earlier start\n')

    add(r'''VALIDATION_OUTCOME=not-attempted
validation_stopped_at_decision_point=0
assembly_attempted=0
artifact_exists=0
if test "$compile_exit_zero" -eq 1 && test -s "$LOCAL_EVIDENCE_DIR/compiled-absent-basins.txt"; then
''')
    if first <= 18:
        add(fence_block(18, fences[17]))
    add("assembly_attempted=1\n")
    if first <= 20:
        add(fence_block(19, fences[18]))
        add(fence_block(20, fences[19]))
    add(r'''if ssh -o BatchMode=yes "root@$SERVER_IP" test -d "$CAMPAIGN_DIR/assembly/dataset"; then artifact_exists=1; fi
milestone 06-assembly-done "artifact_exists=$artifact_exists (strict validation may still be running)"
else
milestone 06-assembly-done "assembly not attempted (compile_exit_zero=$compile_exit_zero, compiled absent count=$(grep -c . "$LOCAL_EVIDENCE_DIR/compiled-absent-basins.txt" || true))"
fi
''')
    add(fence_block(22, fences[21]))
    add(fence_block(23, fences[22]))
    add(r'''milestone 07-preserved "pass 1: state, reports, logs, control builds, basin outputs, extension (exists=$artifact_exists) digest-verified off-VM; S3 prefix $EXTENSION_PREFIX; readback: $(grep -E '^readback_mode' "$LOCAL_EVIDENCE_DIR/extension-s3-preservation.log" 2>/dev/null || echo n/a)"
if test "$assembly_attempted" -eq 1; then
  # OPERATOR: wait for the strict validation attempt with the gate before every poll; third decision point.
  if wait_workload tdx-assemble validation-monitor "$(gate_reserve pre-preservation)" "$(decision_point 2)"; then :; else
    wait_rc=$?; test "$wait_rc" -eq 2
    validation_stopped_at_decision_point=1
    oplog "decision point $(decision_point 2)h: stopping validation; recorded incomplete"
    stop_dispatches || true; sleep 15
  fi
''')
    add(fence_block(21, fences[20]))
    add(r'''  scp -q -o BatchMode=yes "root@$SERVER_IP:$CAMPAIGN_DIR/state/assembly.json" "$LOCAL_EVIDENCE_DIR/assembly-final.json"
  assembly_status=$(jq -r '.status' "$LOCAL_EVIDENCE_DIR/assembly-final.json")
  assembly_reason=$(jq -r '.failure_reason // ""' "$LOCAL_EVIDENCE_DIR/assembly-final.json")
  # OPERATOR: section 13 table; anything not clearly a validator exit 1 is incomplete, never passing.
  if test "$assembly_status" = succeeded; then VALIDATION_OUTCOME=passed
  elif test "$assembly_reason" = 'assembled dataset validation failed; retained for inspection'; then
    if test "$validation_stopped_at_decision_point" -eq 1; then VALIDATION_OUTCOME=incomplete
    elif grep -q -i -E 'out of memory|killed process' "$LOCAL_EVIDENCE_DIR/validation-evidence.txt"; then VALIDATION_OUTCOME=incomplete
    elif grep -q -E 'return code (-[0-9]+|137)' "$LOCAL_EVIDENCE_DIR/validation-evidence.txt"; then VALIDATION_OUTCOME=incomplete
    elif grep -q -E 'return code 1[^0-9]' "$LOCAL_EVIDENCE_DIR/validation-evidence.txt"; then VALIDATION_OUTCOME=failed
    else VALIDATION_OUTCOME=incomplete; fi
  elif test "$assembly_reason" = 'adapter assembly failed' || test "$assembly_reason" = 'adapter assembly failed and left an artifact; retained for inspection'; then VALIDATION_OUTCOME=not-attempted
  else VALIDATION_OUTCOME=incomplete; fi
  printf 'assembly_status=%s\nassembly_failure_reason=%s\nstopped_at_decision_point=%s\nvalidation_outcome=%s\n' "$assembly_status" "$assembly_reason" "$validation_stopped_at_decision_point" "$VALIDATION_OUTCOME" > "$LOCAL_EVIDENCE_DIR/validation-classification.txt"
fi
milestone 08-validation-classified "VALIDATION_OUTCOME=$VALIDATION_OUTCOME"
''')
    add(fence_block(22, fences[21]))
    add(fence_block(24, fences[23]))
    add(fence_block(25, fences[24]))
    add(r'''milestone 09-torn-down "$(grep -F 'zero Hetzner footprint' "$LOCAL_EVIDENCE_DIR/teardown.log" | tail -n 1); lifecycle-result=$(jq -r .result "$LOCAL_EVIDENCE_DIR/lifecycle-result.json")"
trap - EXIT
oplog "driver finished"
''')
    return "".join(parts)


def embedded_fences(driver: str) -> dict[int, str]:
    """Return every embedded fence keyed by number, read back from the driver's markers."""
    found: dict[int, str] = {}
    pattern = re.compile(r"^# >>> runbook fence (\d\d) \([a-z-]+\) >>>\n(.*?)^# <<< runbook fence \1 <<<\n", re.S | re.M)
    for match in pattern.finditer(driver):
        number = int(match.group(1))
        body = match.group(2)
        if number in found and found[number] != body:
            raise ComposeError(f"fence {number} is embedded twice with different bodies")
        found[number] = body
    return found


def proof_text(fences: list[str], driver: str) -> str:
    lines: list[str] = []
    used = embedded_fences(driver)
    for number, label, _ in FENCES:
        if number not in used:
            lines.append(f"fence {number:02d} ({label}): NOT EMBEDDED IN THIS MODE")
            continue
        if used[number] == fences[number - 1]:
            lines.append(f"fence {number:02d} ({label}): IDENTICAL")
        else:
            diff = difflib.unified_diff(
                fences[number - 1].splitlines(keepends=True), used[number].splitlines(keepends=True),
                fromfile=f"runbook-{number:02d}.sh", tofile=f"driver-{number:02d}.sh",
            )
            lines.append(f"fence {number:02d} ({label}): DIFFERS\n" + "".join(diff))
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--runbook", type=pathlib.Path, default=None, help="runbook path; default is the tracked runbook")
    parser.add_argument("--mode", choices=MODES, default="full")
    parser.add_argument("--resume-at", choices=sorted(STAGE_FIRST_FENCE), default=None)
    parser.add_argument("--out", type=pathlib.Path, default=None, help="directory that receives campaign-driver.sh and fence-proof/")
    parser.add_argument("--print-identical-proof", action="store_true", help="print the proof text every full-mode composition must equal")
    arguments = parser.parse_args(argv)
    if arguments.mode == "resume" and arguments.resume_at is None:
        parser.error("--mode resume requires --resume-at")
    if arguments.mode != "resume" and arguments.resume_at is not None:
        parser.error("--resume-at is valid only with --mode resume")
    runbook = arguments.runbook
    if runbook is None:
        runbook = pathlib.Path(__file__).resolve().parents[2] / RUNBOOK_RELATIVE
    try:
        fences = extract_fences(runbook.read_text(encoding="utf-8"))
        driver = compose(fences, arguments.mode, arguments.resume_at)
    except (OSError, ComposeError) as error:
        print(f"compose-campaign-driver: refused: {error}", file=sys.stderr)
        return 1
    proof = proof_text(fences, driver)
    if arguments.print_identical_proof:
        sys.stdout.write(proof)
        return 0
    if arguments.out is None:
        parser.error("--out is required unless --print-identical-proof is given")
    out = arguments.out
    proof_dir = out / "fence-proof"
    proof_dir.mkdir(parents=True, exist_ok=True)
    (out / "campaign-driver.sh").write_text(driver, encoding="utf-8")
    (out / "campaign-driver.sh").chmod(0o755)
    used = embedded_fences(driver)
    for number, _, _ in FENCES:
        (proof_dir / f"runbook-{number:02d}.sh").write_text(fences[number - 1], encoding="utf-8")
        if number in used:
            (proof_dir / f"driver-{number:02d}.sh").write_text(used[number], encoding="utf-8")
    (out / "fence-diff-proof.txt").write_text(proof, encoding="utf-8")
    print(f"driver: {out / 'campaign-driver.sh'}")
    print(f"proof: {out / 'fence-diff-proof.txt'}")
    if "DIFFERS" in proof:
        print("compose-campaign-driver: refused: an embedded fence differs from the runbook", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
