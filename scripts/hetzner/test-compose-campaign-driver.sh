#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
set +x

SCRIPT_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
composer=$SCRIPT_DIR/compose-campaign-driver.py
runbook=$SCRIPT_DIR/RUNBOOK-tdx-hydro-seven-basin-compile.md
tmp=$(mktemp -d "${TMPDIR:-/tmp}/hfx-compose-campaign-driver-test.XXXXXX")
cleanup() { rm -rf -- "$tmp"; }
trap cleanup EXIT
stdout=$tmp/stdout
stderr=$tmp/stderr
passed=0

die() { printf 'test-compose-campaign-driver: error: %s\n' "$1" >&2; exit 1; }
pass() { passed=$((passed + 1)); printf 'ok %d - %s\n' "$passed" "$1"; }
assert_contains() { grep -F -- "$2" "$1" >/dev/null || die "missing '$2' in $1"; }
assert_not_contains() { ! grep -F -- "$2" "$1" >/dev/null || die "unexpected '$2' in $1"; }
compose() { python3 "$composer" "$@" >"$stdout" 2>"$stderr"; }
expect_failure() {
    if compose "$@"; then die "unexpected success: $*"; fi
    [[ -s "$stderr" ]] || die "no diagnostic for: $*"
}

# Extract the runbook's section 4 to 16 fences independently of the composer.
extract_fences() {
    awk -v out="$1" '
        /^## [0-9]+\. / { section = $2 + 0 }
        /^```bash$/ && section >= 4 && section <= 16 { inside = 1; count++; file = sprintf("%s/expected-%02d.sh", out, count); next }
        /^```$/ && inside { inside = 0; close(file); next }
        inside { print > file }
    ' "$runbook"
}
mkdir "$tmp/expected"
extract_fences "$tmp/expected"
[[ $(ls "$tmp/expected" | wc -l | tr -d ' ') == 25 ]] || die 'runbook does not carry 25 fences in sections 4 to 16'

compose --mode full --out "$tmp/full" || die "full composition failed: $(cat "$stderr")"
[[ -x "$tmp/full/campaign-driver.sh" ]] || die 'driver was not written executable'
bash -n "$tmp/full/campaign-driver.sh" || die 'full driver has a syntax error'
for number in $(seq -w 1 25); do
    diff -u "$tmp/expected/expected-$number.sh" "$tmp/full/fence-proof/driver-$number.sh" >/dev/null ||
        die "embedded fence $number differs from the runbook"
    diff -u "$tmp/expected/expected-$number.sh" "$tmp/full/fence-proof/runbook-$number.sh" >/dev/null ||
        die "proof copy of fence $number differs from the runbook"
done
[[ $(grep -c ': IDENTICAL$' "$tmp/full/fence-diff-proof.txt") == 25 ]] || die 'full proof does not report 25 identical fences'
! grep -q 'DIFFERS' "$tmp/full/fence-diff-proof.txt" || die 'full proof reports a differing fence'
python3 "$composer" --print-identical-proof >"$tmp/printed-proof.txt"
diff -u "$tmp/printed-proof.txt" "$tmp/full/fence-diff-proof.txt" >/dev/null || die 'printed proof differs from the composed proof'
# Fence 22 (ordered preservation) runs twice: once when the artifact appears and once after validation.
[[ $(grep -c '^# >>> runbook fence 22 ' "$tmp/full/campaign-driver.sh") == 2 ]] || die 'preservation fence is not embedded twice'
for marker in '01-preflight-passed' '02-provisioned' '03-corpus-verified-on-vm' '04-control-gates-passed' '05-compiles-done' '06-assembly-done' '07-preserved' '08-validation-classified' '09-torn-down'; do
    assert_contains "$tmp/full/campaign-driver.sh" "milestone $marker"
done
assert_contains "$tmp/full/campaign-driver.sh" 'decision_point 0'
assert_contains "$tmp/full/campaign-driver.sh" 'decision_point 1'
assert_contains "$tmp/full/campaign-driver.sh" 'decision_point 2'
assert_contains "$tmp/full/campaign-driver.sh" 'wait_workload tdx-init'
assert_contains "$tmp/full/campaign-driver.sh" 'wait_workload tdx-acquire'
assert_contains "$tmp/full/campaign-driver.sh" 'wait_workload tdx-assemble'
pass 'full mode embeds all 25 fences byte for byte, twice for preservation, with milestones, gates, and decision points'

compose --mode preflight --out "$tmp/preflight" || die "preflight composition failed: $(cat "$stderr")"
bash -n "$tmp/preflight/campaign-driver.sh" || die 'preflight driver has a syntax error'
for number in 01 02 03 04 05; do assert_contains "$tmp/preflight/campaign-driver.sh" "# >>> runbook fence $number "; done
for number in 06 07 08 25; do assert_not_contains "$tmp/preflight/campaign-driver.sh" "# >>> runbook fence $number "; done
assert_not_contains "$tmp/preflight/campaign-driver.sh" 'trap operator_exit_trap EXIT'
assert_contains "$tmp/preflight/campaign-driver.sh" 'no trap installed, no cloud mutation'
pass 'preflight mode stops after section 6 with no trap and no provisioning fence'

declare -a stages=(converge acquire controls compile baseline preserve)
declare -a first_fences=(08 11 12 14 18 22)
declare -a skipped_fences=(07 10 11 13 17 20)
for index in "${!stages[@]}"; do
    stage=${stages[$index]}
    compose --mode resume --resume-at "$stage" --out "$tmp/resume-$stage" || die "resume $stage composition failed: $(cat "$stderr")"
    bash -n "$tmp/resume-$stage/campaign-driver.sh" || die "resume $stage driver has a syntax error"
    assert_contains "$tmp/resume-$stage/campaign-driver.sh" "# >>> runbook fence ${first_fences[$index]} "
    assert_not_contains "$tmp/resume-$stage/campaign-driver.sh" "# >>> runbook fence ${skipped_fences[$index]} "
    assert_not_contains "$tmp/resume-$stage/campaign-driver.sh" '# >>> runbook fence 07 '
    assert_contains "$tmp/resume-$stage/campaign-driver.sh" 'resume-server.json'
    assert_contains "$tmp/resume-$stage/campaign-driver.sh" 'WATCHDOG_PID=$!'
    assert_contains "$tmp/resume-$stage/campaign-driver.sh" '# >>> runbook fence 25 '
    ! grep -q 'DIFFERS' "$tmp/resume-$stage/fence-diff-proof.txt" || die "resume $stage proof reports a differing fence"
done
assert_contains "$tmp/resume-preserve/campaign-driver.sh" 'compile_exit_zero=1   # OPERATOR (resume)'
pass 'resume modes skip the provisioning fence, re-arm the watchdog, re-prove identities, and start at the named stage'

expect_failure --mode resume --out "$tmp/bad"
assert_contains "$stderr" '--mode resume requires --resume-at'
expect_failure --mode full --resume-at acquire --out "$tmp/bad"
assert_contains "$stderr" '--resume-at is valid only with --mode resume'
expect_failure --mode full
assert_contains "$stderr" '--out is required'
sed 's/^campaign_gate pre-compile .*/campaign_gate pre-compile-renamed 40/' "$runbook" >"$tmp/moved-runbook.md"
expect_failure --runbook "$tmp/moved-runbook.md" --mode full --out "$tmp/bad"
assert_contains "$stderr" 'fence 14 (compile-start) starts with'
sed '/^campaign_gate pre-compile /,/^```$/d' "$runbook" >"$tmp/dropped-runbook.md"
expect_failure --runbook "$tmp/dropped-runbook.md" --mode full --out "$tmp/bad"
assert_contains "$stderr" 'expected 25 fences'
pass 'argument errors and a moved, renamed, or dropped fence refuse before composing'

# The exit-trap wrapper must reach campaign_cleanup with the original failing status even though
# errexit is in force inside an EXIT trap. Extract the wrapper from the driver and drive it with stubs.
sed -n '/^refresh_small_roots_on_exit() {$/,/^trap operator_exit_trap EXIT$/p' "$tmp/full/campaign-driver.sh" >"$tmp/wrapper.sh"
[[ -s "$tmp/wrapper.sh" ]] || die 'trap wrapper was not found in the driver'
cat >"$tmp/trap-harness.sh" <<HARNESS
set -Eeuo pipefail
LOCAL_EVIDENCE_DIR=$tmp/trap-evidence
mkdir -p "\$LOCAL_EVIDENCE_DIR/milestones"
printf '1\n' >"\$LOCAL_EVIDENCE_DIR/provisioning-request-epoch.txt"
cleanup_running=0
oplog() { printf '%s\n' "\$*" >>"\$LOCAL_EVIDENCE_DIR/oplog.txt"; }
milestone() { printf '%s\n' "\$*" >>"\$LOCAL_EVIDENCE_DIR/milestones.txt"; }
remote_ip() { return 1; }
copy_remote_root() { return 1; }
campaign_cleanup() { local prior=\$?; cleanup_running=1; printf 'cleanup-saw-status=%s\n' "\$prior" >"\$LOCAL_EVIDENCE_DIR/cleanup.txt"; printf 'hfx: campaign x has zero Hetzner footprint\n' >"\$LOCAL_EVIDENCE_DIR/teardown.log"; return 0; }
source "$tmp/wrapper.sh"
false
HARNESS
status=0
bash "$tmp/trap-harness.sh" >"$stdout" 2>"$stderr" || status=$?
[[ "$status" -eq 0 ]] || die "harness exit was $status, expected the cleanup's 0 ($(cat "$stderr"))"
assert_contains "$tmp/trap-evidence/cleanup.txt" 'cleanup-saw-status=1'
assert_contains "$tmp/trap-evidence/oplog.txt" 'exit trap entered with status 1'
assert_contains "$tmp/trap-evidence/milestones.txt" '99-torn-down'
pass 'the exit-trap wrapper turns errexit off and hands the failing status to campaign_cleanup, which runs to teardown'

printf '1..%d\n' "$passed"
