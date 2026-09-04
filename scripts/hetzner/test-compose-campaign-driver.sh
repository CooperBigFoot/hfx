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
assert_contains "$tmp/full/campaign-driver.sh" 'wait_workload tdx-compile compile-monitor'
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
for stage in "${stages[@]}"; do assert_contains "$tmp/resume-$stage/campaign-driver.sh" 'export HFX_CAMPAIGN_RESUME=1'; done
assert_not_contains "$tmp/full/campaign-driver.sh" 'export HFX_CAMPAIGN_RESUME=1'
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
perl -0pe 's/if \[ "\$control_reference" = preserved-off-vm \]; then\n  test "\$\(jq -r .\.orientation_digest. "\$control_root\/corrected\/\$control_id-orient\.json"\)" = "\$\(jq -r .\.corrected_orientation_digest. "\$adjudication"\)"\nfi\n/test "\$(jq -r \x27.orientation_digest\x27 "\$control_root\/corrected\/\$control_id-orient.json")" = "\$(jq -r \x27.corrected_orientation_digest\x27 "\$adjudication")"\n/' "$runbook" >"$tmp/unguarded-runbook.md"
grep -q '^test "$(jq -r '"'"'.orientation_digest'"'"'' "$tmp/unguarded-runbook.md" || die 'unguarded mutation did not apply'
expect_failure --runbook "$tmp/unguarded-runbook.md" --mode full --out "$tmp/bad"
assert_contains "$stderr" 'reads the adjudication record before it is derived and outside the preserved-off-vm guard'
pass 'the composer refuses a control-build fence that reads the adjudication record before deriving it'

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

# wait_workload tolerates up to ten consecutive transient status failures (a lost SSH connection during a
# 7 to 14 hour compile) and returns 0 when the workload finishes.
sed -n '/^wait_workload() {$/,/^}$/p' "$tmp/full/campaign-driver.sh" >"$tmp/wait_workload.sh"
[[ -s "$tmp/wait_workload.sh" ]] || die 'wait_workload was not found in the driver'
mkdir -p "$tmp/poll/scripts/hetzner"
cat >"$tmp/poll/scripts/hetzner/launch.sh" <<'LAUNCH'
#!/usr/bin/env bash
count_file=$HFX_TEST_POLL_COUNT
count=$(( $(cat "$count_file") + 1 ))
printf '%s\n' "$count" >"$count_file"
if ((count <= 3)); then printf 'hfx: error: ssh lost\n' >&2; exit 1; fi
if ((count <= 5)); then printf 'launch: session is running\n'; exit 0; fi
printf 'launch: session is not running\n'; exit 3
LAUNCH
chmod +x "$tmp/poll/scripts/hetzner/launch.sh"
printf '0\n' >"$tmp/poll/count"
cat >"$tmp/poll-harness.sh" <<HARNESS
set -Eeuo pipefail
cd "$tmp/poll"
export HFX_TEST_POLL_COUNT=$tmp/poll/count
LOCAL_EVIDENCE_DIR=$tmp/poll
CAMPAIGN=campaign-rehearsal
POLL_SECONDS=0
campaign_gate() { return 0; }
before_hour() { return 0; }
oplog() { printf '%s\n' "\$*" >>"$tmp/poll/oplog.txt"; }
sleep() { :; }
source "$tmp/wait_workload.sh"
wait_workload tdx-compile compile-monitor 20 48
HARNESS
bash "$tmp/poll-harness.sh" >"$stdout" 2>"$stderr" || die "wait_workload did not return 0 after transient failures ($(cat "$stderr"))"
[[ $(cat "$tmp/poll/count") == 6 ]] || die 'wait_workload did not keep polling through three transient failures'
[[ $(grep -c 'transient' "$tmp/poll/oplog.txt") == 3 ]] || die 'transient failures were not logged'
printf '0\n' >"$tmp/poll/count"
cat >"$tmp/poll/scripts/hetzner/launch.sh" <<'LAUNCH'
#!/usr/bin/env bash
printf 'hfx: error: ssh lost\n' >&2; exit 1
LAUNCH
status=0
bash "$tmp/poll-harness.sh" >"$stdout" 2>"$stderr" || status=$?
[[ "$status" -eq 1 ]] || die "wait_workload should return 1 after ten persistent failures, returned $status"
pass 'the compile monitor rides through transient status failures and gives up only after ten in a row'

# Fence 17 must record an empty basin-outputs manifest as a truthful outcome instead of aborting.
mkdir -p "$tmp/f17/off-vm/sha256" "$tmp/f17/off-vm/campaign"
: >"$tmp/f17/off-vm/sha256/basin-outputs-sha256.txt"
for id in 1020018110 2020003440 2020065840 2020071190 4020050470 5020049720 6020000010 7020000010; do
    mkdir -p "$tmp/f17/off-vm/campaign/state/basins/$id"
    printf '{"stages":{"compile":{"status":"failed"}}}\n' >"$tmp/f17/off-vm/campaign/state/basins/$id/current.json"
done
cat >"$tmp/f17-harness.sh" <<HARNESS
set -Eeuo pipefail
IFS=\$'\n\t'
LOCAL_EVIDENCE_DIR=$tmp/f17
CAMPAIGN_DIR=/mnt/hfx/work/tdx-hydro-x
SERVER_IP=127.0.0.1
ABSENT_IDS=(1020018110 2020003440 2020065840 2020071190 4020050470 5020049720 6020000010)
ssh() { cat >/dev/null; return 0; }
copy_remote_root() { return 0; }
source "$tmp/full/fence-proof/driver-17.sh"
HARNESS
bash "$tmp/f17-harness.sh" >"$stdout" 2>"$stderr" || die "fence 17 aborted on an empty basin-outputs manifest ($(cat "$stderr"))"
assert_contains "$tmp/f17/basin-outputs-verification.txt" 'no basin output was produced'
[[ ! -s "$tmp/f17/compiled-absent-basins.txt" ]] || die 'compiled-absent-basins.txt should be empty when every compile failed'
pass 'an empty basin-outputs manifest records a truthful empty outcome instead of aborting'

# Every `bash -s --` fence must send its arguments as the quoted remote_args array, and the
# composer must refuse any other form (the 2026-09-04 rehearsal died on a space-joined argument).
sed 's|bash -s -- "$(remote_tokens "${remote_args\[@\]}")" <<'"'"'REMOTE'"'"' 2>&1 \| tee "$LOCAL_EVIDENCE_DIR/converge.log"|bash -s -- "$GROUND_TRUTH_REF" "$(remote_tokens "${remote_args[@]}")" <<'"'"'REMOTE'"'"' 2>\&1 \| tee "$LOCAL_EVIDENCE_DIR/converge.log"|' "$runbook" >"$tmp/unquoted-runbook.md"
grep -q 'bash -s -- "$GROUND_TRUTH_REF" "$(remote_tokens' "$tmp/unquoted-runbook.md" || die 'mutation did not apply'
expect_failure --runbook "$tmp/unquoted-runbook.md" --mode full --out "$tmp/bad"
assert_contains "$stderr" 'sends remote arguments that are not the quoted remote_args array'
perl -0pe 's/  remote_args=\("\$CAMPAIGN_DIR" "\$CONTROL_ROOT"\)\n//' "$runbook" >"$tmp/no-assignment-runbook.md"
grep -q 'remote_args=("$CAMPAIGN_DIR" "$CONTROL_ROOT")' "$tmp/no-assignment-runbook.md" && die 'mutation did not apply'
expect_failure --runbook "$tmp/no-assignment-runbook.md" --mode full --out "$tmp/bad"
assert_contains "$stderr" 'has no remote_args assignment immediately before its ssh line'
pass 'the composer refuses a bash -s fence with unquoted arguments or without a remote_args assignment'

# Execute every bash -s fence's argument construction through a fake ssh that joins its remote
# command into one string and re-splits it, exactly as the real ssh and remote login shell do,
# then run the remote script's positional validation and dump what arrived.
mkdir -p "$tmp/fake-ssh/bin"
cat >"$tmp/fake-ssh/bin/ssh" <<'FAKE'
#!/usr/bin/env bash
# Drop options and the host, join the remaining words with spaces, and let a shell re-split them.
while (($#)); do
    case $1 in
        -o) shift 2 ;;
        -*) shift ;;
        *) break ;;
    esac
done
shift   # host
joined="$*"
exec bash -c "$joined"
FAKE
chmod +x "$tmp/fake-ssh/bin/ssh"
# A resolved rehearsal contract: the tracked record's DERIVED placeholders replaced by numbers.
contract=$tmp/fake-ssh/contract.json
jq '.baseline.exported_bytes = 13184 | .baseline.object_count = 6 | .source_corpus.total_bytes = 786432' "$SCRIPT_DIR/rehearsal-campaign-contract.json" >"$contract"
run_fence_arguments() {
    local number=$1 fence=$tmp/full/fence-proof/driver-$1.sh site=0
    local ssh_lines
    ssh_lines=$(grep -n 'bash -s -- "$(remote_tokens "${remote_args\[@\]}")"' "$fence" | cut -d: -f1)
    [[ -n "$ssh_lines" ]] || die "fence $number has no bash -s site"
    for ssh_line in $ssh_lines; do
        site=$((site + 1))
        local assignment_line=$((ssh_line - 1)) heredoc_start=$((ssh_line + 1)) heredoc_end header_end
        sed -n "${assignment_line}p" "$fence" | grep -q 'remote_args=(' || assignment_line=$((ssh_line - 2))
        heredoc_end=$(awk -v start="$heredoc_start" 'NR >= start && /^REMOTE$/ { print NR; exit }' "$fence")
        header_end=$(awk -v start="$heredoc_start" -v end="$heredoc_end" 'NR >= start && NR < end && /^test "\$#" -eq [0-9]+$/ { print NR; exit }' "$fence")
        [[ -n "$heredoc_end" && -n "$header_end" ]] || die "fence $number site $site lacks a heredoc or its positional count check"
        local expected_count
        expected_count=$(sed -n "${header_end}p" "$fence" | sed -E 's/.* -eq ([0-9]+)$/\1/')
        {
            printf 'set -Eeuo pipefail\nIFS=$'"'"'\\n\\t'"'"'\n'
            printf 'contract_value() { jq -er "$1" %s; }\n' "$contract"
            printf 'remote_tokens() { local token; for token in "$@"; do printf '"'"'%%q '"'"' "$token"; done; }\n'
            printf 'GROUND_TRUTH_REF=0123456789abcdef0123456789abcdef01234567\nSERVER_IP=127.0.0.1\nLOCAL_EVIDENCE_DIR=%s\n' "$tmp/fake-ssh/evidence"
            printf 'CAMPAIGN=campaign-rehearsal\nCAMPAIGN_DIR="/mnt/hfx/work/tdx-hydro-campaign-rehearsal"\nCONTROL_ROOT="/mnt/hfx/work/control builds"\nBASELINE_ROOT="/mnt/hfx/work/base line"\n'
            printf 'CONTROL_ID=7020000010\nCONTROL_FABRIC_VERSION="0.3.0 (rehearsal)"\nCONTROL_REFERENCE=vm-planetary-build\nFABRIC_VERSION="NGA-TDX-Hydro-20230126 rehearsal"\n'
            printf 'BASELINE_PREFIX="s3://pourpoint-hfx/scratch/tdx-hydro-campaign-rehearsal/base line"\nEXTENSION_PREFIX="s3://pourpoint-hfx/scratch/tdx-hydro-campaign-rehearsal"\nS3_ENDPOINT=https://fsn1.your-objectstorage.com\n'
            sed -n "${assignment_line}p" "$fence"
            printf 'printf '"'"'%%s\\n'"'"' "${remote_args[@]}" > %s\n' "$tmp/fake-ssh/expected-$number-$site.txt"
            printf 'ssh -o BatchMode=yes "root@$SERVER_IP" bash -s -- "$(remote_tokens "${remote_args[@]}")" <<'"'"'REMOTE'"'"'\n'
            sed -n "${heredoc_start},${header_end}p" "$fence"
            printf 'printf '"'"'%%s\\n'"'"' "$@" > %s\n' "$tmp/fake-ssh/arrived-$number-$site.txt"
            printf 'REMOTE\n'
        } >"$tmp/fake-ssh/harness-$number-$site.sh"
        mkdir -p "$tmp/fake-ssh/evidence"
        PATH="$tmp/fake-ssh/bin:$PATH" bash "$tmp/fake-ssh/harness-$number-$site.sh" >"$stdout" 2>"$stderr" ||
            die "fence $number site $site: the remote side refused the arguments it received ($(cat "$stderr"))"
        diff -u "$tmp/fake-ssh/expected-$number-$site.txt" "$tmp/fake-ssh/arrived-$number-$site.txt" >/dev/null ||
            die "fence $number site $site: the VM would receive different arguments than the fence sent"
        [[ $(wc -l <"$tmp/fake-ssh/arrived-$number-$site.txt" | tr -d ' ') == "$expected_count" ]] ||
            die "fence $number site $site: arrived count differs from the remote positional count check"
    done
}
for number in 08 12 13 17 18 21 22 23; do run_fence_arguments "$number"; done
[[ $(wc -l <"$tmp/fake-ssh/arrived-08-1.txt" | tr -d ' ') == 6 ]] || die 'fence 8 did not deliver six positionals'
grep -q '^10000000000$' "$tmp/fake-ssh/arrived-08-1.txt" || die 'fence 8 root disk reserve did not arrive intact'
grep -q '^2000000000$' "$tmp/fake-ssh/arrived-08-1.txt" || die 'fence 8 required memory did not arrive intact'
grep -q '^/mnt/hfx/work/control builds$' "$tmp/fake-ssh/arrived-22-1.txt" || die 'a path with a space did not survive the remote split'
pass 'every bash -s fence delivers each argument intact through a joined and re-split ssh command, including fence 8 sizing'

printf '1..%d\n' "$passed"
