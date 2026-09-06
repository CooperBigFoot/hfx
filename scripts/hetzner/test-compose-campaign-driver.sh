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
# Hardening: a comment or printf mentioning the flag is not the derive step, a trailing comment
# or the literal mode name earns no exemption, and a read in the else branch of the guard refuses.
mutate_fence13() {
    # name, then the text to insert before the mkdir line of fence 13 (or an old/new pair with --replace)
    python3 - "$runbook" "$tmp/$1.md" "$2" "${3-}" <<'PY'
import sys
runbook, out, first, second = sys.argv[1:5]
text = open(runbook).read()
anchor = 'mkdir -p "$control_root/corrected" "$control_root/planetary"\n'
if second:
    assert text.count(first) == 1, first
    text = text.replace(first, second)
else:
    assert text.count(anchor) == 1
    text = text.replace(anchor, first + "\n" + anchor)
open(out, "w").write(text)
PY
    printf '%s\n' "$tmp/$1.md"
}
mutated=$(mutate_fence13 f13-comment-derive '# compare_unit_outlets.py --derive-expected "$adjudication" is documented here
jq -r .unit_count "$adjudication" >/dev/null')
expect_failure --runbook "$mutated" --mode full --out "$tmp/bad"
assert_contains "$stderr" 'reads the adjudication record before it is derived'
mutated=$(mutate_fence13 f13-printf-flag "printf '%s\\n' 'compare_unit_outlets.py --derive-expected \"\$adjudication\"'
jq -r .unit_count \"\$adjudication\" >/dev/null")
expect_failure --runbook "$mutated" --mode full --out "$tmp/bad"
assert_contains "$stderr" 'reads the adjudication record before it is derived'
mutated=$(mutate_fence13 f13-trailing-comment 'jq -r .unit_count "$adjudication" >/dev/null # preserved-off-vm')
expect_failure --runbook "$mutated" --mode full --out "$tmp/bad"
assert_contains "$stderr" 'reads the adjudication record before it is derived'
mutated=$(mutate_fence13 f13-else-read 'else test ! -e "$adjudication"; fi' 'else jq -r .unit_count "$adjudication" >/dev/null; fi')
expect_failure --runbook "$mutated" --mode full --out "$tmp/bad"
assert_contains "$stderr" 'reads the adjudication record before it is derived'
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
EXTENSION_PREFIX=s3://pourpoint-hfx/scratch/tdx-hydro-x
ABSENT_IDS=(1020018110 2020003440 2020065840 2020071190 4020050470 5020049720 6020000010)
ssh() { cat >/dev/null; return 0; }
copy_remote_root() { return 0; }
preserve_root_to_bucket() { printf '%s: objects=0 kept=0 uploaded=0 prefix=%s\n' "\$3" "\$2"; }
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
            printf 'CORPUS_SOURCE=bucket\nCORPUS_PREFIX="s3://pourpoint-hfx/scratch/tdx-hydro-campaign-rehearsal/source corpus"\nCORPUS_FILE_COUNT=8\n'
            printf 'source_root="/mnt/hfx/work/control builds/preserved/7020000010"\ndestination_prefix="s3://pourpoint-hfx/scratch/tdx-hydro-campaign-rehearsal/control reference"\nmanifest_name=control-reference\n'
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
for number in 06 08 10 12 13 17 18 21 22 23; do run_fence_arguments "$number"; done
[[ $(wc -l <"$tmp/fake-ssh/arrived-06-1.txt" | tr -d ' ') == 4 ]] || die 'preserve_root_to_bucket did not deliver four positionals'
[[ $(wc -l <"$tmp/fake-ssh/arrived-10-1.txt" | tr -d ' ') == 4 ]] || die 'the bucket corpus pull did not deliver four positionals'
[[ $(wc -l <"$tmp/fake-ssh/arrived-08-1.txt" | tr -d ' ') == 6 ]] || die 'fence 8 did not deliver six positionals'
grep -q '^10000000000$' "$tmp/fake-ssh/arrived-08-1.txt" || die 'fence 8 root disk reserve did not arrive intact'
grep -q '^2000000000$' "$tmp/fake-ssh/arrived-08-1.txt" || die 'fence 8 required memory did not arrive intact'
grep -q '^/mnt/hfx/work/control builds$' "$tmp/fake-ssh/arrived-22-1.txt" || die 'a path with a space did not survive the remote split'
pass 'every bash -s fence delivers each argument intact through a joined and re-split ssh command, including fence 8 sizing'


# Two gates within the same UTC second must write two records: fence 15 runs `campaign_gate compile-monitor`
# and wait_workload runs it again at once, and the 2026-09-05 dry run saw price-preflight refuse the second
# record with `--out already exists`, counted as a transport failure. Run the real helper behind the fakes.
mkdir -p "$tmp/gate/scripts/hetzner" "$tmp/gate/bin" "$tmp/gate/evidence"
cp -- "$SCRIPT_DIR/price-preflight.sh" "$SCRIPT_DIR/common.sh" "$tmp/gate/scripts/hetzner/"
cat >"$tmp/gate/bin/security" <<'FAKE'
#!/usr/bin/env bash
printf 'TEST-HCLOUD-TOKEN\n'
FAKE
cat >"$tmp/gate/bin/curl" <<'FAKE'
#!/usr/bin/env bash
cat >/dev/null
cat -- "$HFX_TEST_PRICE_FIXTURE"
FAKE
chmod +x "$tmp/gate/bin/security" "$tmp/gate/bin/curl"
printf '%s\n' "$(( $(date +%s) - 60 ))" >"$tmp/gate/evidence/provisioning-request-epoch.txt"
cat >"$tmp/gate-harness.sh" <<HARNESS
set -Eeuo pipefail
IFS=\$'\n\t'
cd "$tmp/gate"
export PATH="$tmp/gate/bin:\$PATH"
export HFX_TEST_PRICE_FIXTURE=$SCRIPT_DIR/fixtures/pricing-fsn1.json
LOCAL_EVIDENCE_DIR=$tmp/gate/evidence
SERVER_TYPE=cx23; LOCATION=fsn1; VOLUME_SIZE_GB=10
ELAPSED_CEILING_HOURS=72; BILLABLE_OUTBOUND_BYTES=0; BUDGET_CEILING_EUR=40.00
# Pin the clock so both gates fall in the same second whatever the machine does between them.
date() { case "\$1 \${2-}" in '-u +%Y%m%dT%H%M%SZ') printf '20260905T170000Z\n' ;; *) command date "\$@" ;; esac; }
sleep() { printf 'slept %s\n' "\$1" >>"$tmp/gate/sleeps.txt"; }
source "$tmp/full/fence-proof/driver-04.sh"
campaign_gate compile-monitor 20
campaign_gate compile-monitor 20
HARNESS
bash "$tmp/gate-harness.sh" >"$stdout" 2>"$stderr" || die "two gates in one second did not both pass ($(cat "$stderr"))"
[[ $(ls "$tmp/gate/evidence" | grep -c '^gate-compile-monitor-20260905T170000Z-000[12]\.json$') == 2 ]] ||
    die "two gates in one second did not write two records: $(ls "$tmp/gate/evidence" | tr '\n' ' ')"
[[ $(grep -c '^record=' "$tmp/gate/evidence/gates.log") == 2 ]] || die 'gates.log does not carry both records'
[[ ! -e "$tmp/gate/evidence/gate-transport-failures.log" ]] || die 'a same-second gate was counted as a transport failure'
[[ ! -e "$tmp/gate/sleeps.txt" ]] || die 'a same-second gate slept for a retry'
jq -e '.projected_gross_total_eur | type == "number" or type == "string"' "$tmp/gate/evidence/gate-compile-monitor-20260905T170000Z-0002.json" >/dev/null ||
    die 'the second gate record is not a price preflight record'
pass 'two campaign gates within one second write two distinct records and record no transport failure'

# Every top-level `a && b` list in a fence would continue after a failed non-final member under errexit
# (the swap chain of fence 8 skipped swapon after a failed mkswap). Only conditions, `&& break`,
# `&& status=1`, the per-basin count, `[[ ... ]]`, and `(cd ... && ...)` subshells whose status is
# the list's own may keep the form; everything else is one statement per line.
and_lists() {
    local number
    for number in $(seq -w 1 25); do
        awk -v fence="$number" '/ && / { line = $0; sub(/^[[:space:]]+/, "", line); if (line ~ /^#/) next; print fence ":" line }' \
            "$tmp/full/fence-proof/driver-$number.sh"
    done
}
and_list_count=0
while IFS=: read -r number line; do
    and_list_count=$((and_list_count + 1))
    case $line in
        'if '*|'while '*|'until '*|'elif '*|*' && break'|*' && '*'=1'|'test "$(for id in '*'&& printf '*|'[['*|'(cd '*|'"cd '*'&& sha256sum -c '*) continue ;;
    esac
    die "fence $number carries a top-level && list that fails open under errexit: $line"
done < <(and_lists)
[[ "$and_list_count" -ge 20 ]] || die "the && audit saw only $and_list_count lines; the fence proof was not scanned"
pass 'no fence carries a top-level && list whose non-final member could fail without stopping the driver'

# The maintainer's 2026-09-06 directive: no dataset byte is written to the workstation. The composer refuses
# a driver in which copy_remote_root names a dataset root or an rsync/scp from the VM names anything but a
# single record file; uploads to the VM and the copy_remote_root definition itself stay allowed.
for forbidden in '"$LOCAL_EVIDENCE_DIR/off-vm/campaign/basin-outputs' '"$LOCAL_EVIDENCE_DIR/off-vm/extension' '"$LOCAL_EVIDENCE_DIR/control-reference/' 'copy_remote_root "$CONTROL_ROOT"' 'copy_remote_root "$CAMPAIGN_DIR/basin-outputs"' 'copy_remote_root "$CAMPAIGN_DIR/assembly/dataset"' '"$LOCAL_EVIDENCE_DIR/salvage/extension"'; do
    ! grep -qF -- "$forbidden" "$tmp/full/campaign-driver.sh" || die "the composed driver still names the workstation dataset path $forbidden"
done
grep -qF -- 'preserve_root_to_bucket "$CAMPAIGN_DIR/basin-outputs" "$EXTENSION_PREFIX/basin-outputs" basin-outputs' "$tmp/full/campaign-driver.sh" || die 'basin outputs are not preserved to the bucket'
grep -qF -- 'preserve_root_to_bucket "$CONTROL_ROOT" "$EXTENSION_PREFIX/control-builds" control-builds' "$tmp/full/campaign-driver.sh" || die 'control builds are not preserved to the bucket'
grep -qF -- 'preserve_root_to_bucket "$CAMPAIGN_DIR/downloads" "$EXTENSION_PREFIX/source-corpus" source-corpus' "$tmp/full/campaign-driver.sh" || die 'the source corpus is not preserved to the bucket'
grep -qF -- 'preserve_root_to_bucket "$CONTROL_ROOT/preserved/$CONTROL_ID" "$PRESERVED_CONTROL" control-reference' "$tmp/full/campaign-driver.sh" || die 'the VM-built reference control is not preserved to the bucket'
grep -qF -- '"$EXTENSION_PREFIX/salvage/extension/dataset" salvage-extension-dataset' "$tmp/full/campaign-driver.sh" || die 'salvage does not preserve the assembled dataset to the bucket'
perl -0pe 's/(copy_remote_root "\$CAMPAIGN_DIR\/reports" "\$LOCAL_EVIDENCE_DIR\/off-vm\/campaign"\ncopy_remote_root "\$CAMPAIGN_DIR\/state")/copy_remote_root "\$CAMPAIGN_DIR\/basin-outputs" "\$LOCAL_EVIDENCE_DIR\/off-vm\/campaign"\n$1/' "$runbook" >"$tmp/dataset-root-runbook.md"
grep -q 'copy_remote_root "$CAMPAIGN_DIR/basin-outputs" "$LOCAL_EVIDENCE_DIR/off-vm/campaign"' "$tmp/dataset-root-runbook.md" || die 'mutation did not apply'
expect_failure --runbook "$tmp/dataset-root-runbook.md" --mode full --out "$tmp/bad"
assert_contains "$stderr" 'copies a non-record root from the VM to the workstation'
perl -0pe 's/(  scp -o BatchMode=yes "root@\$SERVER_IP:\/mnt\/hfx\/work\/sha256\/expected-control-sha256\.json" "\$LOCAL_EVIDENCE_DIR\/expected-control-sha256\.json"\n)/$1  rsync -a -e ssh "root\@\$SERVER_IP:\$CONTROL_ROOT\/preserved\/\$CONTROL_ID\/" "\$LOCAL_EVIDENCE_DIR\/control-reference\/"\n/' "$runbook" >"$tmp/dataset-rsync-runbook.md"
grep -q 'rsync -a -e ssh "root@$SERVER_IP:$CONTROL_ROOT/preserved/$CONTROL_ID/"' "$tmp/dataset-rsync-runbook.md" || die 'mutation did not apply'
expect_failure --runbook "$tmp/dataset-rsync-runbook.md" --mode full --out "$tmp/bad"
assert_contains "$stderr" 'copies dataset bytes from the VM to the workstation'
# The check is token based: a copy hidden behind `do`, `&&`, or `time`, an ssh redirected into a dataset
# path, and an `aws s3 cp` onto the workstation are all refused; VM-side heredoc lines stay exempt.
anchor='  scp -o BatchMode=yes "root@$SERVER_IP:/mnt/hfx/work/sha256/expected-control-sha256.json" "$LOCAL_EVIDENCE_DIR/expected-control-sha256.json"'
grep -qF -- "$anchor" "$runbook" || die 'fence 12 anchor line is missing'
inject_after_anchor() {  # $1 = name, $2 = the line to inject after the fence 12 anchor
    ANCHOR="$anchor" INJECT="$2" perl -pe 'BEGIN { $a = $ENV{ANCHOR}; $i = $ENV{INJECT} } if (index($_, $a) == 0) { $_ .= "$i\n" }' "$runbook" >"$tmp/$1-runbook.md"
    grep -qF -- "$2" "$tmp/$1-runbook.md" || die "mutation $1 did not apply"
    expect_failure --runbook "$tmp/$1-runbook.md" --mode full --out "$tmp/bad"
}
inject_after_anchor do-rsync '  for tree in preserved; do rsync -a -e ssh "root@$SERVER_IP:$CONTROL_ROOT/$tree/" "$LOCAL_EVIDENCE_DIR/trees/"; done'
assert_contains "$stderr" 'copies dataset bytes from the VM to the workstation'
inject_after_anchor and-rsync '  test -d "$LOCAL_EVIDENCE_DIR" && rsync -a -e ssh "root@$SERVER_IP:$CONTROL_ROOT/preserved/" "$LOCAL_EVIDENCE_DIR/trees/"'
assert_contains "$stderr" 'copies dataset bytes from the VM to the workstation'
inject_after_anchor time-scp '  time scp -r -o BatchMode=yes "root@$SERVER_IP:$CONTROL_ROOT/preserved" "$LOCAL_EVIDENCE_DIR/trees"'
assert_contains "$stderr" 'copies dataset bytes from the VM to the workstation'
inject_after_anchor ssh-redirect '  ssh -o BatchMode=yes "root@$SERVER_IP" tar -C "$CONTROL_ROOT" -cf - preserved > "$LOCAL_EVIDENCE_DIR/preserved.tar"'
assert_contains "$stderr" 'redirects VM output into a non-record workstation path'
inject_after_anchor ssh-tee '  ssh -o BatchMode=yes "root@$SERVER_IP" cat "$CONTROL_ROOT/preserved/$CONTROL_ID/graph.parquet" | tee "$LOCAL_EVIDENCE_DIR/graph.parquet" >/dev/null'
assert_contains "$stderr" 'redirects VM output into a non-record workstation path'
inject_after_anchor aws-download '  aws s3 cp "$PRESERVED_CONTROL/" "$LOCAL_EVIDENCE_DIR/control-reference/" --recursive --endpoint-url "$S3_ENDPOINT" --region fsn1'
assert_contains "$stderr" 'copies bucket objects onto the workstation'
inject_after_anchor do-copy-root '  for root in basin-outputs; do copy_remote_root "$CAMPAIGN_DIR/$root" "$LOCAL_EVIDENCE_DIR/off-vm/campaign"; done'
assert_contains "$stderr" 'copies a non-record root from the VM to the workstation'
pass 'the composed driver preserves every dataset tree to the bucket, and the composer refuses a copy of dataset bytes from the VM to the workstation behind do, &&, time, an ssh redirection or tee, or aws s3 cp'

# Byte totals are summed in shell integer arithmetic. awk prints an accumulated sum through its default
# number format, and the Debian 12 mawk wrote the 114063230627-byte baseline as 1.14063e+11, which ended the
# 2026-09-05 production lifecycle at the fence 18 byte check after the baseline had been fully pulled. The
# composer refuses that form, and the extracted check must total ten 20 GB sizes exactly with no awk on the path.
byte_sum='{ total=0; while read -r size; do [[ "$size" =~ ^[0-9]+$ ]] || exit 1; total=$((total + size)); done; printf '"'"'%s\n'"'"' "$total"; }'
old_sum="awk '{s+=\$1} END {print s}'"
byte_check=$(grep -F -- '-eq "$baseline_exported_bytes"' "$tmp/expected/expected-18.sh")
[[ $(printf '%s\n' "$byte_check" | wc -l | tr -d ' ') == 1 ]] || die 'fence 18 does not carry exactly one baseline byte check'
[[ "$byte_check" == *"$byte_sum"* ]] || die 'the fence 18 baseline byte check does not sum in shell integer arithmetic'
grep -c -F -- "$byte_sum" "$runbook" | grep -q -x 3 || die 'the runbook does not carry the shell integer sum at the corpus, baseline, and artifact checks'
! grep -q -F -- "END {print s" "$tmp"/expected/expected-*.sh || die 'a fence still prints an awk sum through the default number format'
SUM="$byte_sum" OLD="$old_sum" perl -pe 'BEGIN { $s = $ENV{SUM}; $o = $ENV{OLD} } s/\Q$s\E/$o/ if index($_, q{$baseline_exported_bytes}) >= 0' "$runbook" >"$tmp/awk-sum-runbook.md"
grep -q -F -- "$old_sum" "$tmp/awk-sum-runbook.md" || die 'mutation did not apply'
expect_failure --runbook "$tmp/awk-sum-runbook.md" --mode full --out "$tmp/bad"
grep -E -- "fence 18 \(baseline-pull\) line [0-9]+ prints an awk sum through awk's default number format" "$stderr" >/dev/null || die 'the composer did not name the fence 18 awk sum'
mkdir -p "$tmp/no-awk"
printf '#!/usr/bin/env bash\nprintf "byte check invoked awk\\n" >&2\nexit 99\n' >"$tmp/no-awk/awk"
chmod +x "$tmp/no-awk/awk"
printf '20000000000\n%.0s' 1 2 3 4 5 6 7 8 9 10 >"$tmp/sizes.txt"
write_byte_check() {  # $1 = output script, $2 = the check line; the find over the pulled dataset is replaced by the size list
    local find_sizes='find "$baseline_root/dataset" -type f -exec stat -c '"'"'%s'"'"' {} +'
    [[ "$2" == *"$find_sizes"* ]] || die 'the baseline byte check does not list the pulled sizes with find and stat'
    printf 'set -Eeuo pipefail\nsizes=%q\n%s\n' "$tmp/sizes.txt" "${2/"$find_sizes"/cat \"\$sizes\"}" >"$1"
}
write_byte_check "$tmp/byte-check.sh" "$byte_check"
PATH="$tmp/no-awk:$PATH" baseline_exported_bytes=200000000000 bash "$tmp/byte-check.sh" || die 'the shell integer sum did not total ten 20 GB sizes to 200000000000'
! PATH="$tmp/no-awk:$PATH" baseline_exported_bytes=200000000001 bash "$tmp/byte-check.sh" 2>/dev/null || die 'the byte check accepted a total one byte off'
printf '20000000000\nx\n' >"$tmp/bad-sizes.txt"
! PATH="$tmp/no-awk:$PATH" baseline_exported_bytes=20000000000 bash -c 'sizes=$1; '"$(sed -n 3p "$tmp/byte-check.sh")" _ "$tmp/bad-sizes.txt" 2>/dev/null ||
    die 'the byte check accepted a non-numeric size'
write_byte_check "$tmp/awk-byte-check.sh" "${byte_check/"$byte_sum"/$old_sum}"
grep -q -F -- "$old_sum" "$tmp/awk-byte-check.sh" || die 'the old awk form was not restored for the comparison'
awk_status=0
PATH="$tmp/no-awk:$PATH" baseline_exported_bytes=200000000000 bash "$tmp/awk-byte-check.sh" >"$tmp/awk-byte-check.out" 2>&1 || awk_status=$?
[[ "$awk_status" -ne 0 ]] || die 'the old awk form passed without awk'
grep -q 'byte check invoked awk' "$tmp/awk-byte-check.out" || die 'the old form did not depend on awk'
# Where an awk prints 2^31 in exponent form (the Debian 12 mawk does), the old check fails on the exact total.
if command -v mawk >/dev/null && [[ $(printf '2147483648\n' | mawk '{s+=$1} END {print s}') == *e+* ]]; then
    mkdir -p "$tmp/mawk-as-awk"
    ln -s "$(command -v mawk)" "$tmp/mawk-as-awk/awk"
    ! PATH="$tmp/mawk-as-awk:$PATH" baseline_exported_bytes=200000000000 bash "$tmp/awk-byte-check.sh" 2>"$tmp/mawk.err" || die 'the old awk form passed under an exponent-printing mawk'
    grep -q 'integer expression expected' "$tmp/mawk.err" || die 'the old awk form did not fail on the exponent form'
    PATH="$tmp/mawk-as-awk:$PATH" baseline_exported_bytes=200000000000 bash "$tmp/byte-check.sh" || die 'the shell integer sum failed under an exponent-printing mawk'
fi
pass 'the baseline byte check totals ten 20 GB sizes exactly in shell arithmetic with no awk, and the composer refuses the awk default-format sum'
printf '1..%d\n' "$passed"
