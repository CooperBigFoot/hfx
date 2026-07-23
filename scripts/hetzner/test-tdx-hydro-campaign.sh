#!/usr/bin/env bash

if [ -z "${BASH_VERSION-}" ]; then
    printf '%s\n' 'test-tdx-hydro-campaign: error: Bash >=3.2 is required; observed non-Bash interpreter' >&2
    exit 1
fi
bash_major=${BASH_VERSION%%.*}
bash_remainder=${BASH_VERSION#*.}
bash_minor=${bash_remainder%%.*}
if [ "$bash_major" -lt 3 ] || { [ "$bash_major" -eq 3 ] && [ "$bash_minor" -lt 2 ]; }; then
    printf 'test-tdx-hydro-campaign: error: Bash >=3.2 is required; observed %s\n' "$BASH_VERSION" >&2
    exit 1
fi

set -Eeuo pipefail
IFS=$'\n\t'
set +x

die() {
    printf 'test-tdx-hydro-campaign: error: %s\n' "$*" >&2
    exit 1
}

pass() {
    passed=$((passed + 1))
    printf 'ok %d - %s\n' "$passed" "$1"
}

expect_failure() {
    local label=$1
    shift
    if "$selected_bash" "$runner" "$@" >"$case_stdout" 2>"$case_stderr"; then
        die "$label unexpectedly succeeded"
    fi
    [[ -s "$case_stderr" ]] || die "$label produced no diagnostic"
}

assert_contains() {
    local file=$1
    local text=$2
    grep -F -- "$text" "$file" >/dev/null || die "expected '$text' in $file"
}

run_runner() {
    "$selected_bash" "$runner" "$@"
}

init_args() {
    printf '%s\n' \
        init --campaign "$1" --workspace-root "$2" \
        --available-memory-bytes 11 \
        --available-disk-bytes 26 \
        --retained-input-bytes 5 \
        --retained-basin-output-bytes 6 \
        --assembly-memory-ceiling-bytes 11 \
        --assembly-scratch-ceiling-bytes 7 \
        --assembled-artifact-bytes 8
}

copy_workspace() {
    local name=$1
    local destination=$test_tmp/workspaces/$name
    mkdir "$destination"
    cp -R "$valid_root/tdx-hydro-equal" "$destination/tdx-hydro-equal"
    printf '%s\n' "$destination"
}

SCRIPT_DIR=$(cd -P -- "${BASH_SOURCE[0]%/*}" && pwd)
repo_root=$(cd -P -- "$SCRIPT_DIR/../.." && pwd)
runner=$SCRIPT_DIR/tdx-hydro-campaign.sh
inventory=$repo_root/adapters/tdx-hydro/data/tdx_header_numbers.json

if [[ -x /bin/bash ]]; then
    selected_bash=/bin/bash
else
    if [[ -n "${HFX_TEST_BASH-}" ]]; then
        selected_bash=$HFX_TEST_BASH
    else
        selected_bash=$(command -v bash || true)
    fi
    [[ -n "$selected_bash" && -x "$selected_bash" ]] || die 'no usable Bash interpreter was found'
    printf '%s\n' 'test-tdx-hydro-campaign: warning: /bin/bash is unavailable; the Bash 3.2 floor is not being exercised' >&2
fi
selected_version=$("$selected_bash" -c 'printf "%s\n" "$BASH_VERSION"')
selected_major=${selected_version%%.*}
selected_remainder=${selected_version#*.}
selected_minor=${selected_remainder%%.*}
if [[ "$selected_major" -lt 3 ]] || { [[ "$selected_major" -eq 3 ]] && [[ "$selected_minor" -lt 2 ]]; }; then
    die "selected Bash is older than 3.2: $selected_version"
fi
printf 'test-tdx-hydro-campaign: selected interpreter %s (%s)\n' "$selected_bash" "$selected_version"
if [[ -x /bin/bash && "$selected_bash" != /bin/bash ]]; then
    die 'the harness did not select /bin/bash'
fi

for command_name in jq grep diff find sort wc mktemp mkdir cp rm mv chmod tr sed ln touch git sleep; do
    command -v "$command_name" >/dev/null 2>&1 || die "required command is unavailable: $command_name"
done
[[ -f "$runner" ]] || die "runner is missing: $runner"
[[ -f "$inventory" ]] || die "inventory is missing: $inventory"

test_tmp=$(mktemp -d "${TMPDIR:-/tmp}/hfx-tdx-campaign-test.XXXXXX")
case $test_tmp in
    "${TMPDIR:-/tmp}"/hfx-tdx-campaign-test.*) ;;
    *) die "mktemp returned an unsafe path: $test_tmp" ;;
esac
cleanup() {
    case ${test_tmp-} in
        "${TMPDIR:-/tmp}"/hfx-tdx-campaign-test.*)
            [[ -d "$test_tmp" && ! -L "$test_tmp" ]] && rm -rf -- "$test_tmp"
            ;;
    esac
}
trap cleanup EXIT

mkdir "$test_tmp/fake-bin" "$test_tmp/invocations" "$test_tmp/workspaces"
git -C "$repo_root" status --porcelain=v1 | sed '/^?? pr-body\.md$/d' >"$test_tmp/repository-status-before"
case_stdout=$test_tmp/stdout
case_stderr=$test_tmp/stderr
for poison in hcloud curl aws ssh; do
    sed -e "s/@NAME@/$poison/g" >"$test_tmp/fake-bin/$poison" <<'POISON'
#!/bin/sh
printf '%s\n' "$*" >>"${HFX_TEST_INVOCATIONS:?}/@NAME@.log"
exit 97
POISON
    chmod +x "$test_tmp/fake-bin/$poison"
done
sed >"$test_tmp/fake-ps" <<'FAKE_PS'
#!/bin/sh
if [ "${HFX_TEST_PS_MODE-}" = live ]; then
    printf '%s\n' "$2"
    exit 0
fi
if [ "${HFX_TEST_PS_MODE-}" = error ]; then
    printf '%s\n' 'injected ps failure' >&2
    exit 2
fi
exit 1
FAKE_PS
chmod +x "$test_tmp/fake-ps"
export HFX_TDX_PS=$test_tmp/fake-ps
export HFX_TEST_INVOCATIONS=$test_tmp/invocations
PATH=$test_tmp/fake-bin:$PATH
export PATH
passed=0

run_runner -h >"$case_stdout"
run_runner --help >"$case_stdout"
assert_contains "$case_stdout" 'Usage: tdx-hydro-campaign.sh init'
pass 'sole help arguments succeed'

argument_root=$test_tmp/workspaces/arguments
mkdir "$argument_root"
expect_failure 'missing subcommand'
expect_failure 'invalid campaign' status --campaign Bad --workspace-root "$argument_root"
expect_failure 'duplicate option' status --campaign duplicate --campaign duplicate --workspace-root "$argument_root"
expect_failure 'missing value' status --campaign
expect_failure 'option-shaped value' status --campaign --workspace-root
expect_failure 'unknown option' status --campaign unknown --workspace-root "$argument_root" --bogus value
expect_failure 'relative workspace root' status --campaign relative --workspace-root relative
mkdir "$test_tmp/workspaces/symlink-target"
ln -s "$test_tmp/workspaces/symlink-target" "$test_tmp/workspaces/symlink-root"
expect_failure 'symlink workspace root' status --campaign symlink --workspace-root "$test_tmp/workspaces/symlink-root"
expect_failure 'sizing on status' status --campaign sizing --workspace-root "$argument_root" --available-memory-bytes 1
expect_failure 'zero sizing' init --campaign zero --workspace-root "$argument_root" \
    --available-memory-bytes 0 --available-disk-bytes 4 --retained-input-bytes 1 \
    --retained-basin-output-bytes 1 --assembly-memory-ceiling-bytes 1 \
    --assembly-scratch-ceiling-bytes 1 --assembled-artifact-bytes 1
expect_failure 'negative sizing' init --campaign negative --workspace-root "$argument_root" \
    --available-memory-bytes -1 --available-disk-bytes 4 --retained-input-bytes 1 \
    --retained-basin-output-bytes 1 --assembly-memory-ceiling-bytes 1 \
    --assembly-scratch-ceiling-bytes 1 --assembled-artifact-bytes 1
expect_failure 'nonnumeric sizing' init --campaign text --workspace-root "$argument_root" \
    --available-memory-bytes nope --available-disk-bytes 4 --retained-input-bytes 1 \
    --retained-basin-output-bytes 1 --assembly-memory-ceiling-bytes 1 \
    --assembly-scratch-ceiling-bytes 1 --assembled-artifact-bytes 1
expect_failure 'scalar overflow' init --campaign scalar-overflow --workspace-root "$argument_root" \
    --available-memory-bytes 9223372036854775808 --available-disk-bytes 4 --retained-input-bytes 1 \
    --retained-basin-output-bytes 1 --assembly-memory-ceiling-bytes 1 \
    --assembly-scratch-ceiling-bytes 1 --assembled-artifact-bytes 1
touch "$argument_root/tdx-hydro-unsafe"
expect_failure 'unsafe pre-existing path' init --campaign unsafe --workspace-root "$argument_root" \
    --available-memory-bytes 1 --available-disk-bytes 4 --retained-input-bytes 1 \
    --retained-basin-output-bytes 1 --assembly-memory-ceiling-bytes 1 \
    --assembly-scratch-ceiling-bytes 1 --assembled-artifact-bytes 1
pass 'argument and path validation rejects invalid forms'

for script in "$runner" "$SCRIPT_DIR/test-tdx-hydro-campaign.sh"; do
    forbidden_assoc='(declare|local)[[:space:]]+-A'
    forbidden_wait='wait[[:space:]]+-n'
    forbidden_map='map''file'
    forbidden_read='read''array'
    forbidden_case_change='\$\{[^}]*((\^\^)|(,,))'
    forbidden_glob='shopt[[:space:]]+-s[[:space:]]+glob''star'
    forbidden_negative='\[[[:space:]]*-[0-9]+[[:space:]]*\]'
    forbidden_v='\[\[[^]]+[[:space:]]-v[[:space:]]'
    forbidden_nameref='(declare|local)[[:space:]]+-n'
    forbidden_lock='f''lock'
    if grep -En -e "$forbidden_assoc" -e "$forbidden_wait" -e "$forbidden_map" -e "$forbidden_read" \
        -e "$forbidden_case_change" -e "$forbidden_glob" -e "$forbidden_negative" -e "$forbidden_v" \
        -e "$forbidden_nameref" -e "$forbidden_lock" "$script" >"$case_stdout"; then
        die "forbidden Bash-4 construct found in $script"
    fi
done
assert_contains "$runner" 'https://earth-info.nga.mil/php/download.php?file=<processing-basin-id>-<product>-gpkg'
assert_contains "$runner" 'The exact product set is {basins,streamnet}'
pass 'static Bash 3.2 compatibility checks pass'

valid_root=$test_tmp/workspaces/valid
mkdir "$valid_root"
set -- $(init_args equal "$valid_root")
run_runner "$@" >"$case_stdout"
campaign_dir=$valid_root/tdx-hydro-equal
for relative in downloads basin-outputs reports assembly assembly/scratch publication state state/basins state/locks state/tmp; do
    [[ -d "$campaign_dir/$relative" && ! -L "$campaign_dir/$relative" ]] || die "missing layout directory $relative"
done
jq -e '
    keys == ["campaign","inventory","retention","schema_version","sizing"] and
    .schema_version == 1 and .campaign == "equal" and
    .inventory == {source:"adapters/tdx-hydro/data/tdx_header_numbers.json",count:62} and
    .retention == {
      policy:"retain-all-through-publication",reclaim_inputs:false,
      retain_acquired_inputs:true,retain_basin_outputs:true,retain_external_reports:true
    } and
    .sizing == {
      available_memory_bytes:11,available_disk_bytes:26,retained_input_bytes:5,
      retained_basin_output_bytes:6,assembly_memory_ceiling_bytes:11,
      assembly_scratch_ceiling_bytes:7,assembled_artifact_bytes:8,
      required_memory_bytes:11,required_disk_bytes:26
    }
' "$campaign_dir/state/campaign.json" >/dev/null || die 'campaign JSON shape differs'
jq -S '.' "$inventory" >"$test_tmp/expected-inventory.json"
jq -S '.' "$campaign_dir/state/inventory.json" >"$test_tmp/actual-inventory.json"
diff -u "$test_tmp/expected-inventory.json" "$test_tmp/actual-inventory.json"
[[ $(find "$campaign_dir/state/basins" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ') == 62 ]] ||
    die 'basin directory count differs'
[[ $(find "$campaign_dir/state/basins" -name current.json -type f | wc -l | tr -d ' ') == 62 ]] ||
    die 'basin state count differs'
jq -e -s '
    length == 62 and all(
      .schema_version == 2 and
      (.processing_basin_id | test("^[0-9]{10}$")) and
      .stages == {
        acquire_basins:{status:"pending",attempts:0,failure_reason:null,evidence:null},
        acquire_streamnet:{status:"pending",attempts:0,failure_reason:null,evidence:null},
        compile:{status:"pending",attempts:0,failure_reason:null}
      }
    )
' "$campaign_dir"/state/basins/*/current.json >/dev/null || die 'initial basin states differ'
if grep -F '7020000010' "$runner" >/dev/null; then
    die 'runner contains a transcribed processing-basin ID'
fi
pass 'equal-capacity init creates the complete 62-basin contract'

memory_root=$test_tmp/workspaces/memory
mkdir "$memory_root"
expect_failure 'memory undersizing' init --campaign memory --workspace-root "$memory_root" \
    --available-memory-bytes 10 --available-disk-bytes 26 --retained-input-bytes 5 \
    --retained-basin-output-bytes 6 --assembly-memory-ceiling-bytes 11 \
    --assembly-scratch-ceiling-bytes 7 --assembled-artifact-bytes 8
assert_contains "$case_stderr" 'insufficient memory: available 10 bytes; required 11 bytes'
[[ ! -e "$memory_root/tdx-hydro-memory" ]] || die 'memory refusal created campaign state'

disk_root=$test_tmp/workspaces/disk
mkdir "$disk_root"
expect_failure 'disk undersizing' init --campaign disk --workspace-root "$disk_root" \
    --available-memory-bytes 11 --available-disk-bytes 25 --retained-input-bytes 5 \
    --retained-basin-output-bytes 6 --assembly-memory-ceiling-bytes 11 \
    --assembly-scratch-ceiling-bytes 7 --assembled-artifact-bytes 8
assert_contains "$case_stderr" 'insufficient disk: available 25 bytes; required 26 bytes'
[[ ! -e "$disk_root/tdx-hydro-disk" ]] || die 'disk refusal created campaign state'

overflow_root=$test_tmp/workspaces/overflow
mkdir "$overflow_root"
expect_failure 'sum overflow' init --campaign overflow --workspace-root "$overflow_root" \
    --available-memory-bytes 1 --available-disk-bytes 9223372036854775807 \
    --retained-input-bytes 9223372036854775807 --retained-basin-output-bytes 1 \
    --assembly-memory-ceiling-bytes 1 --assembly-scratch-ceiling-bytes 1 \
    --assembled-artifact-bytes 1
assert_contains "$case_stderr" 'required disk byte sum overflows signed 64-bit range'
[[ ! -e "$overflow_root/tdx-hydro-overflow" ]] || die 'overflow refusal created campaign state'
pass 'memory, disk, and arithmetic preflight refuse before writes'

selected_id=$(jq -r 'keys[0]' "$inventory")
selected_state=$campaign_dir/state/basins/$selected_id/current.json
jq '.stages.compile={status:"succeeded",attempts:3,failure_reason:null}' "$selected_state" >"$selected_state.tmp"
mv "$selected_state.tmp" "$selected_state"
cp "$selected_state" "$test_tmp/preserved-state.json"
cp "$campaign_dir/state/campaign.json" "$test_tmp/preserved-campaign.json"
set -- $(init_args equal "$valid_root")
run_runner "$@" >"$case_stdout"
diff -u "$test_tmp/preserved-state.json" "$selected_state"
expect_failure 'changed contract' init --campaign equal --workspace-root "$valid_root" \
    --available-memory-bytes 12 --available-disk-bytes 26 --retained-input-bytes 5 \
    --retained-basin-output-bytes 6 --assembly-memory-ceiling-bytes 11 \
    --assembly-scratch-ceiling-bytes 7 --assembled-artifact-bytes 8
assert_contains "$case_stderr" 'campaign parameters changed; use a new campaign ID'
diff -u "$test_tmp/preserved-state.json" "$selected_state"
diff -u "$test_tmp/preserved-campaign.json" "$campaign_dir/state/campaign.json"
pass 'init converges only for an equivalent contract and preserves basin state'

fixture_id=7020000010
fixture=$campaign_dir/state/basins/$fixture_id/current.json
jq -n --arg id "$fixture_id" '{
  schema_version:1,processing_basin_id:$id,
  stages:{
    acquire_basins:{status:"running",attempts:1,failure_reason:null},
    acquire_streamnet:{status:"succeeded",attempts:1,failure_reason:null},
    compile:{status:"running",attempts:1,failure_reason:null}
  }
}' >"$fixture.tmp"
mv "$fixture.tmp" "$fixture"
jq -n --arg id "$fixture_id" '{
  schema_version:1,processing_basin_id:$id,
  stages:{
    acquire_basins:{status:"pending",attempts:1,failure_reason:"interrupted before terminal state; reset by recover"},
    acquire_streamnet:{status:"succeeded",attempts:1,failure_reason:null},
    compile:{status:"pending",attempts:1,failure_reason:"interrupted before terminal state; reset by recover"}
  }
}' >"$test_tmp/recovered.json"
run_runner recover --campaign equal --workspace-root "$valid_root" >"$case_stdout"
jq -S '.' "$fixture" >"$test_tmp/recovered-actual.json"
jq -S '.' "$test_tmp/recovered.json" >"$test_tmp/recovered-expected.json"
diff -u "$test_tmp/recovered-expected.json" "$test_tmp/recovered-actual.json"
cp "$fixture" "$test_tmp/recovery-once.json"
run_runner recover --campaign equal --workspace-root "$valid_root" >"$case_stdout"
diff -u "$test_tmp/recovery-once.json" "$fixture"
pass 'recovery resets only interrupted stages and converges'

jq '.stages.compile={status:"running",attempts:4,failure_reason:null}' "$fixture" >"$fixture.tmp"
mv "$fixture.tmp" "$fixture"
cp "$fixture" "$test_tmp/pre-failed-mv.json"
sed >"$test_tmp/failing-mv" <<'FAILING_MV'
#!/bin/sh
exit 88
FAILING_MV
chmod +x "$test_tmp/failing-mv"
if HFX_TDX_MV=$test_tmp/failing-mv run_runner recover --campaign equal --workspace-root "$valid_root" >"$case_stdout" 2>"$case_stderr"; then
    die 'injected failing mv unexpectedly succeeded'
fi
jq -e '.' "$fixture" >/dev/null || die 'failed mv corrupted current state'
diff -u "$test_tmp/pre-failed-mv.json" "$fixture"
run_runner recover --campaign equal --workspace-root "$valid_root" >"$case_stdout"
jq -e '.stages.compile.status == "pending" and .stages.compile.attempts == 4' "$fixture" >/dev/null ||
    die 'recovery after failed mv did not succeed'
pass 'failed atomic replacement preserves prior valid state'

lock=$campaign_dir/state/locks/campaign.lock
mkdir "$lock"
printf '%s\n' 99999999 >"$lock/owner.pid"
run_runner recover --campaign equal --workspace-root "$valid_root" >"$case_stdout"
[[ ! -d "$lock" ]] || die 'recovery left its acquired lock behind'
mkdir "$lock"
printf '%s\n' "$$" >"$lock/owner.pid"
expect_failure 'live lock' recover --campaign equal --workspace-root "$valid_root"
assert_contains "$case_stderr" "campaign lock is held by live PID $$"
[[ -d "$lock" && $(<"$lock/owner.pid") == "$$" ]] || die 'live lock was altered'
rm -r "$lock"
pass 'dead locks are taken over and live locks are preserved'

mkdir "$lock"
printf '%s\n' 99999998 >"$lock/owner.pid"
cp "$lock/owner.pid" "$test_tmp/locked-owner-before"
HFX_TEST_PS_MODE=live expect_failure 'permission-denied equivalent live lock' \
    recover --campaign equal --workspace-root "$valid_root"
assert_contains "$case_stderr" 'campaign lock is held by live PID 99999998'
diff -u "$test_tmp/locked-owner-before" "$lock/owner.pid"
rm -r "$lock"
mkdir "$lock"
printf '%s\n' 99999998 >"$lock/owner.pid"
HFX_TEST_PS_MODE=error expect_failure 'indeterminate ps lock' recover --campaign equal --workspace-root "$valid_root"
assert_contains "$case_stderr" 'is indeterminate; preserved at'
[[ -d "$lock" ]] || die 'indeterminate lock was removed'
rm -r "$lock"
mkdir "$lock"
printf '%s\n' malformed >"$lock/owner.pid"
expect_failure 'malformed lock owner' recover --campaign equal --workspace-root "$valid_root"
[[ -d "$lock" ]] || die 'malformed lock was removed'
rm -r "$lock"
mkdir "$lock"
ln -s "$test_tmp/locked-owner-before" "$lock/owner.pid"
expect_failure 'symlink lock owner' recover --campaign equal --workspace-root "$valid_root"
[[ -L "$lock/owner.pid" ]] || die 'symlink lock owner was altered'
rm -r "$lock"
guard=$campaign_dir/state/locks/campaign.lock.takeover
mkdir "$lock" "$guard"
printf '%s\n' 99999999 >"$lock/owner.pid"
printf '%s\n' 99999997 >"$guard/owner.pid"
expect_failure 'existing takeover guard' recover --campaign equal --workspace-root "$valid_root"
assert_contains "$case_stderr" "stale-lock takeover already in progress at $guard"
[[ -d "$lock" && -d "$guard" ]] || die 'takeover refusal altered guarded lock paths'
rm -r "$lock" "$guard"
pass 'PID adjudication and takeover guard refuse conservatively'

corrupt_root=$(copy_workspace corrupt)
corrupt_file=$(find "$corrupt_root/tdx-hydro-equal/state/basins" -name current.json | sort | sed -n '1p')
rm "$corrupt_file"
expect_failure 'missing basin record' status --campaign equal --workspace-root "$corrupt_root"

extra_root=$(copy_workspace extra)
mkdir "$extra_root/tdx-hydro-equal/state/basins/9999999999"
cp "$fixture" "$extra_root/tdx-hydro-equal/state/basins/9999999999/current.json"
expect_failure 'extra basin record' status --campaign equal --workspace-root "$extra_root"

inventory_root=$(copy_workspace inventory-change)
jq 'to_entries | .[0].value = "999" | from_entries' \
    "$inventory_root/tdx-hydro-equal/state/inventory.json" >"$inventory_root/inventory.tmp"
mv "$inventory_root/inventory.tmp" "$inventory_root/tdx-hydro-equal/state/inventory.json"
expect_failure 'altered inventory' status --campaign equal --workspace-root "$inventory_root"

status_root=$(copy_workspace bad-status)
status_file=$(find "$status_root/tdx-hydro-equal/state/basins" -name current.json | sort | sed -n '1p')
jq '.stages.acquire_basins.status="paused"' "$status_file" >"$status_root/status.tmp"
mv "$status_root/status.tmp" "$status_file"
expect_failure 'malformed status' status --campaign equal --workspace-root "$status_root"

sizing_root=$(copy_workspace bad-sizing)
jq '.sizing.available_memory_bytes=1' \
    "$sizing_root/tdx-hydro-equal/state/campaign.json" >"$sizing_root/campaign.tmp"
mv "$sizing_root/campaign.tmp" "$sizing_root/tdx-hydro-equal/state/campaign.json"
expect_failure 'malformed sizing' status --campaign equal --workspace-root "$sizing_root"

run_runner status --campaign equal --workspace-root "$valid_root" >"$case_stdout"
assert_contains "$case_stdout" 'inventory_count=62'
assert_contains "$case_stdout" 'acquire_basins_pending=62'
assert_contains "$case_stdout" 'acquire_streamnet_succeeded=1'
assert_contains "$case_stdout" 'compile_succeeded=1'
pass 'status rejects all malformed state and reports deterministic counts'

run_runner --help >"$case_stdout"
if grep -i 'reclaim' "$case_stdout" >/dev/null; then
    die 'help exposes an input-reclaim mode'
fi
assert_contains "$campaign_dir/state/campaign.json" '"reclaim_inputs": false'
assert_contains "$case_stdout" '--retained-input-bytes'
assert_contains "$case_stdout" '--retained-basin-output-bytes'
assert_contains "$case_stdout" '--assembly-scratch-ceiling-bytes'
assert_contains "$case_stdout" '--assembled-artifact-bytes'
pass 'retain-all sizing has no reclaim interface'

expect_failure 'missing max parallel' acquire --campaign equal --workspace-root "$valid_root"
expect_failure 'repeated max parallel' acquire --campaign equal --workspace-root "$valid_root" --max-parallel 1 --max-parallel 2
expect_failure 'zero max parallel' acquire --campaign equal --workspace-root "$valid_root" --max-parallel 0
expect_failure 'negative max parallel' acquire --campaign equal --workspace-root "$valid_root" --max-parallel -1
expect_failure 'nonnumeric max parallel' acquire --campaign equal --workspace-root "$valid_root" --max-parallel x
expect_failure 'large max parallel' acquire --campaign equal --workspace-root "$valid_root" --max-parallel 63
expect_failure 'foreign max parallel' status --campaign equal --workspace-root "$valid_root" --max-parallel 1
pass 'acquire concurrency argument is required and bounded'

printf 'SQLite format 3\0fixture\n' >"$test_tmp/geopackage-template"
mkdir "$test_tmp/transfer-state"
sed >"$test_tmp/fake-curl" <<'FAKE_CURL'
#!/bin/bash
set -eu
output=
url=
seen_fail=0
seen_show=0
seen_location=0
seen_connect=0
seen_speed_limit=0
seen_speed_time=0
while [ "$#" -gt 0 ]; do
    case $1 in
        --fail) seen_fail=1 ;;
        --show-error) seen_show=1 ;;
        --location) seen_location=1 ;;
        --connect-timeout)
            shift
            [ "${1-}" = 30 ] || exit 91
            seen_connect=1
            ;;
        --speed-limit)
            shift
            [ "${1-}" = 65536 ] || exit 91
            seen_speed_limit=1
            ;;
        --speed-time)
            shift
            [ "${1-}" = 60 ] || exit 91
            seen_speed_time=1
            ;;
        --output)
            shift
            output=${1-}
            ;;
        --range|-r|--continue-at|-C|--parallel|--head|-I|--retry|--retry-all-errors)
            exit 92
            ;;
        https://earth-info.nga.mil/php/download.php?file=*-basins-gpkg|https://earth-info.nga.mil/php/download.php?file=*-streamnet-gpkg)
            url=$1
            ;;
        *) exit 93 ;;
    esac
    shift
done
[ "$seen_fail$seen_show$seen_location$seen_connect$seen_speed_limit$seen_speed_time" = 111111 ]
[ -n "$output" ] && [ -n "$url" ]
base=${output##*/}
key=${base%.gpkg.partial}
mutex=${HFX_TEST_TRANSFER_STATE:?}/mutex
while ! mkdir "$mutex" 2>/dev/null; do sleep 0.01; done
active=0
[ ! -f "$HFX_TEST_TRANSFER_STATE/active" ] || active=$(cat "$HFX_TEST_TRANSFER_STATE/active")
active=$((active + 1))
printf '%s\n' "$active" >"$HFX_TEST_TRANSFER_STATE/active"
maximum=0
[ ! -f "$HFX_TEST_TRANSFER_STATE/maximum" ] || maximum=$(cat "$HFX_TEST_TRANSFER_STATE/maximum")
[ "$active" -le "$maximum" ] || printf '%s\n' "$active" >"$HFX_TEST_TRANSFER_STATE/maximum"
printf 'start %s\n' "$key" >>"$HFX_TEST_TRANSFER_STATE/events"
rm -r "$mutex"
if [ "${HFX_TEST_INTERRUPT_DRAIN-}" = 1 ]; then
    printf '%s\n' "$$" >"$HFX_TEST_TRANSFER_STATE/curl.$$"
    printf '%s\n' "$PPID" >"$HFX_TEST_TRANSFER_STATE/worker.$PPID"
    if mkdir "$HFX_TEST_TRANSFER_STATE/signal-owner" 2>/dev/null; then
        marker_count=0
        while [ "$marker_count" -lt 3 ]; do
            marker_count=$(find "$HFX_TEST_TRANSFER_STATE" -name 'curl.*' -type f | wc -l | tr -d ' ')
            sleep 0.01
        done
        campaign_dir=${output%/downloads/*}
        runner_pid=$(cat "$campaign_dir/state/locks/campaign.lock/owner.pid")
        kill -TERM "$runner_pid"
        sleep 0.05
        kill -INT "$runner_pid" 2>/dev/null || :
        sleep 0.05
    else
        sleep 0.2
    fi
    exit 0
fi
if [ -n "${HFX_TEST_BARRIER_COUNT-}" ]; then
    touch "$HFX_TEST_TRANSFER_STATE/barrier.$key"
    barrier_deadline=0
    while [ "$(find "$HFX_TEST_TRANSFER_STATE" -name 'barrier.*' -type f 2>/dev/null | wc -l | tr -d ' ')" -lt "$HFX_TEST_BARRIER_COUNT" ]; do
        sleep 0.01
        barrier_deadline=$((barrier_deadline + 1))
        [ "$barrier_deadline" -lt 1000 ] || exit 94
    done
fi
if [ "${HFX_TEST_FAIL_KEY-}" = "$key" ]; then
    printf 'partial\n' >"$output"
    result=22
else
    cp "${HFX_TEST_GPKG_TEMPLATE:?}" "$output"
    result=0
fi
while ! mkdir "$mutex" 2>/dev/null; do sleep 0.01; done
active=$(cat "$HFX_TEST_TRANSFER_STATE/active")
active=$((active - 1))
printf '%s\n' "$active" >"$HFX_TEST_TRANSFER_STATE/active"
printf 'end %s\n' "$key" >>"$HFX_TEST_TRANSFER_STATE/events"
rm -r "$mutex"
exit "$result"
FAKE_CURL
sed >"$test_tmp/fake-sha256sum" <<'FAKE_SHA'
#!/bin/sh
if [ "${HFX_TEST_HASH_MODE-}" = changed ]; then
    printf '%064d  %s\n' 1 "$1"
else
    printf '%064d  %s\n' 0 "$1"
fi
FAKE_SHA
sed >"$test_tmp/fake-ogrinfo" <<'FAKE_OGR'
#!/bin/sh
file=$3
base=${file##*/}
if [ "${HFX_TEST_OGR_MODE-}" = count ]; then
    printf '1: first (Polygon)\n2: second (Polygon)\n'
    exit 0
fi
if [ "${HFX_TEST_OGR_MODE-}" = empty ]; then
    printf '1:  (Polygon)\n'
    exit 0
fi
printf '1: %s (Polygon)\n' "${base%.gpkg*}"
FAKE_OGR
real_jq=$(command -v jq)
sed >"$test_tmp/fake-jq" <<'FAKE_JQ'
#!/bin/bash
set -eu
if [ "${HFX_TEST_INTERRUPT_EMPTY-}" = 1 ] &&
    [ "$#" -eq 3 ] && [ "$1" = -r ] && [ "$2" = 'keys[]' ]; then
    count=0
    [ ! -f "$HFX_TEST_TRANSFER_STATE/jq-keys-count" ] ||
        count=$(cat "$HFX_TEST_TRANSFER_STATE/jq-keys-count")
    count=$((count + 1))
    printf '%s\n' "$count" >"$HFX_TEST_TRANSFER_STATE/jq-keys-count"
    if [ "$count" -eq 5 ]; then
        state_dir=${3%/*}
        runner_pid=$(cat "$state_dir/locks/campaign.lock/owner.pid")
        kill -TERM "$runner_pid"
        exit 0
    fi
fi
exec "${HFX_TEST_REAL_JQ:?}" "$@"
FAKE_JQ
chmod +x "$test_tmp/fake-curl" "$test_tmp/fake-sha256sum" "$test_tmp/fake-ogrinfo" "$test_tmp/fake-jq"
export HFX_TDX_CURL=$test_tmp/fake-curl
export HFX_TDX_SHA256SUM=$test_tmp/fake-sha256sum
export HFX_TDX_OGRINFO=$test_tmp/fake-ogrinfo
export HFX_TEST_TRANSFER_STATE=$test_tmp/transfer-state
export HFX_TEST_GPKG_TEMPLATE=$test_tmp/geopackage-template
export HFX_TEST_REAL_JQ=$real_jq

empty_interrupt_root=$test_tmp/workspaces/interrupt-empty
mkdir "$empty_interrupt_root"
set -- $(init_args interrupt-empty "$empty_interrupt_root")
run_runner "$@" >"$case_stdout"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
empty_interrupt_status=0
HFX_TEST_INTERRUPT_EMPTY=1 HFX_TDX_JQ=$test_tmp/fake-jq \
    run_runner acquire --campaign interrupt-empty --workspace-root "$empty_interrupt_root" --max-parallel 3 \
    >"$case_stdout" 2>"$case_stderr" || empty_interrupt_status=$?
[[ "$empty_interrupt_status" -eq 130 ]] ||
    die "empty-worker TERM exited $empty_interrupt_status instead of 130 after $(<"$test_tmp/transfer-state/jq-keys-count") inventory iterations: $(<"$case_stderr")"
empty_interrupt_lock=$empty_interrupt_root/tdx-hydro-interrupt-empty/state/locks/campaign.lock
[[ ! -d "$empty_interrupt_lock" ]] || die 'empty-worker TERM left the campaign lock behind'
[[ ! -e "$test_tmp/transfer-state/events" ]] || die 'empty-worker TERM allowed an acquisition worker to start'
pass 'TERM with an empty worker array exits 130 and releases the lock'

drain_interrupt_root=$test_tmp/workspaces/interrupt-drain
mkdir "$drain_interrupt_root"
set -- $(init_args interrupt-drain "$drain_interrupt_root")
run_runner "$@" >"$case_stdout"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
drain_interrupt_status=0
HFX_TEST_INTERRUPT_DRAIN=1 \
    run_runner acquire --campaign interrupt-drain --workspace-root "$drain_interrupt_root" --max-parallel 3 \
    >"$case_stdout" 2>"$case_stderr" || drain_interrupt_status=$?
[[ "$drain_interrupt_status" -eq 130 ]] ||
    die "repeated drain INT/TERM exited $drain_interrupt_status instead of 130"
drain_interrupt_lock=$drain_interrupt_root/tdx-hydro-interrupt-drain/state/locks/campaign.lock
[[ ! -d "$drain_interrupt_lock" ]] || die 'repeated drain INT/TERM left the campaign lock behind'
for pid_file in "$test_tmp"/transfer-state/worker.* "$test_tmp"/transfer-state/curl.*; do
    [[ -f "$pid_file" ]] || continue
    interrupted_pid=$(<"$pid_file")
    retries=0
    while kill -0 "$interrupted_pid" 2>/dev/null && [[ "$retries" -lt 200 ]]; do
        sleep 0.01
        retries=$((retries + 1))
    done
    ! kill -0 "$interrupted_pid" 2>/dev/null ||
        die "interrupted acquisition process survived parent: $interrupted_pid"
done
[[ -e "$test_tmp/transfer-state/signal-owner" ]] ||
    die 'repeated drain TERM did not start its signal coordinator'
pass 'repeated INT/TERM during worker drain exits 130, reaps workers, and releases the lock'
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"

acquire_root=$test_tmp/workspaces/acquire
mkdir "$acquire_root"
set -- $(init_args acquire "$acquire_root")
run_runner "$@" >"$case_stdout"
export HFX_TEST_BARRIER_COUNT=2
run_runner acquire --campaign acquire --workspace-root "$acquire_root" --max-parallel 3 >"$case_stdout"
unset HFX_TEST_BARRIER_COUNT
acquire_dir=$acquire_root/tdx-hydro-acquire
[[ $(<"$test_tmp/transfer-state/maximum") -ge 2 && $(<"$test_tmp/transfer-state/maximum") -le 3 ]] ||
    die 'bounded parallel acquisition did not observe deterministic overlap'
[[ $(find "$acquire_dir/downloads" -name '*.gpkg' -type f | wc -l | tr -d ' ') == 124 ]] ||
    die 'complete acquisition did not install 124 files'
[[ $(grep -c '^start ' "$test_tmp/transfer-state/events") == 124 ]] ||
    die 'complete acquisition did not invoke 124 transfers'
jq -e -s '
  length == 62 and all(
    .schema_version == 2 and
    .stages.acquire_basins.status == "succeeded" and
    .stages.acquire_streamnet.status == "succeeded" and
    .stages.acquire_basins.attempts == 1 and
    .stages.acquire_streamnet.attempts == 1 and
    (.stages.acquire_basins.evidence.bytes > 16) and
    (.stages.acquire_streamnet.evidence.bytes > 16) and
    (.stages.acquire_basins.evidence.sha256 | test("^[0-9a-f]{64}$")) and
    (.stages.acquire_streamnet.evidence.sha256 | test("^[0-9a-f]{64}$")) and
    .stages.acquire_basins.evidence.sqlite_identity == "53514c69746520666f726d6174203300" and
    .stages.acquire_streamnet.evidence.sqlite_identity == "53514c69746520666f726d6174203300"
  )
' "$acquire_dir"/state/basins/*/current.json >/dev/null || die 'successful acquisition evidence differs'
events_before=$(wc -l <"$test_tmp/transfer-state/events" | tr -d ' ')
run_runner acquire --campaign acquire --workspace-root "$acquire_root" --max-parallel 3 >"$case_stdout"
events_after=$(wc -l <"$test_tmp/transfer-state/events" | tr -d ' ')
[[ "$events_before" == "$events_after" ]] || die 'verified reuse fetched files again'
pass 'all 62 basins acquire with bounded concurrency and exact reusable evidence'

serial_root=$test_tmp/workspaces/serial
mkdir "$serial_root"
set -- $(init_args serial "$serial_root")
run_runner "$@" >"$case_stdout"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
run_runner acquire --campaign serial --workspace-root "$serial_root" --max-parallel 1 >"$case_stdout"
[[ $(<"$test_tmp/transfer-state/maximum") == 1 ]] || die 'serial acquisition exceeded one active transfer'
pass 'maximum parallel one remains strictly serial'

failure_root=$test_tmp/workspaces/failure
mkdir "$failure_root"
set -- $(init_args failure "$failure_root")
run_runner "$@" >"$case_stdout"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
failure_id=$(jq -r 'keys[0]' "$inventory")
export HFX_TEST_FAIL_KEY=$failure_id-basins
run_runner acquire --campaign failure --workspace-root "$failure_root" --max-parallel 3 >"$case_stdout"
failure_state=$failure_root/tdx-hydro-failure/state/basins/$failure_id/current.json
jq -e '
  .stages.acquire_basins.status == "failed" and
  .stages.acquire_basins.attempts == 1 and
  .stages.acquire_basins.evidence == null and
  (.stages.acquire_basins.failure_reason | length > 0) and
  .stages.acquire_streamnet.status == "succeeded"
' "$failure_state" >/dev/null || die 'isolated transfer failure state differs'
unset HFX_TEST_FAIL_KEY
events_before=$(grep -c '^start ' "$test_tmp/transfer-state/events")
run_runner acquire --campaign failure --workspace-root "$failure_root" --max-parallel 3 >"$case_stdout"
events_after=$(grep -c '^start ' "$test_tmp/transfer-state/events")
[[ $((events_after - events_before)) == 1 ]] || die 'retry fetched work other than the failed product'
jq -e '.stages.acquire_basins.status == "succeeded" and .stages.acquire_basins.attempts == 2' \
    "$failure_state" >/dev/null || die 'failed product did not converge on retry'
pass 'product failure is isolated and only incomplete work retries'

reuse_root=$test_tmp/workspaces/reuse-corrupt
mkdir "$reuse_root"
cp -R "$acquire_dir" "$reuse_root/tdx-hydro-acquire"
reuse_id=$(jq -r 'keys[1]' "$inventory")
reuse_state=$reuse_root/tdx-hydro-acquire/state/basins/$reuse_id/current.json
reuse_final=$reuse_root/tdx-hydro-acquire/downloads/$reuse_id-basins.gpkg
printf 'mutation\n' >>"$reuse_final"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
run_runner acquire --campaign acquire --workspace-root "$reuse_root" --max-parallel 3 >"$case_stdout"
jq -e '.stages.acquire_basins.status == "failed" and .stages.acquire_basins.evidence == null' \
    "$reuse_state" >/dev/null || die 'mutated reuse was not isolated as failed'
[[ -f "$reuse_final" ]] || die 'suspect final was removed'
[[ ! -e "$test_tmp/transfer-state/events" ]] || die 'corrupt reuse triggered a replacement fetch'

hash_root=$test_tmp/workspaces/reuse-hash
mkdir "$hash_root"
cp -R "$acquire_dir" "$hash_root/tdx-hydro-acquire"
export HFX_TEST_HASH_MODE=changed
run_runner acquire --campaign acquire --workspace-root "$hash_root" --max-parallel 3 >"$case_stdout"
unset HFX_TEST_HASH_MODE
jq -e '[.stages.acquire_basins,.stages.acquire_streamnet] | all(.status == "failed" and .evidence == null)' \
    "$hash_root/tdx-hydro-acquire/state/basins/$reuse_id/current.json" >/dev/null ||
    die 'hash behavior mismatch did not refuse reuse'

layer_root=$test_tmp/workspaces/reuse-layer
mkdir "$layer_root"
cp -R "$acquire_dir" "$layer_root/tdx-hydro-acquire"
export HFX_TEST_OGR_MODE=count
run_runner acquire --campaign acquire --workspace-root "$layer_root" --max-parallel 3 >"$case_stdout"
unset HFX_TEST_OGR_MODE
jq -e '.stages.acquire_streamnet.status == "failed" and .stages.acquire_streamnet.evidence == null' \
    "$layer_root/tdx-hydro-acquire/state/basins/$reuse_id/current.json" >/dev/null ||
    die 'multiple layer reuse did not fail'
pass 'corrupt bytes, hashes, and layer inspection refuse reuse without replacement'

adopt_root=$test_tmp/workspaces/adopt-final
mkdir "$adopt_root"
cp -R "$acquire_dir" "$adopt_root/tdx-hydro-acquire"
adopt_state=$adopt_root/tdx-hydro-acquire/state/basins/$reuse_id/current.json
jq '.stages.acquire_basins={status:"pending",attempts:1,failure_reason:null,evidence:null}' \
    "$adopt_state" >"$adopt_state.tmp"
mv "$adopt_state.tmp" "$adopt_state"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
run_runner acquire --campaign acquire --workspace-root "$adopt_root" --max-parallel 3 >"$case_stdout"
jq -e '.stages.acquire_basins.status == "succeeded" and .stages.acquire_basins.attempts == 1' \
    "$adopt_state" >/dev/null || die 'verified final was not adopted without a new attempt'
[[ ! -e "$test_tmp/transfer-state/events" ]] || die 'adoption fetched an existing valid final'

partial_root=$test_tmp/workspaces/partial
mkdir "$partial_root"
cp -R "$acquire_dir" "$partial_root/tdx-hydro-acquire"
partial_state=$partial_root/tdx-hydro-acquire/state/basins/$reuse_id/current.json
partial_final=$partial_root/tdx-hydro-acquire/downloads/$reuse_id-basins.gpkg
rm "$partial_final"
printf 'old incomplete bytes\n' >"$partial_final.partial"
jq '.stages.acquire_basins={status:"failed",attempts:1,failure_reason:"old failure",evidence:null}' \
    "$partial_state" >"$partial_state.tmp"
mv "$partial_state.tmp" "$partial_state"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
run_runner acquire --campaign acquire --workspace-root "$partial_root" --max-parallel 3 >"$case_stdout"
jq -e '.stages.acquire_basins.status == "succeeded" and .stages.acquire_basins.attempts == 2' \
    "$partial_state" >/dev/null || die 'ordinary partial did not restart from a new complete attempt'
[[ ! -e "$partial_final.partial" ]] || die 'ordinary partial remains after successful retry'
[[ $(grep -c "^start $reuse_id-basins$" "$test_tmp/transfer-state/events") == 1 ]] ||
    die 'partial retry did not issue exactly one complete GET'

unsafe_partial_root=$test_tmp/workspaces/unsafe-partial
mkdir "$unsafe_partial_root"
cp -R "$acquire_dir" "$unsafe_partial_root/tdx-hydro-acquire"
unsafe_state=$unsafe_partial_root/tdx-hydro-acquire/state/basins/$reuse_id/current.json
unsafe_final=$unsafe_partial_root/tdx-hydro-acquire/downloads/$reuse_id-basins.gpkg
rm "$unsafe_final"
ln -s "$test_tmp/geopackage-template" "$unsafe_final.partial"
jq '.stages.acquire_basins={status:"pending",attempts:1,failure_reason:null,evidence:null}' \
    "$unsafe_state" >"$unsafe_state.tmp"
mv "$unsafe_state.tmp" "$unsafe_state"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
run_runner acquire --campaign acquire --workspace-root "$unsafe_partial_root" --max-parallel 3 >"$case_stdout"
jq -e '.stages.acquire_basins.status == "failed" and .stages.acquire_basins.attempts == 1' \
    "$unsafe_state" >/dev/null || die 'unsafe partial was not isolated without a new attempt'
[[ -L "$unsafe_final.partial" ]] || die 'unsafe partial was removed or traversed'
[[ ! -e "$test_tmp/transfer-state/events" ]] || die 'unsafe partial triggered a fetch'
pass 'verified finals are adopted and partial paths follow safe all-or-nothing retry rules'

for poison in hcloud curl aws ssh; do
    [[ ! -e "$test_tmp/invocations/$poison.log" ]] || die "poison command was invoked: $poison"
done
git -C "$repo_root" status --porcelain=v1 | sed '/^?? pr-body\.md$/d' >"$test_tmp/repository-status-after"
diff -u "$test_tmp/repository-status-before" "$test_tmp/repository-status-after"
pass 'no cloud, network, SSH, or publication command ran'

printf '1..%d\n' "$passed"
printf 'test-tdx-hydro-campaign: all %d cases passed\n' "$passed"
