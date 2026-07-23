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

for command_name in jq grep diff find sort wc mktemp mkdir cp rm mv chmod tr sed ln touch git; do
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
git -C "$repo_root" status --porcelain=v1 >"$test_tmp/repository-status-before"
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
    if grep -En -e "$forbidden_assoc" -e "$forbidden_wait" -e "$forbidden_map" -e "$forbidden_read" "$script" >"$case_stdout"; then
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
      .schema_version == 1 and
      (.processing_basin_id | test("^[0-9]{10}$")) and
      .stages == {
        acquire_basins:{status:"pending",attempts:0,failure_reason:null},
        acquire_streamnet:{status:"pending",attempts:0,failure_reason:null},
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

for poison in hcloud curl aws ssh; do
    [[ ! -e "$test_tmp/invocations/$poison.log" ]] || die "poison command was invoked: $poison"
done
git -C "$repo_root" status --porcelain=v1 >"$test_tmp/repository-status-after"
diff -u "$test_tmp/repository-status-before" "$test_tmp/repository-status-after"
pass 'no cloud, network, SSH, or publication command ran'

printf '1..%d\n' "$passed"
printf 'test-tdx-hydro-campaign: all %d cases passed\n' "$passed"
