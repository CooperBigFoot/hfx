#!/usr/bin/env bash

bash_version_at_least_3_2() {
    hfx_test_bash_version=$1
    [ -n "$hfx_test_bash_version" ] || return 1
    hfx_test_bash_major=${hfx_test_bash_version%%.*}
    hfx_test_bash_remainder=${hfx_test_bash_version#*.}
    hfx_test_bash_minor=${hfx_test_bash_remainder%%.*}
    case $hfx_test_bash_major in
        ''|*[!0-9]*) return 1 ;;
    esac
    case $hfx_test_bash_minor in
        ''|*[!0-9]*) return 1 ;;
    esac
    [ "$hfx_test_bash_major" -gt 3 ] ||
        { [ "$hfx_test_bash_major" -eq 3 ] && [ "$hfx_test_bash_minor" -ge 2 ]; }
}

if [ -z "${BASH_VERSION-}" ]; then
    printf '%s\n' 'test-tdx-hydro-campaign: error: Bash >=3.2 is required; observed non-Bash interpreter' >&2
    exit 1
fi
if ! bash_version_at_least_3_2 "$BASH_VERSION"; then
    printf 'test-tdx-hydro-campaign: error: Bash >=3.2 is required; observed %s\n' "$BASH_VERSION" >&2
    exit 1
fi

set -Eeuo pipefail
IFS=$'\n\t'
set +x

if [ "${HFX_TEST_GLOBAL_WATCHDOG_CHILD-}" != 1 ]; then
    watchdog_script_dir=$(cd -P -- "${BASH_SOURCE[0]%/*}" && pwd)
    watchdog_script=$watchdog_script_dir/${BASH_SOURCE[0]##*/}
    export HFX_TEST_GLOBAL_WATCHDOG_CHILD=1
    /bin/bash "$watchdog_script" "$@" &
    watchdog_pid=$!
    watchdog_attempt=0
    while [ "$watchdog_attempt" -lt 2400 ]; do
        watchdog_live=0
        for watchdog_job in $(jobs -pr); do
            [ "$watchdog_job" != "$watchdog_pid" ] || watchdog_live=1
        done
        [ "$watchdog_live" -eq 1 ] || break
        watchdog_attempt=$((watchdog_attempt + 1))
        sleep 1
    done
    if [ "$watchdog_attempt" -lt 2400 ]; then
        watchdog_status=0
        wait "$watchdog_pid" || watchdog_status=$?
        exit "$watchdog_status"
    fi
    kill -TERM "$watchdog_pid" 2>/dev/null || :
    watchdog_attempt=0
    while [ "$watchdog_attempt" -lt 5 ]; do
        watchdog_live=0
        for watchdog_job in $(jobs -pr); do
            [ "$watchdog_job" != "$watchdog_pid" ] || watchdog_live=1
        done
        [ "$watchdog_live" -eq 1 ] || break
        watchdog_attempt=$((watchdog_attempt + 1))
        sleep 1
    done
    [ "$watchdog_live" -eq 0 ] || kill -KILL "$watchdog_pid" 2>/dev/null || :
    wait "$watchdog_pid" 2>/dev/null || :
    printf '%s\n' 'test-tdx-hydro-campaign: error: global timeout after 2400 seconds' >&2
    exit 1
fi

die() {
    printf 'test-tdx-hydro-campaign: error: %s\n' "$*" >&2
    exit 1
}

pass() {
    passed=$((passed + 1))
    printf 'ok %d - %s\n' "$passed" "$1"
}

skip() {
    passed=$((passed + 1))
    skipped=$((skipped + 1))
    printf 'ok %d - %s # SKIP %s\n' "$passed" "$1" "$2"
    printf 'test-tdx-hydro-campaign: SKIP: %s\n' "$2" >&2
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

assert_zero_ere() {
    local label=$1
    local pattern=$2
    local file=$3
    local grep_option=${4-}
    local match_count
    if [[ -n "$grep_option" ]]; then
        grep "$grep_option" -En -- "$pattern" "$file" >"$case_stdout" || :
    else
        grep -En -- "$pattern" "$file" >"$case_stdout" || :
    fi
    match_count=$(wc -l <"$case_stdout" | tr -d ' ')
    [[ "$match_count" -eq 0 ]] || die "$label matched $match_count times"
}

run_runner() {
    "$selected_bash" "$runner" "$@"
}

init_args() {
    printf '%s\n' \
        init --campaign "$1" --workspace-root "$2" \
        --available-memory-bytes 11 \
        --available-disk-bytes 29 \
        --retained-input-bytes 5 \
        --retained-basin-output-bytes 6 \
        --assembly-memory-ceiling-bytes 11 \
        --assembly-scratch-ceiling-bytes 7 \
        --assembled-artifact-bytes 8 \
        --active-compile-scratch-bytes 9 \
        --filesystem-overhead-bytes 1
}

subset_init_args() {
    printf '%s\n' \
        init --campaign "$1" --workspace-root "$2" \
        --basin 7020000010 \
        --basin 1020000010 \
        --basin 9020000010 \
        --available-memory-bytes 11 \
        --available-disk-bytes 29 \
        --retained-input-bytes 5 \
        --retained-basin-output-bytes 6 \
        --assembly-memory-ceiling-bytes 11 \
        --assembly-scratch-ceiling-bytes 7 \
        --assembled-artifact-bytes 8 \
        --active-compile-scratch-bytes 9 \
        --filesystem-overhead-bytes 1
}

reclaim_subset_init_args() {
    printf '%s\n' \
        init --campaign "$1" --workspace-root "$2" \
        --basin 7020000010 \
        --basin 1020000010 \
        --basin 9020000010 \
        --retention-policy reclaim-inputs-after-terminal \
        --available-memory-bytes 30000000000 \
        --available-disk-bytes 491737129060 \
        --peak-in-flight-download-bytes 44296724480 \
        --retained-basin-output-bytes 206220202290 \
        --assembly-memory-ceiling-bytes 30000000000 \
        --assembly-scratch-ceiling-bytes 206220202290 \
        --assembled-artifact-bytes 206220202290 \
        --active-compile-scratch-bytes 30000000000 \
        --filesystem-overhead-bytes 5000000000
}

copy_workspace() {
    local name=$1
    local destination=$test_tmp/workspaces/$name
    mkdir "$destination"
    cp -R "$valid_root/tdx-hydro-equal" "$destination/tdx-hydro-equal"
    printf '%s\n' "$destination"
}

mark_compile_succeeded() {
    local campaign_dir=$1
    local basin_id=$2
    local state=$campaign_dir/state/basins/$basin_id/current.json
    local temporary=$state.tmp
    jq '.stages.compile={status:"succeeded",attempts:1,failure_reason:null,diagnostic_report:null}' \
        "$state" >"$temporary"
    mv "$temporary" "$state"
    mkdir -p "$campaign_dir/basin-outputs/$basin_id"
}

write_expected_assembly_argv() {
    local campaign_dir=$1
    local mode=${2-legacy}
    local included_id=${3-7020000010}
    HFX_TEST_EXPECTED_ASSEMBLY_ARGV=$test_tmp/expected-assembly-argv
    export HFX_TEST_EXPECTED_ASSEMBLY_ARGV
    if [[ "$mode" == legacy ]]; then
        printf '%s\n' assemble \
            --input "$campaign_dir/basin-outputs/1020000010" \
            --input "$campaign_dir/basin-outputs/7020000010" \
            --out "$campaign_dir/assembly/dataset" >"$HFX_TEST_EXPECTED_ASSEMBLY_ARGV"
    else
        printf '%s\n' assemble --partial-input "$partial_fabric_root" \
            --partial-roster "$partial_fabric_roster" \
            --input "$campaign_dir/basin-outputs/$included_id" \
            --out "$campaign_dir/assembly/dataset" >"$HFX_TEST_EXPECTED_ASSEMBLY_ARGV"
    fi
}

extension_options() {
    printf '%s\n' --partial-fabric "$partial_fabric_root" \
        --partial-fabric-roster "$partial_fabric_roster" \
        --exclude-control-basin 1020000010
}

write_assembly_state_fixture() {
    local campaign_dir=$1
    local status=$2
    local attempts=$3
    local reason=$4
    local mode=${5-legacy}
    if [[ "$mode" == extension ]]; then
        jq -n --arg status "$status" --argjson attempts "$attempts" --arg reason "$reason" \
            --arg fabric_root "$partial_fabric_root" --arg roster "$partial_fabric_roster" '{
          schema_version:2,status:$status,attempts:$attempts,
          failure_reason:(if $reason == "" then null else $reason end),
          fabric_root:$fabric_root,fabric_roster_path:$roster,
          fabric_basin_ids:["1020000010"],excluded_control_basin_id:"1020000010",
          included_basin_ids:["7020000010"],included_dataset_paths:["basin-outputs/7020000010"],
          output_path:"assembly/dataset",report_path:"reports/assembly.json"
        }' >"$campaign_dir/state/assembly.json"
        return
    fi
    jq -n --arg status "$status" --argjson attempts "$attempts" --arg reason "$reason" '{
      schema_version:1,
      status:$status,
      attempts:$attempts,
      failure_reason:(if $reason == "" then null else $reason end),
      input_basin_ids:["1020000010","7020000010"],
      output_path:"assembly/dataset",
      report_path:"reports/assembly.json"
    }' >"$campaign_dir/state/assembly.json"
}

create_assembly_dataset_fixture() {
    local output=$1
    mkdir -p "$output/aux"
    printf '%s\n' fixture >"$output/catchments.parquet"
    printf '%s\n' fixture >"$output/graph.parquet"
    printf '%s\n' fixture >"$output/manifest.json"
    printf '%s\n' fixture >"$output/aux/snap_stems.parquet"
}

new_assembly_workspace() {
    local name=$1
    local destination=$test_tmp/workspaces/$name
    mkdir "$destination"
    set -- $(init_args equal "$destination")
    run_runner "$@" >"$case_stdout"
    printf '%s\n' "$destination"
}

SCRIPT_DIR=$(cd -P -- "${BASH_SOURCE[0]%/*}" && pwd)
repo_root=$(cd -P -- "$SCRIPT_DIR/../.." && pwd)
runner=$SCRIPT_DIR/tdx-hydro-campaign.sh
bootstrap=$SCRIPT_DIR/bootstrap.sh
runbook=$SCRIPT_DIR/RUNBOOK-tdx-hydro-assembly-subset.md
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
if ! bash_version_at_least_3_2 "$selected_version"; then
    die "selected Bash is older than 3.2: $selected_version"
fi
printf 'test-tdx-hydro-campaign: selected interpreter %s (%s)\n' "$selected_bash" "$selected_version"
if [[ -x /bin/bash && "$selected_bash" != /bin/bash ]]; then
    die 'the harness did not select /bin/bash'
fi

for command_name in jq grep diff find sort wc mktemp mkdir mkfifo cp rm mv chmod tr sed ln touch git sleep head tail cmp perl; do
    command -v "$command_name" >/dev/null 2>&1 || die "required command is unavailable: $command_name"
done
[[ -f "$runner" ]] || die "runner is missing: $runner"
[[ -f "$bootstrap" ]] || die "bootstrap is missing: $bootstrap"
[[ -f "$runbook" ]] || die "runbook is missing: $runbook"
[[ -f "$inventory" ]] || die "inventory is missing: $inventory"

test_tmp=$(mktemp -d "${TMPDIR:-/tmp}/hfx-tdx-campaign-test.XXXXXX")
mutation_runner=
case $test_tmp in
    "${TMPDIR:-/tmp}"/hfx-tdx-campaign-test.*) ;;
    *) die "mktemp returned an unsafe path: $test_tmp" ;;
esac
cleanup() {
    [[ -z "${mutation_runner-}" || ! -e "$mutation_runner" ]] || rm -f -- "$mutation_runner"
    case ${test_tmp-} in
        "${TMPDIR:-/tmp}"/hfx-tdx-campaign-test.*)
            [[ -d "$test_tmp" && ! -L "$test_tmp" ]] && rm -rf -- "$test_tmp"
            ;;
    esac
}
trap cleanup EXIT

mkdir "$test_tmp/fake-bin" "$test_tmp/invocations" "$test_tmp/workspaces"
mkdir -p "$test_tmp/fixtures/partial-fabric/aux"
partial_fabric_root=$(cd -P "$test_tmp/fixtures/partial-fabric" && pwd -P)
for partial_file in catchments.parquet graph.parquet manifest.json aux/snap_stems.parquet; do
    printf '%s\n' fixture >"$partial_fabric_root/$partial_file"
done
printf '%s\n' '["1020000010"]' >"$test_tmp/fixtures/partial-fabric-roster.json"
partial_fabric_roster=$(cd -P "$test_tmp/fixtures" && pwd -P)/partial-fabric-roster.json
if [[ -n "${HFX_TEST_MUTATION_FROM-}" ]]; then
    [[ $(grep -Fxc -- "$HFX_TEST_MUTATION_FROM" "$runner") -eq 1 ]] ||
        die 'mutation anchor must occur exactly once'
    mutation_runner=$SCRIPT_DIR/.tdx-hydro-campaign.mutated.$$
    cp "$runner" "$mutation_runner"
    HFX_TEST_MUTATION_FROM=$HFX_TEST_MUTATION_FROM HFX_TEST_MUTATION_TO=${HFX_TEST_MUTATION_TO-} \
        perl -0pi -e 's/^\Q$ENV{HFX_TEST_MUTATION_FROM}\E$/$ENV{HFX_TEST_MUTATION_TO}/m' \
        "$mutation_runner"
    runner=$mutation_runner
    unset HFX_TEST_MUTATION_FROM HFX_TEST_MUTATION_TO
fi
git -C "$repo_root" status --porcelain=v1 | sed '/^?? pr-body\.md$/d' >"$test_tmp/repository-status-before"
case_stdout=$test_tmp/stdout
case_stderr=$test_tmp/stderr
for poison in curl aws ssh; do
    sed -e "s/@NAME@/$poison/g" >"$test_tmp/fake-bin/$poison" <<'POISON'
#!/bin/sh
printf '%s\n' "$*" >>"${HFX_TEST_INVOCATIONS:?}/@NAME@.log"
exit 97
POISON
    chmod +x "$test_tmp/fake-bin/$poison"
done
sed >"$test_tmp/fake-bin/hcloud" <<'POISON_HCLOUD'
#!/bin/sh
for argument do
    if [ "$argument" = delete ]; then
        printf '%s\n' "$*" >>"${HFX_TEST_INVOCATIONS:?}/hcloud-delete.log"
        exit 96
    fi
done
printf '%s\n' "$*" >>"${HFX_TEST_INVOCATIONS:?}/hcloud.log"
exit 97
POISON_HCLOUD
chmod +x "$test_tmp/fake-bin/hcloud"
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
skipped=0

calibration_fake_setup() {
    if [[ ! -x "$test_tmp/fake-curl" ]]; then
        sed -n "/^printf 'SQLite format 3/,/^export HFX_TEST_DIFF=/p" "$0" >"$test_tmp/calibration-fake-setup"
        [[ -s "$test_tmp/calibration-fake-setup" ]] || die 'calibration fake setup extraction was empty'
        source "$test_tmp/calibration-fake-setup"
    fi
    [[ -n "${HFX_TDX_ADAPTER_PYTHON-}" ]] || die 'calibration fake adapter was not installed'
    if [[ $(wc -c <"$HFX_TEST_GPKG_TEMPLATE" | tr -d ' ') -eq 24 ]]; then
        printf '123456789012345678901234' >>"$HFX_TEST_GPKG_TEMPLATE"
    fi
}

calibration_new_campaign() {
    local name=$1
    calibration_root=$test_tmp/workspaces/$name
    mkdir "$calibration_root"
    run_runner init --campaign "$name" --workspace-root "$calibration_root" \
        --retention-policy reclaim-inputs-after-terminal \
        --available-memory-bytes 30000000000 --available-disk-bytes 560000000000 \
        --peak-in-flight-download-bytes 44296724480 \
        --retained-basin-output-bytes 206220202290 \
        --assembly-memory-ceiling-bytes 30000000000 \
        --assembly-scratch-ceiling-bytes 206220202290 \
        --assembled-artifact-bytes 206220202290 \
        --active-compile-scratch-bytes 30000000000 \
        --filesystem-overhead-bytes 5000000000 >"$case_stdout"
    calibration_dir=$calibration_root/tdx-hydro-$name
    export HFX_TEST_PIPELINE_CAMPAIGN_DIR=$calibration_dir
    export HFX_TEST_PIPELINE_COMPLETION_PATH=$calibration_dir/state/tmp/pipeline-completions.fifo
}

calibration_measurement_json() {
    local throughput=${1-1000000}
    local completions=${2-2}
    jq -cn --argjson throughput "$throughput" --argjson completions "$completions" '{
      raw:{start_timestamp_seconds:100,end_timestamp_seconds:110,bytes:10000000,
        elapsed_seconds:10,retries:0,throughput_bytes_per_second:1000000},
      steady_state:{start_timestamp_seconds:100,end_timestamp_seconds:101,start_bytes:0,
        attempts:[1],end_bytes:$throughput,bytes:$throughput,elapsed_seconds:1,
        throughput_bytes_per_second:$throughput,compile_completions:$completions,
        compile_wall_seconds:(if $completions == 0 then 0 else 1 end)},
      excluded_drain_tail:{start_timestamp_seconds:101,end_timestamp_seconds:110,
        start_bytes:$throughput,end_bytes:10000000,bytes:(10000000-$throughput),elapsed_seconds:9}
    }'
}

calibration_write_state() {
    local dir=$1
    local p2=$2
    local p4=$3
    local selection=${4-null}
    local measurement
    measurement=$(calibration_measurement_json)
    jq -n --arg p2 "$p2" --arg p4 "$p4" --argjson selected "$selection" \
        --argjson measurement "$measurement" 'def cohort($parallel;$ids;$status):
      {max_parallel:$parallel,basin_ids:($ids|split(" ")),status:$status,
       attempts:(if $status == "pending" then 0 else 1 end),
       measurement:(if $status == "measured" then $measurement else null end)};
      {schema_version:1,fabric_version:"fixture-v1",selected_max_parallel:$selected,
       selected_throughput_validity:(if $selected == null then null else "compile-observed" end),
       cohorts:{"parallel-2":cohort(2;"1020011530 3020003790 6020006540 8020008900";$p2),
                "parallel-4":cohort(4;"2020003440 4020006940 7020014250 9020000010";$p4)}}' \
        >"$dir/state/calibration.json"
}

calibration_run() {
    local name=$1
    local parallel=$2
    run_runner calibrate --campaign "$name" --workspace-root "$calibration_root" \
        --max-parallel "$parallel" --fabric-version fixture-v1
}

calibration_prepare_workers() {
    [[ ! -d "$test_tmp/transfer-state" ]] || rm -r -- "$test_tmp/transfer-state"
    mkdir "$test_tmp/transfer-state"
    [[ -e "$test_tmp/invocations/adapter.log" ]] || : >"$test_tmp/invocations/adapter.log"
    [[ -e "$test_tmp/invocations/hfx.log" ]] || : >"$test_tmp/invocations/hfx.log"
    [[ -e "$test_tmp/invocations/hfx-status.log" ]] || : >"$test_tmp/invocations/hfx-status.log"
    export HFX_TEST_PIPELINE_CAMPAIGN_DIR=$calibration_dir
    export HFX_TEST_PIPELINE_COMPLETION_PATH=$calibration_dir/state/tmp/pipeline-completions.fifo
    export HFX_TEST_PIPELINE_AVAILABLE_BYTES=79734104064
    export HFX_TEST_FAIL_KEY=
    export HFX_TEST_FAIL_ONCE_KEY=
    export HFX_TEST_HASH_MODE=
    export HFX_TEST_OGR_MODE=
    export HFX_TEST_EMIT_ETAG=
    export HFX_TEST_TRANSFER_SHAPE=
    export HFX_TEST_TRANSFER_SHAPE_KEY=
    export HFX_TEST_LOWERCASE_HEADERS=
    export HFX_TEST_LEADING_ZERO_LENGTH=
    export HFX_TEST_REDISPATCH_HOLD_KEY=
    export HFX_TEST_REDISPATCH_RELEASE_MARKER=
    export HFX_TEST_PIPELINE_KILL_KEY=
    export HFX_TEST_PIPELINE_BUILD_ACTIVE_LOG=
    export HFX_TEST_FAIL_BUILD_ID=
    export HFX_TEST_FAIL_VALIDATE_ID=
    export HFX_TEST_REQUIRE_LOCK_OWNER=
    export HFX_TEST_REQUIRE_CLEARED_DIAGNOSTIC=
    export HFX_TEST_INTERRUPT_AFTER=
    export HFX_TEST_FAIL_ASSEMBLY=
    export HFX_TEST_FAIL_ASSEMBLY_VALIDATE=
    HFX_TDX_CALIBRATION_NOW_FILE=$test_tmp/calibration-now
    export HFX_TDX_CALIBRATION_NOW_FILE
    calibration_clock=${HFX_TEST_CLOCK_START-100}
    : >"$HFX_TDX_CALIBRATION_NOW_FILE"
    while [[ "$calibration_clock" -le $((${HFX_TEST_CLOCK_START-100} + 400)) ]]; do
        printf '%s\n' "$calibration_clock" >>"$HFX_TDX_CALIBRATION_NOW_FILE"
        calibration_clock=$((calibration_clock + 1))
    done
}

calibration_complete() {
    local name=$1
    local parallel=$2
    HFX_TEST_CLOCK_START=140 calibration_prepare_workers
    calibration_run "$name" "$parallel" >"$case_stdout" 2>"$case_stderr" || {
        sed 's/^/calibration stdout: /' "$case_stdout" >&2
        sed 's/^/calibration complete: /' "$case_stderr" >&2
        die "calibration parallel-$parallel did not complete"
    }
}

calibration_cohort_case() {
    local name
    local mutation
    local p2
    local p4
    local parallel
    local expected
    calibration_fake_setup
    calibration_new_campaign calibration-matrix
    for name in pending:pending:2:disk running:pending:2:disk measured:pending:4:disk \
        pending:running:2:ordering running:running:2:ordering measured:running:4:disk \
        pending:measured:2:ordering running:measured:2:ordering measured:measured:2:success; do
        IFS=: read -r p2 p4 parallel expected <<<"$name"
        calibration_write_state "$calibration_dir" "$p2" "$p4" \
            "$([[ "$p2:$p4" == measured:measured ]] && printf 2 || printf null)"
        cp "$calibration_dir/state/calibration.json" "$test_tmp/matrix-before"
        if [[ "$expected" == success ]]; then
            calibration_run calibration-matrix "$parallel" >"$case_stdout"
            assert_contains "$case_stdout" 'calibration_selected_max_parallel=2'
        else
            if [[ "$expected" == disk ]]; then
                HFX_TEST_PIPELINE_AVAILABLE_BYTES=0 \
                    expect_failure "calibration matrix $p2/$p4" calibrate \
                    --campaign calibration-matrix --workspace-root "$calibration_root" \
                    --max-parallel "$parallel" --fabric-version fixture-v1
                assert_contains "$case_stderr" 'insufficient calibration disk'
            else
                expect_failure "calibration matrix $p2/$p4" calibrate \
                    --campaign calibration-matrix --workspace-root "$calibration_root" \
                    --max-parallel "$parallel" --fabric-version fixture-v1
                assert_contains "$case_stderr" 'calibration cohort status ordering is malformed'
            fi
            cmp "$test_tmp/matrix-before" "$calibration_dir/state/calibration.json"
        fi
    done
    calibration_write_state "$calibration_dir" measured measured 2
    for mutation in \
        '.cohorts["parallel-2"].status = "measured" | .cohorts["parallel-2"].attempts = 1 | .cohorts["parallel-2"].measurement = null' \
        '.cohorts["parallel-2"].measurement.extra = true' \
        '.cohorts["parallel-2"].measurement.raw.elapsed_seconds = 0' \
        '.cohorts["parallel-2"].measurement.steady_state.end_timestamp_seconds = 99' \
        '.cohorts["parallel-2"].measurement.steady_state.bytes = 999999' \
        '.cohorts["parallel-2"].measurement.steady_state.throughput_bytes_per_second = 1' \
        '.cohorts["parallel-2"].measurement.steady_state.compile_completions = 5' \
        '.cohorts["parallel-2"].measurement.steady_state.compile_wall_seconds = 9223372036854775808' \
        '.cohorts["parallel-2"].measurement.raw.bytes = 9223372036854775808'; do
        calibration_write_state "$calibration_dir" measured measured 2
        jq "$mutation" "$calibration_dir/state/calibration.json" >"$calibration_dir/state/tmp/malformed"
        mv "$calibration_dir/state/tmp/malformed" "$calibration_dir/state/calibration.json"
        jq -cS '.cohorts["parallel-4"]' "$calibration_dir/state/calibration.json" >"$test_tmp/p4-before"
        cp "$calibration_dir/state/calibration.json" "$test_tmp/malformed-before"
        expect_failure 'malformed calibration mutation' calibrate --campaign calibration-matrix \
            --workspace-root "$calibration_root" --max-parallel 2 --fabric-version fixture-v1
        assert_contains "$case_stderr" 'calibration state is malformed'
        cmp "$test_tmp/malformed-before" "$calibration_dir/state/calibration.json"
        jq -cS '.cohorts["parallel-4"]' "$calibration_dir/state/calibration.json" >"$test_tmp/p4-after"
        cmp "$test_tmp/p4-before" "$test_tmp/p4-after"
    done
    calibration_write_state "$calibration_dir" measured measured 2
    jq '.cohorts["parallel-2"].measurement.raw.bytes=40000000000 |
        .cohorts["parallel-2"].measurement.raw.throughput_bytes_per_second=4000000000 |
        .cohorts["parallel-2"].measurement.excluded_drain_tail.end_bytes=40000000000 |
        .cohorts["parallel-2"].measurement.excluded_drain_tail.bytes=
          (40000000000-.cohorts["parallel-2"].measurement.excluded_drain_tail.start_bytes)' \
        "$calibration_dir/state/calibration.json" >"$calibration_dir/state/tmp/large"
    mv "$calibration_dir/state/tmp/large" "$calibration_dir/state/calibration.json"
    calibration_run calibration-matrix 2 >"$case_stdout"
    assert_contains "$case_stdout" 'calibration_selected_max_parallel=2'
    calibration_new_campaign calibration-admission
    cp -R "$calibration_dir/state/basins" "$test_tmp/admission-basins"
    HFX_TEST_PIPELINE_AVAILABLE_BYTES=61989017472 \
        expect_failure 'calibration admission shortfall' calibrate --campaign calibration-admission \
        --workspace-root "$calibration_root" --max-parallel 2 --fabric-version fixture-v1
    assert_contains "$case_stderr" 'insufficient calibration disk'
    jq -e '.cohorts["parallel-2"] | .status == "pending" and .attempts == 0' \
        "$calibration_dir/state/calibration.json" >/dev/null ||
        die 'admission shortfall changed the cohort'
    diff -ru "$test_tmp/admission-basins" "$calibration_dir/state/basins"
    [[ ! -e "$calibration_dir/state/pipeline.json" ]] || die 'admission shortfall created pipeline state'
    [[ $(find "$calibration_dir/state/calibration" -name '*.samples.tsv' | wc -l | tr -d ' ') -eq 0 ]] ||
        die 'admission shortfall created an attempt trace'
    pass 'calibration cohorts and schema are immutable and fail closed'
}

calibration_measurement_case() {
    local trace=$test_tmp/calibration-idle-refill.tsv
    local regression=$test_tmp/calibration-regression.tsv
    local attempts
    printf '100\t0\t2\t0\t0\n101\t500000\t1\t0\t0\n102\t1000000\t2\t1\t1\n103\t2000000\t1\t2\t2\n110\t10000000\t0\t2\t2\n' >"$trace"
    printf '100\t0\t2\t0\t0\n101\t500000\t1\t0\t0\n102\t1000000\t2\t1\t1\n103\t800000\t1\t2\t2\n110\t10000000\t0\t2\t2\n' >"$regression"
    [[ $(wc -l <"$trace" | tr -d ' ') -eq 5 && $(wc -l <"$regression" | tr -d ' ') -eq 5 ]] ||
        die 'calibration trace fixtures are incomplete'
    calibration_fake_setup
    calibration_new_campaign calibration-measurement
    calibration_complete calibration-measurement 2
    jq -e '.cohorts["parallel-2"] |
      .status == "measured" and .measurement.raw.bytes == 384 and
      .measurement.steady_state.end_bytes > .measurement.steady_state.start_bytes and
      .measurement.steady_state.compile_completions > 0' \
        "$calibration_dir/state/calibration.json" >/dev/null ||
        die "real parallel-2 calibration measurement differs: $(jq -c '.cohorts["parallel-2"].measurement' "$calibration_dir/state/calibration.json")"
    attempts=$(jq -r '.cohorts["parallel-2"].attempts' "$calibration_dir/state/calibration.json")
    [[ "$attempts" -eq 1 ]] || die 'first calibration did not create attempt one'
    trace=$calibration_dir/state/calibration/parallel-2-attempt-1.samples.tsv
    cp "$regression" "$trace"
    jq '.cohorts["parallel-2"].status="running" |
        .cohorts["parallel-2"].measurement=null' "$calibration_dir/state/calibration.json" \
        >"$calibration_dir/state/tmp/replay.json"
    mv "$calibration_dir/state/tmp/replay.json" "$calibration_dir/state/calibration.json"
    mv "$calibration_dir/state/calibration/parallel-2-pipeline.json" "$calibration_dir/state/pipeline.json"
    HFX_TEST_CLOCK_START=140
    HFX_TEST_CLOCK_START=140 calibration_prepare_workers
    unset HFX_TEST_CLOCK_START
    calibration_run calibration-measurement 2 >"$case_stdout"
    jq -e '.cohorts["parallel-2"] |
      .status == "measured" and .attempts == 1 and
      .measurement.raw.bytes == 10000000 and
      .measurement.steady_state.end_bytes == 1000000 and
      .measurement.steady_state.bytes == 1000000 and
      .measurement.steady_state.throughput_bytes_per_second == 333333 and
      .measurement.steady_state.compile_completions == 2' \
        "$calibration_dir/state/calibration.json" >/dev/null ||
        die 'terminal replay did not clamp the corrected-end byte regression'
    [[ ! -e "$calibration_dir/state/calibration/parallel-2-attempt-2.samples.tsv" ]] ||
        die 'terminal replay scheduled a new attempt'
    printf '100\t0\t0\t0\t0\n101\t0\t2\t0\t0\n102\t48\t1\t1\t1\n' >"$trace"
    printf '110\t48\t1\t1\t1\n121\t144\t0\t2\t2\n' \
        >"$calibration_dir/state/calibration/parallel-2-attempt-2.samples.tsv"
    jq '.cohorts["parallel-2"].status="running" | .cohorts["parallel-2"].attempts=2 |
        .cohorts["parallel-2"].measurement=null' "$calibration_dir/state/calibration.json" \
        >"$calibration_dir/state/tmp/replay.json"
    mv "$calibration_dir/state/tmp/replay.json" "$calibration_dir/state/calibration.json"
    mv "$calibration_dir/state/calibration/parallel-2-pipeline.json" "$calibration_dir/state/pipeline.json"
    printf 'orphan\n' >"$calibration_dir/state/calibration/parallel-2-attempt-3.samples.tsv"
    HFX_TEST_CLOCK_START=140 calibration_prepare_workers
    calibration_run calibration-measurement 2 >"$case_stdout"
    jq -e '.cohorts["parallel-2"] |
      .status == "measured" and .attempts == 2 and
      .measurement.excluded_drain_tail.start_timestamp_seconds == 102 and
      .measurement.excluded_drain_tail.end_timestamp_seconds == 140 and
      .measurement.excluded_drain_tail.elapsed_seconds == 30 and
      .measurement.excluded_drain_tail.elapsed_seconds <
        (.measurement.excluded_drain_tail.end_timestamp_seconds -
         .measurement.excluded_drain_tail.start_timestamp_seconds)' \
        "$calibration_dir/state/calibration.json" >/dev/null ||
        die 'two-attempt drain tail did not exclude inter-attempt idle time'
    [[ ! -e "$calibration_dir/state/calibration/parallel-2-attempt-3.samples.tsv" ]] ||
        die 'terminal replay retained its orphan next-attempt trace'
    rm -f -- "$calibration_dir/state/calibration/parallel-2-attempt-2.samples.tsv"
    printf '100\t0\t0\t0\t0\n101\t0\t2\t0\t0\n112\t144\t1\t2\t2\n113\t144\t0\t2\t2\n' >"$trace"
    jq '.cohorts["parallel-2"].status="running" | .cohorts["parallel-2"].attempts=1 |
        .cohorts["parallel-2"].measurement=null' \
        "$calibration_dir/state/calibration.json" >"$calibration_dir/state/tmp/replay.json"
    mv "$calibration_dir/state/tmp/replay.json" "$calibration_dir/state/calibration.json"
    mv "$calibration_dir/state/calibration/parallel-2-pipeline.json" "$calibration_dir/state/pipeline.json"
    HFX_TEST_CLOCK_START=140 calibration_prepare_workers
    calibration_run calibration-measurement 2 >"$case_stdout"
    [[ $(jq -r '.cohorts["parallel-2"].measurement.steady_state.throughput_bytes_per_second' \
        "$calibration_dir/state/calibration.json") -eq 13 ]] ||
        die 'single-attempt corrected throughput differs from 144 bytes over 11 seconds'
    printf '100\t0\t0\t0\t0\n101\t0\t2\t0\t0\n' >"$trace"
    printf '110\t0\t2\t0\t0\n121\t144\t1\t2\t2\n122\t144\t0\t2\t2\n' \
        >"$calibration_dir/state/calibration/parallel-2-attempt-2.samples.tsv"
    jq '.cohorts["parallel-2"].status="running" | .cohorts["parallel-2"].attempts=2 |
        .cohorts["parallel-2"].measurement=null' "$calibration_dir/state/calibration.json" \
        >"$calibration_dir/state/tmp/replay.json"
    mv "$calibration_dir/state/tmp/replay.json" "$calibration_dir/state/calibration.json"
    mv "$calibration_dir/state/calibration/parallel-2-pipeline.json" "$calibration_dir/state/pipeline.json"
    HFX_TEST_CLOCK_START=140 calibration_prepare_workers
    calibration_run calibration-measurement 2 >"$case_stdout"
    jq -e '.cohorts["parallel-2"].measurement.steady_state |
      .bytes == 144 and .elapsed_seconds == 11 and .throughput_bytes_per_second == 13 and
      .attempts == [1,2]' "$calibration_dir/state/calibration.json" >/dev/null ||
        die 'inter-attempt idle time diluted corrected throughput'
    printf '110\t0\t1\t0\t0\n121\t144\t0\t2\t2\n' \
        >"$calibration_dir/state/calibration/parallel-2-attempt-2.samples.tsv"
    jq '.cohorts["parallel-2"].status="running" | .cohorts["parallel-2"].measurement=null' \
        "$calibration_dir/state/calibration.json" >"$calibration_dir/state/tmp/replay.json"
    mv "$calibration_dir/state/tmp/replay.json" "$calibration_dir/state/calibration.json"
    mv "$calibration_dir/state/calibration/parallel-2-pipeline.json" "$calibration_dir/state/pipeline.json"
    HFX_TEST_CLOCK_START=140 calibration_prepare_workers
    calibration_run calibration-measurement 2 >"$case_stdout"
    jq -e '.cohorts["parallel-2"].measurement.steady_state |
      .elapsed_seconds > 0 and .bytes > 0 and .compile_completions == 0' \
        "$calibration_dir/state/calibration.json" >/dev/null ||
        die 'narrow resume did not retain an explicitly invalid nonzero fallback interval'
    printf '140\t384\t0\t2\t2\n' >>"$calibration_dir/state/calibration/parallel-2-attempt-2.samples.tsv"
    jq '.cohorts["parallel-2"].status="running" | .cohorts["parallel-2"].measurement=null' \
        "$calibration_dir/state/calibration.json" >"$calibration_dir/state/tmp/replay.json"
    mv "$calibration_dir/state/tmp/replay.json" "$calibration_dir/state/calibration.json"
    mv "$calibration_dir/state/calibration/parallel-2-pipeline.json" "$calibration_dir/state/pipeline.json"
    before=$(wc -l <"$calibration_dir/state/calibration/parallel-2-attempt-2.samples.tsv" | tr -d ' ')
    HFX_TEST_CLOCK_START=140 calibration_prepare_workers
    calibration_run calibration-measurement 2 >"$case_stdout"
    [[ $(wc -l <"$calibration_dir/state/calibration/parallel-2-attempt-2.samples.tsv" | tr -d ' ') -eq "$before" ]] ||
        die 'terminal sample de-duplication appended an identical record'
    calibration_write_state "$calibration_dir" measured measured null
    jq --argjson p2 "$(calibration_measurement_json 1000000 2)" \
        --argjson p4 "$(calibration_measurement_json 1100000 0)" \
        '.cohorts["parallel-2"].measurement=$p2 | .cohorts["parallel-4"].measurement=$p4 |
         .selected_max_parallel=2 | .selected_throughput_validity="compile-observed"' "$calibration_dir/state/calibration.json" \
        >"$calibration_dir/state/tmp/ranking"
    mv "$calibration_dir/state/tmp/ranking" "$calibration_dir/state/calibration.json"
    calibration_run calibration-measurement 2 >"$case_stdout"
    assert_contains "$case_stdout" 'calibration_selected_max_parallel=2'
    jq --argjson p2 "$(calibration_measurement_json 1044999 2)" \
        '.cohorts["parallel-2"].measurement=$p2 |
         .cohorts["parallel-4"].measurement.steady_state.compile_completions=2 |
         .cohorts["parallel-4"].measurement.steady_state.compile_wall_seconds=1 |
         .selected_max_parallel=4 | .selected_throughput_validity="compile-observed"' \
        "$calibration_dir/state/calibration.json" >"$calibration_dir/state/tmp/ranking"
    mv "$calibration_dir/state/tmp/ranking" "$calibration_dir/state/calibration.json"
    calibration_run calibration-measurement 4 >"$case_stdout"
    assert_contains "$case_stdout" 'calibration_selected_max_parallel=4'
    pass 'calibration traces recover paid work and compute bounded measurements'
}

calibration_replay_case() {
    local before
    local build_before
    calibration_fake_setup
    calibration_new_campaign calibration-replay
    calibration_write_state "$calibration_dir" running pending null
    mkdir "$calibration_dir/state/calibration"
    jq -n '{schema_version:1,name:"parallel-2",max_parallel:2,
      basin_ids:["1020011530","3020003790","6020006540","8020008900"]}' \
        >"$calibration_dir/state/calibration/parallel-2.json"
    printf '100\t0\t2\t0\t0\n101\t1\t1\t0\t0\n' \
        >"$calibration_dir/state/calibration/parallel-2-attempt-1.samples.tsv"
    printf 'orphan\n' >"$calibration_dir/state/calibration/parallel-2-attempt-2.samples.tsv"
    build_before=$(grep -c '^build' "$test_tmp/invocations/adapter.log" || :)
    HFX_TEST_CLOCK_START=110
    calibration_complete calibration-replay 2
    unset HFX_TEST_CLOCK_START
    jq -e '.cohorts["parallel-2"] |
      .status == "measured" and .attempts == 2 and .measurement.raw.start_timestamp_seconds == 100' \
        "$calibration_dir/state/calibration.json" >/dev/null ||
        die 'running calibration did not resume in attempt two over the ordered concatenation'
    [[ -s "$calibration_dir/state/calibration/parallel-2-attempt-2.samples.tsv" ]] ||
        die 'resumed calibration did not retain attempt two'
    ! grep -q orphan "$calibration_dir/state/calibration/parallel-2-attempt-2.samples.tsv" ||
        die 'orphan attempt trace was not adopted and truncated'
    calibration_complete calibration-replay 4
    jq -e '.selected_max_parallel == 2 and .selected_throughput_validity == "compile-observed" and
      ([.cohorts[].status] | all(. == "measured"))' \
        "$calibration_dir/state/calibration.json" >/dev/null ||
        die 'both cohorts did not freeze the five-percent selection'
    [[ $(find "$calibration_dir/state/calibration" -name '*-pipeline.json' | wc -l | tr -d ' ') -eq 2 ]] ||
        die 'both scheduler snapshots were not archived'
    [[ $(($(grep -c '^build' "$test_tmp/invocations/adapter.log") - build_before)) -eq 8 ]] ||
        die 'both four-basin cohorts were not compiled'
    jq -s -e '[.[] | select(.retention.inputs_reclaimed == true)] | length == 8' \
        "$calibration_dir"/state/basins/{1020011530,3020003790,6020006540,8020008900,2020003440,4020006940,7020014250,9020000010}/current.json \
        >/dev/null || die 'both cohorts were not reclaimed'
    before=$(wc -l <"$test_tmp/invocations/adapter.log" | tr -d ' ')
    calibration_run calibration-replay 4 >"$case_stdout"
    [[ $(wc -l <"$test_tmp/invocations/adapter.log" | tr -d ' ') -eq "$before" ]] ||
        die 'measured replay rescheduled paid work'
    mv "$calibration_dir/state/calibration/parallel-2-pipeline.json" "$calibration_dir/state/pipeline.json"
    jq '.basins["2020003440"]=.basins["1020011530"] |
        .basin_ids=(.basins|keys)' "$calibration_dir/state/pipeline.json" \
        >"$calibration_dir/state/tmp/production-pipeline"
    mv "$calibration_dir/state/tmp/production-pipeline" "$calibration_dir/state/pipeline.json"
    calibration_run calibration-replay 2 >"$case_stdout" 2>"$case_stderr" || {
        sed 's/^/unrelated pipeline: /' "$case_stderr" >&2
        die 'measured replay with unrelated pipeline failed'
    }
    [[ -f "$calibration_dir/state/pipeline.json" &&
        ! -e "$calibration_dir/state/calibration/parallel-2-pipeline.json" ]] ||
        die 'calibration finalization relocated unrelated production pipeline state'
    calibration_selected=$(jq -r '.selected_max_parallel' "$calibration_dir/state/calibration.json")
    [[ "$calibration_selected" -eq 2 ]] && calibration_wrong=4 || calibration_wrong=2
    expect_failure 'pipeline parallelism differs from calibration' pipeline \
        --campaign calibration-replay --workspace-root "$calibration_root" \
        --max-parallel "$calibration_wrong" --fabric-version fixture-v1
    assert_contains "$case_stderr" 'pipeline max-parallel differs from frozen calibration selection'
    pass 'calibration schedules both cohorts, reclaims them, and freezes selection'
}

calibration_disclosure_case() {
    local expected=$test_tmp/calibration-disclosure.expected
    sed >"$expected" <<'DISCLOSURE'
calibration_fabric_version=fixture-v1
calibration_selected_max_parallel=2
calibration_selected_throughput_validity=compile-observed
calibration_parallel_2_status=measured
calibration_parallel_2_raw_start_timestamp_seconds=100
calibration_parallel_2_raw_end_timestamp_seconds=110
calibration_parallel_2_raw_bytes=10000000
calibration_parallel_2_raw_elapsed_seconds=10
calibration_parallel_2_raw_retries=0
calibration_parallel_2_raw_throughput_bytes_per_second=1000000
calibration_parallel_2_steady_start_timestamp_seconds=100
calibration_parallel_2_steady_end_timestamp_seconds=101
calibration_parallel_2_steady_attempts=1
calibration_parallel_2_steady_start_bytes=0
calibration_parallel_2_steady_end_bytes=1000000
calibration_parallel_2_steady_bytes=1000000
calibration_parallel_2_steady_elapsed_seconds=1
calibration_parallel_2_steady_throughput_bytes_per_second=1000000
calibration_parallel_2_steady_compile_completions=2
calibration_parallel_2_steady_compile_wall_seconds=2
calibration_parallel_2_drain_start_timestamp_seconds=101
calibration_parallel_2_drain_end_timestamp_seconds=110
calibration_parallel_2_drain_start_bytes=1000000
calibration_parallel_2_drain_end_bytes=10000000
calibration_parallel_2_drain_bytes=9000000
calibration_parallel_2_drain_elapsed_seconds=9
calibration_parallel_4_status=measured
calibration_parallel_4_raw_start_timestamp_seconds=100
calibration_parallel_4_raw_end_timestamp_seconds=110
calibration_parallel_4_raw_bytes=10000000
calibration_parallel_4_raw_elapsed_seconds=10
calibration_parallel_4_raw_retries=0
calibration_parallel_4_raw_throughput_bytes_per_second=1000000
calibration_parallel_4_steady_start_timestamp_seconds=100
calibration_parallel_4_steady_end_timestamp_seconds=101
calibration_parallel_4_steady_attempts=1
calibration_parallel_4_steady_start_bytes=0
calibration_parallel_4_steady_end_bytes=1000000
calibration_parallel_4_steady_bytes=1000000
calibration_parallel_4_steady_elapsed_seconds=1
calibration_parallel_4_steady_throughput_bytes_per_second=1000000
calibration_parallel_4_steady_compile_completions=0
calibration_parallel_4_steady_compile_wall_seconds=0
calibration_parallel_4_drain_start_timestamp_seconds=101
calibration_parallel_4_drain_end_timestamp_seconds=110
calibration_parallel_4_drain_start_bytes=1000000
calibration_parallel_4_drain_end_bytes=10000000
calibration_parallel_4_drain_bytes=9000000
calibration_parallel_4_drain_elapsed_seconds=9
DISCLOSURE
    [[ $(wc -l <"$expected" | tr -d ' ') -eq 49 ]] || die 'calibration disclosure fixture does not contain 49 lines'
    calibration_fake_setup
    calibration_new_campaign calibration-disclosure
    calibration_write_state "$calibration_dir" measured measured 2
    jq '.cohorts["parallel-2"].measurement.steady_state.compile_wall_seconds=2 |
        .cohorts["parallel-4"].measurement.steady_state.compile_completions=0 |
        .cohorts["parallel-4"].measurement.steady_state.compile_wall_seconds=0' \
        "$calibration_dir/state/calibration.json" >"$calibration_dir/state/tmp/disclosure"
    mv "$calibration_dir/state/tmp/disclosure" "$calibration_dir/state/calibration.json"
    run_runner status --campaign calibration-disclosure --workspace-root "$calibration_root" \
        >"$test_tmp/status-with-calibration"
    run_runner progress --campaign calibration-disclosure --workspace-root "$calibration_root" \
        >"$test_tmp/progress-with-calibration"
    mv "$calibration_dir/state/calibration.json" "$calibration_dir/state/tmp/calibration.saved"
    run_runner status --campaign calibration-disclosure --workspace-root "$calibration_root" \
        >"$test_tmp/status-without-calibration"
    run_runner progress --campaign calibration-disclosure --workspace-root "$calibration_root" \
        >"$test_tmp/progress-without-calibration"
    mv "$calibration_dir/state/tmp/calibration.saved" "$calibration_dir/state/calibration.json"
    cmp "$test_tmp/status-with-calibration" "$test_tmp/status-without-calibration"
    cmp "$test_tmp/progress-with-calibration" "$test_tmp/progress-without-calibration"
    cp "$calibration_dir/state/calibration.json" "$calibration_dir/state/tmp/calibration.saved"
    printf '{\n' >"$calibration_dir/state/calibration.json"
    run_runner status --campaign calibration-disclosure --workspace-root "$calibration_root" >"$case_stdout"
    assert_contains "$case_stdout" 'calibration_state=malformed'
    mv "$calibration_dir/state/tmp/calibration.saved" "$calibration_dir/state/calibration.json"
    calibration_run calibration-disclosure 2 >"$case_stdout"
    tail -n 49 "$case_stdout" >"$test_tmp/disclosure.actual"
    cmp "$expected" "$test_tmp/disclosure.actual"
    cp "$case_stdout" "$test_tmp/disclosure.first"
    calibration_run calibration-disclosure 2 >"$case_stdout"
    cmp "$test_tmp/disclosure.first" "$case_stdout"
    [[ $(tail -n 49 "$case_stdout" | wc -l | tr -d ' ') -eq 49 ]] ||
        die 'calibration disclosure was not the final complete block'
    calibration_new_campaign calibration-disclosure-scheduled
    calibration_complete calibration-disclosure-scheduled 2
    assert_contains "$case_stdout" 'calibration_parallel_2_status=measured'
    pass 'calibration disclosure is complete ordered and byte-preserving'
}

calibration_scheduler_shape_case() {
    local build_before
    calibration_fake_setup
    calibration_new_campaign calibration-scheduler
    calibration_prepare_workers
    build_before=$(grep -c '^build' "$test_tmp/invocations/adapter.log" || :)
    export HFX_TEST_PIPELINE_KILL_KEY=1020011530-basins
    calibration_run calibration-scheduler 2 >"$case_stdout" 2>"$case_stderr" || {
        sed 's/^/calibration scheduler: /' "$case_stderr" >&2
        die 'calibration scheduler callback fixture failed'
    }
    unset HFX_TEST_PIPELINE_KILL_KEY
    jq -e '.cohorts["parallel-2"] |
      .status == "measured" and .measurement.steady_state.compile_completions > 0 and
      .measurement.steady_state.compile_wall_seconds >= 0' \
        "$calibration_dir/state/calibration.json" >/dev/null ||
        die 'scheduler callbacks did not produce a measured cohort'
    [[ $(grep -c '^start 1020011530-basins' "$test_tmp/transfer-state/events") -eq 2 ]] ||
        die 'vanished calibration worker was not swept and retried'
    [[ $(($(grep -c '^build' "$test_tmp/invocations/adapter.log") - build_before)) -eq 4 ]] ||
        die 'compile callbacks did not bracket all four builds'
    pass 'calibration callbacks preserve the scheduler and static contracts'
}

checkpoint_new_campaign() {
    local name=$1
    set -- $(init_args "$name" "$test_tmp")
    run_runner "$@" >/dev/null
}

checkpoint_schema_case() {
    local dir=$test_tmp/tdx-hydro-checkpoint-schema
    checkpoint_new_campaign checkpoint-schema
    cat >"$dir/state/pipeline.json" <<'EOF'
{
  "schema_version": 1,
  "fabric_version": "fixture-v1",
  "max_parallel": 2,
  "basin_ids": ["1020000010", "7020000010", "9020000010"],
  "basins": {
    "1020000010": {"status": "reclaimed", "blocked_reason": null},
    "7020000010": {"status": "reclaimed", "blocked_reason": null},
    "9020000010": {"status": "terminal", "blocked_reason": null}
  }
}
EOF
    run_runner checkpoint --campaign checkpoint-schema --workspace-root "$test_tmp" \
        --expected-terminal-count 2 >"$case_stdout"
    diff -u <(printf '%s\n' checkpoint_expected_terminal_count=2 \
        checkpoint_observed_terminal_count=2 checkpoint_result=met \
        checkpoint_run_state=running checkpoint_signal=not-required) "$case_stdout"
    mv "$dir/state/pipeline.json" "$dir/state/pipeline.evidence.json"
    if run_runner checkpoint --campaign checkpoint-schema --workspace-root "$test_tmp" \
        --expected-terminal-count 3 >"$case_stdout" 2>"$case_stderr"; then
        die 'absent checkpoint pipeline snapshot unexpectedly succeeded'
    fi
    assert_contains "$case_stderr" 'pipeline snapshot is absent; run pipeline with the frozen max-parallel and fabric version, then rerun checkpoint'
    printf '{\n' >"$dir/state/checkpoints.json"
    run_runner checkpoint-resume --campaign checkpoint-schema --workspace-root "$test_tmp" >"$case_stdout"
    assert_contains "$case_stdout" checkpoint_resume=recovered-malformed
    [[ -f "$dir/state/checkpoint-recovery/rejected-1.json" ]] || die 'malformed checkpoint was not archived'
    mv "$dir/state/pipeline.evidence.json" "$dir/state/pipeline.json"
    cat >"$dir/state/checkpoints.json" <<'EOF'
{
  "schema_version": 1,
  "run_state": "running",
  "resume_after_entry_count": null,
  "entries": [
    {
      "expected_terminal_count": 2,
      "observed_terminal_count": 2,
      "result": "met"
    }
  ]
}
EOF
    cp "$dir/state/checkpoints.json" "$test_tmp/checkpoint-met.json"
    if run_runner checkpoint --campaign checkpoint-schema --workspace-root "$test_tmp" \
        --expected-terminal-count 1 >"$case_stdout" 2>"$case_stderr"; then
        die 'decreasing checkpoint expectation unexpectedly succeeded'
    fi
    cmp "$test_tmp/checkpoint-met.json" "$dir/state/checkpoints.json"
    assert_contains "$case_stderr" 'checkpoint expected count cannot decrease below 2; rerun checkpoint with 2 or a higher value'
    cat >"$dir/state/checkpoints.json" <<'EOF'
{
  "schema_version": 1,
  "run_state": "stopped",
  "resume_after_entry_count": null,
  "entries": [
    {
      "expected_terminal_count": 3,
      "observed_terminal_count": 2,
      "result": "missed"
    }
  ]
}
EOF
    cp "$dir/state/checkpoints.json" "$test_tmp/checkpoint-stopped.json"
    run_runner checkpoint --campaign checkpoint-schema --workspace-root "$test_tmp" \
        --expected-terminal-count 3 >"$case_stdout" 2>"$case_stderr" || :
    assert_contains "$case_stdout" checkpoint_observed_terminal_count=2
    cmp "$test_tmp/checkpoint-stopped.json" "$dir/state/checkpoints.json"
    if run_runner checkpoint --campaign checkpoint-schema --workspace-root "$test_tmp" \
        --expected-terminal-count 4 >"$case_stdout" 2>"$case_stderr"; then
        die 'different stopped checkpoint expectation unexpectedly succeeded'
    fi
    assert_contains "$case_stderr" 'stopped checkpoint expects 3; run checkpoint-resume, then checkpoint with an equal or higher expected value'
    cat >"$dir/state/checkpoints.json" <<'EOF'
{
  "schema_version": 1,
  "run_state": "running",
  "resume_after_entry_count": 1,
  "entries": [
    {
      "expected_terminal_count": 3,
      "observed_terminal_count": 2,
      "result": "missed"
    }
  ]
}
EOF
    run_runner progress --campaign checkpoint-schema --workspace-root "$test_tmp" >"$case_stdout"
    assert_contains "$case_stdout" checkpoint_run_state=running
    assert_contains "$case_stdout" checkpoint_result=missed
    cat >"$dir/state/checkpoints.json" <<'EOF'
{
  "schema_version": 1,
  "run_state": "stopped",
  "resume_after_entry_count": null,
  "entries": []
}
EOF
    cp "$dir/state/checkpoints.json" "$test_tmp/rejected-2.expected"
    run_runner progress --campaign checkpoint-schema --workspace-root "$test_tmp" >"$case_stdout"
    assert_contains "$case_stdout" checkpoint_state=malformed
    run_runner checkpoint-resume --campaign checkpoint-schema --workspace-root "$test_tmp" >"$case_stdout"
    cmp "$test_tmp/rejected-2.expected" "$dir/state/checkpoint-recovery/rejected-2.json"
    cat >"$dir/state/checkpoints.json" <<'EOF'
{
  "schema_version": 1,
  "run_state": "stopped",
  "resume_after_entry_count": null,
  "entries": [
    {
      "expected_terminal_count": 2,
      "observed_terminal_count": 2,
      "result": "met"
    }
  ]
}
EOF
    cp "$dir/state/checkpoints.json" "$test_tmp/rejected-3.expected"
    run_runner checkpoint-resume --campaign checkpoint-schema --workspace-root "$test_tmp" >"$case_stdout"
    cmp "$test_tmp/rejected-3.expected" "$dir/state/checkpoint-recovery/rejected-3.json"
    cat >"$dir/state/checkpoints.json" <<'EOF'
{
  "schema_version": 1,
  "run_state": "stopped",
  "resume_after_entry_count": null,
  "entries": [
    {
      "expected_terminal_count": 3,
      "observed_terminal_count": 2,
      "result": "missed",
      "extra": true
    }
  ]
}
EOF
    cp "$dir/state/checkpoints.json" "$test_tmp/rejected-4.expected"
    run_runner checkpoint-resume --campaign checkpoint-schema --workspace-root "$test_tmp" >"$case_stdout"
    cmp "$test_tmp/rejected-4.expected" "$dir/state/checkpoint-recovery/rejected-4.json"
    /bin/rm -- "$dir/state/checkpoints.json"
    run_runner checkpoint-resume --campaign checkpoint-schema --workspace-root "$test_tmp" >"$case_stdout"
    assert_contains "$case_stdout" checkpoint_resume=recovered-malformed
    assert_contains "$case_stdout" checkpoint_recovery_path=state/checkpoint-recovery/rejected-1.json
    pass 'checkpoint schema recovery and absent snapshots remain operator-recoverable'
}

checkpoint_stop_case() {
    local dir=$test_tmp/tdx-hydro-checkpoint-stop
    local owner
    local race_dir=$test_tmp/tdx-hydro-checkpoint-race
    local race_mkdir=$test_tmp/checkpoint-race-mkdir
    checkpoint_new_campaign checkpoint-stop
    cat >"$dir/state/pipeline.json" <<'EOF'
{"schema_version":1,"fabric_version":"fixture-v1","max_parallel":2,"basin_ids":["9020000010"],"basins":{"9020000010":{"status":"terminal","blocked_reason":null}}}
EOF
    if run_runner checkpoint --campaign checkpoint-stop --workspace-root "$test_tmp" \
        --expected-terminal-count 1 >"$case_stdout" 2>"$case_stderr"; then
        die 'missed checkpoint unexpectedly succeeded'
    fi
    diff -u <(printf '%s\n' checkpoint_expected_terminal_count=1 \
        checkpoint_observed_terminal_count=0 checkpoint_result=missed \
        checkpoint_run_state=stopped checkpoint_signal=no-live-owner) "$case_stdout"
    jq -e '.run_state == "stopped" and (.entries | length) == 1' "$dir/state/checkpoints.json" >/dev/null
    run_runner checkpoint-resume --campaign checkpoint-stop --workspace-root "$test_tmp" >"$case_stdout"
    diff -u <(printf '%s\n' checkpoint_resume=resumed checkpoint_run_state=running) "$case_stdout"
    jq -e '.run_state == "running" and .resume_after_entry_count == 1' "$dir/state/checkpoints.json" >/dev/null
    /usr/bin/tail -f /dev/null &
    owner=$!
    mkdir "$dir/state/locks/campaign.lock"
    printf '%s\n' "$owner" >"$dir/state/locks/campaign.lock/owner.pid"
    run_runner checkpoint --campaign checkpoint-stop --workspace-root "$test_tmp" \
        --expected-terminal-count 1 >"$case_stdout" 2>"$case_stderr" || :
    assert_contains "$case_stdout" checkpoint_signal=sent
    if kill -0 "$owner" 2>/dev/null; then
        kill -TERM "$owner" 2>/dev/null || :
        wait "$owner" 2>/dev/null || :
        die 'checkpoint TERM did not stop the validated live owner'
    fi
    wait "$owner" 2>/dev/null || :
    cp "$dir/state/checkpoints.json" "$test_tmp/checkpoint-live-stopped.json"
    run_runner checkpoint --campaign checkpoint-stop --workspace-root "$test_tmp" \
        --expected-terminal-count 1 >"$case_stdout" 2>"$case_stderr" || :
    assert_contains "$case_stdout" checkpoint_signal=no-live-owner
    cmp "$test_tmp/checkpoint-live-stopped.json" "$dir/state/checkpoints.json"
    export HFX_TEST_PS_MODE=error
    if run_runner checkpoint --campaign checkpoint-stop --workspace-root "$test_tmp" \
        --expected-terminal-count 1 >"$case_stdout" 2>"$case_stderr"; then
        die 'indeterminate checkpoint owner unexpectedly succeeded'
    fi
    assert_contains "$case_stderr" "checkpoint owner PID $owner is indeterminate; resolve that PID or ps ambiguity, then rerun the same checkpoint"
    export HFX_TEST_PS_MODE=live
    if run_runner checkpoint --campaign checkpoint-stop --workspace-root "$test_tmp" \
        --expected-terminal-count 1 >"$case_stdout" 2>"$case_stderr"; then
        die 'failed checkpoint TERM delivery unexpectedly succeeded'
    fi
    assert_contains "$case_stderr" "could not deliver TERM to checkpoint owner PID $owner; rerun the same checkpoint with the same expected value"
    unset HFX_TEST_PS_MODE
    printf 'unsafe\n' >"$dir/state/locks/campaign.lock/owner.pid"
    if run_runner checkpoint --campaign checkpoint-stop --workspace-root "$test_tmp" \
        --expected-terminal-count 1 >"$case_stdout" 2>"$case_stderr"; then
        die 'unsafe checkpoint owner contents unexpectedly succeeded'
    fi
    assert_contains "$case_stderr" "checkpoint campaign lock owner contents are unsafe: $dir/state/locks/campaign.lock/owner.pid; correct or move that exact entry, then rerun the same checkpoint"
    cmp "$test_tmp/checkpoint-live-stopped.json" "$dir/state/checkpoints.json"
    rm -r "$dir/state/locks/campaign.lock"
    set -- $(reclaim_subset_init_args checkpoint-race "$test_tmp")
    run_runner "$@" >/dev/null
    cat >"$race_mkdir" <<'EOF'
#!/usr/bin/env bash
for argument in "$@"; do
    if [[ "$argument" == */state/locks/campaign.lock ]]; then
        printf '%s\n' '{"schema_version":1,"run_state":"stopped","resume_after_entry_count":null,"entries":[{"expected_terminal_count":1,"observed_terminal_count":0,"result":"missed"}]}' >"$HFX_TEST_RACE_STATE"
    fi
done
exec /bin/mkdir "$@"
EOF
    chmod +x "$race_mkdir"
    export HFX_TEST_RACE_STATE=$race_dir/state/checkpoints.json
    export HFX_TDX_MKDIR=$race_mkdir
    if run_runner pipeline --campaign checkpoint-race --workspace-root "$test_tmp" \
        --max-parallel 1 --fabric-version fixture-v1 >"$case_stdout" 2>"$case_stderr"; then
        die 'post-lock stopped checkpoint race unexpectedly scheduled pipeline work'
    fi
    assert_contains "$case_stderr" 'pipeline is stopped by checkpoint control; run checkpoint-resume, then rerun the exact pipeline command'
    [[ ! -e "$race_dir/state/pipeline.json" && ! -e "$race_dir/state/tmp/pipeline-completions.fifo" ]] ||
        die 'post-lock checkpoint guard allowed scheduler materialization'
    unset HFX_TDX_MKDIR
    unset HFX_TEST_RACE_STATE
    pass 'missed checkpoint stops the live pipeline and explicit resume preserves durable work'
}

checkpoint_progress_case() {
    local dir=$test_tmp/tdx-hydro-checkpoint-progress
    local original_jq
    local zero_jq=$test_tmp/zero-effective-jq
    set -- $(reclaim_subset_init_args checkpoint-progress "$test_tmp")
    run_runner "$@" >/dev/null
    original_jq=/usr/bin/jq
    cat >"$dir/state/pipeline.json" <<'EOF'
{
  "schema_version": 1,
  "fabric_version": "fixture-v1",
  "max_parallel": 2,
  "basin_ids": ["1020000010", "7020000010", "9020000010"],
  "basins": {
    "1020000010": {"status": "reclaimed", "blocked_reason": null},
    "7020000010": {"status": "reclaimed", "blocked_reason": null},
    "9020000010": {"status": "terminal", "blocked_reason": null}
  }
}
EOF
    cat >"$dir/state/checkpoints.json" <<'EOF'
{
  "schema_version": 1,
  "run_state": "running",
  "resume_after_entry_count": null,
  "entries": []
}
EOF
    cp -R "$dir" "$test_tmp/checkpoint-progress.before"
    run_runner progress --campaign checkpoint-progress --workspace-root "$test_tmp" >"$case_stdout"
    diff -ru "$test_tmp/checkpoint-progress.before" "$dir"
    assert_contains "$case_stdout" checkpoint_run_state=running
    assert_contains "$case_stdout" checkpoint_entry_count=0
    cat >"$dir/state/checkpoints.json" <<'EOF'
{"schema_version":1,"run_state":"running","resume_after_entry_count":null,"entries":[{"expected_terminal_count":2,"observed_terminal_count":2,"result":"met"}]}
EOF
    run_runner progress --campaign checkpoint-progress --workspace-root "$test_tmp" >"$case_stdout"
    assert_contains "$case_stdout" checkpoint_expected_terminal_count=2
    assert_contains "$case_stdout" checkpoint_observed_terminal_count=2
    [[ $(grep -n '^checkpoint_run_state=' "$case_stdout" | cut -d: -f1) -eq \
        $(($(grep -n '^pipeline_max_parallel=' "$case_stdout" | cut -d: -f1) + 9)) ]] ||
        die 'checkpoint progress block does not follow the complete pipeline block'
    [[ $(grep -n '^assemble_pending=' "$case_stdout" | cut -d: -f1) -eq \
        $(($(grep -n '^pipeline_max_parallel=' "$case_stdout" | cut -d: -f1) + 14)) ]] ||
        die 'assembly block does not follow the complete checkpoint block'
    cat >"$zero_jq" <<'EOF'
#!/usr/bin/env bash
if [[ "$#" -eq 3 && "$1" == -r ]] &&
    { [[ "$2" == 'keys[]' ]] || [[ "$2" == '.basin_ids[]' ]]; }; then
    exit 0
fi
exec "$HFX_TEST_REAL_JQ" "$@"
EOF
    chmod +x "$zero_jq"
    export HFX_TEST_REAL_JQ=$original_jq
    export HFX_TDX_JQ=$zero_jq
    run_runner progress --campaign checkpoint-progress --workspace-root "$test_tmp" >"$case_stdout"
    [[ $(grep -Ec '^(acquire_basins|acquire_streamnet|compile)_(pending|running|succeeded|failed)=0$' "$case_stdout") -eq 12 ]] ||
        die 'zero-ID progress did not report twelve exact zero stage counts'
    assert_contains "$case_stdout" inputs_reclaimed=0
    printf '{\n' >"$dir/state/checkpoints.json"
    run_runner progress --campaign checkpoint-progress --workspace-root "$test_tmp" >"$case_stdout"
    assert_contains "$case_stdout" checkpoint_state=malformed
    assert_contains "$case_stdout" 'checkpoint_recovery=run checkpoint-resume'
    unset HFX_TDX_JQ
    unset HFX_TEST_REAL_JQ
    pass 'checkpoint progress is lock-free ordered and safe for empty basin reads'
}

case ${HFX_TEST_FOCUS-} in
    '') ;;
    cohort) calibration_cohort_case; printf '1..%d\n' "$passed"; exit 0 ;;
    measurement) calibration_measurement_case; printf '1..%d\n' "$passed"; exit 0 ;;
    calibration-replay) calibration_replay_case; printf '1..%d\n' "$passed"; exit 0 ;;
    disclosure) calibration_disclosure_case; printf '1..%d\n' "$passed"; exit 0 ;;
    scheduler-shape) calibration_scheduler_shape_case; printf '1..%d\n' "$passed"; exit 0 ;;
    checkpoint-schema) checkpoint_schema_case; printf '1..%d\n' "$passed"; exit 0 ;;
    checkpoint-stop) checkpoint_stop_case; printf '1..%d\n' "$passed"; exit 0 ;;
    checkpoint-progress) checkpoint_progress_case; printf '1..%d\n' "$passed"; exit 0 ;;
    *) die "unknown HFX_TEST_FOCUS: $HFX_TEST_FOCUS" ;;
esac

write_compatibility_inventory() {
    cat >"$1" <<'EOF'
{
  "1020000010": "11",
  "1020011530": "12",
  "1020018110": "13",
  "1020021940": "14",
  "1020027430": "15",
  "1020034170": "16",
  "1020035180": "17",
  "1020040190": "18",
  "2020000010": "21",
  "2020003440": "22",
  "2020018240": "23",
  "2020024230": "24",
  "2020033490": "25",
  "2020041390": "26",
  "2020057170": "27",
  "2020065840": "28",
  "2020071190": "29",
  "3020000010": "31",
  "3020003790": "32",
  "3020005240": "33",
  "3020008670": "34",
  "3020009320": "35",
  "3020024310": "36",
  "4020000010": "41",
  "4020006940": "42",
  "4020015090": "43",
  "4020024190": "44",
  "4020034510": "45",
  "4020050210": "46",
  "4020050220": "47",
  "4020050290": "48",
  "4020050470": "49",
  "5020000010": "51",
  "5020015660": "52",
  "5020037270": "53",
  "5020049720": "54",
  "5020054880": "55",
  "5020055870": "56",
  "5020082270": "57",
  "6020000010": "61",
  "6020006540": "62",
  "6020008320": "63",
  "6020014330": "64",
  "6020017370": "65",
  "6020021870": "66",
  "6020029280": "67",
  "7020000010": "71",
  "7020014250": "72",
  "7020021430": "73",
  "7020024600": "74",
  "7020038340": "75",
  "7020046750": "76",
  "7020047840": "77",
  "7020065090": "78",
  "8020000010": "81",
  "8020008900": "82",
  "8020010700": "83",
  "8020020760": "84",
  "8020022890": "85",
  "8020032840": "86",
  "8020044560": "87",
  "9020000010": "91"
}
EOF
}

create_compatibility_workspace() {
    local root=$1
    local campaign_name=$2
    local basin_version=$3
    local campaign_dir=$root/tdx-hydro-$campaign_name
    local basin_id
    mkdir -p "$campaign_dir/downloads" "$campaign_dir/basin-outputs" \
        "$campaign_dir/reports" "$campaign_dir/assembly" "$campaign_dir/assembly/scratch" \
        "$campaign_dir/publication" "$campaign_dir/state" "$campaign_dir/state/basins" \
        "$campaign_dir/state/locks" "$campaign_dir/state/tmp"
    write_compatibility_inventory "$campaign_dir/state/inventory.json"
    cat >"$campaign_dir/state/selection.json" <<'EOF'
{
  "schema_version": 1,
  "basin_ids": [
    "1020000010"
  ]
}
EOF
    if [[ "$basin_version" == 4 ]]; then
        cat >"$campaign_dir/state/campaign.json" <<EOF
{
  "schema_version": 2,
  "campaign": "$campaign_name",
  "inventory": {
    "source": "adapters/tdx-hydro/data/tdx_header_numbers.json",
    "count": 62
  },
  "retention": {
    "policy": "reclaim-inputs-after-terminal",
    "reclaim_inputs": true,
    "retain_acquired_inputs": false,
    "retain_basin_outputs": true,
    "retain_external_reports": true
  },
  "sizing": {
    "available_memory_bytes": 30000000000,
    "available_disk_bytes": 491737129060,
    "peak_in_flight_download_bytes": 44296724480,
    "retained_basin_output_bytes": 206220202290,
    "assembly_memory_ceiling_bytes": 30000000000,
    "assembly_scratch_ceiling_bytes": 206220202290,
    "assembled_artifact_bytes": 206220202290,
    "active_compile_scratch_bytes": 30000000000,
    "filesystem_overhead_bytes": 5000000000,
    "required_memory_bytes": 30000000000,
    "required_disk_bytes": 491737129060
  }
}
EOF
    else
        cat >"$campaign_dir/state/campaign.json" <<EOF
{
  "schema_version": 2,
  "campaign": "$campaign_name",
  "inventory": {
    "source": "adapters/tdx-hydro/data/tdx_header_numbers.json",
    "count": 62
  },
  "retention": {
    "policy": "retain-all-through-publication",
    "reclaim_inputs": false,
    "retain_acquired_inputs": true,
    "retain_basin_outputs": true,
    "retain_external_reports": true
  },
  "sizing": {
    "available_memory_bytes": 11,
    "available_disk_bytes": 29,
    "retained_input_bytes": 5,
    "retained_basin_output_bytes": 6,
    "assembly_memory_ceiling_bytes": 11,
    "assembly_scratch_ceiling_bytes": 7,
    "assembled_artifact_bytes": 8,
    "active_compile_scratch_bytes": 9,
    "filesystem_overhead_bytes": 1,
    "required_memory_bytes": 11,
    "required_disk_bytes": 29
  }
}
EOF
    fi
    while IFS= read -r basin_id; do
        mkdir "$campaign_dir/state/basins/$basin_id"
        if [[ "$basin_version" == 4 ]]; then
            jq -n --arg basin_id "$basin_id" '{
              schema_version:4,processing_basin_id:$basin_id,
              retention:{inputs_reclaimed:false,policy:"reclaim-inputs-after-terminal"},
              stages:{
                acquire_basins:{status:"pending",attempts:0,failure_reason:null,evidence:null},
                acquire_streamnet:{status:"pending",attempts:0,failure_reason:null,evidence:null},
                compile:{status:"pending",attempts:0,failure_reason:null,diagnostic_report:null}
              }
            }' >"$campaign_dir/state/basins/$basin_id/current.json"
        else
            jq -n --arg basin_id "$basin_id" '{
              schema_version:3,processing_basin_id:$basin_id,
              stages:{
                acquire_basins:{status:"pending",attempts:0,failure_reason:null,evidence:null},
                acquire_streamnet:{status:"pending",attempts:0,failure_reason:null,evidence:null},
                compile:{status:"pending",attempts:0,failure_reason:null,diagnostic_report:null}
              }
            }' >"$campaign_dir/state/basins/$basin_id/current.json"
        fi
    done < <(jq -r 'keys[]' "$campaign_dir/state/inventory.json")
}

compatibility_case() {
    local basin_version=$1
    local campaign_name=$2
    local label=$3
    local root=$test_tmp/workspaces/$campaign_name
    local campaign_dir=$root/tdx-hydro-$campaign_name
    local basin_id
    mkdir "$root"
    create_compatibility_workspace "$root" "$campaign_name" "$basin_version"
    cp -R "$campaign_dir" "$test_tmp/$campaign_name.before"
    run_runner status --campaign "$campaign_name" --workspace-root "$root" >"$case_stdout" ||
        die "$label compatibility status failed"
    assert_contains "$case_stdout" 'selected_basin_count=1'
    [[ "$basin_version" != 4 ]] || assert_contains "$case_stdout" 'inputs_reclaimed=0'
    [[ $(grep -c '_exhausted=' "$case_stdout" || :) -eq 0 ]] ||
        die "$label compatibility status added exhaustion output"
    diff -ru "$test_tmp/$campaign_name.before" "$campaign_dir"
    rm -rf "$test_tmp/transfer-state"
    mkdir "$test_tmp/transfer-state"
    export HFX_TEST_FAIL_KEY= HFX_TEST_FAIL_ONCE_KEY=
    run_runner acquire --campaign "$campaign_name" --workspace-root "$root" \
        --max-parallel 1 --product-attempt-ceiling 1 >"$case_stdout" 2>"$case_stderr" || {
        sed 's/^/compatibility acquire: /' "$case_stderr" >&2
        die "$label bounded acquire failed"
    }
    jq -e --argjson version "$basin_version" '
      .schema_version == 5 and .acquisition.product_attempt_ceiling == 1 and
      .stages.acquire_basins.status == "succeeded" and
      .stages.acquire_streamnet.status == "succeeded" and
      (if $version == 4 then
        .retention == {inputs_reclaimed:false,policy:"reclaim-inputs-after-terminal"}
       else has("retention") | not end)
    ' "$campaign_dir/state/basins/1020000010/current.json" >/dev/null ||
        die "$label bounded conversion differs"
    while IFS= read -r basin_id; do
        [[ "$basin_id" == 1020000010 ]] && continue
        cmp "$test_tmp/$campaign_name.before/state/basins/$basin_id/current.json" \
            "$campaign_dir/state/basins/$basin_id/current.json"
    done < <(jq -r 'keys[]' "$campaign_dir/state/inventory.json")
    cmp "$test_tmp/$campaign_name.before/state/campaign.json" "$campaign_dir/state/campaign.json"
    cmp "$test_tmp/$campaign_name.before/state/inventory.json" "$campaign_dir/state/inventory.json"
    cmp "$test_tmp/$campaign_name.before/state/selection.json" "$campaign_dir/state/selection.json"
    pass "$label compatibility loads unchanged and converts only selected state"
}

pipeline_absence_regression_case() {
    local root=$test_tmp/workspaces/legacy-pipeline-control
    local campaign_dir=$root/tdx-hydro-legacy-pipeline-control
    local pipeline_status=0
    mkdir "$root"
    create_compatibility_workspace "$root" legacy-pipeline-control 4
    rm -rf "$test_tmp/transfer-state"
    mkdir "$test_tmp/transfer-state"
    export HFX_TEST_TRANSFER_STATE=$test_tmp/transfer-state
    export HFX_TEST_PIPELINE_CAMPAIGN_DIR=$campaign_dir
    export HFX_TEST_PIPELINE_COMPLETION_PATH=$campaign_dir/state/tmp/pipeline-completions.fifo
    export HFX_TEST_PIPELINE_AVAILABLE_BYTES=79734104064
    export HFX_TEST_FAIL_KEY=1020000010-basins
    export HFX_TEST_FAIL_ONCE_KEY=
    run_runner pipeline --campaign legacy-pipeline-control --workspace-root "$root" \
        --max-parallel 1 --fabric-version fixture-v1 >"$case_stdout" 2>"$case_stderr" ||
        pipeline_status=$?
    if ! { [[ "$pipeline_status" -ne 0 ]] &&
        [[ $(grep -c '^start 1020000010-basins$' "$test_tmp/transfer-state/events" || :) -eq 2 ]] &&
        [[ $(jq -r '.stages.acquire_basins.attempts' "$campaign_dir/state/basins/1020000010/current.json") -eq 2 ]] &&
        [[ $(jq -r '.basins["1020000010"].status' "$campaign_dir/state/pipeline.json") == pending ]] &&
        grep -Fqx -- 'hfx: error: pipeline incomplete: pending=1 acquiring=0 ready=0 compiling=0 terminal=0 reclaimed=0 blocked=0' "$case_stderr"; }; then
        sed 's/^/pipeline absence: /' "$case_stderr" >&2
        die 'pipeline absence regression: expected two fresh basins attempts, attempts=2, pending scheduler, and exact incomplete diagnostic'
    fi
    unset HFX_TEST_FAIL_KEY
    pass 'pipeline absence regression preserves two unbounded dispatch attempts'
}

run_mutation_discriminator() {
    local mutation_name=$1
    local mutation_case=$2
    local mutation_from=$3
    local mutation_to=$4
    local mutation_expected_failure=$5
    local mutation_output=$test_tmp/$mutation_case.mutation-output
    [[ $(grep -Fxc -- "$mutation_from" "$runner") -eq 1 ]] ||
        die "$mutation_name anchor does not occur exactly once"
    if HFX_TEST_MUTATION_CHILD=1 \
        HFX_TEST_MUTATION_CASE="$mutation_case" \
        HFX_TEST_MUTATION_FROM="$mutation_from" \
        HFX_TEST_MUTATION_TO="$mutation_to" \
        /bin/bash "$SCRIPT_DIR/test-tdx-hydro-campaign.sh" \
        >"$mutation_output" 2>&1; then
        die "$mutation_name unexpectedly survived"
    fi
    grep -Fqx -- "$mutation_expected_failure" "$mutation_output" ||
        die "$mutation_name did not fail at its named compatibility assertion"
    [[ $(grep -Fxc -- "$mutation_expected_failure" "$mutation_output") -eq 1 ]] ||
        die "$mutation_name did not emit its named assertion exactly once"
    [[ $(grep -F 'test-tdx-hydro-campaign: error:' "$mutation_output" | head -n 1) == \
        "$mutation_expected_failure" ]] ||
        die "$mutation_name did not emit its named assertion first"
}

# Install the existing local-only mock block early so compatibility status is the
# first runner invocation and its bounded arm can still use inspected transfers.
sed -n "/^printf 'SQLite format 3/,/^export HFX_TEST_DIFF=/p" "$0" >"$test_tmp/early-fake-setup"
[[ -s "$test_tmp/early-fake-setup" ]] || die 'early fake setup extraction was empty'
source "$test_tmp/early-fake-setup"

if [[ "${HFX_TEST_MUTATION_CHILD-}" != 1 ]]; then
    run_mutation_discriminator \
        'schema-version-3 rejection mutation' schema-version-3 \
        '    (.schema_version == 1 or .schema_version == 2 or .schema_version == 3 or .schema_version == 4 or .schema_version == 5) and' \
        '    (.schema_version == 1 or .schema_version == 2 or .schema_version == 4 or .schema_version == 5) and' \
        'test-tdx-hydro-campaign: error: schema-version-3 compatibility status failed'
    run_mutation_discriminator \
        'schema-version-4 rejection mutation' schema-version-4 \
        '    (.schema_version == 1 or .schema_version == 2 or .schema_version == 3 or .schema_version == 4 or .schema_version == 5) and' \
        '    (.schema_version == 1 or .schema_version == 2 or .schema_version == 3 or .schema_version == 5) and' \
        'test-tdx-hydro-campaign: error: schema-version-4 compatibility status failed'
    run_mutation_discriminator \
        'legacy pipeline bounded-routing mutation' legacy-pipeline \
        '    acquire_basin "$basin_id"' \
        '    acquire_basin_bounded "$basin_id"' \
        'test-tdx-hydro-campaign: error: pipeline absence regression: expected two fresh basins attempts, attempts=2, pending scheduler, and exact incomplete diagnostic'
fi

case ${HFX_TEST_MUTATION_CASE-} in
    schema-version-3)
        compatibility_case 3 legacy-v3-campaign schema-version-3
        ;;
    schema-version-4)
        compatibility_case 4 legacy-reclaim-campaign schema-version-4
        ;;
    legacy-pipeline)
        pipeline_absence_regression_case
        ;;
    '')
        compatibility_case 3 legacy-v3-campaign schema-version-3
        compatibility_case 4 legacy-reclaim-campaign schema-version-4
        compatibility_case 3 legacy-campaign existing-campaign-json
        pipeline_absence_regression_case
        ;;
    *) die "unknown HFX_TEST_MUTATION_CASE: $HFX_TEST_MUTATION_CASE" ;;
esac

# The ordinary suite below owns a fresh instance of the same mock block.
rm -rf "$test_tmp/transfer-state" "$test_tmp/geopackage-template" \
    "$test_tmp/fake-curl" "$test_tmp/recording-mkdir" "$test_tmp/fake-sha256sum" \
    "$test_tmp/fake-ogrinfo" "$test_tmp/fake-jq" "$test_tmp/fake-adapter" \
    "$test_tmp/fake-adapter-python" "$test_tmp/fake-hfx" "$test_tmp/pipeline-mv" \
    "$test_tmp/pipeline-rm"
unset HFX_TDX_CURL HFX_TDX_SHA256SUM HFX_TDX_OGRINFO HFX_TDX_ADAPTER_PYTHON \
    HFX_TDX_HFX HFX_TEST_TRANSFER_STATE HFX_TEST_GPKG_TEMPLATE HFX_TEST_REAL_JQ \
    HFX_TEST_ADAPTER_LOG HFX_TEST_ADAPTER_SCRIPT HFX_TEST_ADAPTER_STUB HFX_TEST_HFX_LOG \
    HFX_TEST_HFX_STATUS_LOG HFX_TEST_DIFF HFX_TEST_FAIL_KEY HFX_TEST_FAIL_ONCE_KEY

run_runner -h >"$case_stdout"
run_runner --help >"$case_stdout"
assert_contains "$case_stdout" 'Usage: tdx-hydro-campaign.sh init --campaign <id> [--workspace-root <path>] [--basin <processing-basin-id>]... [--retention-policy <retain-all-through-publication|reclaim-inputs-after-terminal>] --available-memory-bytes <integer> --available-disk-bytes <integer> (--retained-input-bytes <integer> | --peak-in-flight-download-bytes 44296724480) --retained-basin-output-bytes <integer> --assembly-memory-ceiling-bytes <integer> --assembly-scratch-ceiling-bytes <integer> --assembled-artifact-bytes <integer> --active-compile-scratch-bytes <integer> --filesystem-overhead-bytes <integer>'
assert_contains "$case_stdout" 'tdx-hydro-campaign.sh compile --campaign <id> [--workspace-root <path>] --fabric-version <value>'
assert_contains "$case_stdout" 'tdx-hydro-campaign.sh compile-basin --campaign <id> [--workspace-root <path>] --basin <processing-basin-id> --fabric-version <value>'
assert_contains "$case_stdout" 'tdx-hydro-campaign.sh progress --campaign <id> [--workspace-root <path>]'
assert_contains "$case_stdout" 'tdx-hydro-campaign.sh pipeline --campaign <id> [--workspace-root <path>] --max-parallel <integer> --fabric-version <value>'
assert_contains "$case_stdout" 'tdx-hydro-campaign.sh calibrate --campaign <id> [--workspace-root <path>] --max-parallel <2|4> --fabric-version <value>'
assert_contains "$case_stdout" 'tdx-hydro-campaign.sh assemble --campaign <id> [--workspace-root <path>] [--partial-fabric <dataset-root> --partial-fabric-roster <json-file> --exclude-control-basin <processing-basin-id>]'
assert_contains "$case_stdout" 'tdx-hydro-campaign.sh evidence --campaign <id> [--workspace-root <path>]'
assert_contains "$case_stdout" 'tdx-hydro-campaign.sh publish --campaign <id> [--workspace-root <path>] --out <dataset-dir> --report <path> --notice <path> --citation <path> --scratch-prefix <prefix>'
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
expect_failure 'calibrate missing max parallel' calibrate --campaign missing --workspace-root "$argument_root" --fabric-version fixture-v1
expect_failure 'calibrate duplicate max parallel' calibrate --campaign duplicate-cal --workspace-root "$argument_root" --max-parallel 2 --max-parallel 4 --fabric-version fixture-v1
expect_failure 'calibrate parallel one' calibrate --campaign parallel-one --workspace-root "$argument_root" --max-parallel 1 --fabric-version fixture-v1
expect_failure 'calibrate parallel three' calibrate --campaign parallel-three --workspace-root "$argument_root" --max-parallel 3 --fabric-version fixture-v1
expect_failure 'calibrate parallel five' calibrate --campaign parallel-five --workspace-root "$argument_root" --max-parallel 5 --fabric-version fixture-v1
expect_failure 'calibrate foreign option' calibrate --campaign foreign --workspace-root "$argument_root" --max-parallel 2 --fabric-version fixture-v1 --out nope
expect_failure 'calibrate malformed fabric' calibrate --campaign fabric --workspace-root "$argument_root" --max-parallel 2 --fabric-version $'bad\nvalue'
expect_failure 'max parallel usage scope' status --campaign scope --max-parallel 2
assert_contains "$case_stderr" 'option --max-parallel is valid only for acquire, pipeline, or calibrate'
expect_failure 'fabric version usage scope' status --campaign scope --fabric-version v1
assert_contains "$case_stderr" 'option --fabric-version is valid only for compile, compile-basin, pipeline, or calibrate'
mkdir "$test_tmp/workspaces/symlink-target"
ln -s "$test_tmp/workspaces/symlink-target" "$test_tmp/workspaces/symlink-root"
expect_failure 'symlink workspace root' status --campaign symlink --workspace-root "$test_tmp/workspaces/symlink-root"
expect_failure 'sizing on status' status --campaign sizing --workspace-root "$argument_root" --available-memory-bytes 1
expect_failure 'missing fabric version' compile --campaign compile --workspace-root "$argument_root"
expect_failure 'empty fabric version' compile --campaign compile --workspace-root "$argument_root" \
    --fabric-version ''
expect_failure 'repeated fabric version' compile --campaign compile --workspace-root "$argument_root" \
    --fabric-version version --fabric-version version
expect_failure 'option-shaped fabric version' compile --campaign compile --workspace-root "$argument_root" \
    --fabric-version --campaign
expect_failure 'control fabric version' compile --campaign compile --workspace-root "$argument_root" \
    --fabric-version $'bad\nversion'
expect_failure 'missing compile-basin basin' compile-basin --campaign compile --workspace-root "$argument_root" \
    --fabric-version version
expect_failure 'repeated compile-basin basin' compile-basin --campaign compile --workspace-root "$argument_root" \
    --basin 1020000010 --basin 7020000010 --fabric-version version
expect_failure 'malformed compile-basin basin' compile-basin --campaign compile --workspace-root "$argument_root" \
    --basin malformed --fabric-version version
expect_failure 'missing compile-basin fabric version' compile-basin --campaign compile \
    --workspace-root "$argument_root" --basin 1020000010
expect_failure 'repeated compile-basin fabric version' compile-basin --campaign compile \
    --workspace-root "$argument_root" --basin 1020000010 \
    --fabric-version version --fabric-version version
expect_failure 'empty compile-basin fabric version' compile-basin --campaign compile \
    --workspace-root "$argument_root" --basin 1020000010 --fabric-version ''
expect_failure 'option-shaped compile-basin fabric version' compile-basin --campaign compile \
    --workspace-root "$argument_root" --basin 1020000010 --fabric-version --campaign
expect_failure 'control compile-basin fabric version' compile-basin --campaign compile \
    --workspace-root "$argument_root" --basin 1020000010 --fabric-version $'bad\nversion'
expect_failure 'fabric version on progress' progress --campaign compile --workspace-root "$argument_root" \
    --fabric-version version
expect_failure 'basin on progress' progress --campaign compile --workspace-root "$argument_root" \
    --basin 1020000010
expect_failure 'foreign fabric version' status --campaign compile --workspace-root "$argument_root" \
    --fabric-version version
expect_failure 'fabric version on assemble' assemble --campaign compile --workspace-root "$argument_root" \
    --fabric-version version
for extension_option in --partial-fabric --partial-fabric-roster --exclude-control-basin; do
    expect_failure "$extension_option scope" status --campaign scope --workspace-root "$argument_root" \
        "$extension_option" value
    assert_contains "$case_stderr" "option $extension_option is valid only for assemble"
    expect_failure "$extension_option repeated" assemble --campaign repeat --workspace-root "$argument_root" \
        "$extension_option" value "$extension_option" value
    assert_contains "$case_stderr" "option $extension_option may not be repeated"
    expect_failure "$extension_option missing value" assemble --campaign missing --workspace-root "$argument_root" \
        "$extension_option"
    assert_contains "$case_stderr" "option $extension_option requires a value"
    expect_failure "$extension_option option value" assemble --campaign missing --workspace-root "$argument_root" \
        "$extension_option" --campaign
    assert_contains "$case_stderr" "option $extension_option requires a value"
done
expect_failure 'partial fabric missing roster' assemble --campaign incomplete --workspace-root "$argument_root" \
    --partial-fabric /tmp/fabric --exclude-control-basin 1020000010
assert_contains "$case_stderr" 'option --partial-fabric-roster is required when --partial-fabric is supplied'
expect_failure 'partial fabric missing control' assemble --campaign incomplete --workspace-root "$argument_root" \
    --partial-fabric /tmp/fabric --partial-fabric-roster /tmp/roster
assert_contains "$case_stderr" 'option --exclude-control-basin is required when --partial-fabric is supplied'
expect_failure 'roster without partial fabric' assemble --campaign incomplete --workspace-root "$argument_root" \
    --partial-fabric-roster /tmp/roster
assert_contains "$case_stderr" 'option --partial-fabric is required when --partial-fabric-roster or --exclude-control-basin is supplied'
expect_failure 'control without partial fabric' assemble --campaign incomplete --workspace-root "$argument_root" \
    --exclude-control-basin 1020000010
assert_contains "$case_stderr" 'option --partial-fabric is required when --partial-fabric-roster or --exclude-control-basin is supplied'
expect_failure 'malformed control' assemble --campaign incomplete --workspace-root "$argument_root" \
    --partial-fabric /tmp/fabric --partial-fabric-roster /tmp/roster --exclude-control-basin 12
assert_contains "$case_stderr" "invalid excluded control basin ID '12'; expected an authoritative 10-digit ID"
expect_failure 'empty retention policy' init --campaign arguments --workspace-root "$argument_root" \
    --retention-policy ''
expect_failure 'option-shaped retention policy' init --campaign arguments --workspace-root "$argument_root" \
    --retention-policy --available-memory-bytes
expect_failure 'unknown retention policy' init --campaign arguments --workspace-root "$argument_root" \
    --retention-policy retain-some
expect_failure 'case-changed retention policy' init --campaign arguments --workspace-root "$argument_root" \
    --retention-policy Retain-all-through-publication
expect_failure 'repeated retention policy' init --campaign arguments --workspace-root "$argument_root" \
    --retention-policy retain-all-through-publication \
    --retention-policy retain-all-through-publication
expect_failure 'foreign retention policy' status --campaign arguments --workspace-root "$argument_root" \
    --retention-policy retain-all-through-publication
expect_failure 'missing max parallel' acquire --campaign arguments --workspace-root "$argument_root"
expect_failure 'repeated max parallel' acquire --campaign arguments --workspace-root "$argument_root" \
    --max-parallel 1 --max-parallel 2
expect_failure 'foreign max parallel' status --campaign arguments --workspace-root "$argument_root" \
    --max-parallel 1
for invalid_parallel in 0 -1 x 63; do
    expect_failure "invalid max parallel $invalid_parallel" acquire --campaign arguments \
        --workspace-root "$argument_root" --max-parallel "$invalid_parallel"
    assert_contains "$case_stderr" \
        'hfx: error: option --max-parallel must be a base-10 integer from 1 through 62'
done
for foreign_basin_command in status recover acquire compile progress assemble evidence publish; do
    expect_failure "basin on $foreign_basin_command" "$foreign_basin_command" \
        --campaign foreign --workspace-root "$argument_root" --basin 1020000010
    [[ $(tail -1 "$case_stderr") == 'hfx: error: option --basin is valid only for init or compile-basin' ]] ||
        die "foreign --basin diagnostic differs for $foreign_basin_command"
done
for publication_option in --out --report --notice --citation --scratch-prefix; do
    expect_failure "foreign $publication_option" status --campaign foreign --workspace-root "$argument_root" \
        "$publication_option" value
    expect_failure "$publication_option on assemble" assemble --campaign foreign --workspace-root "$argument_root" \
        "$publication_option" value
    expect_failure "repeated $publication_option" publish --campaign repeated --workspace-root "$argument_root" \
        "$publication_option" value "$publication_option" value
    expect_failure "missing value $publication_option" publish --campaign missing --workspace-root "$argument_root" \
        "$publication_option"
done
expect_failure 'zero sizing' init --campaign zero --workspace-root "$argument_root" \
    --available-memory-bytes 0 --available-disk-bytes 5 --retained-input-bytes 1 \
    --retained-basin-output-bytes 1 --assembly-memory-ceiling-bytes 1 \
    --assembly-scratch-ceiling-bytes 1 --assembled-artifact-bytes 1 \
    --active-compile-scratch-bytes 1 --filesystem-overhead-bytes 1
expect_failure 'negative sizing' init --campaign negative --workspace-root "$argument_root" \
    --available-memory-bytes -1 --available-disk-bytes 5 --retained-input-bytes 1 \
    --retained-basin-output-bytes 1 --assembly-memory-ceiling-bytes 1 \
    --assembly-scratch-ceiling-bytes 1 --assembled-artifact-bytes 1 \
    --active-compile-scratch-bytes 1 --filesystem-overhead-bytes 1
expect_failure 'nonnumeric sizing' init --campaign text --workspace-root "$argument_root" \
    --available-memory-bytes nope --available-disk-bytes 5 --retained-input-bytes 1 \
    --retained-basin-output-bytes 1 --assembly-memory-ceiling-bytes 1 \
    --assembly-scratch-ceiling-bytes 1 --assembled-artifact-bytes 1 \
    --active-compile-scratch-bytes 1 --filesystem-overhead-bytes 1
expect_failure 'scalar overflow' init --campaign scalar-overflow --workspace-root "$argument_root" \
    --available-memory-bytes 9223372036854775808 --available-disk-bytes 5 --retained-input-bytes 1 \
    --retained-basin-output-bytes 1 --assembly-memory-ceiling-bytes 1 \
    --assembly-scratch-ceiling-bytes 1 --assembled-artifact-bytes 1 \
    --active-compile-scratch-bytes 1 --filesystem-overhead-bytes 1
for invalid_byte in 0 -1 nope 9223372036854775808; do
    expect_failure "invalid active scratch $invalid_byte" init --campaign arguments \
        --workspace-root "$argument_root" --available-memory-bytes 1 --available-disk-bytes 5 \
        --retained-input-bytes 1 --retained-basin-output-bytes 1 \
        --assembly-memory-ceiling-bytes 1 --assembly-scratch-ceiling-bytes 1 \
        --assembled-artifact-bytes 1 --active-compile-scratch-bytes "$invalid_byte" \
        --filesystem-overhead-bytes 1
    expect_failure "invalid filesystem overhead $invalid_byte" init --campaign arguments \
        --workspace-root "$argument_root" --available-memory-bytes 1 --available-disk-bytes 5 \
        --retained-input-bytes 1 --retained-basin-output-bytes 1 \
        --assembly-memory-ceiling-bytes 1 --assembly-scratch-ceiling-bytes 1 \
        --assembled-artifact-bytes 1 --active-compile-scratch-bytes 1 \
        --filesystem-overhead-bytes "$invalid_byte"
    expect_failure "invalid reclaim peak $invalid_byte" init --campaign arguments \
        --workspace-root "$argument_root" --retention-policy reclaim-inputs-after-terminal \
        --available-memory-bytes 1 --available-disk-bytes 5 \
        --peak-in-flight-download-bytes "$invalid_byte" --retained-basin-output-bytes 1 \
        --assembly-memory-ceiling-bytes 1 --assembly-scratch-ceiling-bytes 1 \
        --assembled-artifact-bytes 1 --active-compile-scratch-bytes 1 \
        --filesystem-overhead-bytes 1
done
expect_failure 'retain-all incompatible peak' init --campaign arguments --workspace-root "$argument_root" \
    --available-memory-bytes 1 --available-disk-bytes 5 --retained-input-bytes 1 \
    --peak-in-flight-download-bytes 44296724480 --retained-basin-output-bytes 1 \
    --assembly-memory-ceiling-bytes 1 --assembly-scratch-ceiling-bytes 1 \
    --assembled-artifact-bytes 1 --active-compile-scratch-bytes 1 --filesystem-overhead-bytes 1
expect_failure 'reclaim incompatible retained input' init --campaign arguments --workspace-root "$argument_root" \
    --retention-policy reclaim-inputs-after-terminal --available-memory-bytes 1 \
    --available-disk-bytes 44296724484 --retained-input-bytes 1 \
    --peak-in-flight-download-bytes 44296724480 --retained-basin-output-bytes 1 \
    --assembly-memory-ceiling-bytes 1 --assembly-scratch-ceiling-bytes 1 \
    --assembled-artifact-bytes 1 --active-compile-scratch-bytes 1 --filesystem-overhead-bytes 1
for unsafe_peak in 44296724479 44296724481; do
    expect_failure "unsafe reclaim peak $unsafe_peak" init --campaign arguments \
        --workspace-root "$argument_root" --retention-policy reclaim-inputs-after-terminal \
        --available-memory-bytes 1 --available-disk-bytes 44296724484 \
        --peak-in-flight-download-bytes "$unsafe_peak" --retained-basin-output-bytes 1 \
        --assembly-memory-ceiling-bytes 1 --assembly-scratch-ceiling-bytes 1 \
        --assembled-artifact-bytes 1 --active-compile-scratch-bytes 1 \
        --filesystem-overhead-bytes 1
    assert_contains "$case_stderr" 'option --peak-in-flight-download-bytes must equal 44296724480'
done
expect_failure 'missing active scratch' init --campaign arguments --workspace-root "$argument_root" \
    --available-memory-bytes 1 --available-disk-bytes 5 --retained-input-bytes 1 \
    --retained-basin-output-bytes 1 --assembly-memory-ceiling-bytes 1 \
    --assembly-scratch-ceiling-bytes 1 --assembled-artifact-bytes 1 \
    --filesystem-overhead-bytes 1
expect_failure 'missing filesystem overhead' init --campaign arguments --workspace-root "$argument_root" \
    --available-memory-bytes 1 --available-disk-bytes 5 --retained-input-bytes 1 \
    --retained-basin-output-bytes 1 --assembly-memory-ceiling-bytes 1 \
    --assembly-scratch-ceiling-bytes 1 --assembled-artifact-bytes 1 \
    --active-compile-scratch-bytes 1
expect_failure 'missing reclaim peak' init --campaign arguments --workspace-root "$argument_root" \
    --retention-policy reclaim-inputs-after-terminal --available-memory-bytes 1 \
    --available-disk-bytes 5 --retained-basin-output-bytes 1 \
    --assembly-memory-ceiling-bytes 1 --assembly-scratch-ceiling-bytes 1 \
    --assembled-artifact-bytes 1 --active-compile-scratch-bytes 1 \
    --filesystem-overhead-bytes 1
for repeated_byte_option in active-compile-scratch-bytes filesystem-overhead-bytes; do
    set -- $(init_args arguments "$argument_root")
    expect_failure "repeated $repeated_byte_option" "$@" \
        "--$repeated_byte_option" 1
done
touch "$argument_root/tdx-hydro-unsafe"
expect_failure 'unsafe pre-existing path' init --campaign unsafe --workspace-root "$argument_root" \
    --available-memory-bytes 1 --available-disk-bytes 5 --retained-input-bytes 1 \
    --retained-basin-output-bytes 1 --assembly-memory-ceiling-bytes 1 \
    --assembly-scratch-ceiling-bytes 1 --assembled-artifact-bytes 1 \
    --active-compile-scratch-bytes 1 --filesystem-overhead-bytes 1
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
    forbidden_bashpid='BASH''PID'
    forbidden_varfd='\{[A-Za-z_][A-Za-z0-9_]*\}<>'
    if grep -En -e "$forbidden_assoc" -e "$forbidden_wait" -e "$forbidden_map" -e "$forbidden_read" \
        -e "$forbidden_case_change" -e "$forbidden_glob" -e "$forbidden_negative" -e "$forbidden_v" \
        -e "$forbidden_nameref" -e "$forbidden_lock" -e "$forbidden_bashpid" \
        -e "$forbidden_varfd" "$script" >"$case_stdout"; then
        die "forbidden Bash-4 construct found in $script"
    fi
    if grep -En '"\$\{[A-Za-z_][A-Za-z0-9_]*\[@\]\}"' "$script" |
        grep -Ev '\[@\]\+\"\$\{' >"$case_stdout"; then
        die "unsafe bare array expansion found in $script"
    fi
done
assert_contains "$runner" 'https://earth-info.nga.mil/php/download.php?file=<processing-basin-id>-<product>-gpkg'
assert_contains "$runner" 'The exact product set is {basins,streamnet}'
assert_contains "$runner" 'readonly HFX_TDX_DEFAULT_HFX=/root/hfx/target/release/hfx'
assert_contains "$runner" 'readonly HFX_TDX_DEFAULT_ADAPTER_PYTHON=/opt/hfx-geo/bin/python'
assert_contains "$runner" 'ADAPTER_PYTHON=$(resolve_command HFX_TDX_ADAPTER_PYTHON "$HFX_TDX_DEFAULT_ADAPTER_PYTHON")'
assert_contains "$runner" 'ADAPTER_SCRIPT=${HFX_TDX_ADAPTER_SCRIPT-$repo_root/adapters/tdx-hydro/build_adapter.py}'
assert_contains "$runner" 'HFX=$(resolve_command HFX_TDX_HFX "$HFX_TDX_DEFAULT_HFX")'
assert_contains "$runner" 'option --max-parallel must be a base-10 integer from 1 through 62'
assert_contains "$runner" '((max_parallel >= 1 && max_parallel <= 62))'
assert_zero_ere 'runner retains acquisition continuation argument' '--continue-at' "$runner"
assert_contains "$runner" 'header_status" != 200'
assert_contains "$runner" '[Ee][Tt][Aa][Gg]:*'
assert_contains "$runner" '.curl-headers.$basin_id.$product.$$'
assert_contains "$runner" '.curl-stats.$basin_id.$product.$$'
[[ $(grep -Fc '"$JQ" -r '\''keys[]'\'' "$campaign_dir/state/inventory.json"' "$runner") -eq 3 ]] ||
    die 'runner does not contain exactly three textual three-argument keys[] reads'
sed -n '/^acquire_basin() {$/,/^}$/p' "$runner" >"$case_stdout"
[[ $(sed -n '2p' "$case_stdout") == '    lock_owned=0' &&
   $(sed -n '3p' "$case_stdout") == '    takeover_owned=0' ]] ||
    die 'acquisition child ownership resets are not the first two statements'
sed -n '/^pipeline_acquisition_worker() {$/,/^}$/p' "$runner" >"$case_stdout"
[[ $(sed -n '2p' "$case_stdout") == '    lock_owned=0' &&
   $(sed -n '3p' "$case_stdout") == '    takeover_owned=0' ]] ||
    die 'pipeline worker ownership resets are not the first two statements'
[[ $(grep -c '^pipeline_acquisition_worker() {$' "$runner") -eq 1 ]] ||
    die 'pipeline worker definition count differs'
trap_body=$(grep '^    trap ' "$case_stdout")
[[ "$trap_body" != *'$1'* && "$trap_body" != *'$2'* &&
   "$trap_body" != *'$@'* && "$trap_body" != *'$*'* ]] ||
    die 'pipeline worker trap uses positional parameters'
for trap_name in $(printf '%s\n' "$trap_body" | grep -oE '\$[A-Za-z_][A-Za-z0-9_]*' | tr -d '$' | sort -u); do
    ! grep -Eq "^[[:space:]]*local([[:space:]].*)?[[:space:]]$trap_name(=|[[:space:]]|$)" "$case_stdout" ||
        die "pipeline worker trap variable is local: $trap_name"
done
if grep -En '^[[:space:]]*pipeline_acquisition_worker[[:space:]]+[^&]*&' "$runner" >"$case_stdout"; then
    die 'pipeline worker has a bare background invocation'
fi
[[ $(grep -Fc '( pipeline_acquisition_worker "$basin_id" ) &' "$runner") -eq 1 ]] ||
    die 'pipeline worker production call-site count differs'
pass 'static Bash 3.2 compatibility checks pass'

expected_campaign_commands=$test_tmp/expected-campaign-commands
runner_campaign_commands=$test_tmp/runner-campaign-commands
bootstrap_campaign_commands_raw=$test_tmp/bootstrap-campaign-commands-raw
bootstrap_campaign_commands=$test_tmp/bootstrap-campaign-commands
bootstrap_apt_packages=$test_tmp/bootstrap-apt-packages
bootstrap_post_apt_loop=$test_tmp/bootstrap-post-apt-loop
bootstrap_final_loop=$test_tmp/bootstrap-final-loop
runbook_convergence=$test_tmp/runbook-convergence
campaign_array_expansion='"${campaign_command_names[''@]}"'
sed '' >"$expected_campaign_commands" <<'EXPECTED_CAMPAIGN_COMMANDS'
aws
chmod
curl
find
grep
jq
mkdir
mv
od
ogrinfo
ps
rm
sha256sum
sort
tr
wc
EXPECTED_CAMPAIGN_COMMANDS
sed -n 's/.*resolve_command HFX_TDX_[A-Z0-9_]* \([a-z][a-z0-9-]*\).*/\1/p' "$runner" |
    LC_ALL=C sort -u >"$runner_campaign_commands"
if ! diff -u "$expected_campaign_commands" "$runner_campaign_commands"; then
    die "runner command contract differs; paid-run failure was: hfx: error: required command 'jq' is not available"
fi
assert_contains "$runner" 'resolve_command HFX_TDX_ADAPTER_PYTHON "$HFX_TDX_DEFAULT_ADAPTER_PYTHON"'
assert_contains "$runner" 'resolve_command HFX_TDX_HFX "$HFX_TDX_DEFAULT_HFX"'

sed -n '/^campaign_command_names=($/,/^)$/{
    /^campaign_command_names=($/d
    /^)$/d
    s/^[[:space:]]*//
    /./p
}' "$bootstrap" >"$bootstrap_campaign_commands_raw"
[[ $(wc -l <"$bootstrap_campaign_commands_raw" | tr -d ' ') -eq 16 ]] ||
    die 'bootstrap campaign_command_names does not contain exactly 16 nonempty command lines'
LC_ALL=C sort -u "$bootstrap_campaign_commands_raw" >"$bootstrap_campaign_commands"
if ! diff -u "$expected_campaign_commands" "$bootstrap_campaign_commands"; then
    die "bootstrap command contract differs; paid-run failure was: hfx: error: required command 'jq' is not available"
fi

sed -n '/^apt_packages=($/,/^)$/{
    /^apt_packages=($/d
    /^)$/d
    s/^[[:space:]]*//
    /./p
}' "$bootstrap" >"$bootstrap_apt_packages"
[[ $(grep -Fxc -- jq "$bootstrap_apt_packages") -eq 1 ]] ||
    die 'bootstrap apt_packages does not contain exactly one jq entry'
[[ $(grep -Fxc -- procps "$bootstrap_apt_packages") -eq 1 ]] ||
    die 'bootstrap apt_packages does not contain exactly one procps entry'

sed -n '/^for command_name in /{
    N
    /is unavailable after package installation/p
}' "$bootstrap" >"$bootstrap_post_apt_loop"
assert_contains "$bootstrap_post_apt_loop" "for command_name in $campaign_array_expansion tmux git gdal-config clang pkg-config; do"
assert_contains "$bootstrap_post_apt_loop" 'command -v -- "$command_name"'
assert_contains "$bootstrap_post_apt_loop" 'is unavailable after package installation'
sed -n '/^for command_name in /{
    N
    /failed final verification/p
}' "$bootstrap" >"$bootstrap_final_loop"
assert_contains "$bootstrap_final_loop" "for command_name in $campaign_array_expansion tmux gdal-config; do"
assert_contains "$bootstrap_final_loop" 'command -v -- "$command_name"'
assert_contains "$bootstrap_final_loop" 'failed final verification'
assert_contains "$bootstrap" '[[ -x "$HFX_GEO_VENV/bin/python" ]] || bootstrap_die '\''geo environment Python failed final executable verification; rerun bootstrap'\'''
assert_contains "$bootstrap" '[[ -x "$BUILT_CLI" ]] || bootstrap_die '\''release hfx CLI failed final executable verification; rerun bootstrap'\'''

sed -n '/<<REMOTE_REF/,/^REMOTE_REF$/p' "$runbook" >"$runbook_convergence"
assert_contains "$runbook_convergence" 'for command_name in aws jq mv mkdir rm chmod find wc tr ps curl sha256sum od ogrinfo sort grep; do'
assert_contains "$runbook_convergence" 'command -v -- "\$command_name" >/dev/null'
assert_contains "$runbook_convergence" 'test -x /opt/hfx-geo/bin/python'
assert_contains "$runbook_convergence" 'test -x /root/hfx/target/release/hfx'
if grep -F -- 'command -v -- "$command_name"' "$runbook_convergence" >/dev/null; then
    die 'runbook convergence gate expands unescaped command_name in its unquoted heredoc'
fi
pass 'runner, bootstrap, apt ownership, and runbook dependency contracts remain synchronized'

subset_root=$test_tmp/workspaces/subset
mkdir "$subset_root"
set -- $(subset_init_args subset "$subset_root")
run_runner "$@" >"$case_stdout"
subset_campaign_dir=$subset_root/tdx-hydro-subset
jq -e '
  keys == ["basin_ids","schema_version"] and
  .schema_version == 1 and
  .basin_ids == ["1020000010","7020000010","9020000010"]
' "$subset_campaign_dir/state/selection.json" >/dev/null ||
    die 'subset selection was not frozen in sorted canonical form'
[[ $(jq 'length' "$subset_campaign_dir/state/inventory.json") -eq 62 ]] ||
    die 'subset init changed the authoritative inventory'
cp -R "$subset_campaign_dir/state" "$test_tmp/subset-state-before"
set -- $(init_args subset "$subset_root")
run_runner "$@" --basin 9020000010 --basin 7020000010 --basin 1020000010 >"$case_stdout"
diff -ru "$test_tmp/subset-state-before" "$subset_campaign_dir/state"
for changed_selection in \
    '--basin 7020000010 --basin 1020000010' \
    '--basin 7020000010 --basin 1020000010 --basin 9020000010 --basin 1020011530'
do
    set -- $(init_args subset "$subset_root")
    eval "set -- \"\$@\" $changed_selection"
    expect_failure 'changed basin selection' "$@"
    [[ $(tail -1 "$case_stderr") == 'hfx: error: basin selection changed; use a new campaign ID' ]] ||
        die 'changed basin selection diagnostic differs'
    diff -ru "$test_tmp/subset-state-before" "$subset_campaign_dir/state"
done
set -- $(init_args duplicate-subset "$subset_root")
expect_failure 'duplicate basin selection' "$@" --basin 7020000010 --basin 7020000010
[[ $(tail -1 "$case_stderr") == \
    "hfx: error: basin ID '7020000010' was selected more than once" ]] ||
    die 'duplicate basin diagnostic differs'
[[ ! -e "$subset_root/tdx-hydro-duplicate-subset" ]] ||
    die 'duplicate basin request left an accepted campaign'
set -- $(init_args unknown-subset "$subset_root")
expect_failure 'unknown basin selection' "$@" --basin 9999999999
[[ $(tail -1 "$case_stderr") == \
    "hfx: error: unknown basin ID '9999999999'; expected a key in state/inventory.json" ]] ||
    die 'unknown basin diagnostic differs'
[[ ! -e "$subset_root/tdx-hydro-unknown-subset" ]] ||
    die 'unknown basin request left an accepted campaign'
pass 'selection parsing, freezing, and convergence'

valid_root=$test_tmp/workspaces/valid
mkdir "$valid_root"
set -- $(init_args equal "$valid_root")
run_runner "$@" >"$case_stdout"
campaign_dir=$valid_root/tdx-hydro-equal
for relative in downloads basin-outputs reports assembly assembly/scratch publication state state/basins state/locks state/tmp; do
    [[ -d "$campaign_dir/$relative" && ! -L "$campaign_dir/$relative" ]] || die "missing layout directory $relative"
done
if env -u HFX_TDX_ADAPTER -u HFX_TDX_ADAPTER_PYTHON -u HFX_TDX_ADAPTER_SCRIPT -u HFX_TDX_HFX \
    "$selected_bash" "$runner" compile --campaign equal --workspace-root "$valid_root" \
        --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout" 2>"$case_stderr"; then
    die 'compile with frozen defaults unexpectedly succeeded'
fi
assert_contains "$case_stderr" "required command '/opt/hfx-geo/bin/python' is not available"
if env -u HFX_TDX_ADAPTER -u HFX_TDX_ADAPTER_SCRIPT -u HFX_TDX_HFX \
    HFX_TDX_ADAPTER_PYTHON="$selected_bash" \
    "$selected_bash" "$runner" compile --campaign equal --workspace-root "$valid_root" \
        --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout" 2>"$case_stderr"; then
    die 'compile without the frozen HFX binary unexpectedly succeeded'
fi
assert_contains "$case_stderr" "required command '/root/hfx/target/release/hfx' is not available"
pass 'compile resolves both frozen command defaults behaviorally'
jq -e '
    keys == ["campaign","inventory","retention","schema_version","sizing"] and
    .schema_version == 2 and .campaign == "equal" and
    .inventory == {source:"adapters/tdx-hydro/data/tdx_header_numbers.json",count:62} and
    .retention == {
      policy:"retain-all-through-publication",reclaim_inputs:false,
      retain_acquired_inputs:true,retain_basin_outputs:true,retain_external_reports:true
    } and
    .sizing == {
      available_memory_bytes:11,available_disk_bytes:29,retained_input_bytes:5,
      retained_basin_output_bytes:6,assembly_memory_ceiling_bytes:11,
      assembly_scratch_ceiling_bytes:7,assembled_artifact_bytes:8,
      active_compile_scratch_bytes:9,filesystem_overhead_bytes:1,
      required_memory_bytes:11,required_disk_bytes:29
    }
' "$campaign_dir/state/campaign.json" >/dev/null || die 'campaign JSON shape differs'
jq -S '.' "$inventory" >"$test_tmp/expected-inventory.json"
jq -S '.' "$campaign_dir/state/inventory.json" >"$test_tmp/actual-inventory.json"
diff -u "$test_tmp/expected-inventory.json" "$test_tmp/actual-inventory.json"
[[ ! -e "$campaign_dir/state/selection.json" && ! -L "$campaign_dir/state/selection.json" ]] ||
    die 'full-inventory init unexpectedly created selection state'
[[ $(find "$campaign_dir/state/basins" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ') == 62 ]] ||
    die 'basin directory count differs'
[[ $(find "$campaign_dir/state/basins" -name current.json -type f | wc -l | tr -d ' ') == 62 ]] ||
    die 'basin state count differs'
jq -e -s '
    length == 62 and all(
      .schema_version == 3 and
      (.processing_basin_id | test("^[0-9]{10}$")) and
      .stages == {
        acquire_basins:{status:"pending",attempts:0,failure_reason:null,evidence:null},
        acquire_streamnet:{status:"pending",attempts:0,failure_reason:null,evidence:null},
        compile:{status:"pending",attempts:0,failure_reason:null,diagnostic_report:null}
      }
    )
' "$campaign_dir"/state/basins/*/current.json >/dev/null || die 'initial basin states differ'
jq -e '. == {
  schema_version:1,
  status:"pending",
  attempts:0,
  failure_reason:null,
  input_basin_ids:[],
  output_path:"assembly/dataset",
  report_path:"reports/assembly.json"
}' "$campaign_dir/state/assembly.json" >/dev/null || die 'initial assembly state differs'
for compatibility_command in status progress evidence recover; do
    cp "$campaign_dir/state/assembly.json" "$test_tmp/schema-one-$compatibility_command-before"
    run_runner "$compatibility_command" --campaign equal --workspace-root "$valid_root" >"$case_stdout"
    cmp "$test_tmp/schema-one-$compatibility_command-before" "$campaign_dir/state/assembly.json"
done
pass 'schema-1 assembly state remains byte-identical through read-only and recovery commands'
if grep -F '7020000010' "$runner" >/dev/null; then
    die 'runner contains a transcribed processing-basin ID'
fi
pass 'equal-capacity init creates the complete 62-basin contract'

memory_root=$test_tmp/workspaces/memory
mkdir "$memory_root"

under_reserved_root=$test_tmp/workspaces/under-reserved
mkdir "$under_reserved_root"
expect_failure 'missing active compile scratch' init --campaign under-reserved \
    --workspace-root "$under_reserved_root" \
    --available-memory-bytes 11 --available-disk-bytes 26 --retained-input-bytes 5 \
    --retained-basin-output-bytes 6 --assembly-memory-ceiling-bytes 11 \
    --assembly-scratch-ceiling-bytes 7 --assembled-artifact-bytes 8
[[ ! -e "$under_reserved_root/tdx-hydro-under-reserved" ]] ||
    die 'missing active compile scratch refusal created campaign state'

expect_failure 'memory undersizing' init --campaign memory --workspace-root "$memory_root" \
    --available-memory-bytes 10 --available-disk-bytes 29 --retained-input-bytes 5 \
    --retained-basin-output-bytes 6 --assembly-memory-ceiling-bytes 11 \
    --assembly-scratch-ceiling-bytes 7 --assembled-artifact-bytes 8 \
    --active-compile-scratch-bytes 9 --filesystem-overhead-bytes 1
assert_contains "$case_stderr" 'insufficient memory: available 10 bytes; required 11 bytes'
[[ ! -e "$memory_root/tdx-hydro-memory" ]] || die 'memory refusal created campaign state'

disk_root=$test_tmp/workspaces/disk
mkdir "$disk_root"
expect_failure 'disk undersizing' init --campaign disk --workspace-root "$disk_root" \
    --available-memory-bytes 11 --available-disk-bytes 28 --retained-input-bytes 5 \
    --retained-basin-output-bytes 6 --assembly-memory-ceiling-bytes 11 \
    --assembly-scratch-ceiling-bytes 7 --assembled-artifact-bytes 8 \
    --active-compile-scratch-bytes 9 --filesystem-overhead-bytes 1
assert_contains "$case_stderr" 'insufficient disk: available 28 bytes; required 29 bytes'
[[ ! -e "$disk_root/tdx-hydro-disk" ]] || die 'disk refusal created campaign state'

overflow_root=$test_tmp/workspaces/overflow
mkdir "$overflow_root"
expect_failure 'sum overflow' init --campaign overflow --workspace-root "$overflow_root" \
    --available-memory-bytes 1 --available-disk-bytes 9223372036854775807 \
    --retained-input-bytes 9223372036854775807 --retained-basin-output-bytes 1 \
    --assembly-memory-ceiling-bytes 1 --assembly-scratch-ceiling-bytes 1 \
    --assembled-artifact-bytes 1 --active-compile-scratch-bytes 1 \
    --filesystem-overhead-bytes 1
assert_contains "$case_stderr" 'required disk byte sum overflows signed 64-bit range'
[[ ! -e "$overflow_root/tdx-hydro-overflow" ]] || die 'overflow refusal created campaign state'
for overflow_spec in \
    'overflow-active 9223372036854775806 1 1 1' \
    'overflow-assembly 9223372036854775805 1 1 1' \
    'overflow-overhead 9223372036854775804 1 1 1'; do
    overflow_campaign=${overflow_spec%% *}
    overflow_values=${overflow_spec#* }
    overflow_input=${overflow_values%% *}
    expect_failure "$overflow_campaign" init --campaign "$overflow_campaign" \
        --workspace-root "$overflow_root" --available-memory-bytes 1 \
        --available-disk-bytes 9223372036854775807 \
        --retained-input-bytes "$overflow_input" --retained-basin-output-bytes 1 \
        --assembly-memory-ceiling-bytes 1 --assembly-scratch-ceiling-bytes 1 \
        --assembled-artifact-bytes 1 --active-compile-scratch-bytes 1 \
        --filesystem-overhead-bytes 1
    assert_contains "$case_stderr" 'required disk byte sum overflows signed 64-bit range'
    [[ ! -e "$overflow_root/tdx-hydro-$overflow_campaign" ]] ||
        die "$overflow_campaign refusal created campaign state"
done

inverse_assembly_root=$test_tmp/workspaces/inverse-assembly-max
mkdir "$inverse_assembly_root"
run_runner init --campaign inverse-assembly-max --workspace-root "$inverse_assembly_root" \
    --available-memory-bytes 11 --available-disk-bytes 31 --retained-input-bytes 5 \
    --retained-basin-output-bytes 6 --assembly-memory-ceiling-bytes 11 \
    --assembly-scratch-ceiling-bytes 10 --assembled-artifact-bytes 8 \
    --active-compile-scratch-bytes 9 --filesystem-overhead-bytes 1 >"$case_stdout"
jq -e '.sizing.required_disk_bytes == 31 and .sizing.available_disk_bytes == 31' \
    "$inverse_assembly_root/tdx-hydro-inverse-assembly-max/state/campaign.json" >/dev/null ||
    die 'inverse assembly maximum total differs'
expect_failure 'inverse assembly one byte short' init --campaign inverse-assembly-max-short \
    --workspace-root "$inverse_assembly_root" \
    --available-memory-bytes 11 --available-disk-bytes 30 --retained-input-bytes 5 \
    --retained-basin-output-bytes 6 --assembly-memory-ceiling-bytes 11 \
    --assembly-scratch-ceiling-bytes 10 --assembled-artifact-bytes 8 \
    --active-compile-scratch-bytes 9 --filesystem-overhead-bytes 1
assert_contains "$case_stderr" 'insufficient disk: available 30 bytes; required 31 bytes'
[[ ! -e "$inverse_assembly_root/tdx-hydro-inverse-assembly-max-short" ]] ||
    die 'inverse assembly refusal created campaign state'
pass 'memory, disk, and arithmetic preflight refuse before writes'

selected_id=$(jq -r 'keys[0]' "$inventory")
selected_state=$campaign_dir/state/basins/$selected_id/current.json
jq '.stages.compile={status:"succeeded",attempts:3,failure_reason:null,diagnostic_report:null}' "$selected_state" >"$selected_state.tmp"
mv "$selected_state.tmp" "$selected_state"
cp "$selected_state" "$test_tmp/preserved-state.json"
cp "$campaign_dir/state/campaign.json" "$test_tmp/preserved-campaign.json"
run_runner init --campaign equal --workspace-root "$valid_root" \
    --available-memory-bytes 00011 --available-disk-bytes 00029 \
    --retained-input-bytes 0005 --retained-basin-output-bytes 0006 \
    --assembly-memory-ceiling-bytes 00011 --assembly-scratch-ceiling-bytes 0007 \
    --assembled-artifact-bytes 0008 --active-compile-scratch-bytes 0009 \
    --filesystem-overhead-bytes 0001 >"$case_stdout"
diff -u "$test_tmp/preserved-state.json" "$selected_state"
expect_failure 'changed contract' init --campaign equal --workspace-root "$valid_root" \
    --available-memory-bytes 12 --available-disk-bytes 29 --retained-input-bytes 5 \
    --retained-basin-output-bytes 6 --assembly-memory-ceiling-bytes 11 \
    --assembly-scratch-ceiling-bytes 7 --assembled-artifact-bytes 8 \
    --active-compile-scratch-bytes 9 --filesystem-overhead-bytes 1
assert_contains "$case_stderr" 'campaign parameters changed; use a new campaign ID'
diff -u "$test_tmp/preserved-state.json" "$selected_state"
diff -u "$test_tmp/preserved-campaign.json" "$campaign_dir/state/campaign.json"
expect_failure 'changed retention policy' init --campaign equal --workspace-root "$valid_root" \
    --retention-policy reclaim-inputs-after-terminal \
    --available-memory-bytes 30000000000 --available-disk-bytes 491737129060 \
    --peak-in-flight-download-bytes 44296724480 \
    --retained-basin-output-bytes 206220202290 \
    --assembly-memory-ceiling-bytes 30000000000 \
    --assembly-scratch-ceiling-bytes 206220202290 \
    --assembled-artifact-bytes 206220202290 \
    --active-compile-scratch-bytes 30000000000 \
    --filesystem-overhead-bytes 5000000000
assert_contains "$case_stderr" 'campaign parameters changed; use a new campaign ID'
for changed_pair in 'active-compile-scratch-bytes 10 30' 'filesystem-overhead-bytes 2 30' \
    'retained-input-bytes 6 30'; do
    changed_option=${changed_pair%% *}
    changed_remainder=${changed_pair#* }
    changed_value=${changed_remainder%% *}
    changed_disk=${changed_remainder#* }
    if [[ "$changed_option" == active-compile-scratch-bytes ]]; then
        expect_failure "changed $changed_option" init --campaign equal --workspace-root "$valid_root" \
            --available-memory-bytes 11 --available-disk-bytes "$changed_disk" \
            --retained-input-bytes 5 --retained-basin-output-bytes 6 \
            --assembly-memory-ceiling-bytes 11 --assembly-scratch-ceiling-bytes 7 \
            --assembled-artifact-bytes 8 --active-compile-scratch-bytes "$changed_value" \
            --filesystem-overhead-bytes 1
    elif [[ "$changed_option" == filesystem-overhead-bytes ]]; then
        expect_failure "changed $changed_option" init --campaign equal --workspace-root "$valid_root" \
            --available-memory-bytes 11 --available-disk-bytes "$changed_disk" \
            --retained-input-bytes 5 --retained-basin-output-bytes 6 \
            --assembly-memory-ceiling-bytes 11 --assembly-scratch-ceiling-bytes 7 \
            --assembled-artifact-bytes 8 --active-compile-scratch-bytes 9 \
            --filesystem-overhead-bytes "$changed_value"
    else
        expect_failure "changed $changed_option" init --campaign equal --workspace-root "$valid_root" \
            --available-memory-bytes 11 --available-disk-bytes "$changed_disk" \
            --retained-input-bytes "$changed_value" --retained-basin-output-bytes 6 \
            --assembly-memory-ceiling-bytes 11 --assembly-scratch-ceiling-bytes 7 \
            --assembled-artifact-bytes 8 --active-compile-scratch-bytes 9 \
            --filesystem-overhead-bytes 1
    fi
    assert_contains "$case_stderr" 'campaign parameters changed; use a new campaign ID'
done
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

mutate_campaign_state() {
    local name=$1
    local filter=$2
    local root
    local state
    root=$(copy_workspace "$name")
    state=$root/tdx-hydro-equal/state/campaign.json
    jq "$filter" "$state" >"$root/campaign.tmp"
    mv "$root/campaign.tmp" "$state"
    expect_failure "$name" status --campaign equal --workspace-root "$root"
    assert_contains "$case_stderr" "campaign state is malformed: $state"
}
mutate_campaign_state schema-one '.schema_version=1'
mutate_campaign_state extra-sizing-key '.sizing.extra=1'
mutate_campaign_state absent-sizing-key 'del(.sizing.active_compile_scratch_bytes)'
mutate_campaign_state string-sizing '.sizing.active_compile_scratch_bytes="9"'
mutate_campaign_state fractional-sizing '.sizing.active_compile_scratch_bytes=9.5'
mutate_campaign_state zero-sizing '.sizing.active_compile_scratch_bytes=0'
mutate_campaign_state negative-sizing '.sizing.active_compile_scratch_bytes=-1'
mutate_campaign_state oversized-sizing '.sizing.active_compile_scratch_bytes=9223372036854775808'
mutate_campaign_state wrong-reclaim-flag '.retention.reclaim_inputs=true'
mutate_campaign_state wrong-retain-input-flag '.retention.retain_acquired_inputs=false'
mutate_campaign_state altered-required-total '.sizing.required_disk_bytes=30'
mutate_campaign_state insufficient-disk '.sizing.available_disk_bytes=28'
mutate_campaign_state assembly-maximum-drift \
    '.sizing.assembly_scratch_ceiling_bytes=10 | .sizing.required_disk_bytes=29'
unset -f mutate_campaign_state

compile_state_root=$(copy_workspace bad-compile-state)
jq -n '{schema_version:1,fabric_version:"version",extra:true}' \
    >"$compile_state_root/tdx-hydro-equal/state/compile.json"
expect_failure 'malformed compile contract' status --campaign equal --workspace-root "$compile_state_root"

assembly_state_root=$(copy_workspace bad-assembly-state)
jq '.input_basin_ids=["7020000010","1020000010"]' \
    "$assembly_state_root/tdx-hydro-equal/state/assembly.json" >"$assembly_state_root/assembly.tmp"
mv "$assembly_state_root/assembly.tmp" "$assembly_state_root/tdx-hydro-equal/state/assembly.json"
expect_failure 'malformed assembly contract' status --campaign equal --workspace-root "$assembly_state_root"

selection_state_root=$(copy_workspace bad-selection-state)
jq -n '{schema_version:1,basin_ids:[]}' \
    >"$selection_state_root/tdx-hydro-equal/state/selection.json"
expect_failure 'empty selection contract' status --campaign equal --workspace-root "$selection_state_root"
selection_state_root=$(copy_workspace unsorted-selection-state)
jq -n '{schema_version:1,basin_ids:["7020000010","1020000010"]}' \
    >"$selection_state_root/tdx-hydro-equal/state/selection.json"
expect_failure 'unsorted selection contract' status --campaign equal --workspace-root "$selection_state_root"
selection_state_root=$(copy_workspace symlink-selection-state)
ln -s inventory.json "$selection_state_root/tdx-hydro-equal/state/selection.json"
expect_failure 'symlink selection contract' status --campaign equal --workspace-root "$selection_state_root"

jq -e '.schema_version == 3 and (.stages.compile | keys == ["attempts","diagnostic_report","failure_reason","status"])' "$selected_state" >/dev/null ||
    die 'shared status fixture lost the v3 compile diagnostic member'
run_runner status --campaign equal --workspace-root "$valid_root" >"$case_stdout"
assert_contains "$case_stdout" 'inventory_count=62'
assert_contains "$case_stdout" 'retention_policy=retain-all-through-publication'
assert_contains "$case_stdout" 'available_disk_bytes=29'
assert_contains "$case_stdout" 'retained_input_bytes=5'
assert_contains "$case_stdout" 'active_compile_scratch_bytes=9'
assert_contains "$case_stdout" 'filesystem_overhead_bytes=1'
assert_contains "$case_stdout" 'required_disk_bytes=29'
cp "$case_stdout" "$test_tmp/status-first"
run_runner status --campaign equal --workspace-root "$valid_root" >"$case_stdout"
cmp "$test_tmp/status-first" "$case_stdout"
assert_contains "$case_stdout" 'acquire_basins_pending=62'
assert_contains "$case_stdout" 'acquire_streamnet_succeeded=1'
assert_contains "$case_stdout" 'compile_succeeded=1'
assert_contains "$case_stdout" 'assemble_pending=1'
assert_contains "$case_stdout" 'assemble_running=0'
assert_contains "$case_stdout" 'assemble_succeeded=0'
assert_contains "$case_stdout" 'assemble_failed=0'
pass 'status rejects all malformed state and reports deterministic counts'

run_runner --help >"$case_stdout"
assert_contains "$case_stdout" 'retain-all-through-publication'
assert_contains "$case_stdout" 'reclaim-inputs-after-terminal'
policy_root=$test_tmp/workspaces/policy-aware
mkdir "$policy_root"
set -- $(init_args policy-retain-default "$policy_root")
run_runner "$@" >"$case_stdout"
run_runner init --campaign policy-reclaim-equal --workspace-root "$policy_root" \
    --retention-policy reclaim-inputs-after-terminal \
    --available-memory-bytes 30000000000 --available-disk-bytes 491737129060 \
    --peak-in-flight-download-bytes 44296724480 \
    --retained-basin-output-bytes 206220202290 \
    --assembly-memory-ceiling-bytes 30000000000 \
    --assembly-scratch-ceiling-bytes 206220202290 \
    --assembled-artifact-bytes 206220202290 \
    --active-compile-scratch-bytes 30000000000 \
    --filesystem-overhead-bytes 5000000000 >"$case_stdout"
expect_failure 'reclaim one byte short' init --campaign policy-reclaim-short \
    --workspace-root "$policy_root" --retention-policy reclaim-inputs-after-terminal \
    --available-memory-bytes 30000000000 --available-disk-bytes 491737129059 \
    --peak-in-flight-download-bytes 44296724480 \
    --retained-basin-output-bytes 206220202290 \
    --assembly-memory-ceiling-bytes 30000000000 \
    --assembly-scratch-ceiling-bytes 206220202290 \
    --assembled-artifact-bytes 206220202290 \
    --active-compile-scratch-bytes 30000000000 \
    --filesystem-overhead-bytes 5000000000
assert_contains "$case_stderr" \
    'insufficient disk: available 491737129059 bytes; required 491737129060 bytes'
[[ ! -e "$policy_root/tdx-hydro-policy-reclaim-short" ]] ||
    die 'reclaim disk refusal created campaign state'
run_runner init --campaign policy-reclaim-headroom --workspace-root "$policy_root" \
    --retention-policy reclaim-inputs-after-terminal \
    --available-memory-bytes 30000000000 --available-disk-bytes 560000000000 \
    --peak-in-flight-download-bytes 44296724480 \
    --retained-basin-output-bytes 206220202290 \
    --assembly-memory-ceiling-bytes 30000000000 \
    --assembly-scratch-ceiling-bytes 206220202290 \
    --assembled-artifact-bytes 206220202290 \
    --active-compile-scratch-bytes 30000000000 \
    --filesystem-overhead-bytes 5000000000 >"$case_stdout"
jq -e '
  .schema_version == 2 and
  .retention == {
    policy:"retain-all-through-publication",reclaim_inputs:false,
    retain_acquired_inputs:true,retain_basin_outputs:true,retain_external_reports:true
  } and
  (.sizing | has("retained_input_bytes") and (has("peak_in_flight_download_bytes") | not))
' "$policy_root/tdx-hydro-policy-retain-default/state/campaign.json" >/dev/null ||
    die 'default retain-all policy shape differs'
jq -e '
  .retention == {
    policy:"reclaim-inputs-after-terminal",reclaim_inputs:true,
    retain_acquired_inputs:false,retain_basin_outputs:true,retain_external_reports:true
  } and
  .sizing == {
    available_memory_bytes:30000000000,available_disk_bytes:491737129060,
    peak_in_flight_download_bytes:44296724480,retained_basin_output_bytes:206220202290,
    assembly_memory_ceiling_bytes:30000000000,assembly_scratch_ceiling_bytes:206220202290,
    assembled_artifact_bytes:206220202290,active_compile_scratch_bytes:30000000000,
    filesystem_overhead_bytes:5000000000,required_memory_bytes:30000000000,
    required_disk_bytes:491737129060
  }
' "$policy_root/tdx-hydro-policy-reclaim-equal/state/campaign.json" >/dev/null ||
    die 'reclaim policy shape differs'
jq -e '
  .sizing.available_disk_bytes == 560000000000 and
  .sizing.required_disk_bytes == 491737129060 and
  (.sizing.available_disk_bytes - .sizing.required_disk_bytes) == 68262870940
' "$policy_root/tdx-hydro-policy-reclaim-headroom/state/campaign.json" >/dev/null ||
    die 'reclaim headroom model differs'
run_runner status --campaign policy-reclaim-equal --workspace-root "$policy_root" >"$case_stdout"
assert_contains "$case_stdout" 'retention_policy=reclaim-inputs-after-terminal'
assert_contains "$case_stdout" 'peak_in_flight_download_bytes=44296724480'
if grep -F 'retained_input_bytes=' "$case_stdout" >/dev/null; then
    die 'reclaim status printed the inactive input term'
fi
cp "$case_stdout" "$test_tmp/reclaim-status-first"
run_runner status --campaign policy-reclaim-equal --workspace-root "$policy_root" >"$case_stdout"
cmp "$test_tmp/reclaim-status-first" "$case_stdout"
for reclaim_mutation in wrong-peak wrong-input-key wrong-retention-flag; do
    mutation_root=$test_tmp/workspaces/$reclaim_mutation
    mkdir "$mutation_root"
    cp -R "$policy_root/tdx-hydro-policy-reclaim-equal" \
        "$mutation_root/tdx-hydro-policy-reclaim-equal"
    mutation_state=$mutation_root/tdx-hydro-policy-reclaim-equal/state/campaign.json
    case $reclaim_mutation in
        wrong-peak) mutation_filter='.sizing.peak_in_flight_download_bytes=44296724479' ;;
        wrong-input-key)
            mutation_filter='del(.sizing.peak_in_flight_download_bytes) | .sizing.retained_input_bytes=44296724480'
            ;;
        wrong-retention-flag) mutation_filter='.retention.retain_acquired_inputs=true' ;;
    esac
    jq "$mutation_filter" "$mutation_state" >"$mutation_root/campaign.tmp"
    mv "$mutation_root/campaign.tmp" "$mutation_state"
    expect_failure "$reclaim_mutation" status --campaign policy-reclaim-equal \
        --workspace-root "$mutation_root"
    assert_contains "$case_stderr" "campaign state is malformed: $mutation_state"
done
printf '%s\n' retained >"$policy_root/tdx-hydro-policy-reclaim-equal/downloads/input-fixture"
run_runner status --campaign policy-reclaim-equal --workspace-root "$policy_root" >"$case_stdout"
assert_contains "$policy_root/tdx-hydro-policy-reclaim-equal/downloads/input-fixture" retained
pass 'policy-aware sizing preserves retain-all and sizes reclaim mode'

printf 'SQLite format 3\0fixture\n' >"$test_tmp/geopackage-template"
mkdir "$test_tmp/transfer-state"
sed >"$test_tmp/fake-curl" <<'FAKE_CURL'
#!/bin/bash
set -eu
output=
url=
headers=
write_out=
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
        --dump-header)
            shift
            headers=${1-}
            ;;
        --write-out)
            shift
            write_out=${1-}
            ;;
        --continue-at)
            exit 94
            ;;
        --header)
            shift
            case ${1-} in
                [Ii][Ff]-[Rr][Aa][Nn][Gg][Ee]:*) exit 95 ;;
                *) exit 93 ;;
            esac
            ;;
        --range|-r|-C|--parallel|--head|-I|--retry|--retry-all-errors)
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
[ -n "$output" ] && [ -n "$url" ] && [ -n "$headers" ] && [ -n "$write_out" ]
[ "$write_out" = 'http_status=%{http_code}\nnetwork_bytes=%{size_download}\ntime_total_seconds=%{time_total}\naverage_bytes_per_second=%{speed_download}\n' ] || exit 91
base=${output##*/}
key=${base%.gpkg.partial}
stats_path=${headers/.curl-headers./.curl-stats.}
write_nga_headers() {
    response_status=$1
    response_total=$2
    response_length_name=Content-Length
    response_etag_name=ETag
    [ "${HFX_TEST_LOWERCASE_HEADERS-}" != 1 ] || {
        response_length_name=content-length
        response_etag_name=etag
    }
    {
        printf 'HTTP/1.1 %s\r\n' "$response_status"
        printf 'Content-Type: application/octet-stream\r\n'
        if [ "${HFX_TEST_EMIT_ETAG-}" = 1 ]; then
            printf '%s: "fixture-v1"\r\n' "$response_etag_name"
        fi
        printf '%s: %s\r\n' "$response_length_name" "$response_total"
        printf 'Content-Disposition: attachment; filename="%s.gpkg"\r\n\r\n' "$key"
    } >"$headers"
    cp "$headers" "$HFX_TEST_TRANSFER_STATE/headers.$key"
}
printf '%s\t%s\t%s\n' "$key" "$headers" "$stats_path" >>"${HFX_TEST_TRANSFER_STATE:?}/paths"
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
if [ "${HFX_TEST_REDISPATCH_HOLD_KEY-}" = "$key" ] &&
   [ -d "$HFX_TEST_TRANSFER_STATE/fail-once-$key" ]; then
    redispatch_wait=0
    while [ ! -f "${HFX_TEST_REDISPATCH_RELEASE_MARKER:?}" ]; do
        redispatch_wait=$((redispatch_wait + 1))
        [ "$redispatch_wait" -lt 1000 ] || exit 97
        sleep 0.01
    done
fi
if [ -n "${HFX_TEST_PIPELINE_KILL_KEY-}" ] &&
   [ "$key" = "$HFX_TEST_PIPELINE_KILL_KEY" ] &&
   mkdir "$HFX_TEST_TRANSFER_STATE/kill-once-$key" 2>/dev/null; then
    printf '%s\n' "$$" >"$HFX_TEST_TRANSFER_STATE/killed-curl.pid"
    printf '%s\n' "$PPID" >"$HFX_TEST_TRANSFER_STATE/killed-worker.pid"
    kill -KILL "$PPID"
    sleep 1
    exit 98
fi
if [ "${HFX_TEST_FATAL_HOLD-}" = 1 ]; then
    printf '%s\n' "$$" >"$HFX_TEST_TRANSFER_STATE/fatal-curl.$$"
    printf '%s\n' "$PPID" >"$HFX_TEST_TRANSFER_STATE/fatal-worker.$PPID"
    if [ "$key" = "${HFX_TEST_FATAL_INJECT_KEY:?}" ]; then
        fatal_attempt=0
        while [ "$(find "$HFX_TEST_TRANSFER_STATE" -name 'fatal-worker.*' -type f |
                   wc -l | tr -d ' ')" -lt 2 ]; do
            fatal_attempt=$((fatal_attempt + 1))
            [ "$fatal_attempt" -lt 1000 ] || exit 97
            sleep 0.01
        done
        printf '%s\n' malformed >&9
    fi
    fatal_attempt=0
    while [ "$fatal_attempt" -lt 600 ]; do
        fatal_attempt=$((fatal_attempt + 1))
        sleep 0.01
    done
    exit 97
fi
if [ "${HFX_TEST_INTERRUPT_DRAIN-}" = 1 ]; then
    total=$(wc -c <"${HFX_TEST_GPKG_TEMPLATE:?}" | tr -d ' ')
    head -c 18 "$HFX_TEST_GPKG_TEMPLATE" >"$output"
    write_nga_headers '200 OK' "$total"
    printf 'http_status=200\nnetwork_bytes=18\ntime_total_seconds=1.25\naverage_bytes_per_second=14\n'
    printf '%s\n' "$$" >"$HFX_TEST_TRANSFER_STATE/curl.$$"
    printf '%s\n' "$PPID" >"$HFX_TEST_TRANSFER_STATE/worker.$PPID"
    if mkdir "$HFX_TEST_TRANSFER_STATE/signal-owner" 2>/dev/null; then
        marker_count=0
        rendezvous_deadline=0
        rendezvous_limit=${HFX_TEST_RENDEZVOUS_LIMIT-1000}
        while [ "$marker_count" -lt 3 ]; do
            marker_count=$(find "$HFX_TEST_TRANSFER_STATE" -name 'curl.*' -type f | wc -l | tr -d ' ')
            sleep 0.01
            rendezvous_deadline=$((rendezvous_deadline + 1))
            [ "$rendezvous_deadline" -lt "$rendezvous_limit" ] || exit 95
        done
        campaign_dir=${output%/downloads/*}
        owner_file=$campaign_dir/state/locks/campaign.lock/owner.pid
        runner_pid=$(cat "$owner_file")
        child_exit_deadline=0
        child_exited=0
        while [ "$child_exited" -eq 0 ]; do
            for pid_file in "$HFX_TEST_TRANSFER_STATE"/worker.*; do
                [ -f "$pid_file" ] || continue
                observed_pid=$(cat "$pid_file")
                if [ "$observed_pid" != "$PPID" ] && ! kill -0 "$observed_pid" 2>/dev/null; then
                    child_exited=1
                    break
                fi
            done
            child_exit_deadline=$((child_exit_deadline + 1))
            [ "$child_exit_deadline" -lt "$rendezvous_limit" ] || exit 95
            [ "$child_exited" -eq 1 ] || sleep 0.01
        done
        [ -f "$owner_file" ] && [ ! -L "$owner_file" ] || exit 95
        [ "$(cat "$owner_file")" = "$runner_pid" ] || exit 95
        printf '%s\n' "$runner_pid" >"$HFX_TEST_TRANSFER_STATE/lock-owner-stable"
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
total=$(wc -c <"${HFX_TEST_GPKG_TEMPLATE:?}" | tr -d ' ')
if [ "${HFX_TEST_FAIL_KEY-}" = "$key" ] ||
     { [ "${HFX_TEST_FAIL_ONCE_KEY-}" = "$key" ] &&
       mkdir "$HFX_TEST_TRANSFER_STATE/fail-once-$key" 2>/dev/null; }; then
    head -c 18 "$HFX_TEST_GPKG_TEMPLATE" >"$output"
    [ "${HFX_TEST_LEADING_ZERO_LENGTH-}" != 1 ] || total=0$total
    write_nga_headers '200 OK' "$total"
    printf 'http_status=200\nnetwork_bytes=18\ntime_total_seconds=1.25\naverage_bytes_per_second=14\n'
    result=22
else
    transfer_shape=${HFX_TEST_TRANSFER_SHAPE-complete}
    if [ -n "${HFX_TEST_TRANSFER_SHAPE_KEY-}" ] &&
       [ "$key" != "$HFX_TEST_TRANSFER_SHAPE_KEY" ]; then
        transfer_shape=complete
    fi
    case $transfer_shape in
        complete|'')
            cp "${HFX_TEST_GPKG_TEMPLATE:?}" "$output"
            network_bytes=$total
            ;;
        truncated)
            head -c 18 "$HFX_TEST_GPKG_TEMPLATE" >"$output"
            network_bytes=18
            ;;
        substituted)
            head -c "$total" /dev/zero >"$output"
            network_bytes=$total
            ;;
        *) exit 96 ;;
    esac
    write_nga_headers '200 OK' "$total"
    printf 'http_status=200\nnetwork_bytes=%s\ntime_total_seconds=1.25\naverage_bytes_per_second=20\n' "$network_bytes"
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
sed >"$test_tmp/recording-mkdir" <<'RECORDING_MKDIR'
#!/bin/bash
set -eu
if "${HFX_TEST_REAL_MKDIR:?}" "$@"; then
    for argument do
        if [ "$argument" = "${HFX_TEST_REJECTED_LOCK_PATH-}" ]; then
            : >"${HFX_TEST_REJECTED_LOCK_CREATED:?}"
        fi
    done
else
    exit $?
fi
RECORDING_MKDIR
sed >"$test_tmp/fake-sha256sum" <<'FAKE_SHA'
#!/bin/sh
checksum=$(cksum <"$1")
checksum=${checksum%% *}
if [ "${HFX_TEST_HASH_MODE-}" = changed ]; then
    checksum=$((checksum + 1))
fi
printf '%064x  %s\n' "$checksum" "$1"
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
if [ -n "${HFX_TEST_REENTRY_TAMPER-}" ] &&
    [ ! -e "$HFX_TEST_TRANSFER_STATE/reentry-tampered-$HFX_TEST_REENTRY_TAMPER" ]; then
    jq_input=${!#}
    case $jq_input in
        */state/campaign.json)
            campaign_dir=${jq_input%/state/campaign.json}
            lock_path=$campaign_dir/state/locks/campaign.lock
            if [ -d "$lock_path" ] && [ ! -L "$lock_path" ]; then
                case $HFX_TEST_REENTRY_TAMPER in
                    live-owner)
                        printf '%s\n' "${HFX_TEST_COMPETING_PID:?}" >"$lock_path/owner.pid"
                        ;;
                    owner-symlink)
                        printf '%s\n' "$PPID" \
                            >"$HFX_TEST_TRANSFER_STATE/reentry-owner-$HFX_TEST_REENTRY_TAMPER"
                        rm "$lock_path/owner.pid"
                        ln -s "$HFX_TEST_TRANSFER_STATE/reentry-owner-$HFX_TEST_REENTRY_TAMPER" \
                            "$lock_path/owner.pid"
                        ;;
                    lock-symlink)
                        mkdir "$HFX_TEST_TRANSFER_STATE/reentry-lock-$HFX_TEST_REENTRY_TAMPER"
                        printf '%s\n' "$PPID" \
                            >"$HFX_TEST_TRANSFER_STATE/reentry-lock-$HFX_TEST_REENTRY_TAMPER/owner.pid"
                        rm -r "$lock_path"
                        ln -s "$HFX_TEST_TRANSFER_STATE/reentry-lock-$HFX_TEST_REENTRY_TAMPER" \
                            "$lock_path"
                        ;;
                    *) exit 96 ;;
                esac
                : >"$HFX_TEST_TRANSFER_STATE/reentry-tampered-$HFX_TEST_REENTRY_TAMPER"
            fi
            ;;
    esac
fi
exec "${HFX_TEST_REAL_JQ:?}" "$@"
FAKE_JQ
sed >"$test_tmp/fake-adapter" <<'FAKE_ADAPTER'
#!/bin/bash
set -eu
log=${HFX_TEST_ADAPTER_LOG:?}
command_name=${1-}
case $command_name in
    build)
        if [ -n "${HFX_TEST_REDISPATCH_RELEASE_MARKER-}" ]; then
            : >"$HFX_TEST_REDISPATCH_RELEASE_MARKER"
        fi
        [ "$#" -eq 13 ] || exit 81
        [ "$2" = --basins ] || exit 82
        basins=$3
        [ "$4" = --streamnet ] || exit 82
        streamnet=$5
        [ "$6" = --out ] || exit 82
        out=$7
        [ "$8" = --report ] || exit 82
        report=$9
        [ "${10}" = --processing-basin-id ] || exit 82
        basin_id=${11}
        if [ -n "${HFX_TEST_PIPELINE_BUILD_ACTIVE_LOG-}" ]; then
            build_mutex=${HFX_TEST_TRANSFER_STATE:?}/mutex
            build_attempt=0
            while ! mkdir "$build_mutex" 2>/dev/null; do
                build_attempt=$((build_attempt + 1))
                [ "$build_attempt" -lt 1000 ] || exit 95
                sleep 0.01
            done
            build_active=0
            [ ! -f "$HFX_TEST_TRANSFER_STATE/active" ] ||
                build_active=$(cat "$HFX_TEST_TRANSFER_STATE/active")
            printf 'build-active %s %s\n' "$basin_id" "$build_active" \
                >>"$HFX_TEST_PIPELINE_BUILD_ACTIVE_LOG"
            rmdir "$build_mutex"
        fi
        [ "${12}" = --fabric-version ] || exit 82
        fabric_version=${13}
        printf 'build\t--basins\t%s\t--streamnet\t%s\t--out\t%s\t--report\t%s\t--processing-basin-id\t%s\t--fabric-version\t%s\n' \
            "$basins" "$streamnet" "$out" "$report" "$basin_id" "$fabric_version" >>"$log"
        [ -f "$basins" ] && [ ! -L "$basins" ] || exit 83
        [ -f "$streamnet" ] && [ ! -L "$streamnet" ] || exit 83
        case $report in
            "$out"|"$out"/*) exit 84 ;;
        esac
        [ "${out##*/}" = "$basin_id" ] || exit 85
        [ "${report##*/}" = "$basin_id-build-report.json" ] || exit 85
        if [ "${HFX_TEST_REQUIRE_LOCK_OWNER-}" = 1 ]; then
            owner_file=${out%/basin-outputs/*}/state/locks/campaign.lock/owner.pid
            [ -f "$owner_file" ] && [ ! -L "$owner_file" ] || exit 94
            owner=$(cat "$owner_file")
            [ "$owner" = "$PPID" ] || exit 94
            printf '%s\n' "$owner" >"${HFX_TEST_LOCK_OWNER_LOG:?}"
        fi
        if [ "${HFX_TEST_REQUIRE_CLEARED_DIAGNOSTIC-}" = 1 ]; then
            state=${out%/basin-outputs/*}/state/basins/$basin_id/current.json
            "${HFX_TEST_REAL_JQ:?}" -e '
              .stages.compile.status == "running" and
              .stages.compile.diagnostic_report == null
            ' "$state" >/dev/null || exit 93
        fi
        if [ "${HFX_TEST_FAIL_BUILD_ID-}" = "$basin_id" ]; then
            exit 41
        fi
        mkdir -p "$out/aux"
        printf '%s\n' fixture >"$out/catchments.parquet"
        printf '%s\n' fixture >"$out/graph.parquet"
        printf '%s\n' fixture >"$out/manifest.json"
        printf '%s\n' fixture >"$out/aux/snap_stems.parquet"
        dataset_root=$(cd -P "$out" && pwd -P)
        "${HFX_TEST_REAL_JQ:?}" -n \
            --arg basin_id "$basin_id" \
            --arg fabric_version "$fabric_version" \
            --arg dataset_root "$dataset_root" \
            --arg large_diagnostics_id "${HFX_TEST_LARGE_DIAGNOSTICS_ID-}" \
            '{
              build_identity:{
                processing_basin_id:$basin_id,
                fabric_name:"tdx_hydro",
                fabric_version:$fabric_version,
                created_at:"2026-07-23T00:00:00Z",
                adapter_version:"0.1.0",
                dataset_root:$dataset_root
              },
              diagnostics:(
                if $large_diagnostics_id == $basin_id then {
                  fixture_metric: 7,
                  oversized_native_ids: [range(0; 250000)],
                  completion_marker: "diagnostics-complete"
                } else {} end
              )
            }' >"$report"
        ;;
    validate)
        [ "$#" -eq 4 ] || exit 86
        dataset=$2
        [ "$3" = --hfx-binary ] || exit 87
        hfx_binary=$4
        printf 'validate\t%s\t--hfx-binary\t%s\n' "$dataset" "$hfx_binary" >>"$log"
        [ -d "$dataset" ] && [ ! -L "$dataset" ] || exit 88
        if [ "${HFX_TEST_FAIL_VALIDATE_ID-}" = "${dataset##*/}" ] ||
            { [ "${dataset##*/}" = dataset ] && [ "${HFX_TEST_FAIL_ASSEMBLY_VALIDATE-}" = 1 ]; }; then
            exit 42
        fi
        hfx_status=0
        "$hfx_binary" "$dataset" --strict --sample-pct 100 --format text || hfx_status=$?
        printf '%s\t%s\n' "$dataset" "$hfx_status" >>"${HFX_TEST_HFX_STATUS_LOG:?}"
        exit "$hfx_status"
        ;;
    assemble)
        assemble_arguments=("$@")
        shift
        if [ "${1-}" = --partial-input ]; then
            [ "$#" -ge 6 ] || exit 89
            [ -d "$2" ] && [ ! -L "$2" ] || exit 89
            [ "$3" = --partial-roster ] || exit 89
            [ -f "$4" ] && [ ! -L "$4" ] && [ -s "$4" ] || exit 89
            shift 4
        fi
        input_count=0
        while [ "$#" -gt 2 ]; do
            [ "$1" = --input ] || exit 89
            [ -d "$2" ] && [ ! -L "$2" ] || exit 89
            input_count=$((input_count + 1))
            shift 2
        done
        [ "$input_count" -gt 0 ] || exit 89
        [ "$#" -eq 2 ] && [ "$1" = --out ] || exit 89
        out=$2
        printf '%s' "${assemble_arguments[0]}" >>"$log"
        for argument in "${assemble_arguments[@]:1}"; do
            printf '\t%s' "$argument" >>"$log"
        done
        printf '\n' >>"$log"
        if [ "${HFX_TEST_FAIL_ASSEMBLY-}" = 1 ]; then
            exit 43
        fi
        mkdir -p "$out/aux"
        printf '%s\n' fixture >"$out/catchments.parquet"
        printf '%s\n' fixture >"$out/graph.parquet"
        printf '%s\n' fixture >"$out/manifest.json"
        printf '%s\n' fixture >"$out/aux/snap_stems.parquet"
        ;;
    assembly)
        exit 89
        ;;
    *)
        exit 90
        ;;
esac
FAKE_ADAPTER
sed >"$test_tmp/fake-adapter-python" <<'FAKE_ADAPTER_PYTHON'
#!/bin/bash
set -eu
[ "$#" -ge 1 ] || exit 91
mkfifo_program='import os, sys
path = sys.argv[1]
old_umask = os.umask(0)
try:
    os.mkfifo(path, 0o600)
finally:
    os.umask(old_umask)'
statvfs_program='import os, sys
path = sys.argv[1]
s = os.statvfs(path)
print(s.f_bavail * s.f_frsize)'
if [ "$1" = -c ]; then
    [ "$#" -eq 3 ] || exit 93
    if [ "$2" = "$mkfifo_program" ]; then
        [ "$3" = "${HFX_TEST_PIPELINE_COMPLETION_PATH:?}" ] || exit 93
        mkfifo -m 600 "$3"
        exit
    fi
    if [ "$2" = "$statvfs_program" ]; then
        [ "$3" = "${HFX_TEST_PIPELINE_CAMPAIGN_DIR:?}" ] || exit 93
        if [ -n "${HFX_TEST_PIPELINE_AVAILABLE_SEQUENCE-}" ]; then
            sequence=$HFX_TEST_PIPELINE_AVAILABLE_SEQUENCE
            [ -f "$sequence" ] && [ ! -L "$sequence" ] || exit 93
            sequence_mutex=$sequence.mutex
            sequence_attempt=0
            while ! mkdir "$sequence_mutex" 2>/dev/null; do
                sequence_attempt=$((sequence_attempt + 1))
                [ "$sequence_attempt" -lt 1000 ] || exit 93
                sleep 0.01
            done
            sequence_value=$(head -n 1 "$sequence")
            [ -n "$sequence_value" ] || {
                rmdir "$sequence_mutex"
                exit 93
            }
            tail -n +2 "$sequence" >"$sequence.tmp"
            mv "$sequence.tmp" "$sequence"
            rmdir "$sequence_mutex"
            printf '%s\n' "$sequence_value"
        else
            printf '%s\n' "${HFX_TEST_PIPELINE_AVAILABLE_BYTES-44296724480}"
        fi
        exit
    fi
    exit 93
fi
[ "$1" = "${HFX_TEST_ADAPTER_SCRIPT:?}" ] || exit 92
shift
if [ "${1-}" = assemble ]; then
    actual=${HFX_TEST_EXPECTED_ASSEMBLY_ARGV:?}.actual
    : >"$actual"
    for argument do
        printf '%s\n' "$argument" >>"$actual"
    done
    "${HFX_TEST_DIFF:?}" -u "${HFX_TEST_EXPECTED_ASSEMBLY_ARGV:?}" "$actual" || exit 94
fi
exec "${HFX_TEST_ADAPTER_STUB:?}" "$@"
FAKE_ADAPTER_PYTHON
sed >"$test_tmp/pipeline-mv" <<'PIPELINE_MV'
#!/bin/bash
set -eu
source_path=$1
destination_path=$2
if [ "$#" -eq 2 ] &&
   [ "${HFX_TEST_KILLED_BASIN_ID-}" != "" ] &&
   [ "$destination_path" = "${HFX_TEST_KILLED_CURRENT_PATH-}" ] &&
   "${HFX_TEST_REAL_JQ:?}" -e --arg id "$HFX_TEST_KILLED_BASIN_ID" '
     .processing_basin_id == $id and
     ([.stages.acquire_basins,.stages.acquire_streamnet] |
      any(.status == "pending" and
          .failure_reason == "interrupted before terminal state; reset by recover"))
   ' "$source_path" >/dev/null 2>&1; then
    : >"${HFX_TEST_KILLED_PENDING_MARKER:?}"
    if [ -n "${HFX_TEST_STALE_READY_MARKER-}" ]; then
        stale_wait=0
        while [ ! -f "$HFX_TEST_STALE_READY_MARKER" ]; do
            stale_wait=$((stale_wait + 1))
            [ "$stale_wait" -lt 1000 ] || exit 97
            sleep 0.01
        done
        sleep 6
    fi
fi
if [ "$#" -eq 2 ] &&
   [ -n "${HFX_TEST_STALE_CURRENT_PATH-}" ] &&
   [ "$destination_path" = "$HFX_TEST_STALE_CURRENT_PATH" ] &&
   "${HFX_TEST_REAL_JQ:?}" -e '
     [.stages.acquire_basins,.stages.acquire_streamnet] |
     any(.status == "failed")
   ' "$source_path" >/dev/null 2>&1 &&
   mkdir "${HFX_TEST_STALE_READY_MARKER:?}.once" 2>/dev/null; then
    : >"$HFX_TEST_STALE_READY_MARKER"
    sleep 4
fi
exec "${HFX_TEST_REAL_MV:?}" "$@"
PIPELINE_MV
sed >"$test_tmp/pipeline-rm" <<'PIPELINE_RM'
#!/bin/bash
set -eu
last_argument=${!#}
if [ -n "${HFX_TEST_FATAL_FIFO-}" ] &&
   [ "$last_argument" = "$HFX_TEST_FATAL_FIFO" ]; then
    [ -d "${HFX_TEST_FATAL_LOCK:?}" ] || exit 96
    for pid_file in "${HFX_TEST_TRANSFER_STATE:?}"/fatal-worker.*; do
        [ -f "$pid_file" ] || continue
        worker_pid=$(cat "$pid_file")
        if kill -0 "$worker_pid" 2>/dev/null; then
            exit 96
        fi
    done
    printf '%s\n' children-reaped-with-lock >"${HFX_TEST_FATAL_CLEANUP_MARKER:?}"
fi
exec "${HFX_TEST_REAL_RM:?}" "$@"
PIPELINE_RM
sed >"$test_tmp/fake-hfx" <<'FAKE_HFX'
#!/bin/bash
set -eu
case ${1-} in
    assemble|assembly) exit 74 ;;
esac
printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" "$6" >>"${HFX_TEST_HFX_LOG:?}"
[ "$#" -eq 6 ] || exit 71
[ "$1" != validate ] || exit 72
[ "$2" = --strict ] || exit 73
[ "$3" = --sample-pct ] || exit 73
[ "$4" = 100 ] || exit 73
[ "$5" = --format ] || exit 73
[ "$6" = text ] || exit 73
exit 0
FAKE_HFX
chmod +x "$test_tmp/fake-curl" "$test_tmp/recording-mkdir" "$test_tmp/fake-sha256sum" "$test_tmp/fake-ogrinfo" "$test_tmp/fake-jq" \
    "$test_tmp/fake-adapter" "$test_tmp/fake-adapter-python" "$test_tmp/fake-hfx" \
    "$test_tmp/pipeline-mv" "$test_tmp/pipeline-rm"
export HFX_TDX_CURL=$test_tmp/fake-curl
export HFX_TDX_SHA256SUM=$test_tmp/fake-sha256sum
export HFX_TDX_OGRINFO=$test_tmp/fake-ogrinfo
unset HFX_TDX_ADAPTER HFX_TDX_ADAPTER_SCRIPT
export HFX_TDX_ADAPTER_PYTHON=$test_tmp/fake-adapter-python
export HFX_TDX_HFX=$test_tmp/fake-hfx
export HFX_TEST_TRANSFER_STATE=$test_tmp/transfer-state
export HFX_TEST_GPKG_TEMPLATE=$test_tmp/geopackage-template
export HFX_TEST_REAL_JQ=$real_jq
export HFX_TEST_ADAPTER_LOG=$test_tmp/invocations/adapter.log
export HFX_TEST_ADAPTER_SCRIPT=$repo_root/adapters/tdx-hydro/build_adapter.py
export HFX_TEST_ADAPTER_STUB=$test_tmp/fake-adapter
export HFX_TEST_HFX_LOG=$test_tmp/invocations/hfx.log
export HFX_TEST_HFX_STATUS_LOG=$test_tmp/invocations/hfx-status.log
export HFX_TEST_DIFF=$(command -v diff)

new_bounded_campaign() {
    local name=$1
    local root=$test_tmp/workspaces/$name
    mkdir "$root"
    run_runner init --campaign "$name" --workspace-root "$root" --basin 1020000010 \
        --available-memory-bytes 11 --available-disk-bytes 29 --retained-input-bytes 5 \
        --retained-basin-output-bytes 6 --assembly-memory-ceiling-bytes 11 \
        --assembly-scratch-ceiling-bytes 7 --assembled-artifact-bytes 8 \
        --active-compile-scratch-bytes 9 --filesystem-overhead-bytes 1 >"$case_stdout"
    bounded_root=$root
    bounded_dir=$root/tdx-hydro-$name
}

reset_bounded_transfers() {
    rm -rf "$test_tmp/transfer-state"
    mkdir "$test_tmp/transfer-state"
    export HFX_TEST_TRANSFER_STATE=$test_tmp/transfer-state
    export HFX_TEST_FAIL_KEY=
    export HFX_TEST_FAIL_ONCE_KEY=
    export HFX_TEST_HASH_MODE=
    export HFX_TEST_OGR_MODE=
    export HFX_TEST_EMIT_ETAG=
    export HFX_TEST_TRANSFER_SHAPE=
    export HFX_TEST_TRANSFER_SHAPE_KEY=
    export HFX_TEST_LOWERCASE_HEADERS=
    export HFX_TEST_LEADING_ZERO_LENGTH=
}

bounded_acquisition_case() {
    local invalid
    local command_name
    local state
    local report
    local before_events
    local mutation_filter
    local mutation_name
    local mutation_root

    new_bounded_campaign bounded-cli
    assert_contains <(run_runner --help) \
        'tdx-hydro-campaign.sh acquire --campaign <id> [--workspace-root <path>] --max-parallel <integer> [--product-attempt-ceiling <positive-integer>]'
    expect_failure 'missing product ceiling value' acquire --campaign bounded-cli \
        --workspace-root "$bounded_root" --max-parallel 1 --product-attempt-ceiling
    assert_contains "$case_stderr" 'option --product-attempt-ceiling requires a value'
    expect_failure 'repeated product ceiling' acquire --campaign bounded-cli \
        --workspace-root "$bounded_root" --max-parallel 1 \
        --product-attempt-ceiling 1 --product-attempt-ceiling 1
    assert_contains "$case_stderr" 'option --product-attempt-ceiling may not be repeated'
    for invalid in 0 01 -1 x 9223372036854775808; do
        expect_failure "invalid product ceiling $invalid" acquire --campaign bounded-cli \
            --workspace-root "$bounded_root" --max-parallel 1 --product-attempt-ceiling "$invalid"
        assert_contains "$case_stderr" \
            'option --product-attempt-ceiling must be a canonical positive signed-64-bit integer'
    done
    for command_name in init status recover compile compile-basin progress pipeline calibrate \
        checkpoint checkpoint-resume assemble evidence publish; do
        expect_failure "product ceiling ownership $command_name" "$command_name" \
            --campaign bounded-cli --workspace-root "$bounded_root" --product-attempt-ceiling 1
        assert_contains "$case_stderr" 'option --product-attempt-ceiling is valid only for acquire'
    done
    cp -R "$bounded_dir" "$test_tmp/bounded-cli.before"
    reset_bounded_transfers
    expect_failure 'bounded max parallel five' acquire --campaign bounded-cli \
        --workspace-root "$bounded_root" --max-parallel 5 --product-attempt-ceiling 1
    assert_contains "$case_stderr" \
        'option --max-parallel must be a base-10 integer from 1 through 4 when --product-attempt-ceiling is supplied'
    diff -ru "$test_tmp/bounded-cli.before" "$bounded_dir"
    [[ ! -e "$test_tmp/transfer-state/events" ]] || die 'bounded CLI refusal started a transfer'
    [[ ! -e "$bounded_dir/state/locks/campaign.lock" ]] || die 'bounded CLI refusal created a lock'
    pass 'bounded CLI ownership validation and pre-lock concurrency are exact'

    new_bounded_campaign bounded-success
    reset_bounded_transfers
    run_runner acquire --campaign bounded-success --workspace-root "$bounded_root" \
        --max-parallel 1 --product-attempt-ceiling 2 >"$case_stdout" 2>"$case_stderr"
    state=$bounded_dir/state/basins/1020000010/current.json
    jq -e '
      .schema_version == 5 and .acquisition == {product_attempt_ceiling:2} and
      ([.stages.acquire_basins,.stages.acquire_streamnet] | all(
        .status == "succeeded" and .attempts == 1 and .failure_reason == null and
        .evidence.bytes == 24 and
        .evidence.sha256 == "00000000000000000000000000000000000000000000000000000000bcb2f999" and
        .evidence.sqlite_identity == "53514c69746520666f726d6174203300")) and
      .stages.compile == {status:"pending",attempts:0,failure_reason:null,diagnostic_report:null}
    ' "$state" >/dev/null || die 'bounded inspected success state differs'
    [[ $(grep -c '^start 1020000010-' "$test_tmp/transfer-state/events") -eq 2 ]] ||
        die 'bounded inspected success transfer count differs'
    [[ ! -s "$HFX_TEST_ADAPTER_LOG" && ! -s "$HFX_TEST_HFX_LOG" ]] ||
        die 'bounded inspected success invoked compile tools'
    for report in "$bounded_dir"/reports/1020000010-*-acquisition.json; do
        jq -e '.schema_version == 1 and .retry_count == 0 and (.transfers | length) == 1 and
          .transfers[0] == {attempt:1,mode:"fresh",http_status:200,network_bytes:24,
            time_total_seconds:1.25,average_bytes_per_second:20,result:"succeeded"}' \
            "$report" >/dev/null || die 'bounded one-transfer report differs'
    done
    pass 'bounded inspected success records exact evidence and reports'

    new_bounded_campaign bounded-adopt-at-ceiling
    state=$bounded_dir/state/basins/1020000010/current.json
    jq '.schema_version=5 | .acquisition={product_attempt_ceiling:2} |
      .stages.acquire_basins.attempts=2 | .stages.acquire_streamnet.attempts=2' \
        "$state" >"$state.tmp"
    mv "$state.tmp" "$state"
    cp "$test_tmp/geopackage-template" "$bounded_dir/downloads/1020000010-basins.gpkg"
    cp "$test_tmp/geopackage-template" "$bounded_dir/downloads/1020000010-streamnet.gpkg"
    reset_bounded_transfers
    run_runner acquire --campaign bounded-adopt-at-ceiling --workspace-root "$bounded_root" \
        --max-parallel 1 >"$case_stdout" 2>"$case_stderr"
    jq -e '[.stages.acquire_basins,.stages.acquire_streamnet] |
      all(.status == "succeeded" and .attempts == 2 and .evidence.bytes == 24)' \
        "$state" >/dev/null || die 'valid finals at the ceiling were not inspected and adopted'
    [[ ! -e "$test_tmp/transfer-state/events" ]] ||
        die 'valid finals at the ceiling started a transfer'
    pass 'bounded acquisition inspects valid finals before exhausting a stage'

    new_bounded_campaign bounded-retry-success
    reset_bounded_transfers
    export HFX_TEST_FAIL_ONCE_KEY=1020000010-basins
    run_runner acquire --campaign bounded-retry-success --workspace-root "$bounded_root" \
        --max-parallel 1 --product-attempt-ceiling 2 >"$case_stdout" 2>"$case_stderr"
    state=$bounded_dir/state/basins/1020000010/current.json
    jq -e '.stages.acquire_basins == {
      status:"succeeded",attempts:2,failure_reason:null,evidence:{bytes:24,
      sha256:"00000000000000000000000000000000000000000000000000000000bcb2f999",
      sqlite_identity:"53514c69746520666f726d6174203300",layer_name:"1020000010-basins"}}
    ' "$state" >/dev/null || die 'bounded retry-success stage differs'
    report=$bounded_dir/reports/1020000010-basins-acquisition.json
    jq -e '. == {schema_version:1,processing_basin_id:"1020000010",product:"basins",retry_count:1,
      transfers:[
        {attempt:1,mode:"fresh",http_status:200,network_bytes:18,time_total_seconds:1.25,average_bytes_per_second:14,result:"curl_failed"},
        {attempt:2,mode:"fresh",http_status:200,network_bytes:24,time_total_seconds:1.25,average_bytes_per_second:20,result:"succeeded"}]}' \
        "$report" >/dev/null || die 'bounded retry-success report differs'
    [[ $(grep -c '^start 1020000010-basins$' "$test_tmp/transfer-state/events") -eq 2 ]] ||
        die 'bounded retry-success did not make two fresh attempts'
    [[ ! -e "$bounded_dir/downloads/1020000010-basins.gpkg.partial" &&
       ! -e "$bounded_dir/downloads/1020000010-basins.gpkg.partial.json" ]] ||
        die 'bounded retry-success retained a partial'
    pass 'bounded retry succeeds on attempt two with a durable per-attempt report'

    new_bounded_campaign bounded-exhaustion
    reset_bounded_transfers
    export HFX_TEST_FAIL_KEY=1020000010-basins
    run_runner acquire --campaign bounded-exhaustion --workspace-root "$bounded_root" \
        --max-parallel 1 --product-attempt-ceiling 2 >"$case_stdout" 2>"$case_stderr"
    state=$bounded_dir/state/basins/1020000010/current.json
    jq -e '. == {schema_version:5,processing_basin_id:"1020000010",
      acquisition:{product_attempt_ceiling:2},stages:{
        acquire_basins:{status:"exhausted",attempts:2,
          failure_reason:"product attempt ceiling exhausted; retryable acquisition did not succeed",evidence:null},
        acquire_streamnet:{status:"succeeded",attempts:1,failure_reason:null,evidence:{bytes:24,
          sha256:"00000000000000000000000000000000000000000000000000000000bcb2f999",
          sqlite_identity:"53514c69746520666f726d6174203300",layer_name:"1020000010-streamnet"}},
        compile:{status:"pending",attempts:0,failure_reason:null,diagnostic_report:null}}}' \
        "$state" >/dev/null || die 'bounded partial-success exhaustion state differs'
    jq -e '. == {schema_version:1,processing_basin_id:"1020000010",product:"basins",retry_count:1,
      transfers:[
        {attempt:1,mode:"fresh",http_status:200,network_bytes:18,time_total_seconds:1.25,average_bytes_per_second:14,result:"curl_failed"},
        {attempt:2,mode:"fresh",http_status:200,network_bytes:18,time_total_seconds:1.25,average_bytes_per_second:14,result:"curl_failed"}]}' \
        "$bounded_dir/reports/1020000010-basins-acquisition.json" >/dev/null ||
        die 'bounded exhaustion report differs'
    jq -e '. == {schema_version:1,processing_basin_id:"1020000010",product:"streamnet",retry_count:0,
      transfers:[{attempt:1,mode:"fresh",http_status:200,network_bytes:24,
        time_total_seconds:1.25,average_bytes_per_second:20,result:"succeeded"}]}' \
        "$bounded_dir/reports/1020000010-streamnet-acquisition.json" >/dev/null ||
        die 'bounded exhaustion sibling report differs'
    [[ $(grep -Fxc 'hfx: acquisition product=1020000010-basins status=exhausted retry_count=1 last_network_bytes=18 last_time_total_seconds=1.25 last_average_bytes_per_second=14' "$case_stderr") -eq 1 ]] ||
        die 'bounded exhaustion terminal summary differs'
    run_runner status --campaign bounded-exhaustion --workspace-root "$bounded_root" \
        >"$test_tmp/bounded-exhaustion.status"
    cat >"$test_tmp/bounded-exhaustion.expected-status" <<'EOF'
campaign=bounded-exhaustion
inventory_count=62
selected_basin_count=1
unselected_basin_count=61
retention_policy=retain-all-through-publication
available_memory_bytes=11
available_disk_bytes=29
retained_input_bytes=5
retained_basin_output_bytes=6
assembly_memory_ceiling_bytes=11
assembly_scratch_ceiling_bytes=7
assembled_artifact_bytes=8
active_compile_scratch_bytes=9
filesystem_overhead_bytes=1
required_memory_bytes=11
required_disk_bytes=29
acquire_basins_pending=0
acquire_basins_running=0
acquire_basins_succeeded=0
acquire_basins_failed=0
acquire_basins_exhausted=1
acquire_streamnet_pending=0
acquire_streamnet_running=0
acquire_streamnet_succeeded=1
acquire_streamnet_failed=0
acquire_streamnet_exhausted=0
compile_pending=1
compile_running=0
compile_succeeded=0
compile_failed=0
assemble_pending=1
assemble_running=0
assemble_succeeded=0
assemble_failed=0
EOF
    diff -u "$test_tmp/bounded-exhaustion.expected-status" "$test_tmp/bounded-exhaustion.status"
    [[ ! -e "$bounded_dir/downloads/1020000010-basins.gpkg" &&
       ! -e "$bounded_dir/downloads/1020000010-basins.gpkg.partial" &&
       ! -e "$bounded_dir/downloads/1020000010-basins.gpkg.partial.json" ]] ||
        die 'bounded exhaustion installed or retained basins data'
    cmp "$test_tmp/geopackage-template" "$bounded_dir/downloads/1020000010-streamnet.gpkg"
    cp -R "$bounded_dir" "$test_tmp/bounded-exhaustion.before-rerun"
    before_events=$(wc -l <"$test_tmp/transfer-state/events" | tr -d ' ')
    run_runner acquire --campaign bounded-exhaustion --workspace-root "$bounded_root" \
        --max-parallel 1 >"$case_stdout" 2>"$case_stderr"
    [[ $(wc -l <"$test_tmp/transfer-state/events" | tr -d ' ') -eq "$before_events" ]] ||
        die 'stored-ceiling rerun transferred data'
    diff -ru "$test_tmp/bounded-exhaustion.before-rerun" "$bounded_dir"
    run_runner acquire --campaign bounded-exhaustion --workspace-root "$bounded_root" \
        --max-parallel 1 --product-attempt-ceiling 2 >"$case_stdout" 2>"$case_stderr"
    [[ $(wc -l <"$test_tmp/transfer-state/events" | tr -d ' ') -eq "$before_events" ]] ||
        die 'explicit fixed-ceiling rerun transferred data'
    diff -ru "$test_tmp/bounded-exhaustion.before-rerun" "$bounded_dir"
    expect_failure 'changed bounded ceiling' acquire --campaign bounded-exhaustion \
        --workspace-root "$bounded_root" --max-parallel 1 --product-attempt-ceiling 3
    assert_contains "$case_stderr" 'product attempt ceiling changed; use a new campaign ID'
    diff -ru "$test_tmp/bounded-exhaustion.before-rerun" "$bounded_dir"
    pass 'partial success exhaustion status reruns and fixed ceiling are durable'

    mutation_root=$test_tmp/workspaces/bounded-lower-existing
    mkdir "$mutation_root"
    create_compatibility_workspace "$mutation_root" bounded-lower-existing 3
    state=$mutation_root/tdx-hydro-bounded-lower-existing/state/basins/1020000010/current.json
    jq '.stages.acquire_basins={status:"failed",attempts:3,
      failure_reason:"transfer interrupted; retry from byte zero",evidence:null}' "$state" >"$state.tmp"
    mv "$state.tmp" "$state"
    cp -R "$mutation_root/tdx-hydro-bounded-lower-existing" "$test_tmp/bounded-lower.before"
    expect_failure 'ceiling below existing attempts' acquire --campaign bounded-lower-existing \
        --workspace-root "$mutation_root" --max-parallel 1 --product-attempt-ceiling 2
    assert_contains "$case_stderr" \
        'product attempt ceiling 2 is below existing attempt count 3 for 1020000010-basins'
    diff -ru "$test_tmp/bounded-lower.before" "$mutation_root/tdx-hydro-bounded-lower-existing"
    pass 'bounded preflight refuses changed and lower ceilings without partial conversion'

    mutation_root=$test_tmp/workspaces/bounded-mixed
    mkdir "$mutation_root"
    create_compatibility_workspace "$mutation_root" bounded-mixed 3
    jq '.basin_ids=["1020000010","1020011530"]' \
        "$mutation_root/tdx-hydro-bounded-mixed/state/selection.json" >"$mutation_root/selection.tmp"
    mv "$mutation_root/selection.tmp" \
        "$mutation_root/tdx-hydro-bounded-mixed/state/selection.json"
    state=$mutation_root/tdx-hydro-bounded-mixed/state/basins/1020000010/current.json
    jq '.schema_version=5 | .acquisition={product_attempt_ceiling:2}' "$state" >"$state.tmp"
    mv "$state.tmp" "$state"
    cp -R "$mutation_root/tdx-hydro-bounded-mixed" "$test_tmp/bounded-mixed.before"
    reset_bounded_transfers
    expect_failure 'incomplete bounded conversion' acquire --campaign bounded-mixed \
        --workspace-root "$mutation_root" --max-parallel 1
    assert_contains "$case_stderr" \
        'bounded acquisition conversion is incomplete; rerun acquire with --product-attempt-ceiling 2'
    diff -ru "$test_tmp/bounded-mixed.before" "$mutation_root/tdx-hydro-bounded-mixed"
    [[ ! -e "$test_tmp/transfer-state/events" ]] || die 'mixed conversion refusal transferred data'
    run_runner acquire --campaign bounded-mixed --workspace-root "$mutation_root" --max-parallel 1 \
        --product-attempt-ceiling 2 >"$case_stdout" 2>"$case_stderr"
    jq -e -s 'all(.[]; .schema_version == 5 and .acquisition.product_attempt_ceiling == 2)' \
        "$mutation_root/tdx-hydro-bounded-mixed/state/basins/1020000010/current.json" \
        "$mutation_root/tdx-hydro-bounded-mixed/state/basins/1020011530/current.json" >/dev/null ||
        die 'explicit ceiling did not complete mixed conversion'

    mutation_root=$test_tmp/workspaces/bounded-conflict
    mkdir "$mutation_root"
    cp -R "$test_tmp/bounded-mixed.before" "$mutation_root/tdx-hydro-bounded-mixed"
    state=$mutation_root/tdx-hydro-bounded-mixed/state/basins/1020011530/current.json
    jq '.schema_version=5 | .acquisition={product_attempt_ceiling:3}' "$state" >"$state.tmp"
    mv "$state.tmp" "$state"
    cp -R "$mutation_root/tdx-hydro-bounded-mixed" "$test_tmp/bounded-conflict.before"
    expect_failure 'conflicting selected ceilings' acquire --campaign bounded-mixed \
        --workspace-root "$mutation_root" --max-parallel 1
    assert_contains "$case_stderr" \
        'selected basin product attempt ceilings differ; use a new campaign ID'
    diff -ru "$test_tmp/bounded-conflict.before" "$mutation_root/tdx-hydro-bounded-mixed"
    pass 'incomplete and conflicting selected ceiling sets fail before workers'

    cp -R "$test_tmp/workspaces/legacy-reclaim-campaign/tdx-hydro-legacy-reclaim-campaign" \
        "$test_tmp/bounded-fence.before"
    reset_bounded_transfers
    expect_failure 'bounded pipeline fence' pipeline --campaign legacy-reclaim-campaign \
        --workspace-root "$test_tmp/workspaces/legacy-reclaim-campaign" --max-parallel 1 \
        --fabric-version fixture-v1
    assert_contains "$case_stderr" 'bounded acquisition state requires the acquire subcommand'
    expect_failure 'bounded calibrate fence' calibrate --campaign legacy-reclaim-campaign \
        --workspace-root "$test_tmp/workspaces/legacy-reclaim-campaign" --max-parallel 2 \
        --fabric-version fixture-v1
    assert_contains "$case_stderr" 'bounded acquisition state requires the acquire subcommand'
    diff -ru "$test_tmp/bounded-fence.before" \
        "$test_tmp/workspaces/legacy-reclaim-campaign/tdx-hydro-legacy-reclaim-campaign"
    [[ ! -e "$test_tmp/transfer-state/events" ]] || die 'bounded pipeline fence transferred data'
    pass 'pipeline and calibrate fence bounded state before work'

    run_runner status --campaign bounded-exhaustion --workspace-root "$bounded_root" >"$case_stdout"
    for mutation_name in outer acquisition stage evidence compile wrong-reason below above exhausted-evidence succeeded-no-evidence version-six; do
        mutation_root=$test_tmp/workspaces/schema-$mutation_name
        mkdir "$mutation_root"
        cp -R "$bounded_dir" "$mutation_root/tdx-hydro-bounded-exhaustion"
        state=$mutation_root/tdx-hydro-bounded-exhaustion/state/basins/1020000010/current.json
        case $mutation_name in
            outer) mutation_filter='.extra=true' ;;
            acquisition) mutation_filter='.acquisition.extra=true' ;;
            stage) mutation_filter='.stages.acquire_basins.extra=true' ;;
            evidence) mutation_filter='.stages.acquire_streamnet.evidence.extra=true' ;;
            compile) mutation_filter='.stages.compile.extra=true' ;;
            wrong-reason) mutation_filter='.stages.acquire_basins.failure_reason="wrong"' ;;
            below) mutation_filter='.stages.acquire_basins.attempts=1' ;;
            above) mutation_filter='.stages.acquire_basins.attempts=3' ;;
            exhausted-evidence) mutation_filter='.stages.acquire_basins.evidence={}' ;;
            succeeded-no-evidence) mutation_filter='.stages.acquire_streamnet.evidence=null' ;;
            version-six) mutation_filter='.schema_version=6' ;;
        esac
        jq "$mutation_filter" "$state" >"$state.tmp"
        mv "$state.tmp" "$state"
        expect_failure "schema exactness $mutation_name" status --campaign bounded-exhaustion \
            --workspace-root "$mutation_root"
        assert_contains "$case_stderr" "basin state is malformed for 1020000010: $state"
    done
    mutation_root=$test_tmp/workspaces/schema-retention
    mkdir "$mutation_root"
    cp -R "$test_tmp/workspaces/legacy-reclaim-campaign/tdx-hydro-legacy-reclaim-campaign" \
        "$mutation_root/tdx-hydro-legacy-reclaim-campaign"
    state=$mutation_root/tdx-hydro-legacy-reclaim-campaign/state/basins/1020000010/current.json
    jq '.retention.extra=true' "$state" >"$state.tmp" && mv "$state.tmp" "$state"
    expect_failure 'schema exactness retention' status --campaign legacy-reclaim-campaign \
        --workspace-root "$mutation_root"
    assert_contains "$case_stderr" "basin state is malformed for 1020000010: $state"
    sed -n '/^acquire_basin_bounded() {$/,/^}$/p' "$runner" >"$case_stdout"
    [[ $(grep -c 'RM' "$case_stdout" || :) -eq 0 ]] || die 'bounded basin wrapper contains an RM call'
    pass 'version-5 schema exactness and bounded no-deletion shape fail closed'
}

bounded_acquisition_case

run_runner init --campaign reclaim-parallel-reject --workspace-root "$valid_root" \
    --basin 7020000010 --basin 1020000010 --basin 9020000010 \
    --retention-policy reclaim-inputs-after-terminal \
    --available-memory-bytes 30000000000 --available-disk-bytes 491737129060 \
    --peak-in-flight-download-bytes 44296724480 \
    --retained-basin-output-bytes 206220202290 \
    --assembly-memory-ceiling-bytes 30000000000 \
    --assembly-scratch-ceiling-bytes 206220202290 \
    --assembled-artifact-bytes 206220202290 \
    --active-compile-scratch-bytes 30000000000 \
    --filesystem-overhead-bytes 5000000000 >"$case_stdout"
run_runner init --campaign reclaim-parallel-one --workspace-root "$valid_root" \
    --basin 7020000010 --basin 1020000010 --basin 9020000010 \
    --retention-policy reclaim-inputs-after-terminal \
    --available-memory-bytes 30000000000 --available-disk-bytes 491737129060 \
    --peak-in-flight-download-bytes 44296724480 \
    --retained-basin-output-bytes 206220202290 \
    --assembly-memory-ceiling-bytes 30000000000 \
    --assembly-scratch-ceiling-bytes 206220202290 \
    --assembled-artifact-bytes 206220202290 \
    --active-compile-scratch-bytes 30000000000 \
    --filesystem-overhead-bytes 5000000000 >"$case_stdout"
run_runner init --campaign reclaim-parallel-four --workspace-root "$valid_root" \
    --basin 7020000010 --basin 1020000010 --basin 9020000010 \
    --retention-policy reclaim-inputs-after-terminal \
    --available-memory-bytes 30000000000 --available-disk-bytes 491737129060 \
    --peak-in-flight-download-bytes 44296724480 \
    --retained-basin-output-bytes 206220202290 \
    --assembly-memory-ceiling-bytes 30000000000 \
    --assembly-scratch-ceiling-bytes 206220202290 \
    --assembled-artifact-bytes 206220202290 \
    --active-compile-scratch-bytes 30000000000 \
    --filesystem-overhead-bytes 5000000000 >"$case_stdout"
for rejected_parallel in 5 62; do
    rm -r "$test_tmp/transfer-state"
    mkdir "$test_tmp/transfer-state"
    snapshot=$test_tmp/reclaim-parallel-$rejected_parallel
    cp -R "$valid_root/tdx-hydro-reclaim-parallel-reject" "$snapshot"
    expect_failure "reclaim parallelism $rejected_parallel" acquire \
        --campaign reclaim-parallel-reject --workspace-root "$valid_root" \
        --max-parallel "$rejected_parallel"
    assert_contains "$case_stderr" \
        'hfx: error: option --max-parallel must be a base-10 integer from 1 through 4 for retention policy reclaim-inputs-after-terminal'
    diff -ru "$snapshot" "$valid_root/tdx-hydro-reclaim-parallel-reject"
    [[ ! -e "$test_tmp/transfer-state/events" ]] ||
        die 'reclaim parallelism refusal allowed an acquisition worker to start'
    [[ ! -e "$test_tmp/invocations/curl.log" ]] ||
        die 'reclaim parallelism refusal invoked the PATH poison curl'
done
export HFX_TEST_REAL_MKDIR
HFX_TEST_REAL_MKDIR=$(command -v mkdir)
export HFX_TEST_REJECTED_LOCK_PATH=$valid_root/tdx-hydro-reclaim-parallel-reject/state/locks/campaign.lock
export HFX_TEST_REJECTED_LOCK_CREATED=$test_tmp/rejected-reclaim-lock-created
export HFX_TDX_MKDIR=$test_tmp/recording-mkdir
expect_failure 'reclaim parallelism pre-lock ordering' acquire \
    --campaign reclaim-parallel-reject --workspace-root "$valid_root" \
    --max-parallel 5
assert_contains "$case_stderr" \
    'hfx: error: option --max-parallel must be a base-10 integer from 1 through 4 for retention policy reclaim-inputs-after-terminal'
[[ ! -e "$HFX_TEST_REJECTED_LOCK_CREATED" ]] ||
    die 'rejected reclaim parallelism created the campaign lock directory'
[[ ! -d "$HFX_TEST_REJECTED_LOCK_PATH" ]] ||
    die 'rejected reclaim parallelism left the campaign lock directory present'
unset HFX_TDX_MKDIR HFX_TEST_REAL_MKDIR HFX_TEST_REJECTED_LOCK_PATH HFX_TEST_REJECTED_LOCK_CREATED
pass 'reclaim parallelism refusal occurs before campaign lock creation'

printf '%s\n' \
    1020000010-basins 1020000010-streamnet \
    7020000010-basins 7020000010-streamnet \
    9020000010-basins 9020000010-streamnet >"$test_tmp/expected-reclaim-starts"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
run_runner acquire --campaign reclaim-parallel-one --workspace-root "$valid_root" \
    --max-parallel 1 >"$case_stdout"
sed -n 's/^start //p' "$test_tmp/transfer-state/events" | sort >"$test_tmp/reclaim-starts"
diff -u "$test_tmp/expected-reclaim-starts" "$test_tmp/reclaim-starts"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
run_runner acquire --campaign reclaim-parallel-four --workspace-root "$valid_root" \
    --max-parallel 4 >"$case_stdout"
sed -n 's/^start //p' "$test_tmp/transfer-state/events" | sort >"$test_tmp/reclaim-starts"
diff -u "$test_tmp/expected-reclaim-starts" "$test_tmp/reclaim-starts"
pass 'policy-dependent acquisition concurrency preserves retain-all and bounds reclaim'

rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
run_runner acquire --campaign subset --workspace-root "$subset_root" --max-parallel 62 >"$case_stdout"
grep '^start ' "$test_tmp/transfer-state/events" | sed 's/^start //' | sort >"$test_tmp/subset-starts"
printf '%s\n' \
    1020000010-basins 1020000010-streamnet \
    7020000010-basins 7020000010-streamnet \
    9020000010-basins 9020000010-streamnet >"$test_tmp/expected-subset-starts"
diff -u "$test_tmp/expected-subset-starts" "$test_tmp/subset-starts"
[[ $(find "$subset_campaign_dir/downloads" -name '*.gpkg' -type f | wc -l | tr -d ' ') -eq 6 ]] ||
    die 'subset acquisition did not install exactly six files'
jq -e -s '
  map(select(.processing_basin_id as $id |
    ["1020000010","7020000010","9020000010"] | index($id) | not)) |
  length == 59 and all(
    .stages.acquire_basins.status == "pending" and
    .stages.acquire_basins.attempts == 0 and
    .stages.acquire_streamnet.status == "pending" and
    .stages.acquire_streamnet.attempts == 0)
' "$subset_campaign_dir"/state/basins/*/current.json >/dev/null ||
    die 'subset acquisition modified an unselected basin'
assert_contains "$case_stdout" 'acquire_basins_succeeded=3'
assert_contains "$case_stdout" 'acquire_streamnet_succeeded=3'
pass 'three-basin acquisition schedules only the frozen selection'

named_root=$test_tmp/workspaces/named-compile
mkdir "$named_root"
cp -R "$subset_campaign_dir" "$named_root/tdx-hydro-subset"
named_campaign_dir=$named_root/tdx-hydro-subset
cp "$named_campaign_dir/state/basins/7020000010/current.json" "$test_tmp/named-702-before"
cp "$named_campaign_dir/state/basins/9020000010/current.json" "$test_tmp/named-902-before"
: >"$HFX_TEST_ADAPTER_LOG"
export HFX_TEST_REQUIRE_LOCK_OWNER=1
export HFX_TEST_LOCK_OWNER_LOG=$test_tmp/named-lock-owner
run_runner compile-basin --campaign subset --workspace-root "$named_root" \
    --basin 1020000010 --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout"
unset HFX_TEST_REQUIRE_LOCK_OWNER HFX_TEST_LOCK_OWNER_LOG
[[ -s "$test_tmp/named-lock-owner" ]] || die 'named compile adapter did not observe the runner lock owner'
[[ ! -d "$named_campaign_dir/state/locks/campaign.lock" ]] ||
    die 'named compile left its campaign lock behind'
assert_contains "$case_stdout" 'processing_basin_id=1020000010'
assert_contains "$case_stdout" 'compile_status=succeeded'
assert_contains "$case_stdout" 'compile_attempts=1'
assert_contains "$case_stdout" 'inputs_reclaimed=not-applicable'
[[ $(grep -c '^build' "$HFX_TEST_ADAPTER_LOG") -eq 1 ]] ||
    die 'named compile did not issue exactly one adapter build'
[[ $(grep '^build' "$HFX_TEST_ADAPTER_LOG" | cut -f11) == 1020000010 ]] ||
    die 'named compile adapter build targeted another basin'
cmp "$test_tmp/named-702-before" "$named_campaign_dir/state/basins/7020000010/current.json"
cmp "$test_tmp/named-902-before" "$named_campaign_dir/state/basins/9020000010/current.json"
for untouched_id in 7020000010 9020000010; do
    jq -e '.stages.compile == {
      status:"pending",attempts:0,failure_reason:null,diagnostic_report:null
    }' "$named_campaign_dir/state/basins/$untouched_id/current.json" >/dev/null ||
        die "named compile changed pending compile state for $untouched_id"
    [[ ! -e "$named_campaign_dir/basin-outputs/$untouched_id" &&
        ! -e "$named_campaign_dir/reports/$untouched_id-build-report.json" ]] ||
        die "named compile created an artifact for $untouched_id"
done
if grep -R -F 'acquisition prerequisites are not both succeeded' \
    "$named_campaign_dir/state/basins" >"$case_stdout"; then
    die 'named compile manufactured a prerequisite failure'
fi
run_runner evidence --campaign subset --workspace-root "$named_root" >"$case_stdout"
jq -e '
  .excluded_basins == [] and
  (.outcomes | map(select(.processing_basin_id == "1020000010"))[0] |
    .status == "succeeded" and .attempts == 1) and
  (.outcomes | map(select(.processing_basin_id != "1020000010")) |
    all(.status == "pending" and .attempts == 0 and .failure_reason == null))
' "$named_campaign_dir/publication/evidence/outcomes.json" >/dev/null ||
    die 'named compile evidence manufactured an exclusion or changed pending outcomes'
cmp "$test_tmp/named-702-before" "$named_campaign_dir/state/basins/7020000010/current.json"
cmp "$test_tmp/named-902-before" "$named_campaign_dir/state/basins/9020000010/current.json"
pass 'compile-basin re-enters its exact owner lock and preserves untouched basin outcomes'

for reentry_tamper in live-owner owner-symlink lock-symlink; do
    reentry_root=$test_tmp/workspaces/reentry-$reentry_tamper
    mkdir "$reentry_root"
    cp -R "$named_campaign_dir" "$reentry_root/tdx-hydro-subset"
    reentry_campaign_dir=$reentry_root/tdx-hydro-subset
    reentry_lock=$reentry_campaign_dir/state/locks/campaign.lock
    cp "$reentry_campaign_dir/state/basins/7020000010/current.json" \
        "$test_tmp/reentry-$reentry_tamper-current-before"
    cp "$reentry_campaign_dir/state/compile.json" \
        "$test_tmp/reentry-$reentry_tamper-compile-before"
    HFX_TEST_REENTRY_TAMPER=$reentry_tamper HFX_TEST_COMPETING_PID=$$ \
        HFX_TDX_JQ=$test_tmp/fake-jq \
        expect_failure "named compile $reentry_tamper re-entry" compile-basin \
        --campaign subset --workspace-root "$reentry_root" --basin 7020000010 \
        --fabric-version NGA-TDX-Hydro-20230126
    case $reentry_tamper in
        live-owner)
            [[ $(tail -1 "$case_stderr") == "hfx: error: campaign lock is held by live PID $$" ]] ||
                die 'named compile live-owner re-entry diagnostic differs'
            ;;
        owner-symlink|lock-symlink)
            [[ $(tail -1 "$case_stderr") == \
                "hfx: error: campaign lock owner is indeterminate; preserved at $reentry_lock" ]] ||
                die "named compile $reentry_tamper re-entry diagnostic differs"
            ;;
    esac
    cmp "$test_tmp/reentry-$reentry_tamper-current-before" \
        "$reentry_campaign_dir/state/basins/7020000010/current.json"
    cmp "$test_tmp/reentry-$reentry_tamper-compile-before" \
        "$reentry_campaign_dir/state/compile.json"
    [[ ! -e "$reentry_campaign_dir/basin-outputs/7020000010" &&
        ! -e "$reentry_campaign_dir/reports/7020000010-build-report.json" ]] ||
        die "named compile $reentry_tamper re-entry wrote compile artifacts"
done
pass 'compile-basin refuses live-owner and symlink tampering before compile writes'

cleared_repo=$test_tmp/cleared-reentry-repo
mkdir -p "$cleared_repo/scripts/hetzner"
ln -s "$repo_root/adapters" "$cleared_repo/adapters"
sed 's/^    compile_basin_locked "$basin_id"$/    lock_owned=0\
    takeover_owned=0\
&/' "$runner" >"$cleared_repo/scripts/hetzner/tdx-hydro-campaign.sh"
chmod +x "$cleared_repo/scripts/hetzner/tdx-hydro-campaign.sh"
cleared_root=$test_tmp/workspaces/reentry-cleared
mkdir "$cleared_root"
cp -R "$named_campaign_dir" "$cleared_root/tdx-hydro-subset"
cleared_campaign_dir=$cleared_root/tdx-hydro-subset
cp "$cleared_campaign_dir/state/basins/7020000010/current.json" \
    "$test_tmp/reentry-cleared-current-before"
cp "$cleared_campaign_dir/state/compile.json" "$test_tmp/reentry-cleared-compile-before"
cleared_status=0
"$selected_bash" "$cleared_repo/scripts/hetzner/tdx-hydro-campaign.sh" compile-basin \
    --campaign subset --workspace-root "$cleared_root" --basin 7020000010 \
    --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout" 2>"$case_stderr" ||
    cleared_status=$?
[[ "$cleared_status" -ne 0 ]] || die 'named compile with cleared ownership unexpectedly succeeded'
cleared_lock=$cleared_campaign_dir/state/locks/campaign.lock
cleared_owner=$(<"$cleared_lock/owner.pid")
[[ $(tail -1 "$case_stderr") == \
    "hfx: error: campaign lock is held by live PID $cleared_owner" ]] ||
    die 'named compile with cleared ownership diagnostic differs'
cmp "$test_tmp/reentry-cleared-current-before" \
    "$cleared_campaign_dir/state/basins/7020000010/current.json"
cmp "$test_tmp/reentry-cleared-compile-before" "$cleared_campaign_dir/state/compile.json"
[[ ! -e "$cleared_campaign_dir/basin-outputs/7020000010" &&
    ! -e "$cleared_campaign_dir/reports/7020000010-build-report.json" ]] ||
    die 'named compile with cleared ownership wrote compile artifacts'
pass 'compile-basin refuses second lock acquisition after ownership flags are cleared'

jq '.stages.acquire_basins={
  status:"pending",attempts:0,failure_reason:null,evidence:null
}' "$named_campaign_dir/state/basins/7020000010/current.json" >"$test_tmp/named-pending"
mv "$test_tmp/named-pending" "$named_campaign_dir/state/basins/7020000010/current.json"
cp -R "$named_campaign_dir/state" "$test_tmp/named-state-before-refusal"
expect_failure 'named compile pending prerequisite' compile-basin --campaign subset \
    --workspace-root "$named_root" --basin 7020000010 \
    --fabric-version NGA-TDX-Hydro-20230126
[[ $(tail -1 "$case_stderr") == \
    'hfx: error: acquisition prerequisites are not both succeeded for 7020000010' ]] ||
    die 'named compile prerequisite diagnostic differs'
diff -ru "$test_tmp/named-state-before-refusal" "$named_campaign_dir/state"
[[ ! -e "$named_campaign_dir/basin-outputs/7020000010" &&
    ! -e "$named_campaign_dir/reports/7020000010-build-report.json" ]] ||
    die 'named compile prerequisite refusal created artifacts'
pass 'compile-basin prerequisite refusal writes no failure or other basin state'

expect_failure 'named compile unknown inventory basin' compile-basin --campaign subset \
    --workspace-root "$named_root" --basin 9999999999 --fabric-version version
[[ $(tail -1 "$case_stderr") == \
    'hfx: error: processing basin ID is not in the authoritative inventory: 9999999999' ]] ||
    die 'named compile authoritative-inventory diagnostic differs'
expect_failure 'named compile unselected inventory basin' compile-basin --campaign subset \
    --workspace-root "$named_root" --basin 1020011530 --fabric-version version
[[ $(tail -1 "$case_stderr") == \
    'hfx: error: processing basin ID is not in the frozen campaign selection: 1020011530' ]] ||
    die 'named compile frozen-selection diagnostic differs'
pass 'compile-basin validates inventory and frozen selection in an initialized campaign'

named_lock=$named_campaign_dir/state/locks/campaign.lock
mkdir "$named_lock"
printf '%s\n' "$$" >"$named_lock/owner.pid"
cp "$named_lock/owner.pid" "$test_tmp/named-live-owner-before"
expect_failure 'named compile competing live owner' compile-basin --campaign subset \
    --workspace-root "$named_root" --basin 7020000010 --fabric-version version
assert_contains "$case_stderr" "campaign lock is held by live PID $$"
cmp "$test_tmp/named-live-owner-before" "$named_lock/owner.pid"
rm -r "$named_lock"
mkdir "$named_lock"
printf '%s\n' 99999999 >"$named_lock/owner.pid"
expect_failure 'named compile stale owner prerequisite' compile-basin --campaign subset \
    --workspace-root "$named_root" --basin 7020000010 --fabric-version version
assert_contains "$case_stderr" 'acquisition prerequisites are not both succeeded for 7020000010'
[[ ! -d "$named_lock" ]] || die 'named compile stale takeover left its replacement lock behind'
pass 'compile-basin preserves competing live owners and converges stale takeover'

progress_root=$test_tmp/workspaces/progress
mkdir "$progress_root"
cp -R "$named_campaign_dir" "$progress_root/tdx-hydro-subset"
progress_dir=$progress_root/tdx-hydro-subset
mkdir "$progress_dir/state/locks/campaign.lock"
printf '%s\n' "$$" >"$progress_dir/state/locks/campaign.lock/owner.pid"
cp -R "$progress_dir" "$test_tmp/progress-baseline"
run_runner progress --campaign subset --workspace-root "$progress_root" >"$test_tmp/progress-one"
run_runner progress --campaign subset --workspace-root "$progress_root" >"$test_tmp/progress-two"
cmp "$test_tmp/progress-one" "$test_tmp/progress-two"
assert_contains "$test_tmp/progress-one" 'acquire_basins_succeeded=2'
assert_contains "$test_tmp/progress-one" 'compile_succeeded=1'
assert_contains "$test_tmp/progress-one" 'compile_pending=2'
cmp "$test_tmp/progress-baseline/state/locks/campaign.lock/owner.pid" \
    "$progress_dir/state/locks/campaign.lock/owner.pid"
diff -ru "$test_tmp/progress-baseline" "$progress_dir"
rm -r "$progress_dir/state/locks/campaign.lock"
pass 'progress is deterministic and byte-preserving under a live campaign lock'

: >"$HFX_TEST_ADAPTER_LOG"
run_runner compile --campaign subset --workspace-root "$subset_root" \
    --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout"
grep '^build' "$HFX_TEST_ADAPTER_LOG" | cut -f11 | sort >"$test_tmp/subset-build-ids"
printf '%s\n' 1020000010 7020000010 9020000010 >"$test_tmp/expected-subset-build-ids"
diff -u "$test_tmp/expected-subset-build-ids" "$test_tmp/subset-build-ids"
[[ $(grep -c '^build' "$HFX_TEST_ADAPTER_LOG") -eq 3 ]] ||
    die 'subset compile did not issue exactly three builds'
jq '.stages.compile={
  status:"failed",attempts:1,failure_reason:"adapter validation failed",
  diagnostic_report:{path:"reports/7020000010-build-report.json",diagnostics:{fixture_metric:702}}
}' "$subset_campaign_dir/state/basins/7020000010/current.json" >"$test_tmp/subset-failed"
mv "$test_tmp/subset-failed" "$subset_campaign_dir/state/basins/7020000010/current.json"
mark_compile_succeeded "$subset_campaign_dir" 1020011530
mkdir -p "$test_tmp/fixtures/subset-partial-fabric/aux"
subset_partial_fabric_root=$(cd -P "$test_tmp/fixtures/subset-partial-fabric" && pwd -P)
for partial_file in catchments.parquet graph.parquet manifest.json aux/snap_stems.parquet; do
    printf '%s\n' fixture >"$subset_partial_fabric_root/$partial_file"
done
printf '%s\n' '["1020000010"]' >"$test_tmp/fixtures/subset-partial-fabric-roster.json"
subset_partial_fabric_roster=$(cd -P "$test_tmp/fixtures" && pwd -P)/subset-partial-fabric-roster.json
HFX_TEST_EXPECTED_ASSEMBLY_ARGV=$test_tmp/subset-assembly-argv
export HFX_TEST_EXPECTED_ASSEMBLY_ARGV
printf '%s\n' \
    assemble \
    --partial-input "$subset_partial_fabric_root" \
    --partial-roster "$subset_partial_fabric_roster" \
    --input "$subset_campaign_dir/basin-outputs/9020000010" \
    --out "$subset_campaign_dir/assembly/dataset" >"$HFX_TEST_EXPECTED_ASSEMBLY_ARGV"
run_runner assemble --campaign subset --workspace-root "$subset_root" \
    --partial-fabric "$subset_partial_fabric_root" \
    --partial-fabric-roster "$subset_partial_fabric_roster" \
    --exclude-control-basin 1020000010 >"$case_stdout"
jq -e --arg root "$subset_partial_fabric_root" --arg roster "$subset_partial_fabric_roster" '. == {
  schema_version:2,status:"succeeded",attempts:1,failure_reason:null,
  fabric_root:$root,fabric_roster_path:$roster,fabric_basin_ids:["1020000010"],
  excluded_control_basin_id:"1020000010",included_basin_ids:["9020000010"],
  included_dataset_paths:["basin-outputs/9020000010"],output_path:"assembly/dataset",
  report_path:"reports/assembly.json"
}' \
    "$subset_campaign_dir/state/assembly.json" >/dev/null ||
    die 'subset assembly state admitted a failed or unselected basin'
jq -e --arg root "$subset_partial_fabric_root" --arg roster "$subset_partial_fabric_roster" '. == {
  schema_version:2,campaign:"subset",fabric_root:$root,fabric_roster_path:$roster,
  fabric_basin_ids:["1020000010"],excluded_control_basin_id:"1020000010",
  included_basin_ids:["9020000010"],included_dataset_paths:["basin-outputs/9020000010"],
  output_path:"assembly/dataset"
}' \
    "$subset_campaign_dir/reports/assembly.json" >/dev/null ||
    die 'subset assembly report admitted a failed or unselected basin'
pass 'compile and assemble remain scoped to the frozen selection'

mkdir "$test_tmp/subset-lifecycle-before"
cp "$subset_campaign_dir/state/selection.json" "$test_tmp/subset-lifecycle-before/selection.json"
cp "$subset_campaign_dir/state/assembly.json" "$test_tmp/subset-lifecycle-before/assembly.json"
cp "$subset_campaign_dir/state/compile.json" "$test_tmp/subset-lifecycle-before/compile.json"
cp "$subset_campaign_dir/reports/assembly.json" "$test_tmp/subset-lifecycle-before/assembly-report.json"
for preserved_id in 1020000010 7020000010 9020000010 1020011530; do
    cp "$subset_campaign_dir/state/basins/$preserved_id/current.json" \
        "$test_tmp/subset-lifecycle-before/$preserved_id.json"
done
subset_adapter_lines=$(wc -l <"$HFX_TEST_ADAPTER_LOG" | tr -d ' ')
subset_hfx_lines=$(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ')
subset_transfer_lines=$(wc -l <"$test_tmp/transfer-state/events" | tr -d ' ')
set -- $(init_args subset "$subset_root")
expect_failure 'mid-lifecycle basin selection drift' "$@" \
    --basin 1020000010 --basin 9020000010
[[ $(tail -1 "$case_stderr") == 'hfx: error: basin selection changed; use a new campaign ID' ]] ||
    die 'mid-lifecycle basin selection diagnostic differs'
diff -u "$test_tmp/subset-lifecycle-before/assembly-report.json" \
    "$subset_campaign_dir/reports/assembly.json"
for preserved_id in 1020000010 7020000010 9020000010 1020011530; do
    diff -u "$test_tmp/subset-lifecycle-before/$preserved_id.json" \
        "$subset_campaign_dir/state/basins/$preserved_id/current.json"
done
diff -u "$test_tmp/subset-lifecycle-before/selection.json" "$subset_campaign_dir/state/selection.json"
diff -u "$test_tmp/subset-lifecycle-before/compile.json" "$subset_campaign_dir/state/compile.json"
diff -u "$test_tmp/subset-lifecycle-before/assembly.json" "$subset_campaign_dir/state/assembly.json"
[[ $(wc -l <"$HFX_TEST_ADAPTER_LOG" | tr -d ' ') -eq "$subset_adapter_lines" &&
    $(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ') -eq "$subset_hfx_lines" &&
    $(wc -l <"$test_tmp/transfer-state/events" | tr -d ' ') -eq "$subset_transfer_lines" ]] ||
    die 'mid-lifecycle selection refusal invoked a service fake'
pass 'mid-lifecycle basin selection drift refuses without side effects'

subset_evidence_root=$test_tmp/workspaces/subset-evidence
mkdir "$subset_evidence_root"
set -- $(init_args subset-evidence "$subset_evidence_root")
run_runner "$@" --basin 7020000010 --basin 1020000010 >"$case_stdout"
subset_evidence_dir=$subset_evidence_root/tdx-hydro-subset-evidence
assert_contains "$case_stdout" 'inventory_count=62'
assert_contains "$case_stdout" 'selected_basin_count=2'
assert_contains "$case_stdout" 'unselected_basin_count=60'
assert_contains "$case_stdout" 'compile_pending=2'
jq -n --arg id 1020000010 '{
  schema_version:3,processing_basin_id:$id,stages:{
    acquire_basins:{status:"succeeded",attempts:1,failure_reason:null,evidence:{
      bytes:21,sha256:("a"*64),sqlite_identity:"53514c69746520666f726d6174203300",
      layer_name:"basins-1020000010"}},
    acquire_streamnet:{status:"succeeded",attempts:1,failure_reason:null,evidence:{
      bytes:22,sha256:("b"*64),sqlite_identity:"53514c69746520666f726d6174203300",
      layer_name:"streamnet-1020000010"}},
    compile:{status:"succeeded",attempts:1,failure_reason:null,diagnostic_report:{
      path:"reports/1020000010-build-report.json",diagnostics:{fixture_metric:101}}}
  }}' >"$subset_evidence_dir/state/basins/1020000010/current.json"
jq -n --arg id 7020000010 '{
  schema_version:3,processing_basin_id:$id,stages:{
    acquire_basins:{status:"succeeded",attempts:1,failure_reason:null,evidence:{
      bytes:23,sha256:("c"*64),sqlite_identity:"53514c69746520666f726d6174203300",
      layer_name:"basins-7020000010"}},
    acquire_streamnet:{status:"succeeded",attempts:1,failure_reason:null,evidence:{
      bytes:24,sha256:("d"*64),sqlite_identity:"53514c69746520666f726d6174203300",
      layer_name:"streamnet-7020000010"}},
    compile:{status:"failed",attempts:1,failure_reason:"adapter validation failed",diagnostic_report:{
      path:"reports/7020000010-build-report.json",diagnostics:{fixture_metric:702}}}
  }}' >"$subset_evidence_dir/state/basins/7020000010/current.json"
cp "$subset_evidence_dir/state/basins/9020000010/current.json" "$test_tmp/unselected-before-evidence"
run_runner status --campaign subset-evidence --workspace-root "$subset_evidence_root" >"$case_stdout"
assert_contains "$case_stdout" 'acquire_basins_succeeded=2'
assert_contains "$case_stdout" 'acquire_streamnet_succeeded=2'
assert_contains "$case_stdout" 'compile_succeeded=1'
assert_contains "$case_stdout" 'compile_failed=1'
run_runner evidence --campaign subset-evidence --workspace-root "$subset_evidence_root"
diff -u "$test_tmp/unselected-before-evidence" \
    "$subset_evidence_dir/state/basins/9020000010/current.json"
for evidence_name in acquisition outcomes diagnostics; do
    evidence_file=$subset_evidence_dir/publication/evidence/$evidence_name.json
    jq -e '
      .schema_version == 2 and
      .selected_basin_ids == ["1020000010","7020000010"]
    ' "$evidence_file" >/dev/null || die "$evidence_name subset classes differ"
    jq -e --slurpfile inventory "$subset_evidence_dir/state/inventory.json" '
      .unselected_basin_ids == (($inventory[0] | keys) - ["1020000010","7020000010"]) and
      (.unselected_basin_ids | index("9020000010")) != null
    ' "$evidence_file" >/dev/null || die "$evidence_name unselected class differs"
done
jq -e '
  [.basins[].processing_basin_id] == ["1020000010","7020000010"]
' "$subset_evidence_dir/publication/evidence/acquisition.json" >/dev/null ||
    die 'subset acquisition evidence omitted a selected basin'
jq -e '
  {schema_version,campaign,selected_basin_ids,attempted_basin_ids,excluded_basins,outcomes} == {
    schema_version:2,
    campaign:"subset-evidence",
    selected_basin_ids:["1020000010","7020000010"],
    attempted_basin_ids:["1020000010","7020000010"],
    excluded_basins:[{processing_basin_id:"7020000010",
      failure_reason:"adapter validation failed"}],
    outcomes:[
      {processing_basin_id:"1020000010",status:"succeeded",attempts:1,failure_reason:null},
      {processing_basin_id:"7020000010",status:"failed",attempts:1,
        failure_reason:"adapter validation failed"}
    ]
  }
' "$subset_evidence_dir/publication/evidence/outcomes.json" >/dev/null ||
    die 'subset outcome projection differs'
jq -e '
  [.basins[].processing_basin_id] == ["1020000010","7020000010"]
' "$subset_evidence_dir/publication/evidence/diagnostics.json" >/dev/null ||
    die 'subset diagnostics evidence omitted a selected basin'
pass 'subset status and evidence preserve selected, attempted, excluded, and unselected classes'

rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
rendezvous_status=0
HFX_TEST_INTERRUPT_DRAIN=1 HFX_TEST_RENDEZVOUS_LIMIT=3 \
    "$test_tmp/fake-curl" \
        --fail \
        --show-error \
        --location \
        --connect-timeout 30 \
        --speed-limit 65536 \
        --speed-time 60 \
        --dump-header "$test_tmp/lone-basins.headers" \
        --write-out 'http_status=%{http_code}\nnetwork_bytes=%{size_download}\ntime_total_seconds=%{time_total}\naverage_bytes_per_second=%{speed_download}\n' \
        --output "$test_tmp/lone-basins.gpkg.partial" \
        'https://earth-info.nga.mil/php/download.php?file=1020000010-basins-gpkg' \
        >"$test_tmp/lone-basins.stats" 2>"$case_stderr" || rendezvous_status=$?
[[ "$rendezvous_status" -eq 95 ]] ||
    die "single-marker drain rendezvous exited $rendezvous_status instead of 95"
[[ $(find "$test_tmp/transfer-state" -name 'curl.*' -type f | wc -l | tr -d ' ') == 1 ]] ||
    die 'single-marker drain rendezvous did not create exactly one curl marker'
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"

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
# The rendezvous waits for a real acquire_basin subshell to exit before checking
# that the parent still owns the operational lock. The source-order check
# separately enforces the defense-in-depth ownership resets.
[[ -s "$test_tmp/transfer-state/lock-owner-stable" ]] ||
    die 'campaign lock did not remain owned by the parent after an acquire_basin child exited'
drain_campaign_dir=$drain_interrupt_root/tdx-hydro-interrupt-drain
[[ $(find "$drain_campaign_dir/state/tmp" \
    \( -name '.curl-headers.*' -o -name '.curl-stats.*' \) -type f |
    wc -l | tr -d ' ') == 0 ]] ||
    die 'interrupted acquisition retained header or stats temporaries'
[[ $(find "$drain_campaign_dir/downloads" \
    \( -name '*.gpkg.partial' -o -name '*.gpkg.partial.json' \) -print |
    wc -l | tr -d ' ') == 0 ]] ||
    die 'interrupted acquisition retained resumable bytes or provenance'
pass 'repeated INT/TERM during worker drain exits 130, reaps workers, and releases the lock'
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"

acquire_root=$test_tmp/workspaces/acquire
mkdir "$acquire_root"
set -- $(init_args acquire "$acquire_root")
run_runner "$@" >"$case_stdout"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
export HFX_TEST_BARRIER_COUNT=4
if ! run_runner acquire --campaign acquire --workspace-root "$acquire_root" --max-parallel 4 \
    >"$case_stdout" 2>"$case_stderr"; then
    sed 's/^/all-62 acquire: /' "$case_stderr" >&2
    die 'all-62 acquisition failed'
fi
unset HFX_TEST_BARRIER_COUNT
acquire_dir=$acquire_root/tdx-hydro-acquire
[[ $(<"$test_tmp/transfer-state/maximum") == 4 ]] ||
    die 'bounded parallel acquisition did not observe exactly four transfers'
[[ $(find "$acquire_dir/downloads" -name '*.gpkg' -type f | wc -l | tr -d ' ') == 124 ]] ||
    die 'complete acquisition did not install 124 files'
[[ $(grep -c '^start ' "$test_tmp/transfer-state/events") == 124 ]] ||
    die 'complete acquisition did not invoke 124 transfers'
[[ $(grep -c '^hfx: acquisition product=.* status=succeeded ' "$case_stderr") == 124 ]] ||
    die 'complete acquisition did not emit 124 succeeded summaries'
[[ $(find "$acquire_dir/reports" -name '*-acquisition.json' -type f | wc -l | tr -d ' ') == 124 ]] ||
    die 'complete acquisition did not install 124 acquisition reports'
[[ $(cut -f2 "$test_tmp/transfer-state/paths" | sort -u | wc -l | tr -d ' ') == 124 &&
    $(cut -f3 "$test_tmp/transfer-state/paths" | sort -u | wc -l | tr -d ' ') == 124 ]] ||
    die 'acquisition header and stats paths are not distinct'
while IFS=$'\t' read -r captured_key captured_headers captured_stats; do
    captured_basin=${captured_key%-*}
    captured_product=${captured_key##*-}
    case $captured_headers in
        *".curl-headers.$captured_basin.$captured_product."*) ;;
        *) die "header path lacks basin/product identity: $captured_headers" ;;
    esac
    case $captured_stats in
        *".curl-stats.$captured_basin.$captured_product."*) ;;
        *) die "stats path lacks basin/product identity: $captured_stats" ;;
    esac
done <"$test_tmp/transfer-state/paths"
jq -e -s '
  length == 124 and all(
    (keys | sort) == ["processing_basin_id","product","retry_count","schema_version","transfers"] and
    .schema_version == 1 and .retry_count == 0 and (.transfers | length) == 1 and
    (.transfers[0] | keys | sort) == ["attempt","average_bytes_per_second","http_status",
      "mode","network_bytes","result","time_total_seconds"] and
    .transfers[0].mode == "fresh" and .transfers[0].result == "succeeded" and
    .transfers[0].http_status == 200 and .transfers[0].time_total_seconds == 1.25
  )
' "$acquire_dir"/reports/*-acquisition.json >/dev/null ||
    die 'fresh acquisition reports differ'
jq -e -s '
  length == 62 and all(
    .schema_version == 3 and
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
run_runner acquire --campaign acquire --workspace-root "$acquire_root" --max-parallel 4 \
    >"$case_stdout" 2>"$case_stderr"
events_after=$(wc -l <"$test_tmp/transfer-state/events" | tr -d ' ')
[[ "$events_before" == "$events_after" ]] || die 'verified reuse fetched files again'
[[ $(grep -c ' status=reused ' "$case_stderr") == 124 ]] ||
    die 'verified reuse summaries differ'
pass 'all 62 basins acquire with bounded concurrency and exact reusable evidence'

serial_root=$test_tmp/workspaces/serial
mkdir "$serial_root"
set -- $(init_args serial "$serial_root")
run_runner "$@" >"$case_stdout"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
run_runner acquire --campaign serial --workspace-root "$serial_root" --max-parallel 1 \
    >"$case_stdout" 2>"$case_stderr"
[[ $(<"$test_tmp/transfer-state/maximum") == 1 ]] || die 'serial acquisition exceeded one active transfer'
[[ $(find "$serial_root/tdx-hydro-serial/reports" -name '*-acquisition.json' -type f |
    wc -l | tr -d ' ') == 124 ]] ||
    die 'serial acquisition did not atomically install every product report'
[[ $(find "$serial_root/tdx-hydro-serial/reports" -name '.*.tmp.*' -type f |
    wc -l | tr -d ' ') == 0 ]] ||
    die 'serial acquisition left a report writer temporary'
pass 'maximum parallel one remains strictly serial'

no_etag_root=$test_tmp/workspaces/no-etag
mkdir "$no_etag_root"
cp -R "$subset_campaign_dir" "$no_etag_root/tdx-hydro-subset"
no_etag_dir=$no_etag_root/tdx-hydro-subset
no_etag_id=1020000010
no_etag_state=$no_etag_dir/state/basins/$no_etag_id/current.json
no_etag_final=$no_etag_dir/downloads/$no_etag_id-basins.gpkg
no_etag_report=$no_etag_dir/reports/$no_etag_id-basins-acquisition.json
rm -f "$no_etag_final" "$no_etag_final.partial" "$no_etag_report"
jq '.stages.acquire_basins={status:"failed",attempts:1,failure_reason:"fixture",evidence:null}' \
    "$no_etag_state" >"$no_etag_state.tmp"
mv "$no_etag_state.tmp" "$no_etag_state"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
run_runner acquire --campaign subset --workspace-root "$no_etag_root" --max-parallel 4 \
    >"$case_stdout" 2>"$case_stderr"
jq -e '.stages.acquire_basins.status == "succeeded"' "$no_etag_state" >/dev/null ||
    die "complete HTTP 200 no-ETag acquisition was rejected: stage_reason=$(jq -r '.stages.acquire_basins.failure_reason' "$no_etag_state"); final_exists=$([[ -f "$no_etag_final" ]] && printf yes || printf no); $(<"$case_stderr")"
[[ -f "$no_etag_final" ]] ||
    die 'complete HTTP 200 no-ETag acquisition did not install its final'
cmp "$no_etag_final" "$HFX_TEST_GPKG_TEMPLATE" >/dev/null ||
    die 'complete HTTP 200 no-ETag acquisition changed the fixture bytes'
jq -e '.transfers | last |
  .http_status == 200 and .mode == "fresh" and .result == "succeeded"' "$no_etag_report" >/dev/null ||
    die 'complete HTTP 200 no-ETag acquisition report differs'
no_etag_headers=$test_tmp/transfer-state/headers.$no_etag_id-basins
assert_contains "$no_etag_headers" 'HTTP/1.1 200 OK'
assert_contains "$no_etag_headers" 'Content-Type: application/octet-stream'
assert_contains "$no_etag_headers" "Content-Length: $(wc -c <"$HFX_TEST_GPKG_TEMPLATE" | tr -d ' ')"
assert_contains "$no_etag_headers" "Content-Disposition: attachment; filename=\"$no_etag_id-basins.gpkg\""
assert_zero_ere 'no-ETag fixture emitted ETag' 'ETag:' "$no_etag_headers"
assert_zero_ere 'no-ETag fixture emitted Accept-Ranges' 'Accept-Ranges:' "$no_etag_headers"
assert_zero_ere 'no-ETag fixture emitted Content-Range' 'Content-Range:' "$no_etag_headers"
no_etag_stream_final=$no_etag_dir/downloads/$no_etag_id-streamnet.gpkg
no_etag_stream_report=$no_etag_dir/reports/$no_etag_id-streamnet-acquisition.json
rm "$no_etag_stream_final" "$no_etag_stream_report"
jq '.stages.acquire_streamnet={status:"failed",attempts:1,failure_reason:"fixture",evidence:null}' \
    "$no_etag_state" >"$no_etag_state.tmp"
mv "$no_etag_state.tmp" "$no_etag_state"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
HFX_TEST_EMIT_ETAG=1 \
    run_runner acquire --campaign subset --workspace-root "$no_etag_root" --max-parallel 4 \
    >"$case_stdout" 2>"$case_stderr"
jq -e '.stages.acquire_streamnet.status == "succeeded"' "$no_etag_state" >/dev/null ||
    die 'optional strong ETag response metadata was rejected'
assert_contains "$test_tmp/transfer-state/headers.$no_etag_id-streamnet" 'ETag: "fixture-v1"'
pass 'complete NGA-shaped HTTP 200 without ETag installs through acquire_product'

failure_root=$test_tmp/workspaces/failure
mkdir "$failure_root"
set -- $(init_args failure "$failure_root")
run_runner "$@" >"$case_stdout"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
failure_id=$(jq -r 'keys[0]' "$inventory")
export HFX_TEST_FAIL_KEY=$failure_id-basins
run_runner acquire --campaign failure --workspace-root "$failure_root" --max-parallel 3 \
    >"$case_stdout" 2>"$case_stderr"
failure_state=$failure_root/tdx-hydro-failure/state/basins/$failure_id/current.json
failure_partial=$failure_root/tdx-hydro-failure/downloads/$failure_id-basins.gpkg.partial
[[ ! -e "$failure_partial" && ! -e "$failure_partial.json" ]] ||
    die 'interrupted product retained resumable acquisition state'
[[ $(find "$failure_root/tdx-hydro-failure/state/tmp" \
    \( -name ".curl-headers.$failure_id.basins.*" -o -name ".curl-stats.$failure_id.basins.*" \) \
    -type f | wc -l | tr -d ' ') == 0 ]] ||
    die 'failed transfer retained its header or stats temporary'
jq -e '
  .stages.acquire_basins.status == "failed" and
  .stages.acquire_basins.attempts == 1 and
  .stages.acquire_basins.evidence == null and
  .stages.acquire_basins.failure_reason == "transfer interrupted; retry from byte zero" and
  .stages.acquire_streamnet.status == "succeeded"
' "$failure_state" >/dev/null || die 'isolated transfer failure state differs'
unset HFX_TEST_FAIL_KEY
events_before=$(grep -c '^start ' "$test_tmp/transfer-state/events")
run_runner acquire --campaign failure --workspace-root "$failure_root" --max-parallel 3 \
    >"$case_stdout" 2>"$case_stderr"
events_after=$(grep -c '^start ' "$test_tmp/transfer-state/events")
[[ $((events_after - events_before)) == 1 ]] || die 'retry fetched work other than the failed product'
jq -e '.stages.acquire_basins.status == "succeeded" and .stages.acquire_basins.attempts == 2' \
    "$failure_state" >/dev/null || die 'failed product did not converge on retry'
jq -e '.retry_count == 1 and (.transfers | length) == 2 and
  .transfers[0].mode == "fresh" and .transfers[0].result == "curl_failed" and
  .transfers[1].mode == "fresh" and .transfers[1].result == "succeeded" and
  .transfers[1].network_bytes == 24 and
  ([.. | objects | has("resume_offset_bytes")] | all(. == false))' \
    "$failure_root/tdx-hydro-failure/reports/$failure_id-basins-acquisition.json" >/dev/null ||
    die 'failed product retry-from-zero telemetry differs'

leading_zero_root=$test_tmp/workspaces/leading-zero
mkdir "$leading_zero_root"
cp -R "$subset_campaign_dir" "$leading_zero_root/tdx-hydro-subset"
leading_zero_dir=$leading_zero_root/tdx-hydro-subset
leading_zero_id=1020000010
leading_zero_state=$leading_zero_dir/state/basins/$leading_zero_id/current.json
leading_zero_final=$leading_zero_dir/downloads/$leading_zero_id-basins.gpkg
rm "$leading_zero_final"
jq '.stages.acquire_basins={status:"failed",attempts:1,failure_reason:"fixture",evidence:null}' \
    "$leading_zero_state" >"$leading_zero_state.tmp"
mv "$leading_zero_state.tmp" "$leading_zero_state"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
HFX_TEST_FAIL_KEY=$leading_zero_id-basins HFX_TEST_LEADING_ZERO_LENGTH=1 \
    run_runner acquire --campaign subset --workspace-root "$leading_zero_root" --max-parallel 4 \
    >"$case_stdout" 2>"$case_stderr"
jq -e '.stages.acquire_basins.status == "failed" and
  .stages.acquire_basins.failure_reason == "transfer interrupted; retry from byte zero" and
  .stages.acquire_streamnet.status == "succeeded"' "$leading_zero_state" >/dev/null ||
    die 'leading-zero Content-Length was not isolated to its product'
[[ ! -e "$leading_zero_final.partial" && ! -e "$leading_zero_final.partial.json" ]] ||
    die 'leading-zero Content-Length retained unattributable provenance'
pass 'product failures are isolated, retry from zero, and contain malformed Content-Length'

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

retry_root=$test_tmp/workspaces/retry-from-zero
mkdir "$retry_root"
cp -R "$subset_campaign_dir" "$retry_root/tdx-hydro-subset"
retry_dir=$retry_root/tdx-hydro-subset
retry_id=1020000010
retry_state=$retry_dir/state/basins/$retry_id/current.json
retry_final=$retry_dir/downloads/$retry_id-basins.gpkg
retry_report=$retry_dir/reports/$retry_id-basins-acquisition.json
rm "$retry_final" "$retry_report"
jq '.stages.acquire_basins={status:"failed",attempts:1,failure_reason:"fixture",evidence:null}' \
    "$retry_state" >"$retry_state.tmp"
mv "$retry_state.tmp" "$retry_state"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
HFX_TEST_FAIL_KEY=$retry_id-basins \
    run_runner acquire --campaign subset --workspace-root "$retry_root" --max-parallel 4 \
    >"$case_stdout" 2>"$case_stderr"
[[ ! -e "$retry_final" && ! -e "$retry_final.partial" && ! -e "$retry_final.partial.json" ]] ||
    die 'interrupted single-shot transfer retained reusable bytes or provenance'
jq -e '.stages.acquire_basins.failure_reason == "transfer interrupted; retry from byte zero"' \
    "$retry_state" >/dev/null || die 'interrupted transfer reason differs'
run_runner acquire --campaign subset --workspace-root "$retry_root" --max-parallel 4 \
    >"$case_stdout" 2>"$case_stderr"
cmp "$retry_final" "$HFX_TEST_GPKG_TEMPLATE" >/dev/null ||
    die 'retry from byte zero did not install the exact fixture'
jq -e '.retry_count == 1 and (.transfers | length) == 2 and
  all(.transfers[]; .mode == "fresh") and
  .transfers[0].result == "curl_failed" and
  .transfers[1].result == "succeeded" and
  .transfers[1].http_status == 200 and .transfers[1].network_bytes == 24' \
    "$retry_report" >/dev/null || die 'single-shot retry report differs'

for corruption_shape in truncated substituted; do
    corruption_root=$test_tmp/workspaces/download-$corruption_shape
    mkdir "$corruption_root"
    cp -R "$subset_campaign_dir" "$corruption_root/tdx-hydro-subset"
    corruption_dir=$corruption_root/tdx-hydro-subset
    corruption_state=$corruption_dir/state/basins/1020000010/current.json
    corruption_final=$corruption_dir/downloads/1020000010-basins.gpkg
    corruption_report=$corruption_dir/reports/1020000010-basins-acquisition.json
    rm "$corruption_final" "$corruption_report"
    jq '.stages.acquire_basins={status:"failed",attempts:1,failure_reason:"fixture",evidence:null}' \
        "$corruption_state" >"$corruption_state.tmp"
    mv "$corruption_state.tmp" "$corruption_state"
    rm -r "$test_tmp/transfer-state"
    mkdir "$test_tmp/transfer-state"
    HFX_TEST_TRANSFER_SHAPE=$corruption_shape \
        run_runner acquire --campaign subset --workspace-root "$corruption_root" --max-parallel 4 \
        >"$case_stdout" 2>"$case_stderr"
    expected_corruption_reason='download provenance or size verification failed'
    [[ "$corruption_shape" != substituted ]] ||
        expected_corruption_reason='download failed integrity verification'
    jq -e --arg reason "$expected_corruption_reason" '
      .stages.acquire_basins.status == "failed" and
      .stages.acquire_basins.failure_reason == $reason and
      .stages.acquire_basins.evidence == null
    ' "$corruption_state" >/dev/null ||
        die "$corruption_shape download stage result differs"
    jq -e '.transfers | last |
      .mode == "fresh" and .result == "integrity_failed" and .http_status == 200' \
        "$corruption_report" >/dev/null ||
        die "$corruption_shape download report differs"
    [[ ! -e "$corruption_final" && ! -e "$corruption_final.partial" &&
       ! -e "$corruption_final.partial.json" ]] ||
        die "$corruption_shape download retained reusable acquisition bytes"
done
pass 'truncated and substituted no-ETag downloads fail the real integrity path'

lowercase_fresh_root=$test_tmp/workspaces/lowercase-fresh
mkdir "$lowercase_fresh_root"
cp -R "$subset_campaign_dir" "$lowercase_fresh_root/tdx-hydro-subset"
lowercase_fresh_dir=$lowercase_fresh_root/tdx-hydro-subset
rm "$lowercase_fresh_dir/downloads/1020000010-basins.gpkg"
jq '.stages.acquire_basins={status:"failed",attempts:1,failure_reason:"fixture",evidence:null}' \
    "$lowercase_fresh_dir/state/basins/1020000010/current.json" >"$case_stdout"
mv "$case_stdout" "$lowercase_fresh_dir/state/basins/1020000010/current.json"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
HFX_TEST_LOWERCASE_HEADERS=1 \
    run_runner acquire --campaign subset --workspace-root "$lowercase_fresh_root" --max-parallel 4 \
    >"$case_stdout" 2>"$case_stderr"
[[ -f "$lowercase_fresh_dir/downloads/1020000010-basins.gpkg" ]] ||
    die 'lowercase fresh headers were not accepted'

old_report_root=$test_tmp/workspaces/old-acquisition-report
mkdir "$old_report_root"
cp -R "$acquire_dir" "$old_report_root/tdx-hydro-acquire"
old_report_dir=$old_report_root/tdx-hydro-acquire
old_report_state=$old_report_dir/state/basins/$reuse_id/current.json
old_report_final=$old_report_dir/downloads/$reuse_id-basins.gpkg
old_report_path=$old_report_dir/reports/$reuse_id-basins-acquisition.json
rm "$old_report_final"
jq '.resume_count=0 | .range_ignored_restart_count=0 |
  .transfers[0].resume_offset_bytes=0' "$old_report_path" >"$old_report_path.tmp"
mv "$old_report_path.tmp" "$old_report_path"
jq '.stages.acquire_basins={status:"failed",attempts:1,failure_reason:"fixture",evidence:null}' \
    "$old_report_state" >"$old_report_state.tmp"
mv "$old_report_state.tmp" "$old_report_state"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
run_runner acquire --campaign acquire --workspace-root "$old_report_root" --max-parallel 3 \
    >"$case_stdout" 2>"$case_stderr"
jq -e '.stages.acquire_basins.status == "failed" and
  .stages.acquire_basins.failure_reason ==
    "acquisition report is unsafe or malformed; retained for inspection"' \
    "$old_report_state" >/dev/null ||
    die 'old resume-shaped acquisition report was accepted'
[[ ! -e "$test_tmp/transfer-state/events" ]] ||
    die 'old resume-shaped acquisition report triggered a transfer'

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
pass 'verified finals adopt, stale partials discard, and retries remain fresh'

compile_physical_parent=$test_tmp/workspaces/compile-physical-parent
compile_link_parent=$test_tmp/workspaces/compile-link-parent
mkdir "$compile_physical_parent"
ln -s "$compile_physical_parent" "$compile_link_parent"
compile_root=$compile_link_parent/workspace
mkdir "$compile_root"
cp -R "$acquire_dir" "$compile_root/tdx-hydro-acquire"
compile_dir=$compile_root/tdx-hydro-acquire
[ ! -L "$compile_root" ] || die "compile workspace final component is a symlink"
compile_root_physical=$(cd -P "$compile_root" && pwd -P)
[[ "$compile_root" != "$compile_root_physical" ]] ||
    die "compile workspace did not exercise a symlinked parent"
: >"$HFX_TEST_ADAPTER_LOG"
: >"$HFX_TEST_HFX_LOG"
run_runner compile --campaign acquire --workspace-root "$compile_root" \
    --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout"
jq -e -s '
  length == 62 and all(
    .stages.compile.status == "succeeded" and
    .stages.compile.attempts == 1 and
    .stages.compile.failure_reason == null and
    .stages.compile.diagnostic_report == {
      path:("reports/" + .processing_basin_id + "-build-report.json"),
      diagnostics:{}
    }
  )
' "$compile_dir"/state/basins/*/current.json >/dev/null || die 'compile success states differ'
[[ $(find "$compile_dir/basin-outputs" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ') == 62 ]] ||
    die 'compile did not create 62 basin output directories'
[[ $(find "$compile_dir/reports" -mindepth 1 -maxdepth 1 -type f -name '*-build-report.json' | wc -l | tr -d ' ') == 62 ]] ||
    die 'compile did not create 62 sibling reports'
while IFS= read -r compiled_id; do
    compiled_output=$compile_dir/basin-outputs/$compiled_id
    compiled_report=$compile_dir/reports/$compiled_id-build-report.json
    case $compiled_report in
        "$compiled_output"|"$compiled_output"/*) die "report is inside output for $compiled_id" ;;
    esac
    resolved_compiled_output=$(cd -P "$compiled_output" && pwd -P)
    [[ "$resolved_compiled_output" != "$compiled_output" ]] ||
        die "compile output did not preserve the symlink-parent distinction for $compiled_id"
    jq -e --arg id "$compiled_id" --arg root "$resolved_compiled_output" \
        '.build_identity.processing_basin_id == $id and
         .build_identity.fabric_name == "tdx_hydro" and
         .build_identity.fabric_version == "NGA-TDX-Hydro-20230126" and
         .build_identity.dataset_root == $root' "$compiled_report" >/dev/null ||
        die "compile report identity differs for $compiled_id"
done < <(jq -r 'keys[]' "$inventory")
jq -e '
  type == "object" and keys == ["fabric_version","schema_version"] and
  .schema_version == 1 and .fabric_version == "NGA-TDX-Hydro-20230126"
' "$compile_dir/state/compile.json" >/dev/null || die 'compile contract differs'
: >"$test_tmp/expected-adapter.log"
: >"$test_tmp/expected-hfx.log"
while IFS= read -r expected_id; do
    printf 'build\t--basins\t%s\t--streamnet\t%s\t--out\t%s\t--report\t%s\t--processing-basin-id\t%s\t--fabric-version\t%s\n' \
        "$compile_dir/downloads/$expected_id-basins.gpkg" \
        "$compile_dir/downloads/$expected_id-streamnet.gpkg" \
        "$compile_dir/basin-outputs/$expected_id" \
        "$compile_dir/reports/$expected_id-build-report.json" \
        "$expected_id" \
        'NGA-TDX-Hydro-20230126' >>"$test_tmp/expected-adapter.log"
    printf 'validate\t%s\t--hfx-binary\t%s\n' \
        "$compile_dir/basin-outputs/$expected_id" \
        "$test_tmp/fake-hfx" >>"$test_tmp/expected-adapter.log"
    printf '%s\t--strict\t--sample-pct\t100\t--format\ttext\n' \
        "$compile_dir/basin-outputs/$expected_id" >>"$test_tmp/expected-hfx.log"
done < <(jq -r 'keys[]' "$inventory")
diff -u "$test_tmp/expected-adapter.log" "$HFX_TEST_ADAPTER_LOG"
diff -u "$test_tmp/expected-hfx.log" "$HFX_TEST_HFX_LOG"
[[ $(wc -l <"$HFX_TEST_ADAPTER_LOG" | tr -d ' ') == 124 ]] || die 'adapter log line count differs'
[[ $(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ') == 62 ]] || die 'HFX log line count differs'
if grep -E '(^|[[:space:]])validate([[:space:]]|$)' "$HFX_TEST_HFX_LOG" >/dev/null; then
    die 'HFX was invoked with a validate command token'
fi
if grep -E 'assemble|assembly' "$HFX_TEST_ADAPTER_LOG" "$HFX_TEST_HFX_LOG" >/dev/null; then
    die 'compile invoked assembly'
fi
pass 'complete compile command report and validation contract lands every basin'

large_diagnostics_root=$test_tmp/workspaces/large-diagnostics
mkdir "$large_diagnostics_root"
cp -R "$acquire_dir" "$large_diagnostics_root/tdx-hydro-acquire"
large_diagnostics_dir=$large_diagnostics_root/tdx-hydro-acquire
large_diagnostics_id=$(jq -r 'keys[0]' "$inventory")
[[ "$large_diagnostics_id" == 1020000010 ]] || die 'first inventory basin differs'
: >"$HFX_TEST_ADAPTER_LOG"
: >"$HFX_TEST_HFX_LOG"
HFX_TEST_LARGE_DIAGNOSTICS_ID=1020000010 \
    run_runner compile-basin --campaign acquire --workspace-root "$large_diagnostics_root" \
        --basin 1020000010 --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout"
large_diagnostics_report=$large_diagnostics_dir/reports/1020000010-build-report.json
large_diagnostics_output=$large_diagnostics_dir/basin-outputs/1020000010
large_diagnostics_state=$large_diagnostics_dir/state/basins/1020000010/current.json
diagnostics_bytes=$(jq -c '.diagnostics' "$large_diagnostics_report" | wc -c | tr -d ' ')
host_arg_max=$(getconf ARG_MAX)
[[ "$diagnostics_bytes" -gt 131072 && "$diagnostics_bytes" -gt "$host_arg_max" ]] ||
    die "oversized diagnostics measured $diagnostics_bytes bytes; expected more than Linux MAX_ARG_STRLEN 131072 and host ARG_MAX $host_arg_max"
jq -e --slurpfile report "$large_diagnostics_report" '
  .stages.compile == {
    status: "succeeded",
    attempts: 1,
    failure_reason: null,
    diagnostic_report: {
      path: "reports/1020000010-build-report.json",
      diagnostics: $report[0].diagnostics
    }
  } and
  .stages.compile.diagnostic_report.diagnostics == $report[0].diagnostics and
  $report[0].diagnostics == {
    fixture_metric: 7,
    oversized_native_ids: [range(0; 250000)],
    completion_marker: "diagnostics-complete"
  }
' "$large_diagnostics_state" >/dev/null ||
    die 'oversized diagnostics state differs from the complete source report'
[[ -f "$large_diagnostics_report" && ! -L "$large_diagnostics_report" ]] ||
    die 'oversized diagnostics source report is not a regular file'
[[ -d "$large_diagnostics_output" && ! -L "$large_diagnostics_output" ]] ||
    die 'oversized diagnostics output is not a directory'
printf 'build\t--basins\t%s\t--streamnet\t%s\t--out\t%s\t--report\t%s\t--processing-basin-id\t%s\t--fabric-version\t%s\n' \
    "$large_diagnostics_dir/downloads/1020000010-basins.gpkg" \
    "$large_diagnostics_dir/downloads/1020000010-streamnet.gpkg" \
    "$large_diagnostics_output" "$large_diagnostics_report" 1020000010 \
    NGA-TDX-Hydro-20230126 >"$test_tmp/expected-large-diagnostics-adapter.log"
printf 'validate\t%s\t--hfx-binary\t%s\n' \
    "$large_diagnostics_output" "$test_tmp/fake-hfx" \
    >>"$test_tmp/expected-large-diagnostics-adapter.log"
printf '%s\t--strict\t--sample-pct\t100\t--format\ttext\n' \
    "$large_diagnostics_output" >"$test_tmp/expected-large-diagnostics-hfx.log"
diff -u "$test_tmp/expected-large-diagnostics-adapter.log" "$HFX_TEST_ADAPTER_LOG"
diff -u "$test_tmp/expected-large-diagnostics-hfx.log" "$HFX_TEST_HFX_LOG"
if grep -F 'adapter validation failed' "$large_diagnostics_state" >/dev/null; then
    die 'oversized diagnostics compile was relabeled adapter validation failed'
fi
: >"$HFX_TEST_ADAPTER_LOG"
: >"$HFX_TEST_HFX_LOG"
pass 'oversized compile diagnostics cross verification and state persistence through files'

cp -R "$compile_dir/basin-outputs" "$test_tmp/compile-outputs-before"
cp -R "$compile_dir/reports" "$test_tmp/compile-reports-before"
: >"$HFX_TEST_ADAPTER_LOG"
: >"$HFX_TEST_HFX_LOG"
run_runner compile --campaign acquire --workspace-root "$compile_root" \
    --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout"
[[ $(grep -c '^validate' "$HFX_TEST_ADAPTER_LOG") == 62 ]] ||
    die 'compile resume did not validate all 62 basins'
[[ $(grep -c '^build' "$HFX_TEST_ADAPTER_LOG" || :) == 0 ]] ||
    die 'compile resume rebuilt a verified success'
[[ $(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ') == 62 ]] ||
    die 'compile resume did not directly validate all 62 datasets'
jq -e -s 'all(.stages.compile.attempts == 1)' \
    "$compile_dir"/state/basins/*/current.json >/dev/null || die 'compile resume changed attempts'
diff -ru "$test_tmp/compile-outputs-before" "$compile_dir/basin-outputs"
diff -ru "$test_tmp/compile-reports-before" "$compile_dir/reports"
: >"$HFX_TEST_ADAPTER_LOG"
expect_failure 'changed fabric version' compile --campaign acquire --workspace-root "$compile_root" \
    --fabric-version changed-version
assert_contains "$case_stderr" 'fabric version changed; use a new campaign ID'
[[ ! -s "$HFX_TEST_ADAPTER_LOG" ]] || die 'fabric-version mismatch invoked the adapter'
pass 'verified compile success resumes and fabric version is immutable'

compile_failure_root=$test_tmp/workspaces/compile-failure
mkdir "$compile_failure_root"
cp -R "$acquire_dir" "$compile_failure_root/tdx-hydro-acquire"
compile_failure_id=$(jq -r 'keys[0]' "$inventory")
last_inventory_id=$(jq -r 'keys | last' "$inventory")
: >"$HFX_TEST_ADAPTER_LOG"
: >"$HFX_TEST_HFX_LOG"
HFX_TEST_FAIL_BUILD_ID=$compile_failure_id \
    run_runner compile --campaign acquire --workspace-root "$compile_failure_root" \
        --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout"
compile_failure_dir=$compile_failure_root/tdx-hydro-acquire
compile_failure_state=$compile_failure_dir/state/basins/$compile_failure_id/current.json
jq -e '
  .stages.compile.status == "failed" and
  .stages.compile.attempts == 1 and
  .stages.compile.failure_reason == "adapter build failed" and
  .stages.compile.diagnostic_report == null
' "$compile_failure_state" >/dev/null || die 'isolated adapter build failure state differs'
[[ ! -e "$compile_failure_dir/basin-outputs/$compile_failure_id" ]] ||
    die 'failed adapter build left a final output'
[[ ! -e "$compile_failure_dir/reports/$compile_failure_id-build-report.json" ]] ||
    die 'failed adapter build left a final report'
jq -e -s --arg id "$compile_failure_id" '
  all(if .processing_basin_id == $id then true else
    .stages.compile.status == "succeeded" and .stages.compile.attempts == 1 end)
' "$compile_failure_dir"/state/basins/*/current.json >/dev/null || die 'build failure aborted a later basin'
[[ $(grep -c '^build' "$HFX_TEST_ADAPTER_LOG") == 62 ]] || die 'build failure did not attempt all basins'
[[ $(grep -c '^validate' "$HFX_TEST_ADAPTER_LOG") == 61 ]] || die 'build failure validation count differs'
grep -F "$last_inventory_id" "$HFX_TEST_ADAPTER_LOG" >/dev/null || die 'build failure did not reach the last basin'
: >"$HFX_TEST_ADAPTER_LOG"
: >"$HFX_TEST_HFX_LOG"
run_runner compile --campaign acquire --workspace-root "$compile_failure_root" \
    --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout"
jq -e '.stages.compile.status == "succeeded" and .stages.compile.attempts == 2' \
    "$compile_failure_state" >/dev/null || die 'failed build did not converge on retry'
[[ $(grep -c '^build' "$HFX_TEST_ADAPTER_LOG") == 1 ]] || die 'build retry rebuilt prior successes'
[[ $(grep -c '^validate' "$HFX_TEST_ADAPTER_LOG") == 62 ]] || die 'build retry did not validate every basin'
jq -e -s --arg id "$compile_failure_id" '
  all(if .processing_basin_id == $id then .stages.compile.attempts == 2
      else .stages.compile.attempts == 1 end)
' "$compile_failure_dir"/state/basins/*/current.json >/dev/null || die 'build retry changed prior attempts'
pass 'adapter build failure is isolated and retries only clean failed work'

compile_validation_root=$test_tmp/workspaces/compile-validation
mkdir "$compile_validation_root"
cp -R "$acquire_dir" "$compile_validation_root/tdx-hydro-acquire"
compile_validation_id=$(jq -r 'keys[0]' "$inventory")
: >"$HFX_TEST_ADAPTER_LOG"
: >"$HFX_TEST_HFX_LOG"
HFX_TEST_FAIL_VALIDATE_ID=$compile_validation_id \
    run_runner compile --campaign acquire --workspace-root "$compile_validation_root" \
        --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout"
compile_validation_dir=$compile_validation_root/tdx-hydro-acquire
compile_validation_state=$compile_validation_dir/state/basins/$compile_validation_id/current.json
jq -e '
  .stages.compile.status == "failed" and .stages.compile.attempts == 1 and
  .stages.compile.failure_reason == "adapter validation failed" and
  .stages.compile.diagnostic_report == {
    path:("reports/" + .processing_basin_id + "-build-report.json"),
    diagnostics:{}
  }
' "$compile_validation_state" >/dev/null || die 'adapter validation failure state differs'
[[ -d "$compile_validation_dir/basin-outputs/$compile_validation_id" ]] ||
    die 'validation failure did not retain output'
[[ -f "$compile_validation_dir/reports/$compile_validation_id-build-report.json" ]] ||
    die 'validation failure did not retain report'
jq -e -s --arg id "$compile_validation_id" '
  all(if .processing_basin_id == $id then true else .stages.compile.status == "succeeded" end)
' "$compile_validation_dir"/state/basins/*/current.json >/dev/null || die 'validation failure aborted a later basin'
: >"$HFX_TEST_ADAPTER_LOG"
: >"$HFX_TEST_HFX_LOG"
run_runner compile --campaign acquire --workspace-root "$compile_validation_root" \
    --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout"
jq -e '
  .stages.compile.status == "failed" and .stages.compile.attempts == 1 and
  .stages.compile.failure_reason == "compile artifact path already exists; retained for inspection" and
  .stages.compile.diagnostic_report == {
    path:("reports/" + .processing_basin_id + "-build-report.json"),
    diagnostics:{}
  }
' "$compile_validation_state" >/dev/null || die 'retained validation failure state differs on rerun'
if grep -F "$compile_validation_id" "$HFX_TEST_ADAPTER_LOG" >/dev/null; then
    die 'retained validation failure invoked the adapter on rerun'
fi
[[ $(grep -c '^build' "$HFX_TEST_ADAPTER_LOG" || :) == 0 ]] ||
    die 'validation-failure rerun rebuilt a basin'
[[ $(grep -c '^validate' "$HFX_TEST_ADAPTER_LOG") == 61 ]] ||
    die 'validation-failure rerun did not validate prior successes'
rm -r "$compile_validation_dir/basin-outputs/$compile_validation_id"
rm "$compile_validation_dir/reports/$compile_validation_id-build-report.json"
: >"$HFX_TEST_ADAPTER_LOG"
HFX_TEST_REQUIRE_CLEARED_DIAGNOSTIC=1 \
    run_runner compile --campaign acquire --workspace-root "$compile_validation_root" \
        --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout"
jq -e '
  .stages.compile.status == "succeeded" and .stages.compile.attempts == 2 and
  .stages.compile.diagnostic_report == {
    path:("reports/" + .processing_basin_id + "-build-report.json"),
    diagnostics:{}
  }
' "$compile_validation_state" >/dev/null || die 'validation failure retry did not persist fresh diagnostics'
pass 'adapter validation failure retains artifacts and refuses overwrite'

reclaim_root=$test_tmp/workspaces/reclaim-base
mkdir "$reclaim_root"
run_runner init --campaign reclaim-base --workspace-root "$reclaim_root" \
    --basin 1020000010 \
    --retention-policy reclaim-inputs-after-terminal \
    --available-memory-bytes 30000000000 --available-disk-bytes 491737129060 \
    --peak-in-flight-download-bytes 44296724480 \
    --retained-basin-output-bytes 206220202290 \
    --assembly-memory-ceiling-bytes 30000000000 \
    --assembly-scratch-ceiling-bytes 206220202290 \
    --assembled-artifact-bytes 206220202290 \
    --active-compile-scratch-bytes 30000000000 \
    --filesystem-overhead-bytes 5000000000 >"$case_stdout"
reclaim_dir=$reclaim_root/tdx-hydro-reclaim-base
jq -e '
  .schema_version == 4 and
  .retention == {
    inputs_reclaimed:false,
    policy:"reclaim-inputs-after-terminal"
  }
' "$reclaim_dir/state/basins/1020000010/current.json" >/dev/null ||
    die 'reclaim init did not emit the schema-4 retention contract'
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
run_runner acquire --campaign reclaim-base --workspace-root "$reclaim_root" \
    --max-parallel 1 >"$case_stdout"
for reclaim_product in basins streamnet; do
    [[ -f "$reclaim_dir/downloads/1020000010-$reclaim_product.gpkg" ]] ||
        die "nonterminal reclaim acquisition removed $reclaim_product"
done
jq -e '
  .stages.compile.status == "pending" and
  .stages.compile.attempts == 0 and
  .retention.inputs_reclaimed == false
' "$reclaim_dir/state/basins/1020000010/current.json" >/dev/null ||
    die 'nonterminal reclaim acquisition state differs'
pass 'reclaim initialization uses schema 4 and acquisition remains nonterminal'

reclaim_ordinary_root=$test_tmp/workspaces/reclaim-ordinary
mkdir "$reclaim_ordinary_root"
cp -R "$reclaim_dir" "$reclaim_ordinary_root/tdx-hydro-reclaim-base"
reclaim_ordinary_dir=$reclaim_ordinary_root/tdx-hydro-reclaim-base
cp "$reclaim_ordinary_dir/reports/1020000010-basins-acquisition.json" \
    "$test_tmp/reclaim-ordinary-basins-report"
cp "$reclaim_ordinary_dir/reports/1020000010-streamnet-acquisition.json" \
    "$test_tmp/reclaim-ordinary-streamnet-report"
run_runner compile --campaign reclaim-base --workspace-root "$reclaim_ordinary_root" \
    --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout"
for reclaim_product in basins streamnet; do
    for reclaim_suffix in .gpkg .gpkg.partial .gpkg.partial.json; do
        [[ ! -e "$reclaim_ordinary_dir/downloads/1020000010-$reclaim_product$reclaim_suffix" &&
           ! -L "$reclaim_ordinary_dir/downloads/1020000010-$reclaim_product$reclaim_suffix" ]] ||
            die "ordinary landed reclaim retained $reclaim_product$reclaim_suffix"
    done
done
jq -e '
  .schema_version == 4 and
  .stages.compile.status == "succeeded" and
  .stages.compile.attempts == 1 and
  .retention.inputs_reclaimed == true
' "$reclaim_ordinary_dir/state/basins/1020000010/current.json" >/dev/null ||
    die 'ordinary landed reclaim lacks its durable schema-4 marker'
[[ -f "$reclaim_ordinary_dir/basin-outputs/1020000010/catchments.parquet" &&
   -f "$reclaim_ordinary_dir/basin-outputs/1020000010/graph.parquet" &&
   -f "$reclaim_ordinary_dir/basin-outputs/1020000010/aux/snap_stems.parquet" &&
   -f "$reclaim_ordinary_dir/reports/1020000010-build-report.json" ]] ||
    die 'ordinary landed reclaim removed assembly or diagnostic evidence'
diff -u "$test_tmp/reclaim-ordinary-basins-report" \
    "$reclaim_ordinary_dir/reports/1020000010-basins-acquisition.json"
diff -u "$test_tmp/reclaim-ordinary-streamnet-report" \
    "$reclaim_ordinary_dir/reports/1020000010-streamnet-acquisition.json"
pass 'ordinary landed compile reclaims only the exact source pair'

for reclaim_outcome in success build-failure validation-failure; do
    for reclaim_boundary in \
        compile-attempt-complete terminal-state basins-input-reclaimed \
        streamnet-input-reclaimed reclaimed-state; do
        reclaim_case_root=$test_tmp/workspaces/reclaim-$reclaim_outcome-$reclaim_boundary
        mkdir "$reclaim_case_root"
        cp -R "$reclaim_dir" "$reclaim_case_root/tdx-hydro-reclaim-base"
        reclaim_case_dir=$reclaim_case_root/tdx-hydro-reclaim-base
        : >"$HFX_TEST_ADAPTER_LOG"
        : >"$HFX_TEST_HFX_LOG"
        reclaim_status=0
        case $reclaim_outcome in
            success)
                HFX_TEST_INTERRUPT_AFTER="1020000010:$reclaim_boundary" \
                    run_runner compile --campaign reclaim-base \
                        --workspace-root "$reclaim_case_root" \
                        --fabric-version NGA-TDX-Hydro-20230126 \
                        >"$case_stdout" 2>"$case_stderr" || reclaim_status=$?
                ;;
            build-failure)
                HFX_TEST_FAIL_BUILD_ID=1020000010 \
                HFX_TEST_INTERRUPT_AFTER="1020000010:$reclaim_boundary" \
                    run_runner compile --campaign reclaim-base \
                        --workspace-root "$reclaim_case_root" \
                        --fabric-version NGA-TDX-Hydro-20230126 \
                        >"$case_stdout" 2>"$case_stderr" || reclaim_status=$?
                ;;
            validation-failure)
                HFX_TEST_FAIL_VALIDATE_ID=1020000010 \
                HFX_TEST_INTERRUPT_AFTER="1020000010:$reclaim_boundary" \
                    run_runner compile --campaign reclaim-base \
                        --workspace-root "$reclaim_case_root" \
                        --fabric-version NGA-TDX-Hydro-20230126 \
                        >"$case_stdout" 2>"$case_stderr" || reclaim_status=$?
                ;;
        esac
        [[ "$reclaim_status" -ne 0 ]] ||
            die "$reclaim_outcome x $reclaim_boundary interruption unexpectedly succeeded"
        [[ ! -d "$reclaim_case_dir/state/locks/campaign.lock" ]] ||
            die "$reclaim_outcome x $reclaim_boundary retained the live campaign lock"

        if [[ -d "$reclaim_case_dir/basin-outputs/1020000010" ]]; then
            cp -R "$reclaim_case_dir/basin-outputs/1020000010" \
                "$test_tmp/reclaim-output-$reclaim_outcome-$reclaim_boundary"
            cp "$reclaim_case_dir/reports/1020000010-build-report.json" \
                "$test_tmp/reclaim-report-$reclaim_outcome-$reclaim_boundary"
        fi
        cp "$reclaim_case_dir/reports/1020000010-basins-acquisition.json" \
            "$test_tmp/reclaim-basins-report-$reclaim_outcome-$reclaim_boundary"
        cp "$reclaim_case_dir/reports/1020000010-streamnet-acquisition.json" \
            "$test_tmp/reclaim-streamnet-report-$reclaim_outcome-$reclaim_boundary"

        if [[ "$reclaim_boundary" == compile-attempt-complete ]]; then
            case $reclaim_outcome in
                success)
                    run_runner compile --campaign reclaim-base \
                        --workspace-root "$reclaim_case_root" \
                        --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout"
                    ;;
                build-failure)
                    HFX_TEST_FAIL_BUILD_ID=1020000010 \
                        run_runner compile --campaign reclaim-base \
                            --workspace-root "$reclaim_case_root" \
                            --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout"
                    ;;
                validation-failure)
                    HFX_TEST_FAIL_VALIDATE_ID=1020000010 \
                        run_runner compile --campaign reclaim-base \
                            --workspace-root "$reclaim_case_root" \
                            --fabric-version NGA-TDX-Hydro-20230126 >"$case_stdout"
                    ;;
            esac
        else
            run_runner recover --campaign reclaim-base \
                --workspace-root "$reclaim_case_root" >"$case_stdout"
        fi

        reclaim_state=$reclaim_case_dir/state/basins/1020000010/current.json
        if [[ "$reclaim_boundary" == compile-attempt-complete &&
              "$reclaim_outcome" != build-failure ]]; then
            jq -e '
              .stages.compile.status == "failed" and
              .stages.compile.attempts == 1 and
              .stages.compile.failure_reason ==
                "compile artifact path already exists; retained for inspection" and
              .retention.inputs_reclaimed == false
            ' "$reclaim_state" >/dev/null ||
                die "$reclaim_outcome x $reclaim_boundary did not remain an inspection hold"
            for reclaim_product in basins streamnet; do
                [[ -f "$reclaim_case_dir/downloads/1020000010-$reclaim_product.gpkg" ]] ||
                    die "$reclaim_outcome x $reclaim_boundary removed $reclaim_product final"
                [[ ! -e "$reclaim_case_dir/downloads/1020000010-$reclaim_product.gpkg.partial" &&
                   ! -e "$reclaim_case_dir/downloads/1020000010-$reclaim_product.gpkg.partial.json" ]] ||
                    die "$reclaim_outcome x $reclaim_boundary recreated partial provenance"
            done
        else
            jq -e --arg outcome "$reclaim_outcome" '
              .retention.inputs_reclaimed == true and
              .stages.compile.attempts ==
                (if $outcome == "build-failure" and
                    .stages.compile.failure_reason == "adapter build failed"
                 then (if .stages.compile.attempts == 2 then 2 else 1 end)
                 else 1 end) and
              (if $outcome == "success"
               then .stages.compile.status == "succeeded"
               elif $outcome == "build-failure"
               then .stages.compile.failure_reason == "adapter build failed"
               else .stages.compile.failure_reason == "adapter validation failed" end)
            ' "$reclaim_state" >/dev/null ||
                die "$reclaim_outcome x $reclaim_boundary did not converge terminal reclaim"
            for reclaim_product in basins streamnet; do
                for reclaim_suffix in .gpkg .gpkg.partial .gpkg.partial.json; do
                    [[ ! -e "$reclaim_case_dir/downloads/1020000010-$reclaim_product$reclaim_suffix" &&
                       ! -L "$reclaim_case_dir/downloads/1020000010-$reclaim_product$reclaim_suffix" ]] ||
                        die "$reclaim_outcome x $reclaim_boundary retained $reclaim_product$reclaim_suffix"
                done
            done
        fi
        diff -u "$test_tmp/reclaim-basins-report-$reclaim_outcome-$reclaim_boundary" \
            "$reclaim_case_dir/reports/1020000010-basins-acquisition.json"
        diff -u "$test_tmp/reclaim-streamnet-report-$reclaim_outcome-$reclaim_boundary" \
            "$reclaim_case_dir/reports/1020000010-streamnet-acquisition.json"
        if [[ -d "$test_tmp/reclaim-output-$reclaim_outcome-$reclaim_boundary" ]]; then
            diff -ru "$test_tmp/reclaim-output-$reclaim_outcome-$reclaim_boundary" \
                "$reclaim_case_dir/basin-outputs/1020000010"
            diff -u "$test_tmp/reclaim-report-$reclaim_outcome-$reclaim_boundary" \
                "$reclaim_case_dir/reports/1020000010-build-report.json"
        fi
        cp "$reclaim_state" "$test_tmp/reclaim-state-replay"
        : >"$HFX_TEST_ADAPTER_LOG"
        run_runner recover --campaign reclaim-base \
            --workspace-root "$reclaim_case_root" >"$case_stdout"
        diff -u "$test_tmp/reclaim-state-replay" "$reclaim_state"
        [[ ! -s "$HFX_TEST_ADAPTER_LOG" ]] ||
            die "$reclaim_outcome x $reclaim_boundary recover replay invoked adapter"
    done
    pass "reclaim interruption matrix converges $reclaim_outcome outcomes"
done

migration_root=$test_tmp/workspaces/migration
mkdir "$migration_root"
cp -R "$acquire_dir" "$migration_root/tdx-hydro-acquire"
migration_id=$(jq -r 'keys[0]' "$inventory")
migration_state=$migration_root/tdx-hydro-acquire/state/basins/$migration_id/current.json
jq -n --arg id "$migration_id" '{
  schema_version: 1,
  processing_basin_id: $id,
  stages: {
    acquire_basins: {
      status: "succeeded",
      attempts: 1,
      failure_reason: null
    },
    acquire_streamnet: {
      status: "succeeded",
      attempts: 1,
      failure_reason: null
    },
    compile: {
      status: "pending",
      attempts: 0,
      failure_reason: null
    }
  }
}' >"$migration_state.tmp"
mv "$migration_state.tmp" "$migration_state"
rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
run_runner acquire --campaign acquire --workspace-root "$migration_root" --max-parallel 3 >"$case_stdout"
jq -e '
  .schema_version == 3 and
  .stages.acquire_basins.status == "succeeded" and
  .stages.acquire_streamnet.status == "succeeded" and
  .stages.acquire_basins.attempts == 1 and
  .stages.acquire_streamnet.attempts == 1 and
  (.stages.acquire_basins.evidence != null) and
  (.stages.acquire_streamnet.evidence != null) and
  .stages.compile == {status:"pending",attempts:0,failure_reason:null,diagnostic_report:null}
' "$migration_state" >/dev/null || die 'v1 acquisition state did not migrate through acquire'
[[ ! -e "$test_tmp/transfer-state/events" ]] || die 'v1 migration fetched verified final files'
pass 'v1 basin state migrates through acquire without transfer'

rm -r "$test_tmp/transfer-state"
mkdir "$test_tmp/transfer-state"
: >"$test_tmp/transfer-state/events"
post_drain_root=$test_tmp/workspaces/post-drain
mkdir "$post_drain_root"
cp -R "$acquire_dir" "$post_drain_root/tdx-hydro-acquire"
post_drain_status=0
HFX_TDX_TEST_SIGNAL_AFTER_EMPTY_DRAIN=1 \
    run_runner acquire --campaign acquire --workspace-root "$post_drain_root" --max-parallel 3 \
    >"$case_stdout" 2>"$case_stderr" || post_drain_status=$?
[[ "$post_drain_status" -eq 130 ]] ||
    die "post-drain empty-worker TERM exited $post_drain_status instead of 130"
[[ ! -d "$post_drain_root/tdx-hydro-acquire/state/locks/campaign.lock" ]] ||
    die 'post-drain empty-worker TERM left the campaign lock behind'
[[ ! -s "$test_tmp/transfer-state/events" ]] ||
    die 'post-drain empty-worker TERM triggered a transfer'
for pid_file in "$test_tmp"/transfer-state/worker.* "$test_tmp"/transfer-state/curl.*; do
    [[ -f "$pid_file" ]] || continue
    interrupted_pid=$(<"$pid_file")
    ! kill -0 "$interrupted_pid" 2>/dev/null ||
        die "post-drain acquisition worker survived: $interrupted_pid"
done
pass 'TERM after the final worker drain exits 130 and releases the lock'

for poison in hcloud curl aws ssh; do
    [[ ! -e "$test_tmp/invocations/$poison.log" ]] || die "poison command was invoked: $poison"
done
if grep -Ev '^(build|validate)[[:space:]]' "$HFX_TEST_ADAPTER_LOG" >"$case_stdout"; then
    die 'adapter log contains an unknown command'
fi
if grep -E 'assemble|assembly' "$HFX_TEST_ADAPTER_LOG" "$HFX_TEST_HFX_LOG" >/dev/null; then
    die 'local compile tools invoked assembly'
fi
git -C "$repo_root" status --porcelain=v1 | sed '/^?? pr-body\.md$/d' >"$test_tmp/repository-status-after"
diff -u "$test_tmp/repository-status-before" "$test_tmp/repository-status-after"
pass 'no cloud, network, SSH, or publication command ran'

assembly_hfx_start=$(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ')
assembly_success_root=$(new_assembly_workspace assembly-success)
assembly_success_dir=$assembly_success_root/tdx-hydro-equal
mark_compile_succeeded "$assembly_success_dir" 7020000010
mark_compile_succeeded "$assembly_success_dir" 1020000010
write_expected_assembly_argv "$assembly_success_dir"
run_runner assemble --campaign equal --workspace-root "$assembly_success_root" >"$case_stdout" 2>"$case_stderr"
jq -e '
  .schema_version == 1 and .status == "succeeded" and .attempts == 1 and
  .failure_reason == null and .input_basin_ids == ["1020000010","7020000010"] and
  .output_path == "assembly/dataset" and .report_path == "reports/assembly.json"
' "$assembly_success_dir/state/assembly.json" >/dev/null
jq -e --arg campaign equal '
  . == {
    schema_version:1,
    campaign:$campaign,
    input_basin_ids:["1020000010","7020000010"],
    input_dataset_paths:["basin-outputs/1020000010","basin-outputs/7020000010"],
    output_path:"assembly/dataset"
  }
' "$assembly_success_dir/reports/assembly.json" >/dev/null
[[ -d "$assembly_success_dir/assembly/dataset" ]] ||
    die 'assembly success did not publish the dataset'
[[ ! -e "$assembly_success_dir/assembly/dataset/assembly.json" ]] ||
    die 'runner-owned assembly report was placed beneath the dataset'
pass 'assembly succeeds with exact sorted repeated-input argv and external report'

assembly_extension_root=$(new_assembly_workspace assembly-extension-success)
assembly_extension_dir=$assembly_extension_root/tdx-hydro-equal
mark_compile_succeeded "$assembly_extension_dir" 7020000010
mark_compile_succeeded "$assembly_extension_dir" 1020000010
write_expected_assembly_argv "$assembly_extension_dir" extension
set -- $(extension_options)
run_runner assemble --campaign equal --workspace-root "$assembly_extension_root" "$@" >"$case_stdout"
jq -e --arg root "$partial_fabric_root" --arg roster "$partial_fabric_roster" '. == {
  schema_version:2,status:"succeeded",attempts:1,failure_reason:null,
  fabric_root:$root,fabric_roster_path:$roster,fabric_basin_ids:["1020000010"],
  excluded_control_basin_id:"1020000010",included_basin_ids:["7020000010"],
  included_dataset_paths:["basin-outputs/7020000010"],output_path:"assembly/dataset",
  report_path:"reports/assembly.json"
}' "$assembly_extension_dir/state/assembly.json" >/dev/null || die 'extension state differs'
jq -e --arg root "$partial_fabric_root" --arg roster "$partial_fabric_roster" '. == {
  schema_version:2,campaign:"equal",fabric_root:$root,fabric_roster_path:$roster,
  fabric_basin_ids:["1020000010"],excluded_control_basin_id:"1020000010",
  included_basin_ids:["7020000010"],included_dataset_paths:["basin-outputs/7020000010"],
  output_path:"assembly/dataset"
}' "$assembly_extension_dir/reports/assembly.json" >/dev/null || die 'extension report differs'
[[ ! -e "$assembly_extension_dir/assembly/dataset/1020000010" ]] ||
    die 'extension assembly included the control output'
pass 'partial-fabric extension succeeds with complete schema-2 provenance'

for rejected_verb in assemble assembly; do
    literal_status=0
    "$test_tmp/fake-hfx" "$rejected_verb" --strict --sample-pct 100 --format text \
        >"$case_stdout" 2>"$case_stderr" || literal_status=$?
    [[ "$literal_status" -eq 74 ]] ||
        die "fake-hfx literal $rejected_verb verb exited $literal_status instead of 74"
done

assembly_empty_root=$(new_assembly_workspace assembly-empty)
assembly_empty_dir=$assembly_empty_root/tdx-hydro-equal
cp "$assembly_empty_dir/state/assembly.json" "$test_tmp/assembly-empty-state-before"
empty_adapter_lines=$(wc -l <"$HFX_TEST_ADAPTER_LOG" | tr -d ' ')
empty_hfx_lines=$(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ')
expect_failure 'assembly without compiled basins' assemble --campaign equal --workspace-root "$assembly_empty_root"
assert_contains "$case_stderr" 'hfx: error: assembly requires at least one basin with compile status succeeded'
diff -u "$test_tmp/assembly-empty-state-before" "$assembly_empty_dir/state/assembly.json"
[[ $(wc -l <"$HFX_TEST_ADAPTER_LOG" | tr -d ' ') -eq "$empty_adapter_lines" ]] ||
    die 'empty assembly invoked the adapter'
[[ $(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ') -eq "$empty_hfx_lines" ]] ||
    die 'empty assembly invoked HFX'
[[ ! -e "$assembly_empty_dir/assembly/dataset" &&
   ! -e "$assembly_empty_dir/reports/assembly.json" ]] ||
    die 'empty assembly created an artifact'
[[ $(find "$assembly_empty_dir/assembly" -mindepth 1 -maxdepth 1 ! -name scratch | wc -l | tr -d ' ') -eq 0 ]] ||
    die 'empty assembly created a staging entry'
pass 'assembly refuses an empty compiled-basin selection without mutation'

assembly_preflight_root=$(new_assembly_workspace assembly-extension-preflight)
assembly_preflight_dir=$assembly_preflight_root/tdx-hydro-equal
mark_compile_succeeded "$assembly_preflight_dir" 1020000010
mark_compile_succeeded "$assembly_preflight_dir" 7020000010
cp "$assembly_preflight_dir/state/assembly.json" "$test_tmp/extension-preflight-state-before"
preflight_adapter_lines=$(wc -l <"$HFX_TEST_ADAPTER_LOG" | tr -d ' ')
preflight_hfx_lines=$(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ')
printf '%s\n' fixture >"$test_tmp/fixtures/partial-fabric-file"
mkdir "$test_tmp/fixtures/empty-roster-directory"
: >"$test_tmp/fixtures/empty-roster.json"
ln -s "$partial_fabric_root" "$test_tmp/fixtures/partial-fabric-link"
ln -s "$partial_fabric_roster" "$test_tmp/fixtures/partial-roster-link"
for malformed_name in not-json empty unsorted duplicate unknown; do
    case $malformed_name in
        not-json) malformed_bytes=not-json ;;
        empty) malformed_bytes='[]' ;;
        unsorted) malformed_bytes='["7020000010","1020000010"]' ;;
        duplicate) malformed_bytes='["1020000010","1020000010"]' ;;
        unknown) malformed_bytes='["9999999999"]' ;;
    esac
    printf '%s\n' "$malformed_bytes" >"$test_tmp/fixtures/roster-$malformed_name.json"
done
expect_failure 'relative partial fabric' assemble --campaign equal --workspace-root "$assembly_preflight_root" \
    --partial-fabric relative --partial-fabric-roster "$partial_fabric_roster" --exclude-control-basin 1020000010
assert_contains "$case_stderr" 'option --partial-fabric must be an absolute path'
for unsafe_root in "$test_tmp/fixtures/missing-fabric" "$test_tmp/fixtures/partial-fabric-file" \
    "$test_tmp/fixtures/partial-fabric-link"; do
    expect_failure 'unsafe partial fabric' assemble --campaign equal --workspace-root "$assembly_preflight_root" \
        --partial-fabric "$unsafe_root" --partial-fabric-roster "$partial_fabric_roster" --exclude-control-basin 1020000010
    assert_contains "$case_stderr" "partial fabric root is not a safe directory: $unsafe_root"
done
expect_failure 'relative partial roster' assemble --campaign equal --workspace-root "$assembly_preflight_root" \
    --partial-fabric "$partial_fabric_root" --partial-fabric-roster relative --exclude-control-basin 1020000010
assert_contains "$case_stderr" 'option --partial-fabric-roster must be an absolute path'
for unsafe_roster in "$test_tmp/fixtures/missing-roster" "$test_tmp/fixtures/empty-roster.json" \
    "$test_tmp/fixtures/empty-roster-directory" "$test_tmp/fixtures/partial-roster-link"; do
    expect_failure 'unsafe partial roster' assemble --campaign equal --workspace-root "$assembly_preflight_root" \
        --partial-fabric "$partial_fabric_root" --partial-fabric-roster "$unsafe_roster" --exclude-control-basin 1020000010
    assert_contains "$case_stderr" "partial fabric roster is not a safe nonempty regular file: $unsafe_roster"
done
for malformed_name in not-json empty unsorted duplicate unknown; do
    malformed_roster=$(cd -P "$test_tmp/fixtures" && pwd -P)/roster-$malformed_name.json
    expect_failure "malformed roster $malformed_name" assemble --campaign equal --workspace-root "$assembly_preflight_root" \
        --partial-fabric "$partial_fabric_root" --partial-fabric-roster "$malformed_roster" --exclude-control-basin 1020000010
    assert_contains "$case_stderr" "partial fabric roster is malformed: $malformed_roster"
done
cmp "$test_tmp/extension-preflight-state-before" "$assembly_preflight_dir/state/assembly.json"
[[ ! -e "$assembly_preflight_dir/reports/assembly.json" && ! -e "$assembly_preflight_dir/assembly/dataset" ]] ||
    die 'extension preflight refusal created an artifact'
[[ $(wc -l <"$HFX_TEST_ADAPTER_LOG" | tr -d ' ') -eq "$preflight_adapter_lines" &&
   $(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ') -eq "$preflight_hfx_lines" ]] ||
    die 'extension preflight refusal invoked adapter or HFX'
pass 'extension path and roster preflight refusals are mutation-free'

expect_failure 'control absent inventory' assemble --campaign equal --workspace-root "$assembly_preflight_root" \
    --partial-fabric "$partial_fabric_root" --partial-fabric-roster "$partial_fabric_roster" --exclude-control-basin 9999999999
assert_contains "$case_stderr" 'excluded control basin is not in the authoritative inventory: 9999999999'
assembly_selection_root=$test_tmp/workspaces/assembly-control-selection
mkdir "$assembly_selection_root"
set -- $(subset_init_args control-selection "$assembly_selection_root")
run_runner "$@" >"$case_stdout"
printf '%s\n' '["1020011530"]' >"$test_tmp/fixtures/unselected-control-roster.json"
expect_failure 'control absent frozen selection' assemble --campaign control-selection \
    --workspace-root "$assembly_selection_root" --partial-fabric "$partial_fabric_root" \
    --partial-fabric-roster "$test_tmp/fixtures/unselected-control-roster.json" \
    --exclude-control-basin 1020011530
assert_contains "$case_stderr" 'excluded control basin is not in the frozen campaign selection: 1020011530'
printf '%s\n' '["7020000010"]' >"$test_tmp/fixtures/control-absent-roster.json"
expect_failure 'control absent roster' assemble --campaign equal --workspace-root "$assembly_preflight_root" \
    --partial-fabric "$partial_fabric_root" --partial-fabric-roster "$test_tmp/fixtures/control-absent-roster.json" \
    --exclude-control-basin 1020000010
assert_contains "$case_stderr" 'excluded control basin is not in the partial fabric roster: 1020000010'
assembly_pending_root=$(new_assembly_workspace assembly-control-pending)
expect_failure 'control compile pending' assemble --campaign equal --workspace-root "$assembly_pending_root" \
    --partial-fabric "$partial_fabric_root" --partial-fabric-roster "$partial_fabric_roster" --exclude-control-basin 1020000010
assert_contains "$case_stderr" 'excluded control basin compile status is not succeeded: 1020000010'

assembly_no_new_root=$(new_assembly_workspace assembly-no-new)
assembly_no_new_dir=$assembly_no_new_root/tdx-hydro-equal
mark_compile_succeeded "$assembly_no_new_dir" 1020000010
cp "$assembly_no_new_dir/state/assembly.json" "$test_tmp/assembly-no-new-before"
expect_failure 'extension has no new basin' assemble --campaign equal --workspace-root "$assembly_no_new_root" \
    --partial-fabric "$partial_fabric_root" --partial-fabric-roster "$partial_fabric_roster" --exclude-control-basin 1020000010
assert_contains "$case_stderr" 'assembly requires at least one successful non-excluded basin absent from the partial fabric roster'
cmp "$test_tmp/assembly-no-new-before" "$assembly_no_new_dir/state/assembly.json"

printf '%s\n' '["1020000010","7020000010"]' >"$test_tmp/fixtures/control-and-new-roster.json"
control_and_new_roster=$(cd -P "$test_tmp/fixtures" && pwd -P)/control-and-new-roster.json
expect_failure 'compiled roster resident basin' assemble --campaign equal --workspace-root "$assembly_preflight_root" \
    --partial-fabric "$partial_fabric_root" --partial-fabric-roster "$control_and_new_roster" \
    --exclude-control-basin 1020000010
assert_contains "$case_stderr" 'compiled basin is already present in partial fabric roster and is not the excluded control: 7020000010'
pass 'extension control and new-basin selection refusals are deterministic'

assembly_adopt_root=$(new_assembly_workspace assembly-adopt)
assembly_adopt_dir=$assembly_adopt_root/tdx-hydro-equal
mark_compile_succeeded "$assembly_adopt_dir" 7020000010
mark_compile_succeeded "$assembly_adopt_dir" 1020000010
create_assembly_dataset_fixture "$assembly_adopt_dir/assembly/dataset"
adopt_assemble_before=$(grep -c '^assemble' "$HFX_TEST_ADAPTER_LOG" || :)
adopt_validate_before=$(grep -c '^validate' "$HFX_TEST_ADAPTER_LOG" || :)
write_assembly_state_fixture "$assembly_adopt_dir" running 1 '' extension
run_runner recover --campaign equal --workspace-root "$assembly_adopt_root" >"$case_stdout"
set -- $(extension_options)
run_runner assemble --campaign equal --workspace-root "$assembly_adopt_root" "$@" >"$case_stdout"
[[ $(grep -c '^assemble' "$HFX_TEST_ADAPTER_LOG" || :) -eq "$adopt_assemble_before" ]] ||
    die 'attributable adoption invoked assemble'
[[ $(grep -c '^validate' "$HFX_TEST_ADAPTER_LOG" || :) -eq $((adopt_validate_before + 1)) ]] ||
    die 'attributable adoption did not validate exactly once'
jq -e '.status == "succeeded" and .attempts == 1' "$assembly_adopt_dir/state/assembly.json" >/dev/null
[[ -f "$assembly_adopt_dir/reports/assembly.json" ]] ||
    die 'attributable adoption did not regenerate the report'
pass 'recover adopts a verified destination with attributable interrupted provenance'

assembly_refuse_root=$(new_assembly_workspace assembly-refuse)
assembly_refuse_dir=$assembly_refuse_root/tdx-hydro-equal
mark_compile_succeeded "$assembly_refuse_dir" 7020000010
mark_compile_succeeded "$assembly_refuse_dir" 1020000010
mkdir "$assembly_refuse_dir/assembly/dataset"
printf '%s\n' preserve >"$assembly_refuse_dir/assembly/dataset/canary"
refuse_adapter_lines=$(wc -l <"$HFX_TEST_ADAPTER_LOG" | tr -d ' ')
refuse_hfx_lines=$(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ')
expect_failure 'unverified assembly destination' assemble --campaign equal --workspace-root "$assembly_refuse_root"
assert_contains "$case_stderr" 'assembly dataset exists without attributable interrupted or succeeded state; retained for inspection'
[[ $(<"$assembly_refuse_dir/assembly/dataset/canary") == preserve ]] ||
    die 'unverified destination was modified'
[[ $(wc -l <"$HFX_TEST_ADAPTER_LOG" | tr -d ' ') -eq "$refuse_adapter_lines" &&
   $(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ') -eq "$refuse_hfx_lines" ]] ||
    die 'unverified destination refusal invoked validation'
[[ ! -e "$assembly_refuse_dir/reports/assembly.json" ]] ||
    die 'unverified destination refusal created a report'
jq -e '.attempts == 0' "$assembly_refuse_dir/state/assembly.json" >/dev/null
pass 'assembly preserves and refuses an unverified existing destination'

assembly_retry_root=$(new_assembly_workspace assembly-retry)
assembly_retry_dir=$assembly_retry_root/tdx-hydro-equal
mark_compile_succeeded "$assembly_retry_dir" 7020000010
mark_compile_succeeded "$assembly_retry_dir" 1020000010
write_expected_assembly_argv "$assembly_retry_dir" extension
cp -R "$assembly_retry_dir/state/basins" "$test_tmp/retry-basins-before"
HFX_TEST_FAIL_ASSEMBLY=1 expect_failure 'injected assembly failure' \
    assemble --campaign equal --workspace-root "$assembly_retry_root" \
    --partial-fabric "$partial_fabric_root" --partial-fabric-roster "$partial_fabric_roster" \
    --exclude-control-basin 1020000010
jq -e '.status == "failed" and .attempts == 1 and .failure_reason == "adapter assembly failed"' \
    "$assembly_retry_dir/state/assembly.json" >/dev/null
[[ ! -e "$assembly_retry_dir/assembly/dataset" &&
   ! -e "$assembly_retry_dir/reports/assembly.json" ]] ||
    die 'adapter assembly failure left a dataset or report'
diff -ru "$test_tmp/retry-basins-before" "$assembly_retry_dir/state/basins"
retry_assemble_before=$(grep -c '^assemble' "$HFX_TEST_ADAPTER_LOG" || :)
set -- $(extension_options)
run_runner assemble --campaign equal --workspace-root "$assembly_retry_root" "$@" >"$case_stdout"
[[ $(grep -c '^assemble' "$HFX_TEST_ADAPTER_LOG" || :) -eq $((retry_assemble_before + 1)) ]] ||
    die 'assembly retry did not issue exactly one fresh assemble vector'
jq -e '.status == "succeeded" and .attempts == 2' "$assembly_retry_dir/state/assembly.json" >/dev/null
pass 'adapter assembly failure is isolated and a clean retry converges'

assembly_stage_root=$(new_assembly_workspace assembly-stage)
assembly_stage_dir=$assembly_stage_root/tdx-hydro-equal
mark_compile_succeeded "$assembly_stage_dir" 7020000010
mark_compile_succeeded "$assembly_stage_dir" 1020000010
write_assembly_state_fixture "$assembly_stage_dir" running 1 '' extension
mkdir "$assembly_stage_dir/assembly/.dataset.tmp-interrupted"
printf '%s\n' keep >"$assembly_stage_dir/assembly/unrelated"
run_runner recover --campaign equal --workspace-root "$assembly_stage_root" >"$case_stdout"
write_expected_assembly_argv "$assembly_stage_dir" extension
set -- $(extension_options)
run_runner assemble --campaign equal --workspace-root "$assembly_stage_root" "$@" >"$case_stdout"
[[ ! -e "$assembly_stage_dir/assembly/.dataset.tmp-interrupted" ]] ||
    die 'attributable interrupted staging was not removed'
[[ $(<"$assembly_stage_dir/assembly/unrelated") == keep ]] ||
    die 'unrelated assembly entry changed'
jq -e '.status == "succeeded" and .attempts == 2' "$assembly_stage_dir/state/assembly.json" >/dev/null
pass 'recover removes only attributable adapter staging before retry'

cp "$assembly_extension_dir/reports/assembly.json" "$test_tmp/assembly-success-report-before"
success_attempts_before=$(jq -r '.attempts' "$assembly_extension_dir/state/assembly.json")
success_assemble_before=$(grep -c '^assemble' "$HFX_TEST_ADAPTER_LOG" || :)
success_validate_before=$(grep -c '^validate' "$HFX_TEST_ADAPTER_LOG" || :)
set -- $(extension_options)
run_runner assemble --campaign equal --workspace-root "$assembly_extension_root" "$@" >"$case_stdout"
[[ $(grep -c '^assemble' "$HFX_TEST_ADAPTER_LOG" || :) -eq "$success_assemble_before" ]] ||
    die 'succeeded assembly resume invoked assemble'
[[ $(grep -c '^validate' "$HFX_TEST_ADAPTER_LOG" || :) -eq $((success_validate_before + 1)) ]] ||
    die 'succeeded assembly resume did not validate exactly once'
[[ $(jq -r '.attempts' "$assembly_extension_dir/state/assembly.json") -eq "$success_attempts_before" ]] ||
    die 'succeeded assembly resume incremented attempts'
diff -u "$test_tmp/assembly-success-report-before" "$assembly_extension_dir/reports/assembly.json"
run_runner status --campaign equal --workspace-root "$assembly_extension_root" >"$case_stdout"
assert_contains "$case_stdout" 'assemble_pending=0'
assert_contains "$case_stdout" 'assemble_running=0'
assert_contains "$case_stdout" 'assemble_succeeded=1'
assert_contains "$case_stdout" 'assemble_failed=0'
assembly_changed_root=$test_tmp/workspaces/assembly-changed
mkdir "$assembly_changed_root"
cp -R "$assembly_extension_dir" "$assembly_changed_root/tdx-hydro-equal"
assembly_changed_dir=$assembly_changed_root/tdx-hydro-equal
mark_compile_succeeded "$assembly_changed_dir" 9020000010
changed_adapter_lines=$(wc -l <"$HFX_TEST_ADAPTER_LOG" | tr -d ' ')
changed_hfx_lines=$(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ')
expect_failure 'changed succeeded assembly input set' \
    assemble --campaign equal --workspace-root "$assembly_changed_root" \
    --partial-fabric "$partial_fabric_root" --partial-fabric-roster "$partial_fabric_roster" \
    --exclude-control-basin 1020000010
assert_contains "$case_stderr" 'existing succeeded assembly failed resume verification; retained for inspection'
jq -e '
  .status == "failed" and .attempts == 1 and
  .failure_reason == "existing succeeded assembly failed resume verification; retained for inspection" and
  .fabric_basin_ids == ["1020000010"] and .included_basin_ids == ["7020000010"] and
  .included_dataset_paths == ["basin-outputs/7020000010"]
' "$assembly_changed_dir/state/assembly.json" >/dev/null
[[ -d "$assembly_changed_dir/assembly/dataset" &&
   -f "$assembly_changed_dir/reports/assembly.json" ]] ||
    die 'changed succeeded input set did not preserve assembly artifacts'
[[ $(wc -l <"$HFX_TEST_ADAPTER_LOG" | tr -d ' ') -eq "$changed_adapter_lines" &&
   $(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ') -eq "$changed_hfx_lines" ]] ||
    die 'changed succeeded input set invoked adapter or HFX'

mkdir -p "$test_tmp/fixtures/second-partial-fabric/aux"
drift_adapter_lines=$(wc -l <"$HFX_TEST_ADAPTER_LOG" | tr -d ' ')
drift_hfx_lines=$(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ')
second_partial_fabric_root=$(cd -P "$test_tmp/fixtures/second-partial-fabric" && pwd -P)
for partial_file in catchments.parquet graph.parquet manifest.json aux/snap_stems.parquet; do
    printf '%s\n' fixture >"$second_partial_fabric_root/$partial_file"
done
printf '%s\n' '["1020000010"]' >"$test_tmp/fixtures/second-partial-roster.json"
second_partial_fabric_roster=$(cd -P "$test_tmp/fixtures" && pwd -P)/second-partial-roster.json
for drift_kind in root roster; do
    drift_root=$test_tmp/workspaces/assembly-drift-$drift_kind
    mkdir "$drift_root"
    cp -R "$assembly_extension_dir" "$drift_root/tdx-hydro-equal"
    drift_dir=$drift_root/tdx-hydro-equal
    cp "$drift_dir/state/assembly.json" "$test_tmp/drift-$drift_kind-expected"
    jq '.status="failed" | .failure_reason="existing succeeded assembly failed resume verification; retained for inspection"' \
        "$test_tmp/drift-$drift_kind-expected" >"$test_tmp/drift-$drift_kind-failed"
    if [[ "$drift_kind" == root ]]; then
        requested_root=$second_partial_fabric_root
        requested_roster=$partial_fabric_roster
    else
        requested_root=$partial_fabric_root
        requested_roster=$second_partial_fabric_roster
    fi
    expect_failure "$drift_kind provenance drift" assemble --campaign equal --workspace-root "$drift_root" \
        --partial-fabric "$requested_root" --partial-fabric-roster "$requested_roster" \
        --exclude-control-basin 1020000010
    assert_contains "$case_stderr" 'existing succeeded assembly failed resume verification; retained for inspection'
    jq -S . "$drift_dir/state/assembly.json" >"$test_tmp/drift-$drift_kind-actual"
    jq -S . "$test_tmp/drift-$drift_kind-failed" >"$test_tmp/drift-$drift_kind-failed-sorted"
    diff -u "$test_tmp/drift-$drift_kind-failed-sorted" "$test_tmp/drift-$drift_kind-actual"
done

control_drift_root=$test_tmp/workspaces/assembly-drift-control
mkdir "$control_drift_root"
cp -R "$assembly_extension_dir" "$control_drift_root/tdx-hydro-equal"
control_drift_dir=$control_drift_root/tdx-hydro-equal
mark_compile_succeeded "$control_drift_dir" 9020000010
jq '.stages.compile={status:"failed",attempts:1,failure_reason:"adapter validation failed",diagnostic_report:null}' \
    "$control_drift_dir/state/basins/1020000010/current.json" >"$test_tmp/control-drift-basin"
mv "$test_tmp/control-drift-basin" "$control_drift_dir/state/basins/1020000010/current.json"
jq --arg roster "$control_and_new_roster" '
  .fabric_roster_path=$roster | .fabric_basin_ids=["1020000010","7020000010"] |
  .included_basin_ids=["9020000010"] | .included_dataset_paths=["basin-outputs/9020000010"]
' "$control_drift_dir/state/assembly.json" >"$test_tmp/control-drift-state"
mv "$test_tmp/control-drift-state" "$control_drift_dir/state/assembly.json"
jq --arg roster "$control_and_new_roster" '
  .fabric_roster_path=$roster | .fabric_basin_ids=["1020000010","7020000010"] |
  .included_basin_ids=["9020000010"] | .included_dataset_paths=["basin-outputs/9020000010"]
' "$control_drift_dir/reports/assembly.json" >"$test_tmp/control-drift-report"
mv "$test_tmp/control-drift-report" "$control_drift_dir/reports/assembly.json"
expect_failure 'control-only provenance drift' assemble --campaign equal --workspace-root "$control_drift_root" \
    --partial-fabric "$partial_fabric_root" \
    --partial-fabric-roster "$control_and_new_roster" \
    --exclude-control-basin 7020000010
assert_contains "$case_stderr" 'existing succeeded assembly failed resume verification; retained for inspection'
jq -e '
  .status == "failed" and .attempts == 1 and
  .failure_reason == "existing succeeded assembly failed resume verification; retained for inspection" and
  .fabric_basin_ids == ["1020000010","7020000010"] and
  .excluded_control_basin_id == "1020000010" and .included_basin_ids == ["9020000010"]
' "$control_drift_dir/state/assembly.json" >/dev/null || die 'control-only drift state differs'

roster_content_root=$test_tmp/workspaces/assembly-drift-roster-content
mkdir "$roster_content_root"
cp -R "$assembly_extension_dir" "$roster_content_root/tdx-hydro-equal"
cp "$partial_fabric_roster" "$test_tmp/original-partial-roster"
printf '%s\n' '["1020000010","1020011530"]' >"$partial_fabric_roster"
expect_failure 'roster content provenance drift' assemble --campaign equal --workspace-root "$roster_content_root" \
    --partial-fabric "$partial_fabric_root" --partial-fabric-roster "$partial_fabric_roster" \
    --exclude-control-basin 1020000010
assert_contains "$case_stderr" 'existing succeeded assembly failed resume verification; retained for inspection'
mv "$test_tmp/original-partial-roster" "$partial_fabric_roster"

malformed_tuple_root=$test_tmp/workspaces/assembly-malformed-tuple
mkdir "$malformed_tuple_root"
cp -R "$assembly_extension_dir" "$malformed_tuple_root/tdx-hydro-equal"
malformed_tuple_dir=$malformed_tuple_root/tdx-hydro-equal
jq '.included_dataset_paths=["basin-outputs/9020000010"]' \
    "$malformed_tuple_dir/state/assembly.json" >"$test_tmp/malformed-tuple"
mv "$test_tmp/malformed-tuple" "$malformed_tuple_dir/state/assembly.json"
cp "$malformed_tuple_dir/state/assembly.json" "$test_tmp/malformed-tuple-before"
expect_failure 'malformed persisted extension tuple' assemble --campaign equal --workspace-root "$malformed_tuple_root" \
    --partial-fabric "$partial_fabric_root" --partial-fabric-roster "$partial_fabric_roster" \
    --exclude-control-basin 1020000010
assert_contains "$case_stderr" "assembly state is malformed: $malformed_tuple_dir/state/assembly.json"
cmp "$test_tmp/malformed-tuple-before" "$malformed_tuple_dir/state/assembly.json"
[[ $(wc -l <"$HFX_TEST_ADAPTER_LOG" | tr -d ' ') -eq "$drift_adapter_lines" &&
   $(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ') -eq "$drift_hfx_lines" ]] ||
    die 'extension provenance drift invoked adapter or HFX'
pass 'succeeded assembly resumes by verification without report or attempt mutation'

assembly_invalid_root=$(new_assembly_workspace assembly-invalid)
assembly_invalid_dir=$assembly_invalid_root/tdx-hydro-equal
mark_compile_succeeded "$assembly_invalid_dir" 7020000010
mark_compile_succeeded "$assembly_invalid_dir" 1020000010
write_expected_assembly_argv "$assembly_invalid_dir" extension
cp -R "$assembly_invalid_dir/state/basins" "$test_tmp/invalid-basins-before"
HFX_TEST_FAIL_ASSEMBLY_VALIDATE=1 expect_failure 'post-assembly validation failure' \
    assemble --campaign equal --workspace-root "$assembly_invalid_root" \
    --partial-fabric "$partial_fabric_root" --partial-fabric-roster "$partial_fabric_roster" \
    --exclude-control-basin 1020000010
assert_contains "$case_stderr" 'assembled dataset validation failed; retained for inspection'
jq -e '.status == "failed" and .attempts == 1 and .failure_reason == "assembled dataset validation failed; retained for inspection"' \
    "$assembly_invalid_dir/state/assembly.json" >/dev/null
[[ -d "$assembly_invalid_dir/assembly/dataset" &&
   ! -e "$assembly_invalid_dir/reports/assembly.json" ]] ||
    die 'post-validation failure did not retain only the dataset'
diff -ru "$test_tmp/invalid-basins-before" "$assembly_invalid_dir/state/basins"
invalid_adapter_lines=$(wc -l <"$HFX_TEST_ADAPTER_LOG" | tr -d ' ')
invalid_hfx_lines=$(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ')
expect_failure 'post-validation artifact rerun' assemble --campaign equal --workspace-root "$assembly_invalid_root" \
    --partial-fabric "$partial_fabric_root" --partial-fabric-roster "$partial_fabric_roster" \
    --exclude-control-basin 1020000010
assert_contains "$case_stderr" 'assembly dataset exists without attributable interrupted or succeeded state; retained for inspection'
[[ $(wc -l <"$HFX_TEST_ADAPTER_LOG" | tr -d ' ') -eq "$invalid_adapter_lines" &&
   $(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ') -eq "$invalid_hfx_lines" ]] ||
    die 'post-validation artifact rerun invoked adapter or HFX'
pass 'post-assembly validation failure retains evidence and refuses rerun'

assembly_hfx_end=$(wc -l <"$HFX_TEST_HFX_LOG" | tr -d ' ')
tail -n $((assembly_hfx_end - assembly_hfx_start)) "$HFX_TEST_HFX_LOG" >"$test_tmp/assembly-hfx-delta"
if grep -Ev $'/assembly/dataset\t--strict\t--sample-pct\t100\t--format\ttext$' \
    "$test_tmp/assembly-hfx-delta" >"$case_stdout"; then
    die 'assembly HFX delta contains a non-validation vector'
fi
[[ $(wc -l <"$test_tmp/assembly-hfx-delta" | tr -d ' ') -eq 6 ]] ||
    die 'assembly cases produced an unexpected HFX validation count'
if grep -Ev '^(build|validate|assemble)[[:space:]]' "$HFX_TEST_ADAPTER_LOG" >"$case_stdout"; then
    die 'adapter log contains an unknown command after assembly cases'
fi
if grep -E '^assembly[[:space:]]' "$HFX_TEST_ADAPTER_LOG" >/dev/null; then
    die 'adapter log contains the rejected assembly misspelling'
fi

cp "$HFX_TEST_ADAPTER_LOG" "$test_tmp/adapter-before-new-paths"
cp "$HFX_TEST_HFX_LOG" "$test_tmp/hfx-before-new-paths"
evidence_root=$test_tmp/workspaces/evidence
mkdir "$evidence_root"
set -- $(init_args evidence "$evidence_root")
run_runner "$@" >"$case_stdout"
evidence_dir=$evidence_root/tdx-hydro-evidence
jq -n '{
  schema_version:3,processing_basin_id:"1020000010",
  stages:{
    acquire_basins:{status:"succeeded",attempts:1,failure_reason:null,evidence:{bytes:21,sha256:"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",sqlite_identity:"53514c69746520666f726d6174203300",layer_name:"basins-1020000010"}},
    acquire_streamnet:{status:"succeeded",attempts:1,failure_reason:null,evidence:{bytes:22,sha256:"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",sqlite_identity:"53514c69746520666f726d6174203300",layer_name:"streamnet-1020000010"}},
    compile:{status:"succeeded",attempts:1,failure_reason:null,diagnostic_report:{path:"reports/1020000010-build-report.json",diagnostics:{fixture_metric:101}}}
  }
}' >"$evidence_dir/state/basins/1020000010/current.json"
jq -n '{
  schema_version:3,processing_basin_id:"1020011530",
  stages:{
    acquire_basins:{status:"failed",attempts:2,failure_reason:"complete GET failed",evidence:null},
    acquire_streamnet:{status:"succeeded",attempts:1,failure_reason:null,evidence:{bytes:23,sha256:"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",sqlite_identity:"53514c69746520666f726d6174203300",layer_name:"streamnet-1020011530"}},
    compile:{status:"failed",attempts:0,failure_reason:"acquisition prerequisites are not both succeeded",diagnostic_report:null}
  }
}' >"$evidence_dir/state/basins/1020011530/current.json"
jq -n '{
  schema_version:3,processing_basin_id:"1020018110",
  stages:{
    acquire_basins:{status:"succeeded",attempts:1,failure_reason:null,evidence:{bytes:24,sha256:"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",sqlite_identity:"53514c69746520666f726d6174203300",layer_name:"basins-1020018110"}},
    acquire_streamnet:{status:"succeeded",attempts:1,failure_reason:null,evidence:{bytes:25,sha256:"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",sqlite_identity:"53514c69746520666f726d6174203300",layer_name:"streamnet-1020018110"}},
    compile:{status:"failed",attempts:1,failure_reason:"adapter validation failed",diagnostic_report:{path:"reports/1020018110-build-report.json",diagnostics:{fixture_metric:303}}}
  }
}' >"$evidence_dir/state/basins/1020018110/current.json"
jq -n '{
  schema_version:3,processing_basin_id:"1020021940",
  stages:{
    acquire_basins:{status:"pending",attempts:0,failure_reason:null,evidence:null},
    acquire_streamnet:{status:"pending",attempts:0,failure_reason:null,evidence:null},
    compile:{status:"failed",attempts:0,failure_reason:"acquisition prerequisites are not both succeeded",diagnostic_report:null}
  }
}' >"$evidence_dir/state/basins/1020021940/current.json"
run_runner evidence --campaign evidence --workspace-root "$evidence_root" >"$case_stdout"
printf '%s\n' '{"basins":[{"processing_basin_id":"1020000010","products":{"basins":{"attempts":1,"evidence":{"bytes":21,"layer_name":"basins-1020000010","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sqlite_identity":"53514c69746520666f726d6174203300"},"failure_reason":null,"status":"succeeded"},"streamnet":{"attempts":1,"evidence":{"bytes":22,"layer_name":"streamnet-1020000010","sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","sqlite_identity":"53514c69746520666f726d6174203300"},"failure_reason":null,"status":"succeeded"}}},{"processing_basin_id":"1020011530","products":{"basins":{"attempts":2,"evidence":null,"failure_reason":"complete GET failed","status":"failed"},"streamnet":{"attempts":1,"evidence":{"bytes":23,"layer_name":"streamnet-1020011530","sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","sqlite_identity":"53514c69746520666f726d6174203300"},"failure_reason":null,"status":"succeeded"}}},{"processing_basin_id":"1020018110","products":{"basins":{"attempts":1,"evidence":{"bytes":24,"layer_name":"basins-1020018110","sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","sqlite_identity":"53514c69746520666f726d6174203300"},"failure_reason":null,"status":"succeeded"},"streamnet":{"attempts":1,"evidence":{"bytes":25,"layer_name":"streamnet-1020018110","sha256":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","sqlite_identity":"53514c69746520666f726d6174203300"},"failure_reason":null,"status":"succeeded"}}},{"processing_basin_id":"1020021940","products":{"basins":{"attempts":0,"evidence":null,"failure_reason":null,"status":"pending"},"streamnet":{"attempts":0,"evidence":null,"failure_reason":null,"status":"pending"}}}],"campaign":"evidence","schema_version":1}' >"$test_tmp/expected-acquisition.json"
printf '%s\n' '{"attempted_basin_ids":["1020000010","1020011530","1020018110"],"campaign":"evidence","excluded_basins":[{"failure_reason":"acquisition prerequisites are not both succeeded","processing_basin_id":"1020011530"},{"failure_reason":"adapter validation failed","processing_basin_id":"1020018110"},{"failure_reason":"acquisition prerequisites are not both succeeded","processing_basin_id":"1020021940"}],"outcomes":[{"attempts":1,"failure_reason":null,"processing_basin_id":"1020000010","status":"succeeded"},{"attempts":0,"failure_reason":"acquisition prerequisites are not both succeeded","processing_basin_id":"1020011530","status":"failed"},{"attempts":1,"failure_reason":"adapter validation failed","processing_basin_id":"1020018110","status":"failed"},{"attempts":0,"failure_reason":"acquisition prerequisites are not both succeeded","processing_basin_id":"1020021940","status":"failed"}],"schema_version":1}' >"$test_tmp/expected-outcomes.json"
printf '%s\n' '{"basins":[{"diagnostics":{"fixture_metric":101},"processing_basin_id":"1020000010","report_path":"reports/1020000010-build-report.json","unavailable_reason":null},{"diagnostics":null,"processing_basin_id":"1020011530","report_path":null,"unavailable_reason":"acquisition prerequisites are not both succeeded"},{"diagnostics":{"fixture_metric":303},"processing_basin_id":"1020018110","report_path":"reports/1020018110-build-report.json","unavailable_reason":null},{"diagnostics":null,"processing_basin_id":"1020021940","report_path":null,"unavailable_reason":"acquisition prerequisites are not both succeeded"}],"campaign":"evidence","schema_version":1}' >"$test_tmp/expected-diagnostics.json"
diff -u "$test_tmp/expected-acquisition.json" "$evidence_dir/publication/evidence/acquisition.json"
diff -u "$test_tmp/expected-outcomes.json" "$evidence_dir/publication/evidence/outcomes.json"
diff -u "$test_tmp/expected-diagnostics.json" "$evidence_dir/publication/evidence/diagnostics.json"
cp -R "$evidence_dir/publication/evidence" "$test_tmp/evidence-first"
run_runner evidence --campaign evidence --workspace-root "$evidence_root" >"$case_stdout"
diff -ru "$test_tmp/evidence-first" "$evidence_dir/publication/evidence"
[[ $(jq -r '.attempted_basin_ids[]' "$evidence_dir/publication/evidence/outcomes.json" | tr '\n' ' ') == '1020000010 1020011530 1020018110 ' ]] ||
    die 'attempted basin ID set differs'
[[ $(jq -r '.excluded_basins[] | [.processing_basin_id,.failure_reason] | @tsv' "$evidence_dir/publication/evidence/outcomes.json" | tr '\n' '|') == $'1020011530\tacquisition prerequisites are not both succeeded|1020018110\tadapter validation failed|1020021940\tacquisition prerequisites are not both succeeded|' ]] ||
    die 'excluded basin set or reasons differ'
for reportable_id in 1020000010 1020011530 1020018110 1020021940; do
    jq -e --arg id "$reportable_id" '.basins | any(.processing_basin_id == $id)' "$evidence_dir/publication/evidence/acquisition.json" >/dev/null ||
        die "acquisition evidence omits $reportable_id"
    jq -e --arg id "$reportable_id" '.outcomes | any(.processing_basin_id == $id)' "$evidence_dir/publication/evidence/outcomes.json" >/dev/null ||
        die "outcomes evidence omits $reportable_id"
    jq -e --arg id "$reportable_id" '.basins | any(.processing_basin_id == $id)' "$evidence_dir/publication/evidence/diagnostics.json" >/dev/null ||
        die "diagnostic evidence omits $reportable_id"
done
pass 'deterministic acquisition, outcome, and diagnostic evidence is complete and byte-stable'

schema4_evidence_root=$test_tmp/workspaces/schema4-evidence
mkdir "$schema4_evidence_root"
cp -R "$evidence_dir" "$schema4_evidence_root/tdx-hydro-evidence"
schema4_evidence_dir=$schema4_evidence_root/tdx-hydro-evidence
schema4_evidence_state=$schema4_evidence_dir/state/basins/1020000010/current.json
jq '
  .schema_version = 4 |
  .retention = {
    inputs_reclaimed:true,
    policy:"reclaim-inputs-after-terminal"
  }
' "$schema4_evidence_state" >"$schema4_evidence_state.tmp"
mv "$schema4_evidence_state.tmp" "$schema4_evidence_state"
run_runner evidence --campaign evidence --workspace-root "$schema4_evidence_root" >"$case_stdout"
diff -u "$test_tmp/expected-acquisition.json" \
    "$schema4_evidence_dir/publication/evidence/acquisition.json"
diff -u "$test_tmp/expected-outcomes.json" \
    "$schema4_evidence_dir/publication/evidence/outcomes.json"
diff -u "$test_tmp/expected-diagnostics.json" \
    "$schema4_evidence_dir/publication/evidence/diagnostics.json"

schema4_validation_state=$schema4_evidence_dir/state/basins/1020018110/current.json
jq '
  .schema_version = 4 |
  .retention = {
    inputs_reclaimed:true,
    policy:"reclaim-inputs-after-terminal"
  } |
  .stages.compile.diagnostic_report = null
' "$schema4_validation_state" >"$schema4_validation_state.tmp"
mv "$schema4_validation_state.tmp" "$schema4_validation_state"
run_runner evidence --campaign evidence --workspace-root "$schema4_evidence_root" >"$case_stdout"
jq -e '
  .basins[] |
  select(.processing_basin_id == "1020018110") |
  .diagnostics == null and .report_path == null and
  .unavailable_reason == "adapter validation failed"
' "$schema4_evidence_dir/publication/evidence/diagnostics.json" >/dev/null ||
    die 'schema-4 validation failure did not emit the explicit unavailable reason'

for legacy_evidence_version in 1 2; do
    cp "$evidence_dir/state/basins/1020021940/current.json" \
        "$schema4_evidence_dir/state/basins/1020021940/current.json"
    jq --argjson version "$legacy_evidence_version" '
      .schema_version = $version |
      if $version == 1 then
        .stages.acquire_basins |= del(.evidence) |
        .stages.acquire_streamnet |= del(.evidence) |
        .stages.compile |= del(.diagnostic_report)
      else
        .stages.compile |= del(.diagnostic_report)
      end
    ' "$schema4_evidence_dir/state/basins/1020021940/current.json" \
        >"$schema4_evidence_dir/state/basins/1020021940/current.json.tmp"
    mv "$schema4_evidence_dir/state/basins/1020021940/current.json.tmp" \
        "$schema4_evidence_dir/state/basins/1020021940/current.json"
    expect_failure "schema-$legacy_evidence_version evidence gate" evidence \
        --campaign evidence --workspace-root "$schema4_evidence_root"
    assert_contains "$case_stderr" \
        'legacy basin state requires compile rerun before evidence: 1020021940'
done
pass 'schema 3 and 4 evidence matches while legacy schemas retain their refusal'

mkdir "$evidence_dir/downloads/conflict" "$evidence_dir/reports/conflict" "$evidence_dir/basin-outputs/conflict"
printf '%s\n' download >"$evidence_dir/downloads/conflict/value"
printf '%s\n' report >"$evidence_dir/reports/conflict/value"
printf '%s\n' output >"$evidence_dir/basin-outputs/conflict/value"
cp -R "$evidence_dir/publication/evidence" "$test_tmp/evidence-before-artifact-change"
printf '%s\n' changed >"$evidence_dir/downloads/conflict/value"
printf '%s\n' changed >"$evidence_dir/reports/conflict/value"
printf '%s\n' changed >"$evidence_dir/basin-outputs/conflict/value"
run_runner evidence --campaign evidence --workspace-root "$evidence_root" >"$case_stdout"
diff -ru "$test_tmp/evidence-before-artifact-change" "$evidence_dir/publication/evidence"
cp -R "$evidence_dir/publication/evidence" "$test_tmp/evidence-before-corruption"
jq '.stages.compile.diagnostic_report.path="reports/wrong.json"' \
    "$evidence_dir/state/basins/1020000010/current.json" >"$evidence_dir/state/basins/1020000010/current.json.tmp"
mv "$evidence_dir/state/basins/1020000010/current.json.tmp" \
    "$evidence_dir/state/basins/1020000010/current.json"
expect_failure 'malformed persisted evidence state' evidence --campaign evidence --workspace-root "$evidence_root"
diff -ru "$test_tmp/evidence-before-corruption" "$evidence_dir/publication/evidence"
pass 'evidence refuses malformed persisted state and never consults files outside state'

strict_bin=$test_tmp/strict-bin
mkdir "$strict_bin"
sed >"$strict_bin/aws" <<'STRICT_AWS'
#!/bin/bash
set -eu
log=${HFX_TEST_AWS_LOG:?}
remote=${HFX_TEST_AWS_REMOTE:?}
control=${HFX_TEST_AWS_CONTROL:?}
prefix=${HFX_TEST_AWS_PREFIX:?}
fixture=${HFX_TEST_AWS_FIXTURE:?}
mode=${HFX_TEST_AWS_MODE-normal}
printf '%s' "$0" >>"$log"
for arg in "$@"; do
    printf ' %s' "$arg" >>"$log"
done
printf '\n' >>"$log"
if [ "${1-}" = s3api ] && [ "${2-}" = list-objects-v2 ]; then
    token=
    if [ "$#" -eq 13 ]; then
        [ "$3" = --bucket ] && [ "$4" = pourpoint-hfx ] &&
            [ "$5" = --prefix ] && [ "$6" = "$prefix/" ] &&
            [ "$7" = --endpoint-url ] && [ "$8" = https://fsn1.your-objectstorage.com ] &&
            [ "$9" = --region ] && [ "${10}" = fsn1 ] &&
            [ "${11}" = --output ] && [ "${12}" = json ] && [ "${13}" = --no-paginate ] || exit 101
    elif [ "$#" -eq 15 ]; then
        [ "$3" = --bucket ] && [ "$4" = pourpoint-hfx ] &&
            [ "$5" = --prefix ] && [ "$6" = "$prefix/" ] &&
            [ "$7" = --continuation-token ] &&
            [ "$9" = --endpoint-url ] && [ "${10}" = https://fsn1.your-objectstorage.com ] &&
            [ "${11}" = --region ] && [ "${12}" = fsn1 ] &&
            [ "${13}" = --output ] && [ "${14}" = json ] && [ "${15}" = --no-paginate ] || exit 102
        token=$8
    else
        exit 103
    fi
    count=0
    [ ! -f "$control/list-count" ] || count=$(<"$control/list-count")
    count=$((count + 1))
    printf '%s\n' "$count" >"$control/list-count"
    if [ "$mode" = paginated ]; then
        if [ -z "$token" ]; then
            printf '%s\n' '{"Contents":[{"Key":"scratch/tdx-hydro-publish/fixture/CITATION.txt","Size":9},{"Key":"scratch/tdx-hydro-publish/fixture/NOTICE","Size":7},{"Key":"scratch/tdx-hydro-publish/fixture/aux/snap_stems.parquet","Size":5}],"IsTruncated":true,"KeyCount":3,"NextContinuationToken":"fixture-page-2"}'
        elif [ "$token" = fixture-page-2 ]; then
            printf '%s\n' '{"Contents":[{"Key":"scratch/tdx-hydro-publish/fixture/build-report.json","Size":7},{"Key":"scratch/tdx-hydro-publish/fixture/catchments.parquet","Size":11},{"Key":"scratch/tdx-hydro-publish/fixture/graph.parquet","Size":6},{"Key":"scratch/tdx-hydro-publish/fixture/manifest.json","Size":3}],"IsTruncated":false,"KeyCount":4}'
        else
            exit 104
        fi
        exit 0
    fi
    if [ "$mode" = extra ] && [ "$count" -eq 1 ]; then
        "${HFX_TEST_REAL_JQ:?}" -cn --arg key "$prefix/unexpected" \
            '{Contents:[{Key:$key,Size:1}],IsTruncated:false,KeyCount:1}'
        exit 0
    fi
    entries=$control/entries.$$
    : >"$entries"
    if [ -d "$remote/$prefix" ]; then
        find "$remote/$prefix" -type f | LC_ALL=C sort | while IFS= read -r file; do
            key=${file#"$remote/"}
            bytes=$(wc -c <"$file" | tr -d '[:space:]')
            "${HFX_TEST_REAL_JQ:?}" -cn --arg key "$key" --argjson bytes "$bytes" \
                '{Key:$key,Size:$bytes}' >>"$entries"
        done
    fi
    if [ ! -s "$entries" ]; then
        rm "$entries"
        printf '%s\n' '{"IsTruncated":false,"KeyCount":0}'
        exit 0
    fi
    "${HFX_TEST_REAL_JQ:?}" -cs '{Contents:sort_by(.Key),IsTruncated:false,KeyCount:length}' "$entries" >"$entries.json"
    rm "$entries"
    if { [ "$mode" = wrong-initial ] && [ "$count" -eq 1 ]; } ||
       { [ "$mode" = post-wrong ] && [ "$count" -eq 2 ]; }; then
        "${HFX_TEST_REAL_JQ:?}" '.Contents[0].Size += 1' "$entries.json"
    else
        cat "$entries.json"
    fi
    rm "$entries.json"
    exit 0
fi
if [ "${1-}" = s3 ] && [ "${2-}" = cp ]; then
    [ "$#" -eq 9 ] && [ "$5" = --endpoint-url ] &&
        [ "$6" = https://fsn1.your-objectstorage.com ] &&
        [ "$7" = --region ] && [ "$8" = fsn1 ] &&
        [ "$9" = --only-show-errors ] || exit 105
    source=$3
    uri=$4
    key=${uri#s3://pourpoint-hfx/}
    [ "$uri" = "s3://pourpoint-hfx/$key" ] || exit 106
    case ${key#"$prefix/"} in
        CITATION.txt) expected=$fixture/CITATION.txt ;;
        NOTICE) expected=$fixture/NOTICE ;;
        aux/snap_stems.parquet) expected=$fixture/assembled/aux/snap_stems.parquet ;;
        build-report.json) expected=$fixture/assembled-report.json ;;
        catchments.parquet) expected=$fixture/assembled/catchments.parquet ;;
        graph.parquet) expected=$fixture/assembled/graph.parquet ;;
        manifest.json) expected=$fixture/assembled/manifest.json ;;
        *) exit 107 ;;
    esac
    [ "$source" = "$expected" ] || exit 108
    if [ "${HFX_TEST_AWS_FAIL_KEY-}" = "${key#"$prefix/"}" ] && [ ! -e "$control/failed-once" ]; then
        : >"$control/failed-once"
        exit 109
    fi
    mkdir -p "${remote:?}/$(dirname "$key")"
    cp "$source" "$remote/$key"
    exit 0
fi
exit 110
STRICT_AWS
chmod +x "$strict_bin/aws"
aws_log=$test_tmp/aws-strict.log
: >"$aws_log"

recording_bin=$test_tmp/recording-bin
mkdir "$recording_bin"
sed >"$recording_bin/aws" <<'RECORDING_AWS'
#!/bin/bash
set -eu
log=${HFX_TEST_AWS_LOG:?}
remote=${HFX_TEST_AWS_REMOTE:?}
printf '%s' "$0" >>"$log"
for arg in "$@"; do
    printf ' %s' "$arg" >>"$log"
done
printf '\n' >>"$log"
if [ "${1-}" = s3api ] && [ "${2-}" = list-objects-v2 ]; then
    prefix=
    previous=
    for arg in "$@"; do
        if [ "$previous" = --prefix ]; then
            prefix=$arg
            break
        fi
        previous=$arg
    done
    entries=$remote/entries.$$
    : >"$entries"
    if [ -d "$remote/objects/$prefix" ]; then
        find "$remote/objects/$prefix" -type f | LC_ALL=C sort | while IFS= read -r file; do
            key=${file#"$remote/objects/"}
            bytes=$(wc -c <"$file" | tr -d '[:space:]')
            "${HFX_TEST_REAL_JQ:?}" -cn --arg key "$key" --argjson bytes "$bytes" \
                '{Key:$key,Size:$bytes}' >>"$entries"
        done
    fi
    if [ -s "$entries" ]; then
        "${HFX_TEST_REAL_JQ:?}" -cs \
            '{Contents:sort_by(.Key),IsTruncated:false,KeyCount:length}' "$entries"
    else
        printf '%s\n' '{"IsTruncated":false,"KeyCount":0}'
    fi
    rm "$entries"
    exit 0
fi
if [ "${1-}" = s3 ] && [ "${2-}" = cp ]; then
    source=$3
    key=${4#s3://pourpoint-hfx/}
    destination=$remote/objects/$key
    mkdir -p "${destination%/*}"
    cp "$source" "$destination"
    exit 0
fi
exit 111
RECORDING_AWS
chmod +x "$recording_bin/aws"

publish_root=$test_tmp/workspaces/publish
mkdir "$publish_root"
set -- $(init_args publish "$publish_root")
run_runner "$@" >"$case_stdout"
publish_dir=$publish_root/tdx-hydro-publish
publish_fixture=$test_tmp/publication-fixture
mkdir "$publish_fixture" "$publish_fixture/assembled" "$publish_fixture/assembled/aux"
printf '{}\n' >"$publish_fixture/assembled/manifest.json"
printf 'catchments\n' >"$publish_fixture/assembled/catchments.parquet"
printf 'graph\n' >"$publish_fixture/assembled/graph.parquet"
printf 'snap\n' >"$publish_fixture/assembled/aux/snap_stems.parquet"
printf 'report\n' >"$publish_fixture/assembled-report.json"
printf 'notice\n' >"$publish_fixture/NOTICE"
printf 'citation\n' >"$publish_fixture/CITATION.txt"
publish_fixture=$(cd -P "$publish_fixture" && pwd -P)
mkdir "$publish_dir/basin-outputs/1020000010"
printf '%s\n' canary >"$publish_dir/basin-outputs/1020000010/never-publish"
export HFX_TEST_AWS_LOG=$aws_log
export HFX_TEST_AWS_FIXTURE=$publish_fixture
export HFX_TEST_AWS_PREFIX=scratch/tdx-hydro-publish/fixture
export HFX_TEST_AWS_REMOTE=$test_tmp/aws-remote-guards
export HFX_TEST_AWS_CONTROL=$test_tmp/aws-control-guards
mkdir "$HFX_TEST_AWS_REMOTE" "$HFX_TEST_AWS_CONTROL"

for sibling_scope in campaign state assembly; do
    sibling_root=$test_tmp/workspaces/sibling-$sibling_scope
    mkdir "$sibling_root"
    set -- $(init_args sibling-a "$sibling_root")
    run_runner "$@" >"$case_stdout"
    set -- $(init_args sibling-b "$sibling_root")
    run_runner "$@" >"$case_stdout"
    sibling_a_dir=$sibling_root/tdx-hydro-sibling-a
    sibling_b_dir=$sibling_root/tdx-hydro-sibling-b
    printf '%s\n' private-basin >"$sibling_a_dir/basin-outputs/private"
    printf '%s\n' private-scratch >"$sibling_a_dir/assembly/scratch/private"
    case $sibling_scope in
        campaign) sibling_out=$sibling_a_dir ;;
        state) sibling_out=$sibling_a_dir/state ;;
        assembly) sibling_out=$sibling_a_dir/assembly ;;
    esac
    sibling_aws_log=$test_tmp/aws-sibling-$sibling_scope.log
    sibling_remote=$test_tmp/aws-sibling-$sibling_scope
    : >"$sibling_aws_log"
    mkdir "$sibling_remote"
    export HFX_TEST_AWS_LOG=$sibling_aws_log
    export HFX_TEST_AWS_REMOTE=$sibling_remote
    sibling_status=0
    HFX_TDX_AWS=$recording_bin/aws \
        run_runner publish --campaign sibling-b --workspace-root "$sibling_root" \
            --out "$sibling_out" --report "$publish_fixture/assembled-report.json" \
            --notice "$publish_fixture/NOTICE" --citation "$publish_fixture/CITATION.txt" \
            --scratch-prefix "scratch/tdx-hydro-sibling-b/$sibling_scope" \
            >"$case_stdout" 2>"$case_stderr" ||
        sibling_status=$?
    sibling_uploads=$(grep -c ' s3 cp ' "$sibling_aws_log" || true)
    case $sibling_scope in
        campaign)
            sibling_campaign_status=$sibling_status
            sibling_campaign_uploads=$sibling_uploads
            sibling_campaign_contract=$sibling_b_dir/publication/current.json
            ;;
        state)
            sibling_state_status=$sibling_status
            sibling_state_uploads=$sibling_uploads
            sibling_state_contract=$sibling_b_dir/publication/current.json
            ;;
        assembly)
            sibling_assembly_status=$sibling_status
            sibling_assembly_uploads=$sibling_uploads
            sibling_assembly_contract=$sibling_b_dir/publication/current.json
            ;;
    esac
done
if [[ "$sibling_campaign_status" -eq 0 || "$sibling_campaign_uploads" -ne 0 ||
      -e "$sibling_campaign_contract" ||
      "$sibling_state_status" -eq 0 || "$sibling_state_uploads" -ne 0 ||
      -e "$sibling_state_contract" ||
      "$sibling_assembly_status" -eq 0 || "$sibling_assembly_uploads" -ne 0 ||
      -e "$sibling_assembly_contract" ]]; then
    die "sibling privacy guards observed campaign status=$sibling_campaign_status uploads=$sibling_campaign_uploads contract=$([[ -e "$sibling_campaign_contract" ]] && printf yes || printf no); state status=$sibling_state_status uploads=$sibling_state_uploads contract=$([[ -e "$sibling_state_contract" ]] && printf yes || printf no); assembly status=$sibling_assembly_status uploads=$sibling_assembly_uploads contract=$([[ -e "$sibling_assembly_contract" ]] && printf yes || printf no)"
fi
publishable_root=$test_tmp/workspaces/publishable-current
mkdir "$publishable_root"
set -- $(init_args publishable-current "$publishable_root")
run_runner "$@" >"$case_stdout"
publishable_dir=$publishable_root/tdx-hydro-publishable-current
mkdir "$publishable_dir/assembly/dataset"
printf '%s\n' assembled >"$publishable_dir/assembly/dataset/manifest.json"
publishable_aws_log=$test_tmp/aws-publishable-current.log
publishable_remote=$test_tmp/aws-publishable-current
: >"$publishable_aws_log"
mkdir "$publishable_remote"
export HFX_TEST_AWS_LOG=$publishable_aws_log
export HFX_TEST_AWS_REMOTE=$publishable_remote
HFX_TDX_AWS=$recording_bin/aws \
    run_runner publish --campaign publishable-current --workspace-root "$publishable_root" \
        --out "$publishable_dir/assembly/dataset" \
        --report "$publish_fixture/assembled-report.json" \
        --notice "$publish_fixture/NOTICE" --citation "$publish_fixture/CITATION.txt" \
        --scratch-prefix scratch/tdx-hydro-publishable-current/dataset \
        >"$case_stdout" 2>"$case_stderr" ||
    die 'current campaign assembly/dataset publication was refused'
publishable_uploads=$(grep -c ' s3 cp ' "$publishable_aws_log" || true)
[[ "$publishable_uploads" -eq 4 && -f "$publishable_dir/publication/current.json" ]] ||
    die "current campaign assembly/dataset publication uploaded $publishable_uploads objects or did not pin its contract"
pass 'publication refuses a sibling campaign directory before AWS or contract pinning'
pass 'publication refuses sibling campaign state before AWS or contract pinning'
pass 'publication refuses sibling campaign assembly before AWS or contract pinning'

foreign_parent=$test_tmp/workspaces/foreign-roots
foreign_a_root=$foreign_parent/wk-a
foreign_b_root=$foreign_parent/wk-b
mkdir -p "$foreign_a_root" "$foreign_b_root"
set -- $(init_args foreign-old "$foreign_a_root")
run_runner "$@" >"$case_stdout"
set -- $(init_args foreign-new "$foreign_b_root")
run_runner "$@" >"$case_stdout"
foreign_old_dir=$foreign_a_root/tdx-hydro-foreign-old
foreign_new_dir=$foreign_b_root/tdx-hydro-foreign-new

nested_root=$test_tmp/workspaces/nested-root
nested_sub_root=$nested_root/sub
mkdir -p "$nested_sub_root"
set -- $(init_args nested-outer "$nested_root")
run_runner "$@" >"$case_stdout"
set -- $(init_args nested-inner "$nested_sub_root")
run_runner "$@" >"$case_stdout"
nested_outer_dir=$nested_root/tdx-hydro-nested-outer
nested_inner_dir=$nested_sub_root/tdx-hydro-nested-inner

for content_guard_scope in foreign-root nested-outer nested-inner; do
    case $content_guard_scope in
        foreign-root)
            content_guard_campaign=foreign-new
            content_guard_root=$foreign_b_root
            content_guard_out=$foreign_old_dir
            content_guard_contract=$foreign_new_dir/publication/current.json
            ;;
        nested-outer)
            content_guard_campaign=nested-inner
            content_guard_root=$nested_sub_root
            content_guard_out=$nested_outer_dir
            content_guard_contract=$nested_inner_dir/publication/current.json
            ;;
        nested-inner)
            content_guard_campaign=nested-outer
            content_guard_root=$nested_root
            content_guard_out=$nested_inner_dir
            content_guard_contract=$nested_outer_dir/publication/current.json
            ;;
    esac
    content_guard_aws_log=$test_tmp/aws-content-guard-$content_guard_scope.log
    content_guard_remote=$test_tmp/aws-content-guard-$content_guard_scope
    : >"$content_guard_aws_log"
    mkdir "$content_guard_remote"
    export HFX_TEST_AWS_LOG=$content_guard_aws_log
    export HFX_TEST_AWS_REMOTE=$content_guard_remote
    content_guard_status=0
    HFX_TDX_AWS=$recording_bin/aws \
        run_runner publish --campaign "$content_guard_campaign" \
            --workspace-root "$content_guard_root" --out "$content_guard_out" \
            --report "$publish_fixture/assembled-report.json" \
            --notice "$publish_fixture/NOTICE" --citation "$publish_fixture/CITATION.txt" \
            --scratch-prefix "scratch/tdx-hydro-$content_guard_campaign/$content_guard_scope" \
            >"$case_stdout" 2>"$case_stderr" ||
        content_guard_status=$?
    content_guard_uploads=$(grep -c ' s3 cp ' "$content_guard_aws_log" || true)
    case $content_guard_scope in
        foreign-root)
            foreign_root_status=$content_guard_status
            foreign_root_uploads=$content_guard_uploads
            foreign_root_contract=$content_guard_contract
            ;;
        nested-outer)
            nested_outer_status=$content_guard_status
            nested_outer_uploads=$content_guard_uploads
            nested_outer_contract=$content_guard_contract
            ;;
        nested-inner)
            nested_inner_status=$content_guard_status
            nested_inner_uploads=$content_guard_uploads
            nested_inner_contract=$content_guard_contract
            ;;
    esac
done
if [[ "$foreign_root_status" -eq 0 || "$foreign_root_uploads" -ne 0 ||
      -e "$foreign_root_contract" ||
      "$nested_outer_status" -eq 0 || "$nested_outer_uploads" -ne 0 ||
      -e "$nested_outer_contract" ||
      "$nested_inner_status" -eq 0 || "$nested_inner_uploads" -ne 0 ||
      -e "$nested_inner_contract" ]]; then
    die "content privacy guards observed foreign-root status=$foreign_root_status uploads=$foreign_root_uploads contract=$([[ -e "$foreign_root_contract" ]] && printf yes || printf no); nested-outer status=$nested_outer_status uploads=$nested_outer_uploads contract=$([[ -e "$nested_outer_contract" ]] && printf yes || printf no); nested-inner status=$nested_inner_status uploads=$nested_inner_uploads contract=$([[ -e "$nested_inner_contract" ]] && printf yes || printf no)"
fi
pass 'publication refuses a campaign under a foreign workspace root before AWS or contract pinning'
pass 'publication refuses an outer campaign from a nested workspace root before AWS or contract pinning'
pass 'publication refuses a nested campaign from an outer workspace root before AWS or contract pinning'

deep_descendant_publisher_root=$test_tmp/workspaces/deep-descendant-publisher
deep_descendant_out=$test_tmp/workspaces/deep-descendant-out
deep_descendant_foreign_root=$deep_descendant_out/a/b/c/d/e/f
mkdir -p "$deep_descendant_publisher_root" "$deep_descendant_foreign_root"
set -- $(init_args deep-descendant-publisher "$deep_descendant_publisher_root")
run_runner "$@" >"$case_stdout"
set -- $(init_args deep-descendant-foreign "$deep_descendant_foreign_root")
run_runner "$@" >"$case_stdout"
deep_descendant_publisher_dir=$deep_descendant_publisher_root/tdx-hydro-deep-descendant-publisher
deep_descendant_foreign_dir=$deep_descendant_foreign_root/tdx-hydro-deep-descendant-foreign
mkdir "$deep_descendant_foreign_dir/basin-outputs/1020000010"
printf '%s\n' private-catchments \
    >"$deep_descendant_foreign_dir/basin-outputs/1020000010/catchments.parquet"
deep_descendant_aws_log=$test_tmp/aws-deep-descendant.log
deep_descendant_remote=$test_tmp/aws-deep-descendant
: >"$deep_descendant_aws_log"
mkdir "$deep_descendant_remote"
export HFX_TEST_AWS_LOG=$deep_descendant_aws_log
export HFX_TEST_AWS_REMOTE=$deep_descendant_remote
deep_descendant_status=0
HFX_TDX_AWS=$recording_bin/aws \
    run_runner publish --campaign deep-descendant-publisher \
        --workspace-root "$deep_descendant_publisher_root" --out "$deep_descendant_out" \
        --report "$publish_fixture/assembled-report.json" \
        --notice "$publish_fixture/NOTICE" --citation "$publish_fixture/CITATION.txt" \
        --scratch-prefix scratch/tdx-hydro-deep-descendant-publisher/deep-descendant \
        >"$case_stdout" 2>"$case_stderr" ||
    deep_descendant_status=$?
deep_descendant_uploads=$(grep -c ' s3 cp ' "$deep_descendant_aws_log" || true)
if [[ "$deep_descendant_status" -eq 0 || -s "$deep_descendant_aws_log" ||
      -e "$deep_descendant_publisher_dir/publication/current.json" ]]; then
    die "deep descendant privacy guard observed status=$deep_descendant_status uploads=$deep_descendant_uploads aws_lines=$(wc -l <"$deep_descendant_aws_log" | tr -d '[:space:]') contract=$([[ -e "$deep_descendant_publisher_dir/publication/current.json" ]] && printf yes || printf no)"
fi
assert_contains "$case_stderr" 'publication output must not contain another campaign directory'
pass 'publication refuses a deeply nested descendant campaign before AWS or contract pinning'

for non_campaign_name in basin-outputs-extra state-backup down publication-out assembly-x; do
    non_campaign_root=$test_tmp/workspaces/non-campaign-$non_campaign_name
    non_campaign_campaign=accept-${non_campaign_name//-}
    mkdir "$non_campaign_root"
    set -- $(init_args "$non_campaign_campaign" "$non_campaign_root")
    run_runner "$@" >"$case_stdout"
    non_campaign_dir=$non_campaign_root/tdx-hydro-$non_campaign_campaign
    non_campaign_out=$non_campaign_dir/$non_campaign_name
    mkdir "$non_campaign_out"
    printf '%s\n' publishable >"$non_campaign_out/manifest.json"
    non_campaign_aws_log=$test_tmp/aws-non-campaign-$non_campaign_name.log
    non_campaign_remote=$test_tmp/aws-non-campaign-$non_campaign_name
    : >"$non_campaign_aws_log"
    mkdir "$non_campaign_remote"
    export HFX_TEST_AWS_LOG=$non_campaign_aws_log
    export HFX_TEST_AWS_REMOTE=$non_campaign_remote
    HFX_TDX_AWS=$recording_bin/aws \
        run_runner publish --campaign "$non_campaign_campaign" \
            --workspace-root "$non_campaign_root" --out "$non_campaign_out" \
            --report "$publish_fixture/assembled-report.json" \
            --notice "$publish_fixture/NOTICE" --citation "$publish_fixture/CITATION.txt" \
            --scratch-prefix "scratch/tdx-hydro-$non_campaign_campaign/$non_campaign_name" \
            >"$case_stdout" 2>"$case_stderr" ||
        die "non-campaign directory was refused: $non_campaign_name"
    non_campaign_uploads=$(grep -c ' s3 cp ' "$non_campaign_aws_log" || true)
    [[ "$non_campaign_uploads" -eq 4 &&
       -f "$non_campaign_dir/publication/current.json" ]] ||
        die "non-campaign directory $non_campaign_name uploaded $non_campaign_uploads objects or did not pin its contract"
done
pass 'publication accepts campaign-local directories whose names resemble private subtrees but have no campaign marker'

export HFX_TEST_AWS_LOG=$aws_log
export HFX_TEST_AWS_REMOTE=$test_tmp/aws-remote-guards
for privacy_scope in campaign assembly workspace; do
    privacy_campaign=privacy-$privacy_scope
    privacy_root=$test_tmp/workspaces/$privacy_campaign
    mkdir "$privacy_root"
    set -- $(init_args "$privacy_campaign" "$privacy_root")
    run_runner "$@" >"$case_stdout"
    privacy_dir=$privacy_root/tdx-hydro-$privacy_campaign
    printf '%s\n' private-basin >"$privacy_dir/basin-outputs/private"
    printf '%s\n' private-scratch >"$privacy_dir/assembly/scratch/private"
    case $privacy_scope in
        campaign) privacy_out=$privacy_dir ;;
        assembly) privacy_out=$privacy_dir/assembly ;;
        workspace) privacy_out=$privacy_root ;;
    esac
    export HFX_TEST_AWS_PREFIX=scratch/tdx-hydro-$privacy_campaign/fixture
    export HFX_TEST_AWS_REMOTE=$test_tmp/aws-remote-$privacy_campaign
    export HFX_TEST_AWS_CONTROL=$test_tmp/aws-control-$privacy_campaign
    mkdir "$HFX_TEST_AWS_REMOTE" "$HFX_TEST_AWS_CONTROL"
    privacy_aws_before=$(wc -l <"$aws_log" | tr -d ' ')
    privacy_status=0
    HFX_TDX_AWS=$strict_bin/aws \
        run_runner publish --campaign "$privacy_campaign" --workspace-root "$privacy_root" \
            --out "$privacy_out" --report "$publish_fixture/assembled-report.json" \
            --notice "$publish_fixture/NOTICE" --citation "$publish_fixture/CITATION.txt" \
            --scratch-prefix "$HFX_TEST_AWS_PREFIX" >"$case_stdout" 2>"$case_stderr" ||
        privacy_status=$?
    privacy_aws_after=$(wc -l <"$aws_log" | tr -d ' ')
    privacy_aws_lines=$((privacy_aws_after - privacy_aws_before))
    [[ "$privacy_status" -ne 0 ]] || die "$privacy_scope privacy guard unexpectedly succeeded"
    case $privacy_scope in
        campaign)
            campaign_privacy_aws_lines=$privacy_aws_lines
            campaign_privacy_contract=$privacy_dir/publication/current.json
            ;;
        assembly)
            assembly_privacy_aws_lines=$privacy_aws_lines
            assembly_privacy_contract=$privacy_dir/publication/current.json
            ;;
        workspace)
            workspace_privacy_aws_lines=$privacy_aws_lines
            workspace_privacy_contract=$privacy_dir/publication/current.json
            ;;
    esac
done
if [[ "$campaign_privacy_aws_lines" -ne 0 || -e "$campaign_privacy_contract" ||
      "$assembly_privacy_aws_lines" -ne 0 || -e "$assembly_privacy_contract" ||
      "$workspace_privacy_aws_lines" -ne 0 || -e "$workspace_privacy_contract" ]]; then
    die "privacy guards observed AWS lines campaign=$campaign_privacy_aws_lines assembly=$assembly_privacy_aws_lines workspace=$workspace_privacy_aws_lines; contract pinning campaign=$([[ -e "$campaign_privacy_contract" ]] && printf yes || printf no) assembly=$([[ -e "$assembly_privacy_contract" ]] && printf yes || printf no) workspace=$([[ -e "$workspace_privacy_contract" ]] && printf yes || printf no)"
fi
[[ "$campaign_privacy_aws_lines" -eq 0 && ! -e "$campaign_privacy_contract" ]] ||
    die "campaign ancestor privacy guard spent $campaign_privacy_aws_lines AWS calls or pinned its contract"
pass 'publication refuses the campaign directory before AWS or contract pinning'
[[ "$assembly_privacy_aws_lines" -eq 0 && ! -e "$assembly_privacy_contract" ]] ||
    die "assembly privacy guard spent $assembly_privacy_aws_lines AWS calls or pinned its contract"
pass 'publication refuses campaign assembly before AWS or contract pinning'
[[ "$workspace_privacy_aws_lines" -eq 0 && ! -e "$workspace_privacy_contract" ]] ||
    die "workspace ancestor privacy guard spent $workspace_privacy_aws_lines AWS calls or pinned its contract"
pass 'publication refuses the workspace ancestor before AWS or contract pinning'

export HFX_TEST_AWS_PREFIX=scratch/tdx-hydro-publish/fixture
export HFX_TEST_AWS_REMOTE=$test_tmp/aws-remote-guards
export HFX_TEST_AWS_CONTROL=$test_tmp/aws-control-guards
aws_lines_before=$(wc -l <"$aws_log" | tr -d ' ')
expect_failure 'missing publication inputs' publish --campaign publish --workspace-root "$publish_root"
expect_failure 'relative publication output' publish --campaign publish --workspace-root "$publish_root" \
    --out relative --report "$publish_fixture/assembled-report.json" --notice "$publish_fixture/NOTICE" \
    --citation "$publish_fixture/CITATION.txt" --scratch-prefix "$HFX_TEST_AWS_PREFIX"
expect_failure 'empty notice' publish --campaign publish --workspace-root "$publish_root" \
    --out "$publish_fixture/assembled" --report "$publish_fixture/assembled-report.json" --notice /dev/null \
    --citation "$publish_fixture/CITATION.txt" --scratch-prefix "$HFX_TEST_AWS_PREFIX"
ln -s "$publish_fixture/NOTICE" "$publish_fixture/NOTICE-link"
expect_failure 'symlink notice' publish --campaign publish --workspace-root "$publish_root" \
    --out "$publish_fixture/assembled" --report "$publish_fixture/assembled-report.json" \
    --notice "$publish_fixture/NOTICE-link" --citation "$publish_fixture/CITATION.txt" \
    --scratch-prefix "$HFX_TEST_AWS_PREFIX"
expect_failure 'report inside output' publish --campaign publish --workspace-root "$publish_root" \
    --out "$publish_fixture/assembled" --report "$publish_fixture/assembled/manifest.json" --notice "$publish_fixture/NOTICE" \
    --citation "$publish_fixture/CITATION.txt" --scratch-prefix "$HFX_TEST_AWS_PREFIX"
expect_failure 'per-basin output publication' publish --campaign publish --workspace-root "$publish_root" \
    --out "$publish_dir/basin-outputs/1020000010" --report "$publish_fixture/assembled-report.json" --notice "$publish_fixture/NOTICE" \
    --citation "$publish_fixture/CITATION.txt" --scratch-prefix "$HFX_TEST_AWS_PREFIX"
for unsafe_prefix in hfx hfx/release scratch scratch/../hfx /scratch/tdx-hydro-publish/fixture scratch/tdx-hydro-other/fixture scratch/tdx-hydro-publish//fixture; do
    expect_failure "unsafe prefix $unsafe_prefix" publish --campaign publish --workspace-root "$publish_root" \
        --out "$publish_fixture/assembled" --report "$publish_fixture/assembled-report.json" --notice "$publish_fixture/NOTICE" \
        --citation "$publish_fixture/CITATION.txt" --scratch-prefix "$unsafe_prefix"
done
[[ $(wc -l <"$aws_log" | tr -d ' ') == "$aws_lines_before" ]] ||
    die 'publication guard invoked AWS'
pass 'publication argument, attribution, physical report-placement, per-basin-output, and scratch-prefix guards refuse before AWS'

rm -r "$HFX_TEST_AWS_REMOTE" "$HFX_TEST_AWS_CONTROL"
mkdir "$HFX_TEST_AWS_REMOTE" "$HFX_TEST_AWS_CONTROL"
: >"$aws_log"
unset HFX_TDX_AWS
PATH=$strict_bin:$PATH HFX_TEST_AWS_MODE=normal \
    run_runner publish --campaign publish --workspace-root "$publish_root" \
        --out "$publish_fixture/assembled" --report "$publish_fixture/assembled-report.json" \
        --notice "$publish_fixture/NOTICE" --citation "$publish_fixture/CITATION.txt" \
        --scratch-prefix "$HFX_TEST_AWS_PREFIX" >"$case_stdout"
[[ $(grep -c ' s3 cp ' "$aws_log") == 7 ]] || die 'default AWS publication upload count differs'
[[ $(grep -c ' s3api list-objects-v2 ' "$aws_log") == 2 ]] || die 'default AWS publication listing count differs'
{
    printf '%s s3api list-objects-v2 --bucket pourpoint-hfx --prefix scratch/tdx-hydro-publish/fixture/ --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --output json --no-paginate\n' "$strict_bin/aws"
    printf '%s s3 cp %s/CITATION.txt s3://pourpoint-hfx/scratch/tdx-hydro-publish/fixture/CITATION.txt --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --only-show-errors\n' "$strict_bin/aws" "$publish_fixture"
    printf '%s s3 cp %s/NOTICE s3://pourpoint-hfx/scratch/tdx-hydro-publish/fixture/NOTICE --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --only-show-errors\n' "$strict_bin/aws" "$publish_fixture"
    printf '%s s3 cp %s/assembled/aux/snap_stems.parquet s3://pourpoint-hfx/scratch/tdx-hydro-publish/fixture/aux/snap_stems.parquet --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --only-show-errors\n' "$strict_bin/aws" "$publish_fixture"
    printf '%s s3 cp %s/assembled-report.json s3://pourpoint-hfx/scratch/tdx-hydro-publish/fixture/build-report.json --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --only-show-errors\n' "$strict_bin/aws" "$publish_fixture"
    printf '%s s3 cp %s/assembled/catchments.parquet s3://pourpoint-hfx/scratch/tdx-hydro-publish/fixture/catchments.parquet --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --only-show-errors\n' "$strict_bin/aws" "$publish_fixture"
    printf '%s s3 cp %s/assembled/graph.parquet s3://pourpoint-hfx/scratch/tdx-hydro-publish/fixture/graph.parquet --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --only-show-errors\n' "$strict_bin/aws" "$publish_fixture"
    printf '%s s3 cp %s/assembled/manifest.json s3://pourpoint-hfx/scratch/tdx-hydro-publish/fixture/manifest.json --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --only-show-errors\n' "$strict_bin/aws" "$publish_fixture"
    printf '%s s3api list-objects-v2 --bucket pourpoint-hfx --prefix scratch/tdx-hydro-publish/fixture/ --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --output json --no-paginate\n' "$strict_bin/aws"
} >"$test_tmp/expected-successful-aws.log"
diff -u "$test_tmp/expected-successful-aws.log" "$aws_log"
out_physical=$(cd -P "$publish_fixture/assembled" && pwd -P)
report_physical=$(cd -P "${publish_fixture%/*}" && printf '%s/%s\n' "$(pwd -P)" "${publish_fixture##*/}/assembled-report.json")
notice_physical=$(cd -P "$publish_fixture" && printf '%s/NOTICE\n' "$(pwd -P)")
citation_physical=$(cd -P "$publish_fixture" && printf '%s/CITATION.txt\n' "$(pwd -P)")
jq -cnS --arg out "$out_physical" --arg report "$report_physical" --arg notice "$notice_physical" --arg citation "$citation_physical" '{
  bucket:"pourpoint-hfx",citation:$citation,endpoint:"https://fsn1.your-objectstorage.com",notice:$notice,
  objects:[
    {bytes:9,key:"scratch/tdx-hydro-publish/fixture/CITATION.txt",source:$citation},
    {bytes:7,key:"scratch/tdx-hydro-publish/fixture/NOTICE",source:$notice},
    {bytes:5,key:"scratch/tdx-hydro-publish/fixture/aux/snap_stems.parquet",source:($out+"/aux/snap_stems.parquet")},
    {bytes:7,key:"scratch/tdx-hydro-publish/fixture/build-report.json",source:$report},
    {bytes:11,key:"scratch/tdx-hydro-publish/fixture/catchments.parquet",source:($out+"/catchments.parquet")},
    {bytes:6,key:"scratch/tdx-hydro-publish/fixture/graph.parquet",source:($out+"/graph.parquet")},
    {bytes:3,key:"scratch/tdx-hydro-publish/fixture/manifest.json",source:($out+"/manifest.json")}
  ],out:$out,prefix:"scratch/tdx-hydro-publish/fixture",region:"fsn1",report:$report,schema_version:1
}' >"$test_tmp/expected-publication-current.json"
diff -u "$test_tmp/expected-publication-current.json" "$publish_dir/publication/current.json"
jq -cS '{prefix,objects:[.objects[]|{bytes,key}]}' "$publish_dir/publication/current.json" >"$test_tmp/publication-projection.json"
printf '%s\n' '{"objects":[{"bytes":9,"key":"scratch/tdx-hydro-publish/fixture/CITATION.txt"},{"bytes":7,"key":"scratch/tdx-hydro-publish/fixture/NOTICE"},{"bytes":5,"key":"scratch/tdx-hydro-publish/fixture/aux/snap_stems.parquet"},{"bytes":7,"key":"scratch/tdx-hydro-publish/fixture/build-report.json"},{"bytes":11,"key":"scratch/tdx-hydro-publish/fixture/catchments.parquet"},{"bytes":6,"key":"scratch/tdx-hydro-publish/fixture/graph.parquet"},{"bytes":3,"key":"scratch/tdx-hydro-publish/fixture/manifest.json"}],"prefix":"scratch/tdx-hydro-publish/fixture"}' >"$test_tmp/expected-publication-projection.json"
diff -u "$test_tmp/expected-publication-projection.json" "$test_tmp/publication-projection.json"
: >"$aws_log"
rm -f "$HFX_TEST_AWS_CONTROL/list-count"
PATH=$strict_bin:$PATH HFX_TEST_AWS_MODE=paginated \
    run_runner publish --campaign publish --workspace-root "$publish_root" \
        --out "$publish_fixture/assembled" --report "$publish_fixture/assembled-report.json" \
        --notice "$publish_fixture/NOTICE" --citation "$publish_fixture/CITATION.txt" \
        --scratch-prefix "$HFX_TEST_AWS_PREFIX" >"$case_stdout"
[[ $(wc -l <"$aws_log" | tr -d ' ') == 4 ]] || die 'paginated publication vector count differs'
[[ $(grep -c -- '--continuation-token fixture-page-2' "$aws_log") == 2 ]] ||
    die 'paginated publication continuation vectors differ'
[[ $(grep -c ' s3 cp ' "$aws_log" || :) == 0 ]] || die 'paginated rerun uploaded an object'
{
    printf '%s s3api list-objects-v2 --bucket pourpoint-hfx --prefix scratch/tdx-hydro-publish/fixture/ --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --output json --no-paginate\n' "$strict_bin/aws"
    printf '%s s3api list-objects-v2 --bucket pourpoint-hfx --prefix scratch/tdx-hydro-publish/fixture/ --continuation-token fixture-page-2 --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --output json --no-paginate\n' "$strict_bin/aws"
    printf '%s s3api list-objects-v2 --bucket pourpoint-hfx --prefix scratch/tdx-hydro-publish/fixture/ --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --output json --no-paginate\n' "$strict_bin/aws"
    printf '%s s3api list-objects-v2 --bucket pourpoint-hfx --prefix scratch/tdx-hydro-publish/fixture/ --continuation-token fixture-page-2 --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --output json --no-paginate\n' "$strict_bin/aws"
} >"$test_tmp/expected-paginated-aws.log"
diff -u "$test_tmp/expected-paginated-aws.log" "$aws_log"
pass 'the shipped aws default branch publishes the exact seven-object opaque fixture and verifies it with two exact listings'

resume_root=$test_tmp/workspaces/resume
mkdir "$resume_root"
set -- $(init_args resume "$resume_root")
run_runner "$@" >"$case_stdout"
resume_dir=$resume_root/tdx-hydro-resume
export HFX_TEST_AWS_PREFIX=scratch/tdx-hydro-resume/fixture
export HFX_TEST_AWS_REMOTE=$test_tmp/aws-remote-resume
export HFX_TEST_AWS_CONTROL=$test_tmp/aws-control-resume
mkdir "$HFX_TEST_AWS_REMOTE" "$HFX_TEST_AWS_CONTROL"
: >"$aws_log"
resume_status=0
HFX_TDX_AWS=$strict_bin/aws HFX_TEST_AWS_FAIL_KEY=NOTICE \
    run_runner publish --campaign resume --workspace-root "$resume_root" \
        --out "$publish_fixture/assembled" --report "$publish_fixture/assembled-report.json" \
        --notice "$publish_fixture/NOTICE" --citation "$publish_fixture/CITATION.txt" \
        --scratch-prefix "$HFX_TEST_AWS_PREFIX" >"$case_stdout" 2>"$case_stderr" || resume_status=$?
[[ "$resume_status" -ne 0 ]] || die 'one-time publication failure unexpectedly succeeded'
rm -f "$HFX_TEST_AWS_CONTROL/list-count"
HFX_TDX_AWS=$strict_bin/aws \
    run_runner publish --campaign resume --workspace-root "$resume_root" \
        --out "$publish_fixture/assembled" --report "$publish_fixture/assembled-report.json" \
        --notice "$publish_fixture/NOTICE" --citation "$publish_fixture/CITATION.txt" \
        --scratch-prefix "$HFX_TEST_AWS_PREFIX" >"$case_stdout"
[[ $(grep -c 'resume/fixture/CITATION.txt' "$aws_log") == 1 ]] ||
    die 'resume re-uploaded the completed first object'
[[ $(grep -c ' s3 cp .*resume/fixture/NOTICE ' "$aws_log") == 2 ]] ||
    die 'resume did not retry the one-time failed NOTICE upload exactly once'
for suffix in aux/snap_stems.parquet build-report.json catchments.parquet graph.parquet manifest.json; do
    [[ $(grep -c " s3 cp .*resume/fixture/$suffix " "$aws_log") == 1 ]] ||
        die "resume upload count differs for $suffix"
done
: >"$aws_log"
rm -f "$HFX_TEST_AWS_CONTROL/list-count"
HFX_TDX_AWS=$strict_bin/aws \
    run_runner publish --campaign resume --workspace-root "$resume_root" \
        --out "$publish_fixture/assembled" --report "$publish_fixture/assembled-report.json" \
        --notice "$publish_fixture/NOTICE" --citation "$publish_fixture/CITATION.txt" \
        --scratch-prefix "$HFX_TEST_AWS_PREFIX" >"$case_stdout"
[[ $(grep -c ' s3api list-objects-v2 ' "$aws_log") == 2 ]] || die 'converged resume listing count differs'
[[ $(grep -c ' s3 cp ' "$aws_log" || :) == 0 ]] || die 'converged resume uploaded an object'
pass 'the HFX_TDX_AWS override resumes after failure and converges without duplicate uploads'

for guard_mode in extra wrong-initial post-wrong; do
    case $guard_mode in
        extra) guard_campaign=pubextra ;;
        wrong-initial) guard_campaign=pubwrong ;;
        post-wrong) guard_campaign=pubpost ;;
    esac
    guard_root=$test_tmp/workspaces/$guard_campaign
    mkdir "$guard_root"
    set -- $(init_args "$guard_campaign" "$guard_root")
    run_runner "$@" >"$case_stdout"
    export HFX_TEST_AWS_PREFIX=scratch/tdx-hydro-$guard_campaign/fixture
    export HFX_TEST_AWS_REMOTE=$test_tmp/aws-remote-$guard_campaign
    export HFX_TEST_AWS_CONTROL=$test_tmp/aws-control-$guard_campaign
    mkdir "$HFX_TEST_AWS_REMOTE" "$HFX_TEST_AWS_CONTROL"
    if [[ "$guard_mode" == wrong-initial ]]; then
        mkdir -p "$HFX_TEST_AWS_REMOTE/$HFX_TEST_AWS_PREFIX"
        cp "$publish_fixture/CITATION.txt" "$HFX_TEST_AWS_REMOTE/$HFX_TEST_AWS_PREFIX/CITATION.txt"
    fi
    : >"$aws_log"
    if HFX_TDX_AWS=$strict_bin/aws HFX_TEST_AWS_MODE=$guard_mode \
        run_runner publish --campaign "$guard_campaign" --workspace-root "$guard_root" \
            --out "$publish_fixture/assembled" --report "$publish_fixture/assembled-report.json" \
            --notice "$publish_fixture/NOTICE" --citation "$publish_fixture/CITATION.txt" \
            --scratch-prefix "$HFX_TEST_AWS_PREFIX" >"$case_stdout" 2>"$case_stderr"; then
        die "$guard_mode remote inventory unexpectedly succeeded"
    fi
    if [[ "$guard_mode" != post-wrong ]]; then
        [[ $(grep -c ' s3 cp ' "$aws_log" || :) == 0 ]] ||
            die "$guard_mode refusal uploaded an object"
    fi
done
pass 'initial extra or wrong-size inventory and post-upload size mismatch are rejected'

diff -u "$test_tmp/adapter-before-new-paths" "$HFX_TEST_ADAPTER_LOG"
diff -u "$test_tmp/hfx-before-new-paths" "$HFX_TEST_HFX_LOG"
if grep -F 'never-publish' "$aws_log" >/dev/null; then
    die 'publication exposed the per-basin canary'
fi
pass 'publication never invokes assembly and never consults adapter or HFX'

pipeline_root=$test_tmp/workspaces/pipeline
mkdir "$pipeline_root"
set -- $(reclaim_subset_init_args pipeline "$pipeline_root")
run_runner "$@" >"$case_stdout"
pipeline_dir=$pipeline_root/tdx-hydro-pipeline
export HFX_TEST_PIPELINE_CAMPAIGN_DIR=$pipeline_dir
export HFX_TEST_PIPELINE_COMPLETION_PATH=$pipeline_dir/state/tmp/pipeline-completions.fifo
expect_failure 'pipeline missing max parallel' pipeline --campaign pipeline \
    --workspace-root "$pipeline_root" --fabric-version fixture-v1
expect_failure 'pipeline missing fabric version' pipeline --campaign pipeline \
    --workspace-root "$pipeline_root" --max-parallel 2
for invalid_parallel in 0 x 5; do
    expect_failure "pipeline invalid max parallel $invalid_parallel" pipeline --campaign pipeline \
        --workspace-root "$pipeline_root" --max-parallel "$invalid_parallel" \
        --fabric-version fixture-v1
done
expect_failure 'pipeline repeated max parallel' pipeline --campaign pipeline \
    --workspace-root "$pipeline_root" --max-parallel 2 --max-parallel 2 \
    --fabric-version fixture-v1
expect_failure 'pipeline repeated fabric version' pipeline --campaign pipeline \
    --workspace-root "$pipeline_root" --max-parallel 2 \
    --fabric-version fixture-v1 --fabric-version fixture-v1
expect_failure 'pipeline control fabric version' pipeline --campaign pipeline \
    --workspace-root "$pipeline_root" --max-parallel 2 --fabric-version $'bad\nversion'
expect_failure 'pipeline option ownership max' status --campaign pipeline \
    --workspace-root "$pipeline_root" --max-parallel 2
assert_contains "$case_stderr" 'option --max-parallel is valid only for acquire, pipeline, or calibrate'
expect_failure 'pipeline option ownership fabric' status --campaign pipeline \
    --workspace-root "$pipeline_root" --fabric-version fixture-v1
assert_contains "$case_stderr" \
    'option --fabric-version is valid only for compile, compile-basin, pipeline, or calibrate'
retain_pipeline_root=$test_tmp/workspaces/pipeline-retain
mkdir "$retain_pipeline_root"
set -- $(subset_init_args retainpipe "$retain_pipeline_root")
run_runner "$@" >"$case_stdout"
expect_failure 'pipeline retain-all policy' pipeline --campaign retainpipe \
    --workspace-root "$retain_pipeline_root" --max-parallel 2 --fabric-version fixture-v1
assert_contains "$case_stderr" 'pipeline requires retention policy reclaim-inputs-after-terminal'
[[ ! -e "$retain_pipeline_root/tdx-hydro-retainpipe/state/pipeline.json" ]] ||
    die 'retain-all pipeline refusal created scheduler state'
ln -s "$pipeline_dir" "$pipeline_root/tdx-hydro-symlinkpipe"
expect_failure 'pipeline symlinked campaign' pipeline --campaign symlinkpipe \
    --workspace-root "$pipeline_root" --max-parallel 2 --fabric-version fixture-v1
assert_contains "$case_stderr" \
    "hfx: error: campaign does not exist safely: $pipeline_root/tdx-hydro-symlinkpipe"
[[ ! -e "$pipeline_dir/state/locks/campaign.lock" ]] ||
    die 'symlinked pipeline campaign reached lock creation'
nonexistent_tool=$test_tmp/does-not-exist
for missing_pipeline_tool in HFX_TDX_CURL HFX_TDX_SHA256SUM HFX_TDX_OD HFX_TDX_OGRINFO; do
    pipeline_status=0
    env "$missing_pipeline_tool=$nonexistent_tool" \
        "$selected_bash" "$runner" pipeline --campaign pipeline \
        --workspace-root "$pipeline_root" --max-parallel 2 \
        --fabric-version fixture-v1 >"$case_stdout" 2>"$case_stderr" ||
        pipeline_status=$?
    [[ "$pipeline_status" -ne 0 ]] || die 'missing pipeline tool unexpectedly succeeded'
    assert_contains "$case_stderr" "required command '$nonexistent_tool' is not available"
done
pipeline_status=0
HFX_TEST_PIPELINE_AVAILABLE_BYTES=0 \
run_runner pipeline --campaign pipeline --workspace-root "$pipeline_root" \
    --max-parallel 2 --fabric-version fixture-v1 \
    >"$case_stdout" 2>"$case_stderr" || pipeline_status=$?
[[ "$pipeline_status" -ne 0 ]] || die 'case 88 projected-capacity accepted pending termination'
grep -F 'pipeline_durable_unreclaimed=3' "$case_stdout" >/dev/null ||
    die 'case 89 disk refusal pre-state bounded refusal differs'
assert_contains "$case_stderr" \
    'pipeline incomplete: pending=3 acquiring=0 ready=0 compiling=0 terminal=0 reclaimed=0 blocked=0'
[[ ! -e "$pipeline_dir/state/locks/campaign.lock" ]] ||
    die 'pipeline left the campaign lock behind'
sed -n '/^pipeline_campaign() {$/,/^}$/p' "$runner" >"$test_tmp/pipeline-coordinator"
assert_contains "$test_tmp/pipeline-coordinator" 'pipeline_ordered_sweep'
assert_contains "$test_tmp/pipeline-coordinator" 'pipeline_completion_create_open'
assert_contains "$test_tmp/pipeline-coordinator" '( pipeline_acquisition_worker "$basin_id" ) &'
grep -F 'compile_basin_locked "$pipeline_consumer_basin_id" true' "$test_tmp/pipeline-coordinator" >/dev/null ||
    die 'case 88 projected-capacity serial consumer is absent'
assert_contains "$test_tmp/pipeline-coordinator" 'pipeline_sweep_vanished_workers'
pass 'pipeline refuses pre-state errors and installs scheduler machinery'

pipeline_state=$pipeline_dir/state/pipeline.json
jq -e '
  keys == ["basin_ids","basins","fabric_version","max_parallel","schema_version"] and
  .schema_version == 1 and .fabric_version == "fixture-v1" and .max_parallel == 2 and
  .basin_ids == ["1020000010","7020000010","9020000010"] and
  ([.basins[] | keys == ["blocked_reason","status"] and
    .status == "pending" and .blocked_reason == null] | all)
' "$pipeline_state" >/dev/null || die 'materialized pipeline document differs'
[[ $(stat -f '%Lp' "$pipeline_state") == 644 ]] || die 'pipeline state mode differs'
cp "$pipeline_state" "$test_tmp/pipeline-stable"
HFX_TEST_PIPELINE_AVAILABLE_BYTES=0 expect_failure 'identical pipeline replay remains incomplete' pipeline --campaign pipeline \
    --workspace-root "$pipeline_root" --max-parallel 2 --fabric-version fixture-v1
cmp "$test_tmp/pipeline-stable" "$pipeline_state"
expect_failure 'pipeline fabric drift' pipeline --campaign pipeline \
    --workspace-root "$pipeline_root" --max-parallel 2 --fabric-version fixture-v2
assert_contains "$case_stderr" 'pipeline parameters changed; use a new campaign ID'
HFX_TEST_PIPELINE_AVAILABLE_BYTES=0 expect_failure 'pipeline max reduction remains incomplete' pipeline --campaign pipeline \
    --workspace-root "$pipeline_root" --max-parallel 1 --fabric-version fixture-v1
jq -e '.max_parallel == 1' "$pipeline_state" >/dev/null ||
    die 'pipeline max-parallel reduction was not persisted'
for pipeline_mutation in malformed extra-key bad-status excessive-acquiring; do
    mutation_root=$test_tmp/workspaces/pipeline-$pipeline_mutation
    mkdir "$mutation_root"
    cp -R "$pipeline_dir" "$mutation_root/tdx-hydro-pipeline"
    mutation_state=$mutation_root/tdx-hydro-pipeline/state/pipeline.json
    case $pipeline_mutation in
        malformed) printf '%s\n' '{' >"$mutation_state" ;;
        extra-key) jq '.extra=true' "$mutation_state" >"$mutation_state.tmp" && mv "$mutation_state.tmp" "$mutation_state" ;;
        bad-status) jq '.basins["1020000010"].status="unknown"' "$mutation_state" >"$mutation_state.tmp" && mv "$mutation_state.tmp" "$mutation_state" ;;
        excessive-acquiring)
            jq '.max_parallel=1 | .basins["1020000010"].status="acquiring" |
                .basins["7020000010"].status="acquiring"' "$mutation_state" \
                >"$mutation_state.tmp" && mv "$mutation_state.tmp" "$mutation_state"
            ;;
    esac
    expect_failure "pipeline validator $pipeline_mutation" progress --campaign pipeline \
        --workspace-root "$mutation_root"
    assert_contains "$case_stderr" 'pipeline state is malformed:'
done
definitions=$test_tmp/pipeline-definitions.sh
sed '/^if (($# == 1))/,$d' \
    "$runner" >"$definitions"
(
    # shellcheck source=/dev/null
    source "$definitions"
    JQ=$(command -v jq)
    MV=$(command -v mv)
    MKDIR=$(command -v mkdir)
    RM=$(command -v rm)
    CHMOD=$(command -v chmod)
    campaign_dir=$pipeline_dir
    lock_path=$pipeline_dir/state/locks/campaign.lock
    mkdir "$lock_path"
    printf '%s\n' "$$" >"$lock_path/owner.pid"
    lock_owned=1
    transition_pipeline_basin 1020000010 ready
)
jq -e '.basins["1020000010"] == {status:"ready",blocked_reason:null} and
  .basins["7020000010"].status == "pending"' "$pipeline_state" >/dev/null ||
    die 'parent transition did not change exactly one record'
rm -r "$pipeline_dir/state/locks/campaign.lock"
pass 'pipeline state is validated, atomic, replayable, and parent-owned'

mkdir "$pipeline_dir/state/locks/campaign.lock"
printf '%s\n' "$$" >"$pipeline_dir/state/locks/campaign.lock/owner.pid"
cp -R "$pipeline_dir" "$test_tmp/pipeline-progress-before"
run_runner progress --campaign pipeline --workspace-root "$pipeline_root" >"$case_stdout"
inputs_line=$(grep -n '^inputs_reclaimed=' "$case_stdout" | cut -d: -f1)
pipeline_line=$(grep -n '^pipeline_max_parallel=' "$case_stdout" | cut -d: -f1)
assembly_line=$(grep -n '^assemble_pending=' "$case_stdout" | cut -d: -f1)
[[ "$pipeline_line" -eq "$((inputs_line + 1))" && "$assembly_line" -eq "$((pipeline_line + 9))" ]] ||
    die 'pipeline progress field ordering differs'
for scheduler_status in pending acquiring ready compiling terminal reclaimed blocked; do
    assert_contains "$case_stdout" "pipeline_${scheduler_status}="
done
diff -ru "$test_tmp/pipeline-progress-before" "$pipeline_dir"
rm -r "$pipeline_dir/state/locks/campaign.lock"
for basin_id in 1020000010 7020000010 9020000010; do
    basin_state=$pipeline_dir/state/basins/$basin_id/current.json
    jq --arg id "$basin_id" '
      .stages.acquire_basins={status:"succeeded",attempts:1,failure_reason:null,
        evidence:{bytes:1,sha256:("0"*64),sqlite_identity:"53514c69746520666f726d6174203300",layer_name:($id+"_basins")}} |
      .stages.acquire_streamnet={status:"succeeded",attempts:1,failure_reason:null,
        evidence:{bytes:1,sha256:("0"*64),sqlite_identity:"53514c69746520666f726d6174203300",layer_name:($id+"_streamnet")}} |
      .stages.compile={status:"failed",attempts:1,failure_reason:"adapter build failed",diagnostic_report:null} |
      .retention.inputs_reclaimed=true
    ' "$basin_state" >"$basin_state.tmp"
    mv "$basin_state.tmp" "$basin_state"
done
run_runner pipeline --campaign pipeline --workspace-root "$pipeline_root" \
    --max-parallel 1 --fabric-version fixture-v1 >"$case_stdout"
assert_contains "$case_stdout" 'pipeline_durable_unreclaimed=0'
jq -e '[.basins[].status == "reclaimed"] | all' "$pipeline_state" >/dev/null ||
    die 'durable reclaimed state did not replace stale scheduler records'
cp "$pipeline_state" "$test_tmp/pipeline-authoritative-stable"
run_runner pipeline --campaign pipeline --workspace-root "$pipeline_root" \
    --max-parallel 1 --fabric-version fixture-v1 >"$case_stdout"
cmp "$test_tmp/pipeline-authoritative-stable" "$pipeline_state"
pass 'pipeline progress is lock-free and durable basin state stays authoritative'

recovery_state_fixture() {
    recovery_id=$1
    recovery_acquire_status=$2
    recovery_acquire_reason=$3
    recovery_compile_status=$4
    recovery_compile_attempts=$5
    recovery_compile_reason=$6
    recovery_reclaimed=$7
    recovery_state=$pipeline_dir/state/basins/$recovery_id/current.json
    jq --arg id "$recovery_id" --arg acquire_status "$recovery_acquire_status" \
        --arg acquire_reason "$recovery_acquire_reason" \
        --arg compile_status "$recovery_compile_status" \
        --argjson compile_attempts "$recovery_compile_attempts" \
        --arg compile_reason "$recovery_compile_reason" --argjson reclaimed "$recovery_reclaimed" '
      def evidence($product): {
        bytes:1,sha256:("0"*64),sqlite_identity:"53514c69746520666f726d6174203300",
        layer_name:($id+"_"+$product)
      };
      .schema_version=4 |
      .retention={policy:"reclaim-inputs-after-terminal",inputs_reclaimed:$reclaimed} |
      .stages.acquire_basins={
        status:$acquire_status,attempts:1,
        failure_reason:(if $acquire_reason == "" then null else $acquire_reason end),
        evidence:(if $acquire_status == "succeeded" then evidence("basins") else null end)
      } |
      .stages.acquire_streamnet={
        status:"succeeded",attempts:1,failure_reason:null,evidence:evidence("streamnet")
      } |
      .stages.compile={
        status:$compile_status,attempts:$compile_attempts,
        failure_reason:(if $compile_reason == "" then null else $compile_reason end),
        diagnostic_report:null
      }
    ' "$recovery_state" >"$recovery_state.tmp"
    mv "$recovery_state.tmp" "$recovery_state"
}

for recovery_id in 1020000010 7020000010 9020000010; do
    jq --arg id "$recovery_id" '.basins[$id]={status:"pending",blocked_reason:null}' \
        "$pipeline_state" >"$pipeline_state.tmp"
    mv "$pipeline_state.tmp" "$pipeline_state"
done
jq '.basins["9020000010"].status="compiling"' "$pipeline_state" >"$pipeline_state.tmp"
mv "$pipeline_state.tmp" "$pipeline_state"
recovery_state_fixture 1020000010 succeeded '' failed 1 'adapter build failed' true
recovery_state_fixture 7020000010 succeeded '' failed 1 'adapter build failed' false
recovery_state_fixture 9020000010 succeeded '' failed 0 \
    'acquisition prerequisites are not both succeeded' false
for recovery_product in basins streamnet; do
    touch "$pipeline_dir/downloads/7020000010-$recovery_product.gpkg" \
        "$pipeline_dir/downloads/7020000010-$recovery_product.gpkg.partial" \
        "$pipeline_dir/downloads/7020000010-$recovery_product.gpkg.partial.json"
done
sed >"$test_tmp/recovery-rm" <<'RECOVERY_RM'
#!/bin/sh
set -eu
for recovery_path do
    case $recovery_path in
        */downloads/7020000010-*)
            "${HFX_TEST_REAL_JQ:?}" -e '
              .basins["7020000010"].status == "terminal"
            ' "${HFX_TEST_RECOVERY_DIR:?}/state/pipeline.json" >/dev/null
            "${HFX_TEST_REAL_JQ:?}" -e '
              .retention.inputs_reclaimed == false
            ' "${HFX_TEST_RECOVERY_DIR:?}/state/basins/7020000010/current.json" >/dev/null
            printf '%s\n' "$recovery_path" >>"${HFX_TEST_RECOVERY_RM_LOG:?}"
            ;;
    esac
done
exec "${HFX_TEST_REAL_RM:?}" "$@"
RECOVERY_RM
chmod +x "$test_tmp/recovery-rm"
export HFX_TEST_RECOVERY_DIR=$pipeline_dir
export HFX_TEST_RECOVERY_RM_LOG=$test_tmp/recovery-rm.log
HFX_TEST_REAL_RM=$(command -v rm)
export HFX_TEST_REAL_RM
export HFX_TDX_RM=$test_tmp/recovery-rm
: >"$HFX_TEST_ADAPTER_LOG"
(
    # shellcheck source=/dev/null
    source "$definitions"
    JQ=$(command -v jq)
    MV=$(command -v mv)
    MKDIR=$(command -v mkdir)
    RM=$HFX_TDX_RM
    CHMOD=$(command -v chmod)
    campaign_dir=$pipeline_dir
    lock_path=$pipeline_dir/state/locks/campaign.lock
    mkdir "$lock_path"
    printf '%s\n' "$$" >"$lock_path/owner.pid"
    lock_owned=1
    pipeline_recover_reconstruct_basin 1020000010
    pipeline_recover_reconstruct_basin 7020000010
    pipeline_recover_reconstruct_basin 9020000010
)
rm -r "$pipeline_dir/state/locks/campaign.lock"
unset HFX_TDX_RM
jq -e '
  .basins["1020000010"].status == "reclaimed" and
  .basins["7020000010"].status == "reclaimed" and
  .basins["9020000010"].status == "ready"
' "$pipeline_state" >/dev/null || die 'ordered rule 1, rule 2, and rule 5 results differ'
[[ ! -s "$HFX_TEST_ADAPTER_LOG" ]] || die 'eligible recovery invoked the adapter'
[[ $(wc -l <"$HFX_TEST_RECOVERY_RM_LOG" | tr -d ' ') -eq 6 ]] ||
    die 'source removal was not observed after the terminal transition'
for recovery_product in basins streamnet; do
    for recovery_suffix in .gpkg .gpkg.partial .gpkg.partial.json; do
        [[ ! -e "$pipeline_dir/downloads/7020000010-$recovery_product$recovery_suffix" ]] ||
            die 'rule 2 left a source path'
    done
done
definitions=$test_tmp/pipeline-recovery-definitions.sh
sed '/^if (($# == 1))/,$d' "$runner" >"$definitions"
cp "$pipeline_dir/state/basins/7020000010/current.json" "$test_tmp/recovery-helper-state"
for recovery_reason in \
    'acquisition report is unsafe or malformed; retained for inspection' \
    'existing final file failed integrity verification; retained for inspection' \
    'persisted evidence does not match final file; retained for inspection' \
    'installed final failed integrity verification; retained for inspection'; do
    jq --arg reason "$recovery_reason" \
        '.stages.acquire_basins.failure_reason=$reason' \
        "$test_tmp/recovery-helper-state" >"$pipeline_dir/state/basins/7020000010/current.json"
    (
        # shellcheck source=/dev/null
        source "$definitions"
        JQ=$(command -v jq)
        campaign_dir=$pipeline_dir
        [[ $(reclaim_eligibility 7020000010) == ineligible ]]
    ) || die "shared eligibility accepted excluded reason: $recovery_reason"
done
cp "$test_tmp/recovery-helper-state" "$pipeline_dir/state/basins/7020000010/current.json"
while IFS='|' read -r recovery_class recovery_reason; do
    [[ -n "$recovery_class" ]] || continue
    if [[ "$recovery_reason" == 'interrupted before terminal state; reset by recover' ]]; then
        recovery_state_fixture 1020000010 pending "$recovery_reason" pending 0 '' false
    else
        recovery_state_fixture 1020000010 failed "$recovery_reason" pending 0 '' false
    fi
    (
        # shellcheck source=/dev/null
        source "$definitions"
        JQ=$(command -v jq)
        MV=$(command -v mv)
        MKDIR=$(command -v mkdir)
        RM=$(command -v rm)
        CHMOD=$(command -v chmod)
        campaign_dir=$pipeline_dir
        lock_path=$pipeline_dir/state/locks/campaign.lock
        mkdir "$lock_path"
        printf '%s\n' "$$" >"$lock_path/owner.pid"
        lock_owned=1
        pipeline_recover_reconstruct_basin 1020000010
    )
    rm -r "$pipeline_dir/state/locks/campaign.lock"
    recovery_actual=$(jq -r '.basins["1020000010"].status' "$pipeline_state")
    [[ "$recovery_actual" == "$recovery_class" ]] ||
        die "acquisition reason classified $recovery_actual instead of $recovery_class: $recovery_reason"
    if [[ "$recovery_class" == blocked ]]; then
        [[ $(jq -r '.basins["1020000010"].blocked_reason' "$pipeline_state") == "$recovery_reason" ]] ||
            die "blocked acquisition reason changed: $recovery_reason"
    fi
done <<'RECOVERY_ACQUISITION_TABLE'
pending|interrupted before terminal state; reset by recover
pending|transfer interrupted; retry from byte zero
pending|download provenance or size verification failed
pending|download failed integrity verification
blocked|acquisition report is unsafe or malformed; retained for inspection
blocked|existing final file failed integrity verification; retained for inspection
blocked|persisted evidence does not match final file; retained for inspection
blocked|partial path is unsafe; retained without traversal
blocked|installed final failed integrity verification; retained for inspection
RECOVERY_ACQUISITION_TABLE
recovery_state_fixture 1020000010 failed 'unknown acquisition failure' pending 0 '' false
(
    # shellcheck source=/dev/null
    source "$definitions"
    JQ=$(command -v jq)
    campaign_dir=$pipeline_dir
    lock_path=$pipeline_dir/state/locks/campaign.lock
    mkdir "$lock_path"
    printf '%s\n' "$$" >"$lock_path/owner.pid"
    lock_owned=1
    pipeline_recover_reconstruct_basin 1020000010
) 2>"$case_stderr" && die 'unknown acquisition reason unexpectedly succeeded'
rm -r "$pipeline_dir/state/locks/campaign.lock"
assert_contains "$case_stderr" 'pipeline acquisition classifier rejected failure reason'
recovery_state_fixture 1020000010 running '' pending 0 '' false
(
    # shellcheck source=/dev/null
    source "$definitions"
    JQ=$(command -v jq)
    campaign_dir=$pipeline_dir
    classify_pipeline_acquisition_stage 1020000010 acquire_basins
) >"$case_stdout" 2>"$case_stderr" &&
    die 'residual running acquisition reached rule 8'
assert_contains "$case_stderr" 'pipeline acquisition classifier found residual running stage'
recovery_state_fixture 1020000010 succeeded '' failed 1 'adapter build failed' true
sed -n '/^compile_basin_core() {$/,/^}$/p' "$runner" >"$test_tmp/recovery-core"
sed -n '/^compile_basin_locked() {$/,/^}$/p' "$runner" >"$test_tmp/recovery-locked"
[[ $(grep -Fc '[[ "$defer_reclaim" == false ]]' "$test_tmp/recovery-core") -eq 5 ]] ||
    die 'deferred reclaim does not gate exactly five core sites'
assert_contains "$test_tmp/recovery-locked" 'compile_basin_core "$basin_id" "$defer_reclaim"'
assert_contains "$test_tmp/recovery-core" 'if reconcile_reclaim_basin "$basin_id" true; then'
assert_contains "$test_tmp/recovery-locked" 'if reconcile_reclaim_basin "$basin_id" true; then'
recovery_case_83_passed=1

recovery_state_fixture 1020000010 failed 'transfer interrupted; retry from byte zero' pending 0 '' false
recovery_state_fixture 7020000010 failed \
    'acquisition report is unsafe or malformed; retained for inspection' pending 0 '' false
recovery_state_fixture 9020000010 succeeded '' pending 1 '' false
for recovery_id in 1020000010 7020000010; do
    for recovery_product in basins streamnet; do
        touch "$pipeline_dir/downloads/$recovery_id-$recovery_product.gpkg"
    done
done
: >"$HFX_TEST_ADAPTER_LOG"
git -C "$repo_root" status --porcelain=v1 >"$test_tmp/recovery-status-before"
(
    # shellcheck source=/dev/null
    source "$definitions"
    JQ=$(command -v jq)
    MV=$(command -v mv)
    MKDIR=$(command -v mkdir)
    RM=$(command -v rm)
    CHMOD=$(command -v chmod)
    campaign_dir=$pipeline_dir
    lock_path=$pipeline_dir/state/locks/campaign.lock
    mkdir "$lock_path"
    printf '%s\n' "$$" >"$lock_path/owner.pid"
    lock_owned=1
    for recovery_id in 1020000010 7020000010 9020000010; do
        pipeline_recover_reconstruct_basin "$recovery_id"
    done
)
rm -r "$pipeline_dir/state/locks/campaign.lock"
jq -e '
  .basins["1020000010"].status == "pending" and
  .basins["7020000010"] == {
    status:"blocked",
    blocked_reason:"acquisition report is unsafe or malformed; retained for inspection"
  } and
  .basins["9020000010"] == {
    status:"blocked",
    blocked_reason:"interrupted compile attempt cannot be safely repeated; retained for inspection"
  }
' "$pipeline_state" >/dev/null || die 'nonterminal and positive-attempt recovery results differ'
HFX_TEST_PIPELINE_AVAILABLE_BYTES=0 expect_failure \
    'blocked recovery remains incomplete' pipeline --campaign pipeline \
    --workspace-root "$pipeline_root" --max-parallel 1 --fabric-version fixture-v1
jq -e '.basins["7020000010"].status == "blocked"' "$pipeline_state" >/dev/null ||
    die 'blocked basin was relabelled by bounded incomplete termination'
[[ $(grep -c '^build' "$HFX_TEST_ADAPTER_LOG" || :) -eq 0 ]] ||
    die 'positive-attempt recovery repeated a build'
for recovery_id in 1020000010 7020000010; do
    for recovery_product in basins streamnet; do
        [[ -f "$pipeline_dir/downloads/$recovery_id-$recovery_product.gpkg" ]] ||
            die 'acquisition recovery removed a retained final'
    done
done
for recovery_id in 1020000010 7020000010 9020000010; do
    recovery_state_fixture "$recovery_id" succeeded '' failed 1 'adapter build failed' true
    rm -f "$pipeline_dir/downloads/$recovery_id-basins.gpkg" \
        "$pipeline_dir/downloads/$recovery_id-streamnet.gpkg"
done
run_runner pipeline --campaign pipeline --workspace-root "$pipeline_root" \
    --max-parallel 1 --fabric-version fixture-v1 >"$case_stdout"
cp -R "$pipeline_dir" "$test_tmp/recovery-replay-before"
run_runner pipeline --campaign pipeline --workspace-root "$pipeline_root" \
    --max-parallel 1 --fabric-version fixture-v1 >"$case_stdout"
diff -ru "$test_tmp/recovery-replay-before" "$pipeline_dir"
git -C "$repo_root" status --porcelain=v1 >"$test_tmp/recovery-status-after"
diff -u "$test_tmp/recovery-status-before" "$test_tmp/recovery-status-after"
recovery_case_84_passed=1

guard_dir=$test_tmp/pipeline-guards
mkdir -p "$guard_dir/downloads" "$guard_dir/state/tmp"
for readme_contract in \
    'pipeline occupancy invariant exceeded: observed <actual> pairs;' \
    'insufficient pipeline dispatch disk: available <actual> bytes; required' \
    'state/tmp/pipeline-completions.fifo' \
    '( pipeline_acquisition_worker "$basin_id" ) &' \
    '5 * 8,859,344,896 = 44,296,724,480 peak input bytes'; do
    assert_contains "$SCRIPT_DIR/README.md" "$readme_contract"
done
sed -n '/^The authored 600 GB reclaim model is:$/,/^| `560,000,000,000` usable bytes/p' \
    "$SCRIPT_DIR/README.md" >"$test_tmp/current-authored-model"
current_authored_hash=$(shasum -a 256 "$test_tmp/current-authored-model" | awk '{print $1}')
[[ "$current_authored_hash" == 1dc7d7e7574f6e56b980d8f888865b991dbafbb39c1c0a6266187b71c508813b ]] ||
    die 'authored 600 GB reclaim model changed'
cp "$repo_root/adapters/tdx-hydro/data/tdx_header_numbers.json" "$guard_dir/state/inventory.json"
guard_ids=()
while IFS= read -r guard_id; do
    guard_ids[${#guard_ids[@]}]=$guard_id
done < <(jq -r 'keys[0:6][]' "$guard_dir/state/inventory.json")
[[ ${#guard_ids[@]} -eq 6 ]] || die 'guard fixture does not contain six authoritative IDs'
jq -n --args '{schema_version:1,basin_ids:$ARGS.positional,
  basins:($ARGS.positional|map({key:.,value:{status:"pending",blocked_reason:null}})|from_entries),
  fabric_version:"fixture-v1",max_parallel:4}' -- \
  ${guard_ids[@]+"${guard_ids[@]}"} >"$guard_dir/state/pipeline.json"
jq -n --args '{schema_version:1,basin_ids:$ARGS.positional}' -- \
  ${guard_ids[@]+"${guard_ids[@]}"} >"$guard_dir/state/selection.json"
(
    # shellcheck source=/dev/null
    source "$definitions"
    JQ=$(command -v jq)
    RM=$(command -v rm)
    ADAPTER_PYTHON=$test_tmp/fake-adapter-python
    campaign_dir=$guard_dir
    export HFX_TEST_PIPELINE_CAMPAIGN_DIR=$campaign_dir
    export HFX_TEST_PIPELINE_COMPLETION_PATH=$campaign_dir/state/tmp/pipeline-completions.fifo
    set_pipeline_status() {
        "$JQ" --arg id "$1" --arg status "$2" '.basins[$id].status=$status' \
            "$campaign_dir/state/pipeline.json" >"$campaign_dir/state/tmp/p.json"
        mv "$campaign_dir/state/tmp/p.json" "$campaign_dir/state/pipeline.json"
    }
    clear_guard_sources() {
        rm -f "$campaign_dir"/downloads/*
    }
    suffixes=(basins.gpkg basins.gpkg.partial basins.gpkg.partial.json
        streamnet.gpkg streamnet.gpkg.partial streamnet.gpkg.partial.json)
    for suffix in ${suffixes[@]+"${suffixes[@]}"}; do
        clear_guard_sources
        path=$campaign_dir/downloads/${guard_ids[0]}-$suffix
        if [[ "$suffix" == streamnet.gpkg.partial.json ]]; then
            ln -s "$campaign_dir/missing-target" "$path"
        else
            : >"$path"
        fi
        pipeline_has_physical_pair "${guard_ids[0]}" ||
            die "physical predicate missed $suffix"
        pipeline_count_occupancy
        [[ "$pipeline_occupancy_count" -eq 1 ]] ||
            die "physical path counted incorrectly: $suffix"
    done
    clear_guard_sources
    for status in pending acquiring ready compiling terminal reclaimed blocked; do
        set_pipeline_status "${guard_ids[0]}" "$status"
        if [[ "$status" == acquiring || "$status" == ready ||
              "$status" == compiling || "$status" == terminal ]]; then
            pipeline_basin_occupies_pair "${guard_ids[0]}" ||
                die "reserving status omitted: $status"
        elif pipeline_basin_occupies_pair "${guard_ids[0]}"; then
            die "nonreserving status included: $status"
        fi
    done
    set_pipeline_status "${guard_ids[0]}" blocked
    : >"$campaign_dir/downloads/${guard_ids[0]}-basins.gpkg.partial"
    pipeline_basin_occupies_pair "${guard_ids[0]}" ||
        die 'source-retaining blocked basin was omitted'
    clear_guard_sources
    ! pipeline_basin_occupies_pair "${guard_ids[0]}" ||
        die 'path-free blocked basin was included'
    : >"$campaign_dir/downloads/${guard_ids[0]}-basins.gpkg"
    : >"$campaign_dir/downloads/${guard_ids[0]}-streamnet.gpkg.partial"
    set_pipeline_status "${guard_ids[0]}" ready
    pipeline_count_occupancy
    [[ "$pipeline_occupancy_count" -eq 1 ]] ||
        die 'multiple paths and reserving status counted one ID more than once'
    clear_guard_sources
    for index in 0 1 2 3 4 5; do
        set_pipeline_status "${guard_ids[$index]}" pending
    done
    for index in 0 1 2 3 4; do
        set_pipeline_status "${guard_ids[$index]}" acquiring
    done
    pipeline_count_occupancy
    [[ "$pipeline_occupancy_count" -eq 5 ]] || die 'current occupancy five differs'
    union_status=0
    ( set_pipeline_status "${guard_ids[5]}" ready; pipeline_count_occupancy ) \
        >"$case_stdout" 2>"$case_stderr" || union_status=$?
    [[ "$union_status" -eq 1 ]] || die 'current occupancy six was not fatal'
    [[ $(<"$case_stderr") == 'hfx: error: pipeline occupancy invariant exceeded: observed 6 pairs; maximum 5' ]] ||
        die 'current occupancy-six diagnostic differs'
    set_pipeline_status "${guard_ids[5]}" pending
    set_pipeline_status "${guard_ids[4]}" pending
    pipeline_projected_occupancy_allows "${guard_ids[5]}" ||
        die 'projected occupancy five was rejected'
    set_pipeline_status "${guard_ids[4]}" acquiring
    projected_status=0
    pipeline_projected_occupancy_allows "${guard_ids[5]}" \
        >"$case_stdout" 2>"$case_stderr" || projected_status=$?
    projected_survived=1
    [[ "$projected_status" -eq 1 && ! -s "$case_stdout" && ! -s "$case_stderr" &&
       "$projected_survived" -eq 1 ]] || die 'projected occupancy six contract differs'
    pipeline_projected_occupancy_allows "${guard_ids[0]}" ||
        die 'already-occupied proposal added another pair'
    for index in 0 1 2 3 4; do
        set_pipeline_status "${guard_ids[$index]}" pending
    done
    for pair_count in 0 1 2 3 4 5; do
        clear_guard_sources
        for ((index = 0; index < pair_count; index++)); do
            : >"$campaign_dir/downloads/${guard_ids[$index]}-basins.gpkg"
            : >"$campaign_dir/downloads/${guard_ids[$index]}-streamnet.gpkg"
        done
        required=$((44296724480 - pair_count * 8859344896))
        HFX_TEST_PIPELINE_AVAILABLE_BYTES=$required pipeline_dispatch_disk_guard ||
            die "disk equality rejected for $pair_count pairs"
        if ((pair_count < 5)); then
            disk_status=0
            HFX_TEST_PIPELINE_AVAILABLE_BYTES=$((required - 1)) \
                pipeline_dispatch_disk_guard >"$case_stdout" 2>"$case_stderr" ||
                disk_status=$?
            disk_survived=1
            [[ "$disk_status" -eq 1 && ! -s "$case_stdout" &&
               "$disk_survived" -eq 1 ]] || die 'disk shortfall contract differs'
            [[ $(<"$case_stderr") == "insufficient pipeline dispatch disk: available $((required - 1)) bytes; required $required bytes" ]] ||
                die 'disk shortfall diagnostic differs'
        fi
    done
    clear_guard_sources
    : >"$campaign_dir/downloads/${guard_ids[0]}-basins.gpkg.partial"
    disk_status=0
    HFX_TEST_PIPELINE_AVAILABLE_BYTES=44296724479 \
        pipeline_dispatch_disk_guard >"$case_stdout" 2>"$case_stderr" ||
        disk_status=$?
    [[ "$disk_status" -eq 1 ]] || die 'partial-only basin received disk credit'
    clear_guard_sources
    : >"$campaign_dir/downloads/${guard_ids[0]}-basins.gpkg"
    disk_status=0
    HFX_TEST_PIPELINE_AVAILABLE_BYTES=44296724479 \
        pipeline_dispatch_disk_guard >"$case_stdout" 2>"$case_stderr" ||
        disk_status=$?
    [[ "$disk_status" -eq 1 ]] || die 'one-final basin received disk credit'
    for bad_probe in command-failure empty negative nonnumeric multiline overflow; do
        probe_status=0
        if [[ "$bad_probe" == command-failure ]]; then
            ( ADAPTER_PYTHON=false; pipeline_available_bytes ) \
                >"$case_stdout" 2>"$case_stderr" || probe_status=$?
        else
            case $bad_probe in
                empty) probe_value= ;;
                negative) probe_value=-1 ;;
                nonnumeric) probe_value=x ;;
                multiline) probe_value=$'1\n2' ;;
                overflow) probe_value=9223372036854775808 ;;
            esac
            ( HFX_TEST_PIPELINE_AVAILABLE_BYTES=$probe_value; export HFX_TEST_PIPELINE_AVAILABLE_BYTES
              pipeline_available_bytes ) >"$case_stdout" 2>"$case_stderr" ||
                probe_status=$?
        fi
        [[ "$probe_status" -eq 1 && ! -s "$case_stdout" ]] ||
            die "statvfs $bad_probe was not fatal"
        [[ $(<"$case_stderr") == 'hfx: error: pipeline statvfs probe failed: expected one nonnegative signed-64-bit byte count' ]] ||
            die "statvfs $bad_probe diagnostic differs"
    done
    disk_status=0
    HFX_TEST_PIPELINE_AVAILABLE_BYTES=00000000000000000001 \
        pipeline_dispatch_disk_guard >"$case_stdout" 2>"$case_stderr" ||
        disk_status=$?
    [[ "$disk_status" -eq 1 ]] || die 'leading-zero probe unexpectedly sufficed'
    assert_contains "$case_stderr" 'available 1 bytes'
)
pass 'pipeline guard counts conservative pairs and exact disk headroom'

completion_dir=$test_tmp/pipeline-completion
mkdir -p "$completion_dir/state/tmp"
completion_path=$completion_dir/state/tmp/pipeline-completions.fifo
(
    # shellcheck source=/dev/null
    source "$definitions"
    RM=$(command -v rm)
    ADAPTER_PYTHON=$test_tmp/fake-adapter-python
    campaign_dir=$completion_dir
    export HFX_TEST_PIPELINE_CAMPAIGN_DIR=$campaign_dir
    export HFX_TEST_PIPELINE_COMPLETION_PATH=$completion_path
    pipeline_completion_create_open
    [[ -p "$completion_path" && ! -L "$completion_path" ]] ||
        die 'completion path is not a safe named pipe'
    [[ $(stat -f '%Lp' "$completion_path") == 600 ]] ||
        die 'completion path mode differs'
    printf '1020000010\t0\n' >&9
    read_status=0
    IFS=$'\t' read -r -u 9 -t 1 completion_id completion_status ||
        read_status=$?
    [[ "$read_status" -eq 0 && "$completion_id" == 1020000010 &&
       "$completion_status" == 0 ]] || die 'bounded completion record differs'
    pipeline_completion_close_remove
    [[ ! -e "$completion_path" && ! -L "$completion_path" ]] ||
        die 'completion path was not removed'
    mkfifo "$completion_path"
    pipeline_completion_create_open
    pipeline_completion_close_remove
    for unsafe_type in file directory symlink; do
        target=$completion_dir/state/tmp/target
        printf preserved >"$target"
        case $unsafe_type in
            file) printf preserved >"$completion_path" ;;
            directory) mkdir "$completion_path" ;;
            symlink) ln -s "$target" "$completion_path" ;;
        esac
        unsafe_status=0
        ( pipeline_completion_create_open ) >"$case_stdout" 2>"$case_stderr" ||
            unsafe_status=$?
        [[ "$unsafe_status" -eq 1 ]] || die "unsafe $unsafe_type path was accepted"
        [[ $(<"$case_stderr") == "hfx: error: unsafe pipeline completion path: $completion_path; expected a non-symlink named pipe" ]] ||
            die "unsafe $unsafe_type diagnostic differs"
        [[ $(<"$target") == preserved ]] || die "unsafe $unsafe_type traversed"
        case $unsafe_type in
            file) [[ -f "$completion_path" && $(<"$completion_path") == preserved ]] ||
                die 'unsafe file changed' ;;
            directory) [[ -d "$completion_path" ]] || die 'unsafe directory changed' ;;
            symlink) [[ -L "$completion_path" ]] || die 'unsafe symlink changed' ;;
        esac
        case $unsafe_type in
            directory) rmdir "$completion_path" ;;
            *) rm "$completion_path" ;;
        esac
        rm "$target"
    done
)
pass 'pipeline completion path is safe, mode 0600, and bounded'

run_trap_matrix() {
    local interpreter=$1
    local expected_version=$2
    local observed
    local matrix_dir=$3
    observed=$("$interpreter" -c 'printf %s "$BASH_VERSION"')
    if [[ "$expected_version" == 5.x ]]; then
        [[ "$observed" == 5.* ]] ||
            die "Bash-5 trap interpreter mismatch: observed $observed"
    else
        [[ "$observed" == "$expected_version" ]] ||
            die "Bash-3.2 trap interpreter mismatch: observed $observed"
    fi
    mkdir -p "$matrix_dir"
    matrix_status=0
    "$interpreter" -c 'f(){ trap '\''printf fired >"$1"'\'' EXIT; :; }; f "$1" & p=$!; s=0; wait "$p" || s=$?; exit "$s"' \
        matrix "$matrix_dir/bare" || matrix_status=$?
    [[ "$matrix_status" -eq 0 ]] || die "$observed bare background wait failed"
    if [[ "$expected_version" == 5.x ]]; then
        [[ $(<"$matrix_dir/bare") == fired ]] ||
            die "$observed bare background did not fire EXIT"
    else
        [[ ! -e "$matrix_dir/bare" ]] ||
            die "$observed bare background unexpectedly fired EXIT"
    fi
    matrix_status=0
    "$interpreter" -c 'f(){ trap '\''printf fired >"$1"'\'' EXIT; :; }; ( f "$1" ) & p=$!; s=0; wait "$p" || s=$?; exit "$s"' \
        matrix "$matrix_dir/subshell" || matrix_status=$?
    [[ "$matrix_status" -eq 0 && $(<"$matrix_dir/subshell") == fired ]] ||
        die "$observed explicit subshell did not fire EXIT"
    local_status=0
    "$interpreter" -c 'set -eu; f(){ local record=$1; trap '\''printf fired >"$record"'\'' EXIT; :; }; f "$1"' \
        matrix "$matrix_dir/local" >/dev/null 2>&1 || local_status=$?
    [[ "$local_status" -eq 1 && ! -e "$matrix_dir/local" ]] ||
        die "$observed local trap contract differs"
}

run_trap_matrix /bin/bash '3.2.57(1)-release' "$test_tmp/trap-32"
if [[ -x /opt/homebrew/bin/bash ]]; then
    run_trap_matrix /opt/homebrew/bin/bash 5.x "$test_tmp/trap-5"
else
    printf '%s\n' 'test-tdx-hydro-campaign: SKIP: case 87 Bash-5 empirical arm unavailable' >&2
fi
worker_dir=$test_tmp/pipeline-worker
mkdir -p "$worker_dir/state/tmp"
for stub_status in 0 7; do
    worker_shell_status=0
    HFX_TEST_DEFINITIONS=$definitions \
    HFX_TEST_PIPELINE_CAMPAIGN_DIR=$worker_dir \
    HFX_TEST_PIPELINE_COMPLETION_PATH=$worker_dir/state/tmp/pipeline-completions.fifo \
    HFX_TEST_WORKER_ID=${guard_ids[0]} HFX_TEST_WORKER_STATUS=$stub_status \
    HFX_TEST_FAKE_PYTHON=$test_tmp/fake-adapter-python \
    /bin/bash -c '
      set -Eeuo pipefail
      IFS=$'\''\n\t'\''
      source "$HFX_TEST_DEFINITIONS"
      RM=$(command -v rm)
      ADAPTER_PYTHON=$HFX_TEST_FAKE_PYTHON
      campaign_dir=$HFX_TEST_PIPELINE_CAMPAIGN_DIR
      acquire_basin(){ return "$HFX_TEST_WORKER_STATUS"; }
      pipeline_completion_create_open
      worker_status=0
      ( pipeline_acquisition_worker "$HFX_TEST_WORKER_ID" ) || worker_status=$?
      read_status=0
      IFS=$'\''\t'\'' read -r -u 9 -t 1 record_id record_status || read_status=$?
      printf "%s\t%s\t%s\t%s\n" "$record_id" "$record_status" "$worker_status" "$read_status"
      pipeline_completion_close_remove
      exit "$worker_status"
    ' >"$case_stdout" 2>"$case_stderr" || worker_shell_status=$?
    [[ "$worker_shell_status" -eq "$stub_status" ]] ||
        die "worker shell status differs for stub $stub_status"
    [[ $(<"$case_stdout") == "${guard_ids[0]}"$'\t'"$stub_status"$'\t'"$stub_status"$'\t0' ]] ||
        die "worker record differs for stub $stub_status"
done
pass 'pipeline worker notification obeys Bash 3.2 trap contracts'

scheduler_init() {
    scheduler_root=$1 scheduler_name=$2
    shift 2
    mkdir "$scheduler_root"
    scheduler_args=(init --campaign "$scheduler_name" --workspace-root "$scheduler_root")
    for scheduler_id do scheduler_args[${#scheduler_args[@]}]=--basin
        scheduler_args[${#scheduler_args[@]}]=$scheduler_id
    done
    scheduler_args+=(--retention-policy reclaim-inputs-after-terminal
        --available-memory-bytes 30000000000 --available-disk-bytes 491737129060
        --peak-in-flight-download-bytes 44296724480 --retained-basin-output-bytes 206220202290
        --assembly-memory-ceiling-bytes 30000000000 --assembly-scratch-ceiling-bytes 206220202290
        --assembled-artifact-bytes 206220202290 --active-compile-scratch-bytes 30000000000
        --filesystem-overhead-bytes 5000000000)
    run_runner ${scheduler_args[@]+"${scheduler_args[@]}"} >"$case_stdout"
}
scheduler_ids=(1020000010 2020000010 3020000010 4020000010 5020000010 7020000010)

scheduler_init "$test_tmp/workspaces/scheduler88" scheduler88 \
    ${scheduler_ids[@]+"${scheduler_ids[@]}"}
scheduler88=$test_tmp/workspaces/scheduler88/tdx-hydro-scheduler88
cp -R "$scheduler88" "$test_tmp/scheduler88-pristine"
rm -r "$test_tmp/transfer-state"; mkdir "$test_tmp/transfer-state"
run_runner acquire --campaign scheduler88 --workspace-root "$test_tmp/workspaces/scheduler88" \
    --max-parallel 4 >"$case_stdout"
for scheduler_index in 2 3 4 5; do
    scheduler_id=${scheduler_ids[$scheduler_index]}
    cp "$test_tmp/scheduler88-pristine/state/basins/$scheduler_id/current.json" \
        "$scheduler88/state/basins/$scheduler_id/current.json"
    rm -f "$scheduler88/downloads/$scheduler_id-"*
    rm -f "$scheduler88/reports/$scheduler_id-basins-acquisition.json" \
        "$scheduler88/reports/$scheduler_id-streamnet-acquisition.json"
done
export HFX_TEST_PIPELINE_CAMPAIGN_DIR=$scheduler88
export HFX_TEST_PIPELINE_COMPLETION_PATH=$scheduler88/state/tmp/pipeline-completions.fifo
export HFX_TEST_BARRIER_COUNT=4
export HFX_TEST_PIPELINE_BUILD_ACTIVE_LOG=$test_tmp/scheduler88-build-active
: >"$HFX_TEST_ADAPTER_LOG"
run_runner pipeline --campaign scheduler88 --workspace-root "$test_tmp/workspaces/scheduler88" \
    --max-parallel 4 --fabric-version fixture-v1 >"$case_stdout" ||
    die 'case 88 projected-capacity pipeline failed'
unset HFX_TEST_BARRIER_COUNT HFX_TEST_PIPELINE_BUILD_ACTIVE_LOG
jq -e '[.basins[] | .status == "reclaimed" and .blocked_reason == null] | all' \
    "$scheduler88/state/pipeline.json" >/dev/null ||
    die 'case 88 projected-capacity final statuses differ'
[[ $(<"$test_tmp/transfer-state/maximum") -eq 4 &&
   $(grep -c '^build' "$HFX_TEST_ADAPTER_LOG") -eq 6 &&
   $(grep -c '^validate' "$HFX_TEST_ADAPTER_LOG") -eq 12 ]] ||
    die "case 88 projected-capacity concurrency or compile counts differ: max=$(<"$test_tmp/transfer-state/maximum") build=$(grep -c '^build' "$HFX_TEST_ADAPTER_LOG" || :) validate=$(grep -c '^validate' "$HFX_TEST_ADAPTER_LOG" || :)"
awk '$1=="build-active" && $3>0 {found=1} END{exit !found}' \
    "$test_tmp/scheduler88-build-active" ||
    die 'case 88 projected-capacity compile did not overlap acquisition'
[[ ! -e "$HFX_TEST_PIPELINE_COMPLETION_PATH" &&
   ! -e "$scheduler88/state/locks/campaign.lock" ]] ||
    die 'case 88 projected-capacity cleanup differs'
(
    # shellcheck source=/dev/null
    source "$definitions"
    max_parallel=4
    pipeline_worker_pids=(11 12 13)
    ! pipeline_parallel_limit_reached
    pipeline_worker_pids[3]=14
    pipeline_parallel_limit_reached
) || die 'case 88 projected-capacity parallel equality differs'
scheduler_init "$test_tmp/workspaces/scheduler88fail" scheduler88fail 1020000010
export HFX_TEST_PIPELINE_CAMPAIGN_DIR=$test_tmp/workspaces/scheduler88fail/tdx-hydro-scheduler88fail
export HFX_TEST_PIPELINE_COMPLETION_PATH=$HFX_TEST_PIPELINE_CAMPAIGN_DIR/state/tmp/pipeline-completions.fifo
rm -r "$test_tmp/transfer-state"; mkdir "$test_tmp/transfer-state"
pipeline_status=0
HFX_TEST_TRANSFER_SHAPE=truncated HFX_TEST_TRANSFER_SHAPE_KEY=1020000010-basins \
run_runner pipeline --campaign scheduler88fail \
    --workspace-root "$test_tmp/workspaces/scheduler88fail" --max-parallel 1 \
    --fabric-version fixture-v1 >"$case_stdout" 2>"$case_stderr" || pipeline_status=$?
[[ "$pipeline_status" -ne 0 ]] &&
    jq -e '.stages.acquire_basins.attempts == 2' \
        "$HFX_TEST_PIPELINE_CAMPAIGN_DIR/state/basins/1020000010/current.json" >/dev/null ||
    die 'case 88 projected-capacity dispatch exhaustion differs'
assert_contains "$case_stderr" 'pipeline incomplete: pending=1 acquiring=0 ready=0 compiling=0 terminal=0 reclaimed=0 blocked=0'
jq -e '.basins["1020000010"].status == "pending"' "$HFX_TEST_PIPELINE_CAMPAIGN_DIR/state/pipeline.json" >/dev/null ||
    die 'case 88 projected-capacity exhausted basin changed state'
pass 'case 88 projected-capacity'

scheduler_init "$test_tmp/workspaces/scheduler89" scheduler89 1020000010 2020000010
scheduler89=$test_tmp/workspaces/scheduler89/tdx-hydro-scheduler89
cp -R "$scheduler89" "$test_tmp/scheduler89-pristine"
rm -r "$test_tmp/transfer-state"; mkdir "$test_tmp/transfer-state"
run_runner acquire --campaign scheduler89 --workspace-root "$test_tmp/workspaces/scheduler89" \
    --max-parallel 2 >"$case_stdout"
cp "$test_tmp/scheduler89-pristine/state/basins/2020000010/current.json" \
    "$scheduler89/state/basins/2020000010/current.json"
rm -f "$scheduler89/downloads/2020000010-"*
rm -f "$scheduler89/reports/2020000010-basins-acquisition.json" \
    "$scheduler89/reports/2020000010-streamnet-acquisition.json"
export HFX_TEST_PIPELINE_CAMPAIGN_DIR=$scheduler89
export HFX_TEST_PIPELINE_COMPLETION_PATH=$scheduler89/state/tmp/pipeline-completions.fifo
sequence=$test_tmp/scheduler89-sequence
printf '%s\n' 35437379583 44296724480 >"$sequence"
export HFX_TEST_PIPELINE_AVAILABLE_SEQUENCE=$sequence
: >"$HFX_TEST_ADAPTER_LOG"
run_runner pipeline --campaign scheduler89 --workspace-root "$test_tmp/workspaces/scheduler89" \
    --max-parallel 4 --fabric-version fixture-v1 >"$case_stdout" 2>"$case_stderr" ||
    die 'case 89 disk refusal pipeline failed'
unset HFX_TEST_PIPELINE_AVAILABLE_SEQUENCE
[[ ! -s "$sequence" ]] || die 'case 89 disk refusal guard-call count differs'
assert_contains "$case_stderr" 'insufficient pipeline dispatch disk: available 35437379583 bytes; required 35437379584 bytes'
jq -e '[.basins[].status == "reclaimed"] | all' "$scheduler89/state/pipeline.json" >/dev/null ||
    die 'case 89 disk refusal final statuses differ'
pass 'case 89 disk refusal'

scheduler_init "$test_tmp/workspaces/scheduler-no-progress" scheduler-no-progress 1020000010
scheduler_no_progress=$test_tmp/workspaces/scheduler-no-progress/tdx-hydro-scheduler-no-progress
export HFX_TEST_PIPELINE_CAMPAIGN_DIR=$scheduler_no_progress
export HFX_TEST_PIPELINE_COMPLETION_PATH=$scheduler_no_progress/state/tmp/pipeline-completions.fifo
no_progress_sequence=$test_tmp/scheduler-no-progress-sequence
printf '%s\n' 0 >"$no_progress_sequence"
export HFX_TEST_PIPELINE_AVAILABLE_SEQUENCE=$no_progress_sequence
pipeline_status=0
run_runner pipeline --campaign scheduler-no-progress \
    --workspace-root "$test_tmp/workspaces/scheduler-no-progress" \
    --max-parallel 1 --fabric-version fixture-v1 >"$case_stdout" 2>"$case_stderr" ||
    pipeline_status=$?
unset HFX_TEST_PIPELINE_AVAILABLE_SEQUENCE
[[ "$pipeline_status" -ne 0 && ! -s "$no_progress_sequence" ]] ||
    die 'no-progress round did not terminate after one refused dispatch'
assert_contains "$case_stderr" \
    'pipeline incomplete: pending=1 acquiring=0 ready=0 compiling=0 terminal=0 reclaimed=0 blocked=0'
jq -e '.basins["1020000010"] == {status:"pending",blocked_reason:null}' \
    "$scheduler_no_progress/state/pipeline.json" >/dev/null ||
    die 'no-progress termination changed the recoverable pending state'
pass 'pipeline no-progress round terminates through the bounded final-state check'

scheduler_init "$test_tmp/workspaces/scheduler90" scheduler90 1020000010 2020000010
scheduler90=$test_tmp/workspaces/scheduler90/tdx-hydro-scheduler90
export HFX_TEST_PIPELINE_CAMPAIGN_DIR=$scheduler90
export HFX_TEST_PIPELINE_COMPLETION_PATH=$scheduler90/state/tmp/pipeline-completions.fifo
export HFX_TEST_PIPELINE_KILL_KEY=1020000010-basins
export HFX_TEST_KILLED_BASIN_ID=1020000010
export HFX_TEST_KILLED_CURRENT_PATH=$scheduler90/state/basins/1020000010/current.json
export HFX_TEST_KILLED_PENDING_MARKER=$test_tmp/scheduler90-pending
export HFX_TEST_STALE_CURRENT_PATH=$scheduler90/state/basins/2020000010/current.json
export HFX_TEST_STALE_READY_MARKER=$test_tmp/scheduler90-stale-ready
export HFX_TEST_REAL_MV=$(command -v mv)
export HFX_TDX_MV=$test_tmp/pipeline-mv
rm -r "$test_tmp/transfer-state"; mkdir "$test_tmp/transfer-state"
export HFX_TEST_FAIL_ONCE_KEY=2020000010-basins
export HFX_TEST_REDISPATCH_HOLD_KEY=2020000010-basins
export HFX_TEST_REDISPATCH_RELEASE_MARKER=$test_tmp/scheduler90-redispatch-release
run_runner pipeline --campaign scheduler90 --workspace-root "$test_tmp/workspaces/scheduler90" \
    --max-parallel 2 --fabric-version fixture-v1 >"$case_stdout" ||
    die 'case 90 vanished worker and queued completion pipeline failed'
unset HFX_TEST_PIPELINE_KILL_KEY HFX_TDX_MV HFX_TEST_STALE_CURRENT_PATH \
    HFX_TEST_STALE_READY_MARKER HFX_TEST_FAIL_ONCE_KEY HFX_TEST_REDISPATCH_HOLD_KEY \
    HFX_TEST_REDISPATCH_RELEASE_MARKER
[[ -f "$HFX_TEST_KILLED_PENDING_MARKER" &&
   -f "$test_tmp/scheduler90-stale-ready" &&
   $(grep -c '^start 1020000010-basins' "$test_tmp/transfer-state/events") -eq 2 &&
   $(grep -c '^start 2020000010-basins' "$test_tmp/transfer-state/events") -eq 2 ]] ||
    die 'case 90 vanished worker, queued completion, or retry differs'
jq -e '[.basins[].status == "reclaimed"] | all' "$scheduler90/state/pipeline.json" >/dev/null ||
    die 'case 90 vanished worker queued-completion final statuses differ'
helper_state=$scheduler90/state/basins/1020000010/current.json
jq '.retention.inputs_reclaimed=false |
  .stages.acquire_basins={status:"running",attempts:1,failure_reason:null,evidence:null} |
  .stages.acquire_streamnet={status:"pending",attempts:0,failure_reason:null,evidence:null} |
  .stages.compile={status:"pending",attempts:0,failure_reason:null,diagnostic_report:null}' \
    "$helper_state" >"$helper_state.tmp"
mv "$helper_state.tmp" "$helper_state"
jq '.basins["1020000010"]={status:"acquiring",blocked_reason:null}' \
    "$scheduler90/state/pipeline.json" >"$scheduler90/state/pipeline.json.tmp"
mv "$scheduler90/state/pipeline.json.tmp" "$scheduler90/state/pipeline.json"
export HFX_TEST_KILLED_PENDING_MARKER=$test_tmp/scheduler90-helper-pending
(
    ( exit 0 ) & helper_pid=$!
    # shellcheck source=/dev/null
    source "$definitions"
    JQ=$(command -v jq); MV=$test_tmp/pipeline-mv
    MKDIR=$(command -v mkdir); RM=$(command -v rm); CHMOD=$(command -v chmod)
    campaign_dir=$scheduler90
    lock_path=$scheduler90/state/locks/campaign.lock
    mkdir "$lock_path"; printf '%s\n' "$$" >"$lock_path/owner.pid"; lock_owned=1
    pipeline_selected_basin_ids=(1020000010)
    pipeline_swept_completion_statuses=("")
    pipeline_worker_pids=("$helper_pid")
    pipeline_worker_basin_ids=(1020000010)
    pipeline_round_reaped=0
    pipeline_sweep_vanished_workers
    ((${#pipeline_worker_pids[@]} == 0))
    [[ -f "$HFX_TEST_KILLED_PENDING_MARKER" ]]
) || die 'case 90 vanished SIGKILL isolated liveness recovery differs'
rm -r "$scheduler90/state/locks/campaign.lock"
(
    # shellcheck source=/dev/null
    source "$definitions"
    pipeline_read_timed_out 1 && pipeline_read_timed_out 142 &&
        pipeline_read_timed_out 37 && ! pipeline_read_timed_out 0
) || die 'case 90 vanished SIGKILL timeout classification differs'
pass 'case 90 sweep tolerates an already-accounted queued completion'

scheduler_init "$test_tmp/workspaces/scheduler91" scheduler91 1020000010 2020000010
scheduler91=$test_tmp/workspaces/scheduler91/tdx-hydro-scheduler91
grep -F '        kill -TERM "$cleanup_pid" 2>/dev/null || :' "$runner" >/dev/null || die 'case 91 fatal cleanup child TERM is absent'
grep -F '        wait "$cleanup_pid" || cleanup_wait_status=$?' "$runner" >/dev/null || die 'case 91 fatal cleanup child wait is absent'
export HFX_TEST_PIPELINE_CAMPAIGN_DIR=$scheduler91
export HFX_TEST_PIPELINE_COMPLETION_PATH=$scheduler91/state/tmp/pipeline-completions.fifo
export HFX_TEST_FATAL_HOLD=1 HFX_TEST_FATAL_INJECT_KEY=1020000010-basins
export HFX_TEST_FATAL_FIFO=$HFX_TEST_PIPELINE_COMPLETION_PATH
export HFX_TEST_FATAL_LOCK=$scheduler91/state/locks/campaign.lock
export HFX_TEST_FATAL_CLEANUP_MARKER=$test_tmp/scheduler91-cleanup
export HFX_TEST_REAL_RM=$(command -v rm)
export HFX_TDX_RM=$test_tmp/pipeline-rm
rm -r "$test_tmp/transfer-state"; mkdir "$test_tmp/transfer-state"
pipeline_status=0
run_runner pipeline --campaign scheduler91 --workspace-root "$test_tmp/workspaces/scheduler91" \
    --max-parallel 2 --fabric-version fixture-v1 >"$case_stdout" 2>"$case_stderr" ||
    pipeline_status=$?
[[ "$pipeline_status" -ne 0 && $(<"$HFX_TEST_FATAL_CLEANUP_MARKER") == children-reaped-with-lock ]] ||
    die 'case 91 fatal cleanup ordering differs'
assert_contains "$case_stderr" 'malformed pipeline completion record: expected selected basin ID and exit status'
unset HFX_TEST_FATAL_HOLD HFX_TDX_RM HFX_TEST_FATAL_FIFO
run_runner pipeline --campaign scheduler91 --workspace-root "$test_tmp/workspaces/scheduler91" \
    --max-parallel 2 --fabric-version fixture-v1 >"$case_stdout" ||
    die 'case 91 fatal cleanup recovery replay failed'
jq -e '[.basins[].status == "reclaimed"] | all' "$scheduler91/state/pipeline.json" >/dev/null ||
    die 'case 91 fatal cleanup replay statuses differ'
pass 'case 91 fatal cleanup'

for accepted_version in 3.2.0 '3.2.57(1)-release' 4.0 10.1.2; do
    bash_version_at_least_3_2 "$accepted_version" ||
        die "Bash floor predicate rejected $accepted_version"
done
for rejected_version in 3.1.99 2.05 '' x.2 3.x; do
    if bash_version_at_least_3_2 "$rejected_version"; then
        die "Bash floor predicate accepted $rejected_version"
    fi
done
if [[ -x /bin/bash ]]; then
    [[ "$selected_bash" == /bin/bash ]] || die 'the exercised interpreter is not /bin/bash'
fi
bash_version_at_least_3_2 "$selected_version" ||
    die "recorded selected Bash does not satisfy the floor: $selected_version"
sed -n '3,13p' "$runner" >"$test_tmp/runner-bash-floor"
assert_contains "$test_tmp/runner-bash-floor" 'Bash >=3.2 is required; observed non-Bash interpreter'
assert_contains "$test_tmp/runner-bash-floor" 'Bash >=3.2 is required; observed %s'
assert_contains "$test_tmp/runner-bash-floor" '[ "$bash_major" -lt 3 ]'
assert_contains "$test_tmp/runner-bash-floor" '[ "$bash_minor" -lt 2 ]'
pass 'the harness records its exercised Bash and rejects the insufficient-version fixtures'

logical_runner=$test_tmp/runner-logical-lines
sed ':join
/\\$/ {
    N
    s/\\\n/ /
    b join
}' "$runner" >"$logical_runner"
assert_zero_ere 'teardown script token' '(^|[[:space:]/"'\''])teardown[.]sh([[:space:];|"'\'']|$)' "$logical_runner"
assert_zero_ere 'hcloud token' 'hcloud' "$logical_runner" -i
assert_zero_ere 'standalone delete verb' '(^|[^[:alnum:]_])delete([^[:alnum:]_]|$)' "$logical_runner"
assert_zero_ere 'word-bounded ssh token' '(^|[^[:alnum:]_])ssh([^[:alnum:]_]|$)' "$logical_runner"
assert_zero_ere 'label selector' '(^|[[:space:]])(--selector|--label-selector)(=|[[:space:]"'\'']|$)' "$logical_runner"
grep -Fn -- 'grit-d8-m3' "$logical_runner" >"$case_stdout" || :
[[ $(wc -l <"$case_stdout" | tr -d ' ') -eq 0 ]] ||
    die "protected retained volume matched $(wc -l <"$case_stdout" | tr -d ' ') times"
assert_zero_ere 'absolute real service client' '/(usr/)?(local/)?bin/(ssh|hcloud|curl|aws)([[:space:]"'\'']|$)' "$logical_runner"
[[ $(grep -Fc -- 'list-objects-v2' "$runner") -eq 2 ]] ||
    die 'legitimate list-objects-v2 occurrence count differs'
[[ $(grep -Fc -- 'list_remote_inventory' "$runner") -eq 3 ]] ||
    die 'legitimate list_remote_inventory occurrence count differs'
[[ $(grep -Ec -- '[[:space:]]-name[[:space:]]+campaign[.]json' "$runner") -eq 1 ]] ||
    die 'legitimate local campaign marker find count differs'
pass 'the campaign runner has no teardown, destructive hcloud, resource sweep, or protected-volume reference'

hcloud_probe_invocations=$test_tmp/hcloud-probe-invocations
mkdir "$hcloud_probe_invocations"
hcloud_server_delete_status=0
HFX_TEST_INVOCATIONS=$hcloud_probe_invocations hcloud server delete 123 \
    >"$case_stdout" 2>"$case_stderr" || hcloud_server_delete_status=$?
hcloud_volume_delete_status=0
HFX_TEST_INVOCATIONS=$hcloud_probe_invocations hcloud volume delete 456 \
    >"$case_stdout" 2>"$case_stderr" || hcloud_volume_delete_status=$?
hcloud_list_status=0
HFX_TEST_INVOCATIONS=$hcloud_probe_invocations hcloud server list \
    >"$case_stdout" 2>"$case_stderr" || hcloud_list_status=$?
[[ "$hcloud_server_delete_status" -eq 96 ]] ||
    die "hcloud server delete poison exited $hcloud_server_delete_status instead of 96"
[[ "$hcloud_volume_delete_status" -eq 96 ]] ||
    die "hcloud volume delete poison exited $hcloud_volume_delete_status instead of 96"
[[ "$hcloud_list_status" -eq 97 ]] ||
    die "hcloud server list poison exited $hcloud_list_status instead of 97"
printf '%s\n' 'server delete 123' 'volume delete 456' >"$test_tmp/expected-hcloud-delete.log"
printf '%s\n' 'server list' >"$test_tmp/expected-hcloud.log"
diff -u "$test_tmp/expected-hcloud-delete.log" "$hcloud_probe_invocations/hcloud-delete.log"
diff -u "$test_tmp/expected-hcloud.log" "$hcloud_probe_invocations/hcloud.log"
for poison in hcloud curl aws ssh; do
    poison_path=$(command -v "$poison")
    [[ "$poison_path" == "$test_tmp/fake-bin/$poison" ]] ||
        die "$poison does not resolve to the intended PATH poison"
done
pass 'PATH poisons trap every hcloud delete verb and shadow all real service clients'

shellcheck_path=$(command -v shellcheck || true)
if [[ -n "$shellcheck_path" ]]; then
    "$shellcheck_path" -S warning -e SC2046 \
        "$SCRIPT_DIR/test-tdx-hydro-campaign.sh"
    pass 'shellcheck reports no warning-or-higher findings in the harness'
else
    skip 'shellcheck checks' 'shellcheck is unavailable'
fi

for poison in hcloud curl aws ssh; do
    [[ ! -e "$test_tmp/invocations/$poison.log" ]] || die "poison command was invoked: $poison"
done
[[ ! -e "$test_tmp/invocations/hcloud-delete.log" ]] ||
    die 'poison command was invoked: hcloud delete'
[[ -s "$aws_log" ]] || die 'deliberate strict fake AWS log is empty'
git -C "$repo_root" status --porcelain=v1 | sed '/^?? pr-body\.md$/d' >"$test_tmp/repository-status-final"
diff -u "$test_tmp/repository-status-before" "$test_tmp/repository-status-final"
pass 'poison commands remain uninvoked and repository status preserves only the allowed PR body'

[[ "${recovery_case_83_passed-}" == 1 ]] ||
    die 'pipeline recovery classification case did not complete'
pass 'pipeline recovery evaluates eight rules and preserves acquisition terminality'
[[ "${recovery_case_84_passed-}" == 1 ]] ||
    die 'pipeline recovery replay case did not complete'
pass 'pipeline recovery never reacquires or repeats a basin build and replay is stable'

calibration_cohort_case
calibration_measurement_case
calibration_replay_case
calibration_disclosure_case
calibration_scheduler_shape_case
checkpoint_schema_case
checkpoint_stop_case
checkpoint_progress_case

printf '1..%d\n' "$passed"
if [[ "$skipped" -eq 0 ]]; then
    printf 'test-tdx-hydro-campaign: all %d cases passed\n' "$passed"
else
    printf 'test-tdx-hydro-campaign: all %d cases completed (%d skipped)\n' "$passed" "$skipped"
fi
