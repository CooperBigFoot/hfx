#!/usr/bin/env bash

if [ -z "${BASH_VERSION-}" ]; then
    printf '%s\n' 'hfx: error: Bash >=3.2 is required; observed non-Bash interpreter' >&2
    exit 1
fi
bash_major=${BASH_VERSION%%.*}
bash_remainder=${BASH_VERSION#*.}
bash_minor=${bash_remainder%%.*}
if [ "$bash_major" -lt 3 ] || { [ "$bash_major" -eq 3 ] && [ "$bash_minor" -lt 2 ]; }; then
    printf 'hfx: error: Bash >=3.2 is required; observed %s\n' "$BASH_VERSION" >&2
    exit 1
fi

set -Eeuo pipefail
IFS=$'\n\t'
set +x

readonly HFX_TDX_INVENTORY_SOURCE=adapters/tdx-hydro/data/tdx_header_numbers.json
readonly HFX_TDX_MAX_I64=9223372036854775807
readonly HFX_TDX_SQLITE_MAGIC=53514c69746520666f726d6174203300
readonly HFX_TDX_DEFAULT_ADAPTER_PYTHON=/opt/hfx-geo/bin/python
readonly HFX_TDX_DEFAULT_HFX=/root/hfx/target/release/hfx
readonly HFX_TDX_MAX_LIST_PAGES=1000
# Reclaim mode reserves five copies of the largest measured source pair:
# 5 * (6,979,305,472 basins bytes + 1,880,039,424 streamnet bytes).
readonly HFX_TDX_RECLAIM_PAIR_COUNT=5
readonly HFX_TDX_RECLAIM_MAX_PARALLEL=4
readonly HFX_TDX_RECLAIM_PAIR_BYTES=8859344896
readonly HFX_TDX_RECLAIM_PEAK_BYTES=44296724480

# NGA acquisition contract for later slices:
# https://earth-info.nga.mil/php/download.php?file=<processing-basin-id>-<product>-gpkg
# The exact product set is {basins,streamnet}; this offline slice makes no request.

hfx_die() {
    local message
    local IFS=' '
    message="$*"
    printf 'hfx: error: %s\n' "$message" >&2
    exit 1
}

((HFX_TDX_RECLAIM_PAIR_COUNT * HFX_TDX_RECLAIM_PAIR_BYTES == HFX_TDX_RECLAIM_PEAK_BYTES)) ||
    hfx_die 'internal reclaim sizing constants are inconsistent'

hfx_log() {
    local message
    local IFS=' '
    message="$*"
    printf 'hfx: %s\n' "$message" >&2
}

usage() {
    printf '%s\n' \
        'Usage: tdx-hydro-campaign.sh init --campaign <id> [--workspace-root <path>] [--basin <processing-basin-id>]... [--retention-policy <retain-all-through-publication|reclaim-inputs-after-terminal>] --available-memory-bytes <integer> --available-disk-bytes <integer> (--retained-input-bytes <integer> | --peak-in-flight-download-bytes 44296724480) --retained-basin-output-bytes <integer> --assembly-memory-ceiling-bytes <integer> --assembly-scratch-ceiling-bytes <integer> --assembled-artifact-bytes <integer> --active-compile-scratch-bytes <integer> --filesystem-overhead-bytes <integer>' \
        '       tdx-hydro-campaign.sh status --campaign <id> [--workspace-root <path>]' \
        '       tdx-hydro-campaign.sh recover --campaign <id> [--workspace-root <path>]' \
        '       tdx-hydro-campaign.sh acquire --campaign <id> [--workspace-root <path>] --max-parallel <integer>' \
        '       tdx-hydro-campaign.sh compile --campaign <id> [--workspace-root <path>] --fabric-version <value>' \
        '       tdx-hydro-campaign.sh compile-basin --campaign <id> [--workspace-root <path>] --basin <processing-basin-id> --fabric-version <value>' \
        '       tdx-hydro-campaign.sh progress --campaign <id> [--workspace-root <path>]' \
        '       tdx-hydro-campaign.sh pipeline --campaign <id> [--workspace-root <path>] --max-parallel <integer> --fabric-version <value>' \
        '       tdx-hydro-campaign.sh assemble --campaign <id> [--workspace-root <path>]' \
        '       tdx-hydro-campaign.sh evidence --campaign <id> [--workspace-root <path>]' \
        '       tdx-hydro-campaign.sh publish --campaign <id> [--workspace-root <path>] --out <dataset-dir> --report <path> --notice <path> --citation <path> --scratch-prefix <prefix>'
}

usage_error() {
    usage >&2
    hfx_die "$@"
}

resolve_command() {
    local variable_name=$1
    local default_name=$2
    local requested
    eval "requested=\${$variable_name-\$default_name}"
    command -v -- "$requested" 2>/dev/null || hfx_die "required command '$requested' is not available"
}

validate_campaign() {
    [[ "$1" =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]] ||
        hfx_die "invalid campaign '$1'; use 1-32 lowercase letters, digits, or hyphens, starting with a letter or digit"
}

validate_workspace_root() {
    local path=$1
    [[ "$path" == /* ]] || hfx_die "workspace root must be absolute: $path"
    [[ -d "$path" ]] || hfx_die "workspace root is not an existing directory: $path"
    [[ ! -L "$path" ]] || hfx_die "workspace root final component must not be a symlink: $path"
    [[ -w "$path" ]] || hfx_die "workspace root is not writable: $path"
}

normalize_positive_i64() {
    local label=$1
    local value=$2
    local normalized=$value
    [[ "$value" =~ ^[0-9]+$ ]] || hfx_die "invalid $label '$value'; expected a positive base-10 byte count"
    while [[ ${#normalized} -gt 1 && ${normalized#0} != "$normalized" ]]; do
        normalized=${normalized#0}
    done
    [[ "$normalized" != 0 ]] || hfx_die "invalid $label '$value'; expected a value greater than zero"
    if [[ ${#normalized} -gt 19 ]] ||
        [[ ${#normalized} -eq 19 && "$normalized" > "$HFX_TDX_MAX_I64" ]]; then
        hfx_die "invalid $label '$value'; exceeds signed 64-bit range"
    fi
    printf '%s\n' "$normalized"
}

checked_add() {
    local total=$1
    local addend=$2
    if ((addend > HFX_TDX_MAX_I64 - total)); then
        hfx_die 'required disk byte sum overflows signed 64-bit range'
    fi
    printf '%s\n' "$((total + addend))"
}

validate_inventory_file() {
    local file=$1
    "$JQ" -e '
        type == "object" and
        length == 62 and
        (to_entries | all(
            (.key | test("^[0-9]{10}$")) and
            (.value | type == "string" and test("^[0-9]+$") and length > 0)
        ))
    ' "$file" >/dev/null 2>&1 || hfx_die "authoritative inventory is invalid: $file"
}

validate_selection_file() {
    local file=$1
    local inventory_file=${2-$campaign_dir/state/inventory.json}
    "$JQ" -e --slurpfile inventory "$inventory_file" '
        type == "object" and
        keys == ["basin_ids","schema_version"] and
        .schema_version == 1 and
        (.basin_ids | type == "array" and length > 0 and
            . == (sort | unique) and
            all(.[]; type == "string" and test("^[0-9]{10}$") and
                $inventory[0][.] != null))
    ' "$file" >/dev/null 2>&1 || hfx_die "basin selection state is malformed: $file"
}

effective_basin_ids() {
    if [[ -f "$campaign_dir/state/selection.json" && ! -L "$campaign_dir/state/selection.json" ]]; then
        "$JQ" -r '.basin_ids[]' "$campaign_dir/state/selection.json"
    else
        "$JQ" -r 'keys[]' "$campaign_dir/state/inventory.json"
    fi
}

validate_campaign_json() {
    local file=$1
    local policy
    local input_bytes
    local retained_output_bytes
    local active_scratch_bytes
    local assembly_scratch_bytes
    local artifact_bytes
    local overhead_bytes
    local assembly_peak_bytes
    local expected_disk_bytes=0
    local persisted_disk_bytes
    "$JQ" -e --arg campaign "$campaign" --argjson max_i64 "$HFX_TDX_MAX_I64" \
        --argjson reclaim_peak "$HFX_TDX_RECLAIM_PEAK_BYTES" '
        def positive_i64:
            type == "number" and . == floor and . > 0 and . <= $max_i64;
        type == "object" and
        (keys == ["campaign","inventory","retention","schema_version","sizing"]) and
        .schema_version == 2 and
        .campaign == $campaign and
        (.inventory | type == "object" and keys == ["count","source"] and
            .source == "adapters/tdx-hydro/data/tdx_header_numbers.json" and .count == 62) and
        if .retention.policy == "retain-all-through-publication" then
            (.retention == {
                policy: "retain-all-through-publication",
                reclaim_inputs: false,
                retain_acquired_inputs: true,
                retain_basin_outputs: true,
                retain_external_reports: true
            }) and
            (.sizing | type == "object" and
                keys == ["active_compile_scratch_bytes","assembled_artifact_bytes",
                         "assembly_memory_ceiling_bytes","assembly_scratch_ceiling_bytes",
                         "available_disk_bytes","available_memory_bytes","filesystem_overhead_bytes",
                         "required_disk_bytes","required_memory_bytes","retained_basin_output_bytes",
                         "retained_input_bytes"] and
                (to_entries | all(.value | positive_i64)))
        elif .retention.policy == "reclaim-inputs-after-terminal" then
            (.retention == {
                policy: "reclaim-inputs-after-terminal",
                reclaim_inputs: true,
                retain_acquired_inputs: false,
                retain_basin_outputs: true,
                retain_external_reports: true
            }) and
            (.sizing | type == "object" and
                keys == ["active_compile_scratch_bytes","assembled_artifact_bytes",
                         "assembly_memory_ceiling_bytes","assembly_scratch_ceiling_bytes",
                         "available_disk_bytes","available_memory_bytes","filesystem_overhead_bytes",
                         "peak_in_flight_download_bytes","required_disk_bytes","required_memory_bytes",
                         "retained_basin_output_bytes"] and
                (to_entries | all(.value | positive_i64)) and
                .peak_in_flight_download_bytes == $reclaim_peak)
        else false
        end and
        (.sizing.required_memory_bytes == .sizing.assembly_memory_ceiling_bytes) and
        (.sizing.available_memory_bytes >= .sizing.required_memory_bytes) and
        (.sizing.available_disk_bytes >= .sizing.required_disk_bytes)
    ' "$file" >/dev/null 2>&1 || hfx_die "campaign state is malformed: $file"

    policy=$("$JQ" -r '.retention.policy' "$file")
    case $policy in
        retain-all-through-publication)
            input_bytes=$("$JQ" -r '.sizing.retained_input_bytes' "$file")
            ;;
        reclaim-inputs-after-terminal)
            input_bytes=$("$JQ" -r '.sizing.peak_in_flight_download_bytes' "$file")
            ;;
        *) hfx_die "campaign state is malformed: $file" ;;
    esac
    retained_output_bytes=$("$JQ" -r '.sizing.retained_basin_output_bytes' "$file")
    active_scratch_bytes=$("$JQ" -r '.sizing.active_compile_scratch_bytes' "$file")
    assembly_scratch_bytes=$("$JQ" -r '.sizing.assembly_scratch_ceiling_bytes' "$file")
    artifact_bytes=$("$JQ" -r '.sizing.assembled_artifact_bytes' "$file")
    overhead_bytes=$("$JQ" -r '.sizing.filesystem_overhead_bytes' "$file")
    persisted_disk_bytes=$("$JQ" -r '.sizing.required_disk_bytes' "$file")
    assembly_peak_bytes=$assembly_scratch_bytes
    if ((artifact_bytes > assembly_peak_bytes)); then
        assembly_peak_bytes=$artifact_bytes
    fi
    expected_disk_bytes=$(checked_add "$expected_disk_bytes" "$input_bytes")
    expected_disk_bytes=$(checked_add "$expected_disk_bytes" "$retained_output_bytes")
    expected_disk_bytes=$(checked_add "$expected_disk_bytes" "$active_scratch_bytes")
    expected_disk_bytes=$(checked_add "$expected_disk_bytes" "$assembly_peak_bytes")
    expected_disk_bytes=$(checked_add "$expected_disk_bytes" "$overhead_bytes")
    [[ "$persisted_disk_bytes" == "$expected_disk_bytes" ]] ||
        hfx_die "campaign state is malformed: $file"
}

validate_compile_json() {
    local file=$1
    "$JQ" -e '
        type == "object" and
        keys == ["fabric_version","schema_version"] and
        .schema_version == 1 and
        (.fabric_version | type == "string" and length > 0 and
            (test("[\u0000-\u001f\u007f]") | not))
    ' "$file" >/dev/null 2>&1 || hfx_die "compile state is malformed: $file"
}

validate_pipeline_json() {
    local file=$1
    "$JQ" -e '
        def valid_id: type == "string" and test("^[0-9]{10}$");
        def valid_status:
            . == "pending" or . == "acquiring" or . == "ready" or
            . == "compiling" or . == "terminal" or . == "reclaimed" or
            . == "blocked";
        type == "object" and
        keys == ["basin_ids","basins","fabric_version","max_parallel","schema_version"] and
        .schema_version == 1 and
        (.fabric_version | type == "string" and length > 0 and
            (test("[[:cntrl:]]") | not)) and
        (.max_parallel | type == "number" and . == floor and . >= 1 and . <= 4) and
        (.basin_ids | type == "array" and length > 0 and
            all(.[]; valid_id) and . == (sort | unique)) and
        (.basins | type == "object") and
        (.basins | keys) == .basin_ids and
        ([.basins[] |
            type == "object" and keys == ["blocked_reason","status"] and
            (.status | valid_status) and
            (if .status == "blocked" then
                (.blocked_reason | type == "string" and length > 0 and
                    (test("[[:cntrl:]]") | not))
             else .blocked_reason == null end)
        ] | all) and
        ([.basins[].status | select(. == "compiling")] | length) <= 1 and
        ([.basins[].status | select(. == "acquiring")] | length) <= .max_parallel
    ' "$file" >/dev/null 2>&1 || hfx_die "pipeline state is malformed: $file"
}

validate_assembly_json() {
    local file=$1
    "$JQ" -e --slurpfile inventory "$campaign_dir/state/inventory.json" '
        def valid_ids:
            type == "array" and
            . == (sort | unique) and
            all(.[]; type == "string" and test("^[0-9]{10}$") and $inventory[0][.] != null);
        type == "object" and
        keys == ["attempts","failure_reason","input_basin_ids","output_path","report_path","schema_version","status"] and
        .schema_version == 1 and
        (.status == "pending" or .status == "running" or .status == "succeeded" or .status == "failed") and
        (.attempts | type == "number" and . == floor and . >= 0) and
        (.failure_reason == null or (.failure_reason | type == "string")) and
        (.input_basin_ids | valid_ids) and
        .output_path == "assembly/dataset" and
        .report_path == "reports/assembly.json" and
        if .status == "running" or .status == "succeeded" then
            .attempts > 0 and (.input_basin_ids | length) > 0 and .failure_reason == null
        elif .status == "failed" then
            .attempts > 0 and (.failure_reason | type == "string" and length > 0)
        else
            if .attempts == 0 then
                .input_basin_ids == [] and .failure_reason == null
            else
                (.input_basin_ids | length) > 0 and
                .failure_reason == "interrupted before terminal state; reset by recover"
            end
        end
    ' "$file" >/dev/null 2>&1 || hfx_die "assembly state is malformed: $file"
}

assembly_report_is_valid() {
    local file=$1
    "$JQ" -e --arg campaign "$campaign" --slurpfile inventory "$campaign_dir/state/inventory.json" '
        type == "object" and
        keys == ["campaign","input_basin_ids","input_dataset_paths","output_path","schema_version"] and
        .schema_version == 1 and
        .campaign == $campaign and
        .output_path == "assembly/dataset" and
        (.input_basin_ids | type == "array" and . == (sort | unique) and
            all(.[]; type == "string" and test("^[0-9]{10}$") and $inventory[0][.] != null)) and
        (.input_dataset_paths | type == "array") and
        .input_dataset_paths == [.input_basin_ids[] | "basin-outputs/" + .]
    ' "$file" >/dev/null 2>&1
}

validate_assembly_report() {
    local file=$1
    assembly_report_is_valid "$file" || hfx_die "assembly report is malformed: $file"
}

assembly_report_matches_inputs() {
    local file=$1
    assembly_report_is_valid "$file" &&
        [[ $("$JQ" -c '.input_basin_ids' "$file") == "$assembly_inputs_json" ]]
}

validate_basin_json() {
    local file=$1
    local basin_id=$2
    "$JQ" -e --arg basin_id "$basin_id" --arg magic "$HFX_TDX_SQLITE_MAGIC" --argjson max_i64 "$HFX_TDX_MAX_I64" '
        def valid_v1_stage:
            type == "object" and
            keys == ["attempts","failure_reason","status"] and
            (.status == "pending" or .status == "running" or .status == "succeeded" or .status == "failed") and
            (.attempts | type == "number" and . == floor and . >= 0) and
            (.failure_reason == null or (.failure_reason | type == "string"));
        def valid_diagnostic:
            type == "object" and
            keys == ["diagnostics","path"] and
            (.diagnostics | type == "object") and
            .path == ("reports/" + $basin_id + "-build-report.json");
        def valid_v3_compile:
            type == "object" and
            keys == ["attempts","diagnostic_report","failure_reason","status"] and
            (.status == "pending" or .status == "running" or .status == "succeeded" or .status == "failed") and
            (.attempts | type == "number" and . == floor and . >= 0) and
            (.failure_reason == null or (.failure_reason | type == "string")) and
            (.diagnostic_report == null or (.diagnostic_report | valid_diagnostic)) and
            (if .status == "pending" or .status == "running" then .diagnostic_report == null else true end);
        def valid_evidence:
            type == "object" and
            keys == ["bytes","layer_name","sha256","sqlite_identity"] and
            (.bytes | type == "number" and . == floor and . > 0 and . <= $max_i64) and
            (.sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
            .sqlite_identity == $magic and
            (.layer_name | type == "string" and length > 0 and (test("[\u0000-\u001f\u007f]") | not));
        def valid_v2_acquire:
            type == "object" and
            keys == ["attempts","evidence","failure_reason","status"] and
            (.attempts | type == "number" and . == floor and . >= 0) and
            if .status == "succeeded" then
                .attempts > 0 and .failure_reason == null and (.evidence | valid_evidence)
            elif .status == "failed" then
                (.failure_reason | type == "string" and length > 0) and .evidence == null
            elif .status == "running" then
                .failure_reason == null and .evidence == null
            elif .status == "pending" then
                (.failure_reason == null or .failure_reason == "interrupted before terminal state; reset by recover") and
                .evidence == null
            else false end;
        def valid_v4_terminal:
            (.stages.acquire_basins.status == "succeeded") and
            (.stages.acquire_streamnet.status == "succeeded") and
            (.stages.compile.attempts > 0) and
            ((.stages.compile.status == "succeeded" and
              .stages.compile.failure_reason == null and
              (.stages.compile.diagnostic_report | valid_diagnostic)) or
             (.stages.compile.status == "failed" and
              (.stages.compile.failure_reason == "adapter build failed" or
               .stages.compile.failure_reason == "adapter validation failed") and
              (.stages.compile.failure_reason | length > 0) and
              (.stages.compile.diagnostic_report == null or
               (.stages.compile.diagnostic_report | valid_diagnostic))));
        type == "object" and
        (if .schema_version == 4
         then keys == ["processing_basin_id","retention","schema_version","stages"]
         else keys == ["processing_basin_id","schema_version","stages"] end) and
        (.schema_version == 1 or .schema_version == 2 or .schema_version == 3 or .schema_version == 4) and
        .processing_basin_id == $basin_id and
        (.stages | type == "object" and
            keys == ["acquire_basins","acquire_streamnet","compile"]) and
        if .schema_version == 1 then
            (.stages.compile | valid_v1_stage) and
            (.stages.acquire_basins | valid_v1_stage) and
            (.stages.acquire_streamnet | valid_v1_stage)
        elif .schema_version == 2 then
            (.stages.compile | valid_v1_stage) and
            (.stages.acquire_basins | valid_v2_acquire) and
            (.stages.acquire_streamnet | valid_v2_acquire)
        elif .schema_version == 3 then
            (.stages.compile | valid_v3_compile) and
            (.stages.acquire_basins | valid_v2_acquire) and
            (.stages.acquire_streamnet | valid_v2_acquire)
        else
            (.retention | type == "object" and
             keys == ["inputs_reclaimed","policy"] and
             (.inputs_reclaimed | type == "boolean") and
             .policy == "reclaim-inputs-after-terminal") and
            (.stages.compile | valid_v3_compile) and
            (.stages.acquire_basins | valid_v2_acquire) and
            (.stages.acquire_streamnet | valid_v2_acquire) and
            (if .retention.inputs_reclaimed then valid_v4_terminal else true end)
        end
    ' "$file" >/dev/null 2>&1 || hfx_die "basin state is malformed for $basin_id: $file"
}

atomic_install() {
    local temporary=$1
    local destination=$2
    local validator=$3
    local validator_arg=${4-}
    [[ -f "$temporary" && ! -L "$temporary" ]] || hfx_die "temporary state is not a regular file: $temporary"
    if [[ -n "$validator_arg" ]]; then
        "$validator" "$temporary" "$validator_arg"
    else
        "$validator" "$temporary"
    fi
    "$CHMOD" 0644 "$temporary" || hfx_die "could not set state mode on $temporary"
    "$MV" "$temporary" "$destination" || hfx_die "atomic state replacement failed for $destination"
}

lock_owned=0
lock_path=
takeover_owned=0
takeover_path=
campaign_lock_is_owned() {
    if ((lock_owned == 1)) &&
        [[ -d "$lock_path" && ! -L "$lock_path" ]] &&
        [[ -f "$lock_path/owner.pid" && ! -L "$lock_path/owner.pid" ]] &&
        [[ $(<"$lock_path/owner.pid") == "$$" ]]; then
        return 0
    fi
    return 1
}

release_takeover_guard() {
    if ((takeover_owned == 1)) && [[ -n "$takeover_path" && -d "$takeover_path" && ! -L "$takeover_path" ]] &&
        [[ -f "$takeover_path/owner.pid" && ! -L "$takeover_path/owner.pid" ]] &&
        [[ $(<"$takeover_path/owner.pid") == "$$" ]]; then
        "$RM" -r -- "$takeover_path"
    fi
    takeover_owned=0
}

release_lock() {
    # Release retains its pre-existing missing-symlink guard on owner.pid. Re-entry
    # is stronger because it authorizes work and therefore explicitly rejects one.
    if ((lock_owned == 1)) && [[ -n "$lock_path" && -d "$lock_path" && ! -L "$lock_path" ]] &&
        [[ -f "$lock_path/owner.pid" ]] && [[ $(<"$lock_path/owner.pid") == "$$" ]]; then
        "$RM" -r -- "$lock_path"
    fi
    lock_owned=0
}

pid_state() {
    local owner=$1
    local output_file=$campaign_dir/state/tmp/.ps.$$
    local error_file=$campaign_dir/state/tmp/.ps-error.$$
    local ps_status
    local output
    if kill -0 "$owner" 2>/dev/null; then
        printf '%s\n' live
        return
    fi
    : >"$output_file"
    : >"$error_file"
    if "$PS" -p "$owner" -o pid= >"$output_file" 2>"$error_file"; then
        ps_status=0
    else
        ps_status=$?
    fi
    output=$("$TR" -d '[:space:]' <"$output_file")
    if [[ -s "$error_file" ]]; then
        printf '%s\n' indeterminate
    elif [[ "$output" == "$owner" && "$ps_status" -eq 0 ]]; then
        printf '%s\n' live
    elif [[ -z "$output" && "$ps_status" -eq 1 ]]; then
        printf '%s\n' dead
    else
        printf '%s\n' indeterminate
    fi
    "$RM" -- "$output_file" "$error_file"
}

acquire_campaign_lock() {
    local locks_dir=$campaign_dir/state/locks
    local owner=
    local stale=
    local state
    lock_path=$locks_dir/campaign.lock
    takeover_path=$locks_dir/campaign.lock.takeover
    if campaign_lock_is_owned; then
        return
    fi
    if "$MKDIR" "$lock_path" 2>/dev/null; then
        printf '%s\n' "$$" >"$lock_path/owner.pid"
        lock_owned=1
        return
    fi
    if ! "$MKDIR" "$takeover_path" 2>/dev/null; then
        hfx_die "stale-lock takeover already in progress at $takeover_path"
    fi
    printf '%s\n' "$$" >"$takeover_path/owner.pid"
    takeover_owned=1
    if [[ ! -e "$lock_path" && ! -L "$lock_path" ]]; then
        if "$MKDIR" "$lock_path" 2>/dev/null; then
            printf '%s\n' "$$" >"$lock_path/owner.pid"
            lock_owned=1
            release_takeover_guard
            return
        fi
        release_takeover_guard
        hfx_die 'campaign lock was acquired concurrently'
    fi
    if [[ ! -d "$lock_path" || -L "$lock_path" || ! -f "$lock_path/owner.pid" || -L "$lock_path/owner.pid" ]]; then
        release_takeover_guard
        hfx_die "campaign lock owner is indeterminate; preserved at $lock_path"
    fi
    owner=$(<"$lock_path/owner.pid")
    if [[ ! "$owner" =~ ^[0-9]+$ ]]; then
        release_takeover_guard
        hfx_die "campaign lock owner is indeterminate; preserved at $lock_path"
    fi
    state=$(pid_state "$owner")
    if [[ "$state" == live ]]; then
        release_takeover_guard
        hfx_die "campaign lock is held by live PID $owner"
    elif [[ "$state" != dead ]]; then
        release_takeover_guard
        hfx_die "campaign lock owner PID $owner is indeterminate; preserved at $lock_path"
    fi
    stale=$locks_dir/.campaign.lock.stale.$$
    [[ ! -e "$stale" && ! -L "$stale" ]] || hfx_die "stale-lock destination already exists: $stale"
    if ! "$MV" "$lock_path" "$stale"; then
        release_takeover_guard
        hfx_die 'campaign lock changed during stale-lock takeover'
    fi
    if ! "$MKDIR" "$lock_path" 2>/dev/null; then
        release_takeover_guard
        hfx_die 'campaign lock was acquired concurrently'
    fi
    printf '%s\n' "$$" >"$lock_path/owner.pid"
    lock_owned=1
    [[ -d "$stale" && ! -L "$stale" ]] || hfx_die "renamed stale lock is unsafe: $stale"
    "$RM" -r -- "$stale"
    release_takeover_guard
}

validate_campaign_structure() {
    [[ -d "$campaign_dir" && ! -L "$campaign_dir" ]] || hfx_die "campaign path is not a safe directory: $campaign_dir"
    for required_dir in downloads basin-outputs reports assembly assembly/scratch publication state state/basins state/locks state/tmp; do
        [[ -d "$campaign_dir/$required_dir" && ! -L "$campaign_dir/$required_dir" ]] ||
            hfx_die "required campaign directory is missing or unsafe: $campaign_dir/$required_dir"
    done
    validate_campaign_json "$campaign_dir/state/campaign.json"
    if [[ -e "$campaign_dir/state/compile.json" || -L "$campaign_dir/state/compile.json" ]]; then
        [[ -f "$campaign_dir/state/compile.json" && ! -L "$campaign_dir/state/compile.json" ]] ||
            hfx_die "compile state is not a regular file: $campaign_dir/state/compile.json"
        validate_compile_json "$campaign_dir/state/compile.json"
    fi
    if [[ -e "$campaign_dir/state/assembly.json" || -L "$campaign_dir/state/assembly.json" ]]; then
        [[ -f "$campaign_dir/state/assembly.json" && ! -L "$campaign_dir/state/assembly.json" ]] ||
            hfx_die "assembly state is not a regular file: $campaign_dir/state/assembly.json"
        validate_assembly_json "$campaign_dir/state/assembly.json"
    fi
    if [[ -e "$campaign_dir/state/pipeline.json" || -L "$campaign_dir/state/pipeline.json" ]]; then
        [[ -f "$campaign_dir/state/pipeline.json" && ! -L "$campaign_dir/state/pipeline.json" ]] ||
            hfx_die "pipeline state is not a regular file: $campaign_dir/state/pipeline.json"
        validate_pipeline_json "$campaign_dir/state/pipeline.json"
    fi
    validate_inventory_file "$campaign_dir/state/inventory.json"
    "$JQ" -e -S --slurp '.[0] == .[1]' "$inventory_source" "$campaign_dir/state/inventory.json" >/dev/null 2>&1 ||
        hfx_die 'campaign inventory differs from the authoritative tracked crosswalk'
    if [[ -e "$campaign_dir/state/selection.json" || -L "$campaign_dir/state/selection.json" ]]; then
        [[ -f "$campaign_dir/state/selection.json" && ! -L "$campaign_dir/state/selection.json" ]] ||
            hfx_die "basin selection state is not a regular file: $campaign_dir/state/selection.json"
        validate_selection_file "$campaign_dir/state/selection.json"
    fi
}

validate_reclaimed_sources_absent() {
    local basin_id=$1
    local candidate_path
    if [[ $("$JQ" -r '.schema_version == 4 and .retention.inputs_reclaimed' "$campaign_dir/state/basins/$basin_id/current.json") == true ]]; then
        for candidate_path in \
            "$campaign_dir/downloads/$basin_id-basins.gpkg" \
            "$campaign_dir/downloads/$basin_id-basins.gpkg.partial" \
            "$campaign_dir/downloads/$basin_id-basins.gpkg.partial.json" \
            "$campaign_dir/downloads/$basin_id-streamnet.gpkg" \
            "$campaign_dir/downloads/$basin_id-streamnet.gpkg.partial" \
            "$campaign_dir/downloads/$basin_id-streamnet.gpkg.partial.json"; do
            [[ ! -e "$candidate_path" && ! -L "$candidate_path" ]] ||
                hfx_die "reclaimed basin source artifact remains for $basin_id: $candidate_path; move that exact path out of downloads, then rerun recover"
        done
    fi
}

validate_target_basin() {
    local basin_id=$1
    local basin_dir=$campaign_dir/state/basins/$basin_id
    [[ "$basin_id" =~ ^[0-9]{10}$ ]] ||
        hfx_die "invalid processing basin ID '$basin_id'; expected an authoritative 10-digit ID"
    "$JQ" -e --arg id "$basin_id" 'has($id)' "$campaign_dir/state/inventory.json" >/dev/null ||
        hfx_die "processing basin ID is not in the authoritative inventory: $basin_id"
    if [[ -f "$campaign_dir/state/selection.json" && ! -L "$campaign_dir/state/selection.json" ]]; then
        "$JQ" -e --arg id "$basin_id" '.basin_ids | index($id) != null' \
            "$campaign_dir/state/selection.json" >/dev/null ||
            hfx_die "processing basin ID is not in the frozen campaign selection: $basin_id"
    fi
    [[ -d "$basin_dir" && ! -L "$basin_dir" ]] ||
        hfx_die "basin directory is missing or unsafe: $basin_id"
    validate_basin_json "$basin_dir/current.json" "$basin_id"
    validate_reclaimed_sources_absent "$basin_id"
}

validate_target_workspace_state() {
    local basin_id=$1
    validate_campaign_structure
    validate_target_basin "$basin_id"
}

validate_workspace_state() {
    local expected_count
    local basin_id
    local basin_dir
    local actual_dirs
    local actual_files

    validate_campaign_structure
    expected_count=$("$JQ" -r 'length' "$campaign_dir/state/inventory.json")
    actual_dirs=$("$FIND" "$campaign_dir/state/basins" -mindepth 1 -maxdepth 1 -type d | "$WC" -l | "$TR" -d ' ')
    [[ "$actual_dirs" == "$expected_count" ]] || hfx_die "expected $expected_count basin directories; found $actual_dirs"
    actual_files=$("$FIND" "$campaign_dir/state/basins" -mindepth 2 -maxdepth 2 -type f -name current.json | "$WC" -l | "$TR" -d ' ')
    [[ "$actual_files" == "$expected_count" ]] || hfx_die "expected $expected_count basin state files; found $actual_files"
    while IFS= read -r basin_id; do
        basin_dir=$campaign_dir/state/basins/$basin_id
        [[ -d "$basin_dir" && ! -L "$basin_dir" ]] || hfx_die "basin directory is missing or unsafe: $basin_id"
        validate_basin_json "$basin_dir/current.json" "$basin_id"
        validate_reclaimed_sources_absent "$basin_id"
    done < <("$JQ" -r 'keys[]' "$campaign_dir/state/inventory.json")
    if "$FIND" "$campaign_dir/state/basins" -mindepth 1 -maxdepth 1 -type d |
        while IFS= read -r basin_dir; do
            basin_id=${basin_dir##*/}
            "$JQ" -e --arg id "$basin_id" 'has($id)' "$campaign_dir/state/inventory.json" >/dev/null || exit 1
        done; then
        :
    else
        hfx_die 'an extra basin directory is present'
    fi
}

print_sizing() {
    "$JQ" -r '
        . as $root |
        $root.sizing |
        "retention_policy=\($root.retention.policy)",
        "available_memory_bytes=\(.available_memory_bytes)",
        "available_disk_bytes=\(.available_disk_bytes)",
        (if $root.retention.policy == "retain-all-through-publication"
         then "retained_input_bytes=\(.retained_input_bytes)"
         else "peak_in_flight_download_bytes=\(.peak_in_flight_download_bytes)" end),
        "retained_basin_output_bytes=\(.retained_basin_output_bytes)",
        "assembly_memory_ceiling_bytes=\(.assembly_memory_ceiling_bytes)",
        "assembly_scratch_ceiling_bytes=\(.assembly_scratch_ceiling_bytes)",
        "assembled_artifact_bytes=\(.assembled_artifact_bytes)",
        "active_compile_scratch_bytes=\(.active_compile_scratch_bytes)",
        "filesystem_overhead_bytes=\(.filesystem_overhead_bytes)",
        "required_memory_bytes=\(.required_memory_bytes)",
        "required_disk_bytes=\(.required_disk_bytes)"
    ' "$campaign_dir/state/campaign.json"
}

print_status() {
    local stage
    local status
    local basin_id
    local selected_count
    local state_files=()
    printf 'campaign=%s\n' "$campaign"
    printf 'inventory_count=62\n'
    while IFS= read -r basin_id; do
        state_files[${#state_files[@]}]=$campaign_dir/state/basins/$basin_id/current.json
    done < <(effective_basin_ids)
    selected_count=${#state_files[@]}
    if [[ -f "$campaign_dir/state/selection.json" && ! -L "$campaign_dir/state/selection.json" ]]; then
        printf 'selected_basin_count=%s\n' "$selected_count"
        printf 'unselected_basin_count=%s\n' "$((62 - selected_count))"
    fi
    print_sizing
    for stage in acquire_basins acquire_streamnet compile; do
        for status in pending running succeeded failed; do
            printf '%s_%s=' "$stage" "$status"
            "$JQ" -s --arg stage "$stage" --arg status "$status" \
                '[.[].stages[$stage].status | select(. == $status)] | length' \
                ${state_files[@]+"${state_files[@]}"}
        done
    done
    if [[ $("$JQ" -r '.retention.policy' "$campaign_dir/state/campaign.json") == reclaim-inputs-after-terminal ]]; then
        printf 'inputs_reclaimed='
        "$JQ" -s '[.[] | select(.schema_version == 4 and .retention.inputs_reclaimed == true)] | length' \
            ${state_files[@]+"${state_files[@]}"}
    fi
    if [[ -f "$campaign_dir/state/pipeline.json" && ! -L "$campaign_dir/state/pipeline.json" ]]; then
        "$JQ" -r '
            "pipeline_max_parallel=\(.max_parallel)",
            "pipeline_fabric_version=\(.fabric_version)",
            ("pending","acquiring","ready","compiling","terminal","reclaimed","blocked") as $status |
            "pipeline_\($status)=\([.basins[].status | select(. == $status)] | length)"
        ' "$campaign_dir/state/pipeline.json"
    fi
    if [[ -f "$campaign_dir/state/assembly.json" && ! -L "$campaign_dir/state/assembly.json" ]]; then
        stage=$("$JQ" -r '.status' "$campaign_dir/state/assembly.json")
    else
        stage=pending
    fi
    for status in pending running succeeded failed; do
        if [[ "$stage" == "$status" ]]; then
            printf 'assemble_%s=1\n' "$status"
        else
            printf 'assemble_%s=0\n' "$status"
        fi
    done
}

pipeline_temporary_path() {
    printf '%s\n' "$campaign_dir/state/tmp/pipeline.json.tmp.$$"
}

prepare_pipeline_temporary() {
    local temporary
    temporary=$(pipeline_temporary_path)
    if [[ -e "$temporary" || -L "$temporary" ]]; then
        [[ -f "$temporary" && ! -L "$temporary" ]] ||
            hfx_die "pipeline temporary is unsafe: $temporary"
    fi
    printf '%s\n' "$temporary"
}

transition_pipeline_basin() {
    local basin_id=$1
    local status=$2
    local blocked_reason=${3-}
    local state=$campaign_dir/state/pipeline.json
    local temporary
    campaign_lock_is_owned || hfx_die 'pipeline state transition requires the parent campaign lock'
    validate_pipeline_json "$state"
    "$JQ" -e --arg id "$basin_id" \
        '(.basin_ids | index($id) != null) and (.basins | has($id))' "$state" >/dev/null ||
        hfx_die "pipeline basin is not selected: $basin_id"
    case $status in
        pending|acquiring|ready|compiling|terminal|reclaimed)
            [[ -z "$blocked_reason" ]] ||
                hfx_die "pipeline status $status requires a null blocked reason"
            ;;
        blocked)
            [[ -n "$blocked_reason" && ! "$blocked_reason" =~ [[:cntrl:]] ]] ||
                hfx_die 'pipeline blocked status requires a nonempty reason without ASCII control characters'
            ;;
        *) hfx_die "invalid pipeline status: $status" ;;
    esac
    temporary=$(prepare_pipeline_temporary)
    "$JQ" --arg id "$basin_id" --arg status "$status" --arg reason "$blocked_reason" '
        .basins[$id].status = $status |
        .basins[$id].blocked_reason = (if $status == "blocked" then $reason else null end)
    ' "$state" >"$temporary"
    atomic_install "$temporary" "$state" validate_pipeline_json
}

materialize_pipeline_state() {
    local state=$campaign_dir/state/pipeline.json
    local temporary
    local existing_fabric
    local same_ids
    local selected_ids_json
    local basin_id
    local selected_ids=()
    campaign_lock_is_owned || hfx_die 'pipeline state materialization requires the parent campaign lock'
    while IFS= read -r basin_id; do
        selected_ids[${#selected_ids[@]}]=$basin_id
    done < <(effective_basin_ids)
    selected_ids_json=$("$JQ" -cn --args '$ARGS.positional' -- \
        ${selected_ids[@]+"${selected_ids[@]}"})
    if [[ -e "$state" || -L "$state" ]]; then
        [[ -f "$state" && ! -L "$state" ]] ||
            hfx_die "pipeline state is not a regular file: $state"
        validate_pipeline_json "$state"
        existing_fabric=$("$JQ" -r '.fabric_version' "$state")
        same_ids=$("$JQ" -e --argjson expected "$selected_ids_json" \
            '.basin_ids == $expected' "$state" >/dev/null &&
            printf true || printf false)
        [[ "$existing_fabric" == "$fabric_version" && "$same_ids" == true ]] ||
            hfx_die 'pipeline parameters changed; use a new campaign ID'
        if [[ $("$JQ" -r '.max_parallel' "$state") != "$max_parallel" ]]; then
            temporary=$(prepare_pipeline_temporary)
            "$JQ" --argjson max_parallel "$max_parallel" \
                '.max_parallel = $max_parallel' "$state" >"$temporary"
            atomic_install "$temporary" "$state" validate_pipeline_json
        fi
        return
    fi
    temporary=$(prepare_pipeline_temporary)
    "$JQ" -n --arg fabric_version "$fabric_version" \
        --argjson max_parallel "$max_parallel" --args '
        $ARGS.positional as $ids |
        {
          schema_version: 1,
          fabric_version: $fabric_version,
          max_parallel: $max_parallel,
          basin_ids: $ids,
          basins: ($ids | map({key: ., value: {status:"pending",blocked_reason:null}}) | from_entries)
        }
    ' -- ${selected_ids[@]+"${selected_ids[@]}"} >"$temporary"
    atomic_install "$temporary" "$state" validate_pipeline_json
}

pipeline_campaign() {
    local locked_policy
    local durable_unreclaimed=0
    local scheduler_counts
    acquire_campaign_lock
    validate_workspace_state
    locked_policy=$("$JQ" -r '.retention.policy' "$campaign_dir/state/campaign.json")
    [[ "$locked_policy" == "$acquire_retention_policy" ]] ||
        hfx_die 'campaign retention policy changed while acquiring the campaign lock'
    materialize_pipeline_state
    require_pipeline_selection
    prepare_pipeline_durable_state
    validate_workspace_state
    establish_compile_contract
    reconstruct_pipeline_records
    validate_workspace_state
    durable_unreclaimed=$(pipeline_unreclaimed_count)
    scheduler_counts=$("$JQ" -r '
        . as $root |
        ["pending","acquiring","ready","compiling","terminal","reclaimed","blocked"] |
        map(. as $status | [$status, ([$root.basins[].status | select(. == $status)] | length)]) |
        map(.[1]) | @tsv
    ' "$campaign_dir/state/pipeline.json")
    print_status
    printf 'pipeline_durable_unreclaimed=%s\n' "$durable_unreclaimed"
    if ((durable_unreclaimed > 0)); then
        local IFS=$'\t'
        set -- $scheduler_counts
        hfx_die "pipeline incomplete: pending=$1 acquiring=$2 ready=$3 compiling=$4 terminal=$5 reclaimed=$6 blocked=$7"
    fi
}

require_pipeline_selection() {
    local selected_ids_json
    local basin_id
    local selected_ids=()
    while IFS= read -r basin_id; do
        selected_ids[${#selected_ids[@]}]=$basin_id
    done < <(effective_basin_ids)
    selected_ids_json=$("$JQ" -cn --args '$ARGS.positional' -- \
        ${selected_ids[@]+"${selected_ids[@]}"})
    "$JQ" -e --argjson selected "$selected_ids_json" '
        .basin_ids == $selected and (.basins | keys) == $selected
    ' "$campaign_dir/state/pipeline.json" >/dev/null ||
        hfx_die 'pipeline selection differs from durable campaign selection'
}

prepare_pipeline_durable_state() {
    local basin_id
    while IFS= read -r basin_id; do
        migrate_basin_state "$basin_id"
    done < <(effective_basin_ids)
    while IFS= read -r basin_id; do
        recover_running_stages_for_basin "$basin_id" false
    done < <(effective_basin_ids)
}

pipeline_unreclaimed_count() {
    local basin_id
    local count=0
    while IFS= read -r basin_id; do
        if [[ $("$JQ" -r '.schema_version == 4 and .retention.inputs_reclaimed == true' \
            "$campaign_dir/state/basins/$basin_id/current.json") != true ]]; then
            count=$((count + 1))
        fi
    done < <(effective_basin_ids)
    printf '%s\n' "$count"
}

reclaim_eligibility() {
    local basin_id=$1
    "$JQ" -r '
      def eligible:
        .stages.acquire_basins.status == "succeeded" and
        .stages.acquire_streamnet.status == "succeeded" and
        ([.stages.acquire_basins.failure_reason,.stages.acquire_streamnet.failure_reason] |
         all(. != "acquisition report is unsafe or malformed; retained for inspection" and
             . != "existing final file failed integrity verification; retained for inspection" and
             . != "persisted evidence does not match final file; retained for inspection" and
             . != "installed final failed integrity verification; retained for inspection")) and
        .stages.compile.attempts > 0 and
        ((.stages.compile.status == "succeeded" and
          .stages.compile.failure_reason == null and
          .stages.compile.diagnostic_report != null) or
         (.stages.compile.status == "failed" and
          (.stages.compile.failure_reason == "adapter build failed" or
           .stages.compile.failure_reason == "adapter validation failed")));
      if eligible then "eligible"
      elif (.stages.compile.diagnostic_report = {} | eligible) then "missing-diagnostic"
      else "ineligible"
      end
    ' "$campaign_dir/state/basins/$basin_id/current.json"
}

classify_pipeline_acquisition_stage() {
    local basin_id=$1
    local stage=$2
    local current=$campaign_dir/state/basins/$basin_id/current.json
    local status
    local reason
    status=$("$JQ" -r --arg stage "$stage" '.stages[$stage].status' "$current")
    reason=$("$JQ" -r --arg stage "$stage" '.stages[$stage].failure_reason // ""' "$current")
    case $status in
        succeeded) printf '%s\n' terminal ;;
        pending) printf '%s\n' retryable ;;
        failed)
            case $reason in
                'partial changed during ignored-Range continuation'|\
                'continuation response failed provenance verification'|\
                'transfer failed'|\
                'download provenance or size verification failed'|\
                'download failed integrity verification')
                    printf '%s\n' retryable
                    ;;
                'acquisition report is unsafe or malformed; retained for inspection'|\
                'existing final file failed integrity verification; retained for inspection'|\
                'persisted evidence does not match final file; retained for inspection'|\
                'partial provenance path is unsafe; retained without traversal'|\
                'installed final failed integrity verification; retained for inspection')
                    printf '%s\n' inspection
                    ;;
                *) hfx_die "pipeline acquisition classifier rejected failure reason for $basin_id $stage: $reason" ;;
            esac
            ;;
        running) hfx_die "pipeline acquisition classifier found residual running stage for $basin_id $stage" ;;
        *) hfx_die "pipeline acquisition classifier rejected status for $basin_id $stage: $status" ;;
    esac
}

resolve_pipeline_compile_tools() {
    if [[ -z "${ADAPTER_PYTHON-}" ]]; then
        ADAPTER_PYTHON=$(resolve_command HFX_TDX_ADAPTER_PYTHON "$HFX_TDX_DEFAULT_ADAPTER_PYTHON")
        ADAPTER_SCRIPT=${HFX_TDX_ADAPTER_SCRIPT-$repo_root/adapters/tdx-hydro/build_adapter.py}
        HFX=$(resolve_command HFX_TDX_HFX "$HFX_TDX_DEFAULT_HFX")
    fi
}

pipeline_finish_terminal() {
    local basin_id=$1
    if [[ $("$JQ" -r '.stages.compile.status' \
        "$campaign_dir/state/basins/$basin_id/current.json") == succeeded ]]; then
        resolve_pipeline_compile_tools
    fi
    transition_pipeline_basin "$basin_id" terminal
    reconcile_reclaim_basin "$basin_id" true ||
        hfx_die "pipeline terminal basin was not reclaimable: $basin_id"
    [[ $("$JQ" -r '.schema_version == 4 and .retention.inputs_reclaimed == true' \
        "$campaign_dir/state/basins/$basin_id/current.json") == true ]] ||
        hfx_die "pipeline reclaim did not persist terminal state: $basin_id"
    validate_reclaimed_sources_absent "$basin_id"
    transition_pipeline_basin "$basin_id" reclaimed
}

pipeline_rule_1() {
    local basin_id=$1
    if [[ $("$JQ" -r '.schema_version == 4 and .retention.inputs_reclaimed == true' \
        "$campaign_dir/state/basins/$basin_id/current.json") == true ]]; then
        validate_reclaimed_sources_absent "$basin_id"
        transition_pipeline_basin "$basin_id" reclaimed
        return 0
    fi
    return 1
}

pipeline_rule_2() {
    local basin_id=$1
    [[ $(reclaim_eligibility "$basin_id") == eligible ]] || return 1
    [[ $("$JQ" -r '.retention.inputs_reclaimed == false' \
        "$campaign_dir/state/basins/$basin_id/current.json") == true ]] || return 1
    pipeline_finish_terminal "$basin_id"
}

pipeline_rule_3() {
    local basin_id=$1
    local eligibility
    local reason
    [[ $(reclaim_eligibility "$basin_id") == missing-diagnostic ]] || return 1
    resolve_pipeline_compile_tools
    compile_basin_locked "$basin_id" true
    eligibility=$(reclaim_eligibility "$basin_id")
    if [[ "$eligibility" == eligible ]]; then
        pipeline_finish_terminal "$basin_id"
        return 0
    fi
    reason=$("$JQ" -r '.stages.compile.failure_reason // ""' \
        "$campaign_dir/state/basins/$basin_id/current.json")
    if [[ "$reason" == 'existing compile artifacts failed resume verification; retained for inspection' ]]; then
        transition_pipeline_basin "$basin_id" blocked "$reason"
        return 0
    fi
    hfx_die "pipeline rule 3 produced an unsupported durable result for $basin_id"
}

pipeline_rule_4() {
    local basin_id=$1
    local basins_class=$2
    local streamnet_class=$3
    local current=$campaign_dir/state/basins/$basin_id/current.json
    local reason
    local eligibility
    [[ "$basins_class" == terminal && "$streamnet_class" == terminal ]] || return 1
    [[ $("$JQ" -r '.stages.compile.attempts > 0' "$current") == true ]] || return 1
    reason=$("$JQ" -r '.stages.compile.failure_reason // ""' "$current")
    case $reason in
        'compile artifact path already exists; retained for inspection'|\
        'existing compile artifacts failed resume verification; retained for inspection')
            transition_pipeline_basin "$basin_id" blocked "$reason"
            return 0
            ;;
    esac
    if [[ -e "$campaign_dir/basin-outputs/$basin_id" ||
        -L "$campaign_dir/basin-outputs/$basin_id" ||
        -e "$campaign_dir/reports/$basin_id-build-report.json" ||
        -L "$campaign_dir/reports/$basin_id-build-report.json" ]]; then
        resolve_pipeline_compile_tools
        compile_basin_locked "$basin_id" true
        reason=$("$JQ" -r '.stages.compile.failure_reason // ""' "$current")
        if [[ "$reason" == 'compile artifact path already exists; retained for inspection' ]]; then
            transition_pipeline_basin "$basin_id" blocked "$reason"
            return 0
        fi
        eligibility=$(reclaim_eligibility "$basin_id")
        if [[ "$eligibility" == eligible ]]; then
            pipeline_finish_terminal "$basin_id"
            return 0
        fi
        hfx_die "pipeline rule 4 produced an unsupported durable result for $basin_id"
    fi
    transition_pipeline_basin "$basin_id" blocked \
        'interrupted compile attempt cannot be safely repeated; retained for inspection'
}

pipeline_rule_5() {
    local basin_id=$1
    local basins_class=$2
    local streamnet_class=$3
    local current=$campaign_dir/state/basins/$basin_id/current.json
    [[ "$basins_class" == terminal && "$streamnet_class" == terminal ]] || return 1
    [[ $("$JQ" -r '
      .stages.acquire_basins.status == "succeeded" and
      .stages.acquire_streamnet.status == "succeeded" and
      .stages.compile.attempts == 0 and
      ((.stages.compile.status == "pending" and
        .stages.compile.failure_reason == null) or
       (.stages.compile.status == "failed" and
        .stages.compile.failure_reason == "acquisition prerequisites are not both succeeded")) and
      .stages.compile.diagnostic_report == null and
      .retention.inputs_reclaimed == false
    ' "$current") == true ]] || return 1
    [[ ! -e "$campaign_dir/basin-outputs/$basin_id" &&
        ! -L "$campaign_dir/basin-outputs/$basin_id" &&
        ! -e "$campaign_dir/reports/$basin_id-build-report.json" &&
        ! -L "$campaign_dir/reports/$basin_id-build-report.json" ]] || return 1
    transition_pipeline_basin "$basin_id" ready
}

pipeline_rule_6() {
    local basin_id=$1
    local basins_class=$2
    local streamnet_class=$3
    local current=$campaign_dir/state/basins/$basin_id/current.json
    local reason
    if [[ "$basins_class" == inspection || "$streamnet_class" == inspection ]]; then
        if [[ "$basins_class" == inspection ]]; then
            reason=$("$JQ" -r '.stages.acquire_basins.failure_reason' "$current")
        else
            reason=$("$JQ" -r '.stages.acquire_streamnet.failure_reason' "$current")
        fi
        transition_pipeline_basin "$basin_id" blocked "$reason"
        return 0
    fi
    if [[ "$basins_class" == retryable || "$streamnet_class" == retryable ]]; then
        transition_pipeline_basin "$basin_id" pending
        return 0
    fi
    return 1
}

pipeline_rule_7() {
    local basin_id=$1
    local basins_class=$2
    local streamnet_class=$3
    local current=$campaign_dir/state/basins/$basin_id/current.json
    local reason
    [[ "$basins_class" == terminal && "$streamnet_class" == terminal ]] || return 1
    reason=$("$JQ" -r '.stages.compile.failure_reason // ""' "$current")
    case $reason in
        'compile artifact path already exists; retained for inspection'|\
        'existing compile artifacts failed resume verification; retained for inspection')
            transition_pipeline_basin "$basin_id" blocked "$reason"
            return 0
            ;;
    esac
    if [[ $("$JQ" -r '.stages.compile.attempts == 0' "$current") == true ]] &&
        [[ "$reason" == 'acquisition prerequisites are not both succeeded' || -z "$reason" ]] &&
        [[ -e "$campaign_dir/basin-outputs/$basin_id" ||
            -L "$campaign_dir/basin-outputs/$basin_id" ||
            -e "$campaign_dir/reports/$basin_id-build-report.json" ||
            -L "$campaign_dir/reports/$basin_id-build-report.json" ]]; then
        if [[ -z "$reason" ]]; then
            reason='compile artifact path already exists; retained for inspection'
        fi
        transition_pipeline_basin "$basin_id" blocked "$reason"
        return 0
    fi
    return 1
}

pipeline_rule_8() {
    local basin_id=$1
    hfx_die "pipeline durable compile state is contradictory for $basin_id"
}

reconstruct_pipeline_basin() {
    local basin_id=$1
    local basins_class
    local streamnet_class
    basins_class=$(classify_pipeline_acquisition_stage "$basin_id" acquire_basins)
    streamnet_class=$(classify_pipeline_acquisition_stage "$basin_id" acquire_streamnet)
    pipeline_rule_1 "$basin_id" && return
    pipeline_rule_2 "$basin_id" && return
    pipeline_rule_3 "$basin_id" && return
    pipeline_rule_4 "$basin_id" "$basins_class" "$streamnet_class" && return
    pipeline_rule_5 "$basin_id" "$basins_class" "$streamnet_class" && return
    pipeline_rule_6 "$basin_id" "$basins_class" "$streamnet_class" && return
    pipeline_rule_7 "$basin_id" "$basins_class" "$streamnet_class" && return
    pipeline_rule_8 "$basin_id"
}

reconstruct_pipeline_records() {
    local basin_id
    while IFS= read -r basin_id; do
        reconstruct_pipeline_basin "$basin_id"
    done < <(effective_basin_ids)
}

materialize_assembly_state() {
    local state=$campaign_dir/state/assembly.json
    local temporary
    if [[ -e "$state" || -L "$state" ]]; then
        [[ -f "$state" && ! -L "$state" ]] || hfx_die "assembly state is not a regular file: $state"
        validate_assembly_json "$state"
        return
    fi
    temporary=$campaign_dir/state/tmp/assembly.json.tmp.$$
    "$JQ" -n '{
      schema_version: 1,
      status: "pending",
      attempts: 0,
      failure_reason: null,
      input_basin_ids: [],
      output_path: "assembly/dataset",
      report_path: "reports/assembly.json"
    }' >"$temporary"
    atomic_install "$temporary" "$state" validate_assembly_json
}

initialize_campaign() {
    local created=0
    local basin_id
    local temporary
    local existing_canonical
    local requested_canonical
    local requested_basin_id
    local seen_basin_ids=' '
    local selection_temporary=$campaign_dir/state/.selection.json.tmp.$$
    local retention_json
    local sizing_input_json

    if [[ -e "$campaign_dir" || -L "$campaign_dir" ]]; then
        [[ -d "$campaign_dir" && ! -L "$campaign_dir" ]] ||
            hfx_die "unsafe pre-existing campaign path: $campaign_dir"
        [[ -d "$campaign_dir/state" && -f "$campaign_dir/state/campaign.json" ]] ||
            hfx_die "refusing unknown pre-existing campaign content: $campaign_dir"
    else
        "$MKDIR" "$campaign_dir" || hfx_die "could not create campaign directory: $campaign_dir"
        created=1
    fi

    if ((created == 1)); then
        "$MKDIR" "$campaign_dir/downloads" "$campaign_dir/basin-outputs" "$campaign_dir/reports" \
            "$campaign_dir/assembly" "$campaign_dir/assembly/scratch" "$campaign_dir/publication" \
            "$campaign_dir/state" "$campaign_dir/state/basins" "$campaign_dir/state/locks" "$campaign_dir/state/tmp"
    fi
    acquire_campaign_lock

    temporary=$campaign_dir/state/.inventory.json.tmp.$$
    "$JQ" -S '.' "$inventory_source" >"$temporary"
    validate_inventory_file "$temporary"
    if ((${#basin_ids[@]} > 0)); then
        for requested_basin_id in ${basin_ids[@]+"${basin_ids[@]}"}; do
            case $seen_basin_ids in
                *" $requested_basin_id "*)
                    "$RM" -- "$temporary"
                    if ((created == 1)); then
                        "$RM" -r -- "$campaign_dir"
                    fi
                    usage_error "basin ID '$requested_basin_id' was selected more than once"
                    ;;
            esac
            seen_basin_ids="$seen_basin_ids$requested_basin_id "
            if ! "$JQ" -e --arg id "$requested_basin_id" 'has($id)' "$temporary" >/dev/null; then
                "$RM" -- "$temporary"
                if ((created == 1)); then
                    "$RM" -r -- "$campaign_dir"
                fi
                usage_error "unknown basin ID '$requested_basin_id'; expected a key in state/inventory.json"
            fi
        done
        "$JQ" -n '
          {schema_version:1,basin_ids:($ARGS.positional | sort)}
        ' --args ${basin_ids[@]+"${basin_ids[@]}"} >"$selection_temporary"
        validate_selection_file "$selection_temporary" "$temporary"
    fi

    case $retention_policy in
        retain-all-through-publication)
            retention_json='{"policy":"retain-all-through-publication","reclaim_inputs":false,"retain_acquired_inputs":true,"retain_basin_outputs":true,"retain_external_reports":true}'
            sizing_input_json="{\"retained_input_bytes\":$retained_input_bytes}"
            ;;
        reclaim-inputs-after-terminal)
            retention_json='{"policy":"reclaim-inputs-after-terminal","reclaim_inputs":true,"retain_acquired_inputs":false,"retain_basin_outputs":true,"retain_external_reports":true}'
            sizing_input_json="{\"peak_in_flight_download_bytes\":$peak_in_flight_download_bytes}"
            ;;
        *) hfx_die "unsupported retention policy: $retention_policy" ;;
    esac

    temporary=$campaign_dir/state/.campaign.json.tmp.$$
    "$JQ" -n \
        --arg campaign "$campaign" \
        --argjson retention "$retention_json" \
        --argjson sizing_input "$sizing_input_json" \
        --argjson available_memory_bytes "$available_memory_bytes" \
        --argjson available_disk_bytes "$available_disk_bytes" \
        --argjson retained_basin_output_bytes "$retained_basin_output_bytes" \
        --argjson assembly_memory_ceiling_bytes "$assembly_memory_ceiling_bytes" \
        --argjson assembly_scratch_ceiling_bytes "$assembly_scratch_ceiling_bytes" \
        --argjson assembled_artifact_bytes "$assembled_artifact_bytes" \
        --argjson active_compile_scratch_bytes "$active_compile_scratch_bytes" \
        --argjson filesystem_overhead_bytes "$filesystem_overhead_bytes" \
        --argjson required_memory_bytes "$required_memory_bytes" \
        --argjson required_disk_bytes "$required_disk_bytes" '
        {
          schema_version: 2,
          campaign: $campaign,
          inventory: {source: "adapters/tdx-hydro/data/tdx_header_numbers.json", count: 62},
          retention: $retention,
          sizing: ({
            available_memory_bytes: $available_memory_bytes,
            available_disk_bytes: $available_disk_bytes,
            retained_basin_output_bytes: $retained_basin_output_bytes,
            assembly_memory_ceiling_bytes: $assembly_memory_ceiling_bytes,
            assembly_scratch_ceiling_bytes: $assembly_scratch_ceiling_bytes,
            assembled_artifact_bytes: $assembled_artifact_bytes,
            active_compile_scratch_bytes: $active_compile_scratch_bytes,
            filesystem_overhead_bytes: $filesystem_overhead_bytes,
            required_memory_bytes: $required_memory_bytes,
            required_disk_bytes: $required_disk_bytes
          } + $sizing_input)
        }' >"$temporary"
    validate_campaign_json "$temporary"

    if ((created == 0)); then
        validate_workspace_state
        if [[ -f "$campaign_dir/state/selection.json" ]]; then
            if ((${#basin_ids[@]} == 0)); then
                "$RM" -- "$temporary" "$campaign_dir/state/.inventory.json.tmp.$$"
                hfx_die 'basin selection changed; use a new campaign ID'
            fi
            existing_canonical=$("$JQ" -cS '.' "$campaign_dir/state/selection.json")
            requested_canonical=$("$JQ" -cS '.' "$selection_temporary")
            if [[ "$existing_canonical" != "$requested_canonical" ]]; then
                "$RM" -- "$temporary" "$campaign_dir/state/.inventory.json.tmp.$$" "$selection_temporary"
                hfx_die 'basin selection changed; use a new campaign ID'
            fi
        elif ((${#basin_ids[@]} > 0)); then
            "$RM" -- "$temporary" "$campaign_dir/state/.inventory.json.tmp.$$" "$selection_temporary"
            hfx_die 'basin selection changed; use a new campaign ID'
        fi
        existing_canonical=$("$JQ" -cS '.' "$campaign_dir/state/campaign.json")
        requested_canonical=$("$JQ" -cS '.' "$temporary")
        if [[ "$existing_canonical" != "$requested_canonical" ]]; then
            "$RM" -- "$temporary" "$campaign_dir/state/.inventory.json.tmp.$$"
            if [[ -e "$selection_temporary" ]]; then
                "$RM" -- "$selection_temporary"
            fi
            hfx_die 'campaign parameters changed; use a new campaign ID'
        fi
        existing_canonical=$("$JQ" -cS '.' "$campaign_dir/state/inventory.json")
        requested_canonical=$("$JQ" -cS '.' "$campaign_dir/state/.inventory.json.tmp.$$")
        if [[ "$existing_canonical" != "$requested_canonical" ]]; then
            "$RM" -- "$temporary" "$campaign_dir/state/.inventory.json.tmp.$$"
            if [[ -e "$selection_temporary" ]]; then
                "$RM" -- "$selection_temporary"
            fi
            hfx_die 'campaign inventory changed; use a new campaign ID'
        fi
        "$RM" -- "$temporary" "$campaign_dir/state/.inventory.json.tmp.$$"
        if [[ -e "$selection_temporary" ]]; then
            "$RM" -- "$selection_temporary"
        fi
        print_status
        return
    fi

    atomic_install "$campaign_dir/state/.inventory.json.tmp.$$" "$campaign_dir/state/inventory.json" validate_inventory_file
    if ((${#basin_ids[@]} > 0)); then
        atomic_install "$selection_temporary" "$campaign_dir/state/selection.json" validate_selection_file
    fi
    atomic_install "$temporary" "$campaign_dir/state/campaign.json" validate_campaign_json
    materialize_assembly_state
    while IFS= read -r basin_id; do
        "$MKDIR" "$campaign_dir/state/basins/$basin_id"
        temporary=$campaign_dir/state/basins/$basin_id/.current.json.tmp.$$
        "$JQ" -n --arg basin_id "$basin_id" --arg policy "$retention_policy" '{
          schema_version: (if $policy == "reclaim-inputs-after-terminal" then 4 else 3 end),
          processing_basin_id: $basin_id,
          retention: (if $policy == "reclaim-inputs-after-terminal"
                      then {inputs_reclaimed:false,policy:$policy} else null end),
          stages: {
            acquire_basins: {status: "pending", attempts: 0, failure_reason: null, evidence: null},
            acquire_streamnet: {status: "pending", attempts: 0, failure_reason: null, evidence: null},
            compile: {status: "pending", attempts: 0, failure_reason: null, diagnostic_report: null}
          }
        } | if $policy == "reclaim-inputs-after-terminal" then . else del(.retention) end' >"$temporary"
        atomic_install "$temporary" "$campaign_dir/state/basins/$basin_id/current.json" validate_basin_json "$basin_id"
    done < <("$JQ" -r 'keys[]' "$campaign_dir/state/inventory.json")
    validate_workspace_state
    print_status
}

migrate_basin_state() {
    local basin_id=$1
    local current
    local temporary
    local policy
    local target_version
    policy=$("$JQ" -r '.retention.policy' "$campaign_dir/state/campaign.json")
    if [[ "$policy" == reclaim-inputs-after-terminal ]]; then
        target_version=4
    else
        target_version=3
    fi
    current=$campaign_dir/state/basins/$basin_id/current.json
    if [[ $("$JQ" -r '.schema_version' "$current") != "$target_version" ]]; then
        temporary=$campaign_dir/state/basins/$basin_id/.current.json.tmp.$$
        "$JQ" --argjson target_version "$target_version" --arg policy "$policy" '
                .schema_version as $version |
                .schema_version = $target_version |
                if $version == 1 then
                    .stages.acquire_basins.evidence = null |
                    .stages.acquire_streamnet.evidence = null |
                    .stages.acquire_basins |=
                        if .status == "succeeded" then .status = "pending" else . end |
                    .stages.acquire_streamnet |=
                        if .status == "succeeded" then .status = "pending" else . end
                else . end |
                if $version == 1 or $version == 2 then
                    .stages.compile.diagnostic_report = null
                else . end |
                if $target_version == 4 then
                    .retention = {inputs_reclaimed:false,policy:$policy}
                else del(.retention) end
        ' "$current" >"$temporary"
        atomic_install "$temporary" "$current" validate_basin_json "$basin_id"
    fi
}

migrate_basin_states() {
    local basin_id
    while IFS= read -r basin_id; do
        migrate_basin_state "$basin_id"
    done < <(effective_basin_ids)
}

recover_running_stages_for_basin() {
    local basin_id=$1
    local acquisition_only=${2-false}
    local current
    local temporary
    current=$campaign_dir/state/basins/$basin_id/current.json
    if "$JQ" -e '[.stages[].status] | any(. == "running")' "$current" >/dev/null; then
        temporary=$campaign_dir/state/basins/$basin_id/.current.json.tmp.$$
        "$JQ" --argjson acquisition_only "$acquisition_only" '
                .stages |= with_entries(
                    if .value.status == "running" and
                        (($acquisition_only | not) or .key == "acquire_basins" or .key == "acquire_streamnet") then
                        .value.status = "pending" |
                        .value.failure_reason = "interrupted before terminal state; reset by recover" |
                        if (.value | has("evidence")) then .value.evidence = null else . end |
                        if .key == "compile" and (.value | has("diagnostic_report")) then .value.diagnostic_report = null else . end
                    else .
                    end
                )
        ' "$current" >"$temporary"
        atomic_install "$temporary" "$current" validate_basin_json "$basin_id"
    fi
}

recover_running_stages() {
    local acquisition_only=${1-false}
    local basin_id
    while IFS= read -r basin_id; do
        recover_running_stages_for_basin "$basin_id" "$acquisition_only"
    done < <(effective_basin_ids)
}

recover_campaign() {
    local basin_id
    acquire_campaign_lock
    validate_workspace_state
    materialize_assembly_state
    recover_running_stages false
    while IFS= read -r basin_id; do
        if reconcile_reclaim_basin "$basin_id" false; then
            :
        fi
    done < <(effective_basin_ids)
    if [[ $("$JQ" -r '.status' "$campaign_dir/state/assembly.json") == running ]]; then
        write_assembly_state pending \
            "$("$JQ" -r '.attempts' "$campaign_dir/state/assembly.json")" \
            'interrupted before terminal state; reset by recover' \
            "$("$JQ" -c '.input_basin_ids' "$campaign_dir/state/assembly.json")"
    fi
    validate_workspace_state
    print_status
}

write_assembly_state() {
    local status=$1
    local attempts=$2
    local reason=$3
    local input_ids=$4
    local state=$campaign_dir/state/assembly.json
    local temporary=$campaign_dir/state/tmp/assembly.json.tmp.$$
    "$JQ" -n --arg status "$status" --arg reason "$reason" --argjson attempts "$attempts" \
        --argjson input_ids "$input_ids" '{
          schema_version: 1,
          status: $status,
          attempts: $attempts,
          failure_reason: (if $reason == "" then null else $reason end),
          input_basin_ids: $input_ids,
          output_path: "assembly/dataset",
          report_path: "reports/assembly.json"
        }' >"$temporary"
    atomic_install "$temporary" "$state" validate_assembly_json
}

write_assembly_report() {
    local input_ids=$1
    local report=$campaign_dir/reports/assembly.json
    local temporary=$campaign_dir/state/tmp/assembly-report.json.tmp.$$
    "$JQ" -n --arg campaign "$campaign" --argjson input_ids "$input_ids" '{
      schema_version: 1,
      campaign: $campaign,
      input_basin_ids: $input_ids,
      input_dataset_paths: [$input_ids[] | "basin-outputs/" + .],
      output_path: "assembly/dataset"
    }' >"$temporary"
    atomic_install "$temporary" "$report" validate_assembly_report
}

assembly_inputs_json=
assembly_args=()
select_assembly_inputs() {
    local basin_id
    local current
    local selected_file=$campaign_dir/state/tmp/assembly-inputs.$$.txt
    : >"$selected_file"
    assembly_args=()
    while IFS= read -r basin_id; do
        current=$campaign_dir/state/basins/$basin_id/current.json
        if [[ $("$JQ" -r '.stages.compile.status' "$current") == succeeded ]]; then
            assembly_args[${#assembly_args[@]}]=--input
            assembly_args[${#assembly_args[@]}]=$campaign_dir/basin-outputs/$basin_id
            printf '%s\n' "$basin_id" >>"$selected_file"
        fi
    done < <(effective_basin_ids)
    if ((${#assembly_args[@]} == 0)); then
        "$RM" -- "$selected_file"
        hfx_die 'assembly requires at least one basin with compile status succeeded'
    fi
    assembly_inputs_json=$("$JQ" -c -R -s 'split("\n") | map(select(length > 0))' "$selected_file")
    "$RM" -- "$selected_file"
}

assembly_provenance_matches() {
    local state=$campaign_dir/state/assembly.json
    [[ $("$JQ" -r '.status' "$state") == pending ]] &&
        [[ $("$JQ" -r '.failure_reason' "$state") == \
            'interrupted before terminal state; reset by recover' ]] &&
        [[ $("$JQ" -c '.input_basin_ids' "$state") == "$assembly_inputs_json" ]]
}

verify_assembly_dataset() {
    local output=$campaign_dir/assembly/dataset
    [[ -d "$output" && ! -L "$output" ]] || return 1
    "$ADAPTER_PYTHON" "$ADAPTER_SCRIPT" validate "$output" --hfx-binary "$HFX"
}

clean_attributable_assembly_staging() {
    local entry
    while IFS= read -r entry; do
        [[ -d "$entry" && ! -L "$entry" ]] ||
            hfx_die "assembly staging path is unsafe: $entry"
        "$RM" -r -- "$entry"
    done < <("$FIND" "$campaign_dir/assembly" -mindepth 1 -maxdepth 1 -name '.dataset.tmp-*')
}

assemble_campaign() {
    local state=$campaign_dir/state/assembly.json
    local output=$campaign_dir/assembly/dataset
    local report=$campaign_dir/reports/assembly.json
    local status
    local attempts
    local persisted_inputs

    acquire_campaign_lock
    validate_workspace_state
    select_assembly_inputs
    materialize_assembly_state
    status=$("$JQ" -r '.status' "$state")
    attempts=$("$JQ" -r '.attempts' "$state")
    persisted_inputs=$("$JQ" -c '.input_basin_ids' "$state")

    if [[ "$status" == succeeded ]]; then
        if [[ "$persisted_inputs" == "$assembly_inputs_json" ]] &&
            [[ -f "$report" && ! -L "$report" ]] &&
            assembly_report_matches_inputs "$report" &&
            verify_assembly_dataset; then
            print_status
            return
        fi
        write_assembly_state failed "$attempts" \
            'existing succeeded assembly failed resume verification; retained for inspection' \
            "$persisted_inputs"
        hfx_die 'existing succeeded assembly failed resume verification; retained for inspection'
    fi

    if [[ -e "$output" || -L "$output" ]]; then
        if assembly_provenance_matches; then
            [[ -d "$output" && ! -L "$output" ]] || {
                hfx_die 'assembly dataset exists without attributable interrupted or succeeded state; retained for inspection'
            }
            if ! verify_assembly_dataset; then
                write_assembly_state failed "$attempts" \
                    'assembled dataset validation failed; retained for inspection' "$persisted_inputs"
                hfx_die 'assembled dataset validation failed; retained for inspection'
            fi
            if [[ -e "$report" || -L "$report" ]]; then
                [[ -f "$report" && ! -L "$report" ]] ||
                    hfx_die 'assembly report exists without attributable dataset; retained for inspection'
            fi
            if [[ ! -f "$report" ]] || ! assembly_report_matches_inputs "$report"; then
                write_assembly_report "$assembly_inputs_json"
            fi
            write_assembly_state succeeded "$attempts" '' "$assembly_inputs_json"
            print_status
            return
        fi
        hfx_die 'assembly dataset exists without attributable interrupted or succeeded state; retained for inspection'
    fi

    if [[ -e "$report" || -L "$report" ]]; then
        if assembly_provenance_matches && [[ -f "$report" && ! -L "$report" ]]; then
            "$RM" -- "$report"
        else
            hfx_die 'assembly report exists without attributable dataset; retained for inspection'
        fi
    fi

    if assembly_provenance_matches; then
        clean_attributable_assembly_staging
    else
        while IFS= read -r persisted_inputs; do
            hfx_die "assembly staging path exists without attributable interrupted state; retained for inspection: $persisted_inputs"
        done < <("$FIND" "$campaign_dir/assembly" -mindepth 1 -maxdepth 1 -name '.dataset.tmp-*')
    fi

    attempts=$((attempts + 1))
    write_assembly_state running "$attempts" '' "$assembly_inputs_json"
    if ! "$ADAPTER_PYTHON" "$ADAPTER_SCRIPT" assemble \
        ${assembly_args[@]+"${assembly_args[@]}"} \
        --out "$output"; then
        if [[ -e "$output" || -L "$output" || -e "$report" || -L "$report" ]]; then
            write_assembly_state failed "$attempts" \
                'adapter assembly failed and left an artifact; retained for inspection' "$assembly_inputs_json"
            hfx_die 'adapter assembly failed and left an artifact; retained for inspection'
        else
            write_assembly_state failed "$attempts" 'adapter assembly failed' "$assembly_inputs_json"
            hfx_die 'adapter assembly failed'
        fi
    fi
    if ! verify_assembly_dataset; then
        write_assembly_state failed "$attempts" \
            'assembled dataset validation failed; retained for inspection' "$assembly_inputs_json"
        hfx_die 'assembled dataset validation failed; retained for inspection'
    fi
    write_assembly_report "$assembly_inputs_json"
    write_assembly_state succeeded "$attempts" '' "$assembly_inputs_json"
    validate_workspace_state
    print_status
}

write_acquire_stage() {
    local basin_id=$1
    local stage=$2
    local status=$3
    local attempts=$4
    local reason=$5
    local evidence_json=$6
    local current=$campaign_dir/state/basins/$basin_id/current.json
    local temporary=$campaign_dir/state/basins/$basin_id/.current.json.tmp.$$
    "$JQ" --arg stage "$stage" --arg status "$status" --arg reason "$reason" \
        --argjson attempts "$attempts" --argjson evidence "$evidence_json" '
        .stages[$stage].status = $status |
        .stages[$stage].attempts = $attempts |
        .stages[$stage].failure_reason = (if $reason == "" then null else $reason end) |
        .stages[$stage].evidence = $evidence
    ' "$current" >"$temporary"
    atomic_install "$temporary" "$current" validate_basin_json "$basin_id"
}

write_compile_stage() {
    local basin_id=$1
    local status=$2
    local attempts=$3
    local reason=$4
    local diagnostic_report=${5-null}
    local current=$campaign_dir/state/basins/$basin_id/current.json
    local temporary=$campaign_dir/state/basins/$basin_id/.current.json.tmp.$$
    "$JQ" --arg status "$status" --arg reason "$reason" --argjson attempts "$attempts" \
        --argjson diagnostic_report "$diagnostic_report" '
        .stages.compile.status = $status |
        .stages.compile.attempts = $attempts |
        .stages.compile.failure_reason = (if $reason == "" then null else $reason end) |
        .stages.compile.diagnostic_report = $diagnostic_report
    ' "$current" >"$temporary"
    atomic_install "$temporary" "$current" validate_basin_json "$basin_id"
}

establish_compile_contract() {
    local contract=$campaign_dir/state/compile.json
    local temporary
    local persisted_fabric_version
    if [[ -e "$contract" || -L "$contract" ]]; then
        [[ -f "$contract" && ! -L "$contract" ]] ||
            hfx_die "compile state is not a regular file: $contract"
        validate_compile_json "$contract"
        persisted_fabric_version=$("$JQ" -r '.fabric_version' "$contract")
        [[ "$persisted_fabric_version" == "$fabric_version" ]] ||
            hfx_die 'fabric version changed; use a new campaign ID'
        return
    fi
    temporary=$campaign_dir/state/.compile.json.tmp.$$
    "$JQ" -n --arg fabric_version "$fabric_version" '{
      schema_version: 1,
      fabric_version: $fabric_version
    }' >"$temporary"
    atomic_install "$temporary" "$contract" validate_compile_json
}

diagnostic_report_json=
verify_compile_report() {
    local basin_id=$1
    local output=$2
    local report=$3
    local resolved_output
    local diagnostics
    [[ -d "$output" && ! -L "$output" ]] || return 1
    [[ -f "$report" && ! -L "$report" && -s "$report" ]] || return 1
    resolved_output=$(cd -P "$output" && pwd -P) || return 1
    "$JQ" -e --arg basin_id "$basin_id" --arg fabric_version "$fabric_version" \
        --arg dataset_root "$resolved_output" '
        .build_identity.processing_basin_id == $basin_id and
        .build_identity.fabric_name == "tdx_hydro" and
        .build_identity.fabric_version == $fabric_version and
        .build_identity.dataset_root == $dataset_root and
        (.diagnostics | type == "object")
    ' "$report" >/dev/null 2>&1 || return 1
    diagnostics=$("$JQ" -cS '.diagnostics' "$report") || return 1
    diagnostic_report_json=$("$JQ" -cnS --arg path "reports/$basin_id-build-report.json" \
        --argjson diagnostics "$diagnostics" '{path:$path,diagnostics:$diagnostics}') || return 1
}

verify_compile_artifacts() {
    local basin_id=$1
    local output=$2
    local report=$3
    verify_compile_report "$basin_id" "$output" "$report" || return 1
    "$ADAPTER_PYTHON" "$ADAPTER_SCRIPT" validate "$output" --hfx-binary "$HFX"
}

interrupt_reclaim_boundary() {
    local basin_id=$1
    local boundary=$2
    if [[ ${HFX_TEST_INTERRUPT_AFTER-} == "$basin_id:$boundary" ]]; then
        kill -TERM "$$"
        exit 143
    fi
}

reclaim_terminal_inputs() {
    local basin_id=$1
    local downloads=$campaign_dir/downloads
    local current=$campaign_dir/state/basins/$basin_id/current.json
    local temporary=$campaign_dir/state/basins/$basin_id/.current.json.tmp.$$
    local candidate_path
    local paths=(
        "$downloads/$basin_id-basins.gpkg"
        "$downloads/$basin_id-basins.gpkg.partial"
        "$downloads/$basin_id-basins.gpkg.partial.json"
        "$downloads/$basin_id-streamnet.gpkg"
        "$downloads/$basin_id-streamnet.gpkg.partial"
        "$downloads/$basin_id-streamnet.gpkg.partial.json"
    )

    [[ -d "$downloads" && ! -L "$downloads" ]] ||
        hfx_die "reclaim source path is unsafe for $basin_id: $downloads; move that exact path out of downloads, replace downloads with a non-symlink directory if needed, then rerun recover"
    for candidate_path in ${paths[@]+"${paths[@]}"}; do
        if [[ -e "$candidate_path" || -L "$candidate_path" ]]; then
            [[ -f "$candidate_path" && ! -L "$candidate_path" ]] ||
                hfx_die "reclaim source path is unsafe for $basin_id: $candidate_path; move that exact path out of downloads, replace downloads with a non-symlink directory if needed, then rerun recover"
        fi
    done

    "$RM" -f -- "${paths[0]}" "${paths[1]}" "${paths[2]}"
    interrupt_reclaim_boundary "$basin_id" basins-input-reclaimed
    "$RM" -f -- "${paths[3]}" "${paths[4]}" "${paths[5]}"
    interrupt_reclaim_boundary "$basin_id" streamnet-input-reclaimed
    "$JQ" '.retention.inputs_reclaimed = true' "$current" >"$temporary"
    atomic_install "$temporary" "$current" validate_basin_json "$basin_id"
    interrupt_reclaim_boundary "$basin_id" reclaimed-state
}

reconcile_reclaim_basin() {
    local basin_id=$1
    local verify_success=$2
    local current=$campaign_dir/state/basins/$basin_id/current.json
    local schema_version
    local compile_status
    local output=$campaign_dir/basin-outputs/$basin_id
    local report=$campaign_dir/reports/$basin_id-build-report.json
    local retained_path

    validate_basin_json "$current" "$basin_id"
    [[ $("$JQ" -r '.retention.policy' "$campaign_dir/state/campaign.json") == reclaim-inputs-after-terminal ]] ||
        return 1
    schema_version=$("$JQ" -r '.schema_version' "$current")
    [[ "$schema_version" == 4 ]] || return 1
    compile_status=$("$JQ" -r '.stages.compile.status' "$current")

    if [[ $("$JQ" -r '.retention.inputs_reclaimed' "$current") == true ]]; then
        for retained_path in \
            "$campaign_dir/downloads/$basin_id-basins.gpkg" \
            "$campaign_dir/downloads/$basin_id-basins.gpkg.partial" \
            "$campaign_dir/downloads/$basin_id-basins.gpkg.partial.json" \
            "$campaign_dir/downloads/$basin_id-streamnet.gpkg" \
            "$campaign_dir/downloads/$basin_id-streamnet.gpkg.partial" \
            "$campaign_dir/downloads/$basin_id-streamnet.gpkg.partial.json"; do
            [[ ! -e "$retained_path" && ! -L "$retained_path" ]] ||
                hfx_die "reclaimed basin source artifact remains for $basin_id: $retained_path; move that exact path out of downloads, then rerun recover"
        done
        if [[ "$verify_success" == true && "$compile_status" == succeeded ]]; then
            diagnostic_report_json=
            verify_compile_artifacts "$basin_id" "$output" "$report" ||
                hfx_die "reclaimed compile artifacts failed verification for $basin_id; restore retained output and report, then rerun compile"
        fi
        return 0
    fi

    [[ $(reclaim_eligibility "$basin_id") == eligible ]] || return 1

    if [[ "$compile_status" == succeeded ]]; then
        if [[ "$verify_success" == true ]]; then
            diagnostic_report_json=
            verify_compile_artifacts "$basin_id" "$output" "$report" ||
                hfx_die "compile artifacts failed verification before reclaim for $basin_id"
        else
            for retained_path in \
                "$output/catchments.parquet" \
                "$output/graph.parquet" \
                "$output/aux/snap_stems.parquet" \
                "$report"; do
                [[ -f "$retained_path" && ! -L "$retained_path" ]] ||
                    hfx_die "retained compile evidence is missing or unsafe for $basin_id: $retained_path"
            done
        fi
    fi
    reclaim_terminal_inputs "$basin_id"
    return 0
}

compile_basin_core() {
    local basin_id=$1
    local defer_reclaim=${2-false}
    local current
    local acquire_basins_status
    local acquire_streamnet_status
    local compile_status
    local attempts
    local basins
    local streamnet
    local output
    local report
    [[ "$defer_reclaim" == true || "$defer_reclaim" == false ]] ||
        hfx_die "invalid deferred reclaim value: $defer_reclaim"

    if reconcile_reclaim_basin "$basin_id" true; then
        return
    fi
    current=$campaign_dir/state/basins/$basin_id/current.json
    acquire_basins_status=$("$JQ" -r '.stages.acquire_basins.status' "$current")
    acquire_streamnet_status=$("$JQ" -r '.stages.acquire_streamnet.status' "$current")
    compile_status=$("$JQ" -r '.stages.compile.status' "$current")
    attempts=$("$JQ" -r '.stages.compile.attempts' "$current")

    [[ "$acquire_basins_status" == succeeded && "$acquire_streamnet_status" == succeeded ]] ||
        hfx_die "acquisition prerequisites are not both succeeded for $basin_id"

    basins=$campaign_dir/downloads/$basin_id-basins.gpkg
    streamnet=$campaign_dir/downloads/$basin_id-streamnet.gpkg
    output=$campaign_dir/basin-outputs/$basin_id
    report=$campaign_dir/reports/$basin_id-build-report.json

    if [[ "$compile_status" == succeeded ]]; then
        diagnostic_report_json=
        if verify_compile_artifacts "$basin_id" "$output" "$report"; then
            if [[ $("$JQ" -r '.stages.compile.diagnostic_report == null' "$current") == true ]]; then
                write_compile_stage "$basin_id" succeeded "$attempts" '' "$diagnostic_report_json"
                if [[ "$defer_reclaim" == false ]] &&
                    reconcile_reclaim_basin "$basin_id" true; then
                    :
                fi
            fi
            return
        fi
        write_compile_stage "$basin_id" failed "$attempts" \
            'existing compile artifacts failed resume verification; retained for inspection' \
            "${diagnostic_report_json:-null}"
        return
    fi

    if [[ -e "$output" || -L "$output" || -e "$report" || -L "$report" ]]; then
        diagnostic_report_json=
        verify_compile_report "$basin_id" "$output" "$report" || :
        write_compile_stage "$basin_id" failed "$attempts" \
            'compile artifact path already exists; retained for inspection' \
            "${diagnostic_report_json:-null}"
        return
    fi

    attempts=$((attempts + 1))
    write_compile_stage "$basin_id" running "$attempts" ''
    if ! "$ADAPTER_PYTHON" "$ADAPTER_SCRIPT" build \
        --basins "$basins" \
        --streamnet "$streamnet" \
        --out "$output" \
        --report "$report" \
        --processing-basin-id "$basin_id" \
        --fabric-version "$fabric_version"; then
        diagnostic_report_json=
        verify_compile_report "$basin_id" "$output" "$report" || :
        interrupt_reclaim_boundary "$basin_id" compile-attempt-complete
        write_compile_stage "$basin_id" failed "$attempts" 'adapter build failed' \
            "${diagnostic_report_json:-null}"
        interrupt_reclaim_boundary "$basin_id" terminal-state
        if [[ "$defer_reclaim" == false ]] &&
            reconcile_reclaim_basin "$basin_id" true; then
            :
        fi
        return
    fi
    diagnostic_report_json=
    if ! verify_compile_report "$basin_id" "$output" "$report"; then
        interrupt_reclaim_boundary "$basin_id" compile-attempt-complete
        write_compile_stage "$basin_id" failed "$attempts" 'adapter validation failed'
        interrupt_reclaim_boundary "$basin_id" terminal-state
        if [[ "$defer_reclaim" == false ]] &&
            reconcile_reclaim_basin "$basin_id" true; then
            :
        fi
        return
    fi
    if ! "$ADAPTER_PYTHON" "$ADAPTER_SCRIPT" validate "$output" --hfx-binary "$HFX"; then
        interrupt_reclaim_boundary "$basin_id" compile-attempt-complete
        write_compile_stage "$basin_id" failed "$attempts" 'adapter validation failed' "$diagnostic_report_json"
        interrupt_reclaim_boundary "$basin_id" terminal-state
        if [[ "$defer_reclaim" == false ]] &&
            reconcile_reclaim_basin "$basin_id" true; then
            :
        fi
        return
    fi
    interrupt_reclaim_boundary "$basin_id" compile-attempt-complete
    write_compile_stage "$basin_id" succeeded "$attempts" '' "$diagnostic_report_json"
    interrupt_reclaim_boundary "$basin_id" terminal-state
    if [[ "$defer_reclaim" == false ]] &&
        reconcile_reclaim_basin "$basin_id" true; then
        :
    fi
}

print_basin_compile_status() {
    local basin_id=$1
    local current=$campaign_dir/state/basins/$basin_id/current.json
    printf 'processing_basin_id=%s\n' "$basin_id"
    "$JQ" -r '
      "compile_status=\(.stages.compile.status)",
      "compile_attempts=\(.stages.compile.attempts)",
      (if .schema_version == 4
       then "inputs_reclaimed=\(.retention.inputs_reclaimed)"
       else "inputs_reclaimed=not-applicable" end)
    ' "$current"
}

compile_basin_locked() {
    local basin_id=$1
    local defer_reclaim=${2-false}
    [[ "$defer_reclaim" == true || "$defer_reclaim" == false ]] ||
        hfx_die "invalid deferred reclaim value: $defer_reclaim"
    acquire_campaign_lock
    validate_target_workspace_state "$basin_id"
    migrate_basin_state "$basin_id"
    recover_running_stages_for_basin "$basin_id" false
    validate_target_workspace_state "$basin_id"
    if reconcile_reclaim_basin "$basin_id" true; then
        :
    else
        [[ $("$JQ" -r '.stages.acquire_basins.status' "$campaign_dir/state/basins/$basin_id/current.json") == succeeded &&
            $("$JQ" -r '.stages.acquire_streamnet.status' "$campaign_dir/state/basins/$basin_id/current.json") == succeeded ]] ||
            hfx_die "acquisition prerequisites are not both succeeded for $basin_id"
    fi
    establish_compile_contract
    compile_basin_core "$basin_id" "$defer_reclaim"
    validate_target_workspace_state "$basin_id"
    print_basin_compile_status "$basin_id"
}

compile_selected_basin() {
    local basin_id=$1
    acquire_campaign_lock
    validate_target_workspace_state "$basin_id"
    compile_basin_locked "$basin_id"
}

compile_campaign() {
    local basin_id
    local current
    local attempts
    acquire_campaign_lock
    validate_workspace_state
    establish_compile_contract
    migrate_basin_states
    recover_running_stages false
    validate_workspace_state
    while IFS= read -r basin_id; do
        if reconcile_reclaim_basin "$basin_id" true; then
            continue
        fi
        current=$campaign_dir/state/basins/$basin_id/current.json
        attempts=$("$JQ" -r '.stages.compile.attempts' "$current")
        if [[ $("$JQ" -r '.stages.acquire_basins.status' "$current") != succeeded ||
            $("$JQ" -r '.stages.acquire_streamnet.status' "$current") != succeeded ]]; then
            write_compile_stage "$basin_id" failed "$attempts" \
                'acquisition prerequisites are not both succeeded'
            continue
        fi
        compile_basin_core "$basin_id"
    done < <(effective_basin_ids)
    validate_workspace_state
    print_status
}

validate_acquisition_evidence() {
    local candidate=$1
    if [[ -f "$campaign_dir/state/selection.json" ]]; then
        "$JQ" -e --arg campaign "$campaign" \
            --slurpfile inventory "$campaign_dir/state/inventory.json" \
            --slurpfile selection "$campaign_dir/state/selection.json" '
          ($selection[0].basin_ids) as $selected |
          (($inventory[0] | keys) - $selected) as $unselected |
          type == "object" and
          keys == ["basins","campaign","schema_version","selected_basin_ids","unselected_basin_ids"] and
          .schema_version == 2 and .campaign == $campaign and
          .selected_basin_ids == $selected and .unselected_basin_ids == $unselected and
          (($selected + $unselected) | sort) == ($inventory[0] | keys) and
          ([.basins[].processing_basin_id] == $selected) and
          (.basins | all(type == "object" and keys == ["processing_basin_id","products"] and
            (.products | type == "object" and keys == ["basins","streamnet"] and
              all(.[]; type == "object" and keys == ["attempts","evidence","failure_reason","status"]))))
        ' "$candidate" >/dev/null 2>&1 ||
            hfx_die "acquisition evidence is malformed: $candidate"
        return
    fi
    "$JQ" -e --arg campaign "$campaign" '
      type == "object" and keys == ["basins","campaign","schema_version"] and
      .schema_version == 1 and .campaign == $campaign and
      (.basins | type == "array" and
        all(type == "object" and keys == ["processing_basin_id","products"] and
          (.processing_basin_id | test("^[0-9]{10}$")) and
          (.products | type == "object" and keys == ["basins","streamnet"] and
            all(.[]; type == "object" and keys == ["attempts","evidence","failure_reason","status"])))) and
      ([.basins[].processing_basin_id] as $ids | $ids == ($ids | sort) and
        ($ids | length) == ($ids | unique | length))
    ' "$candidate" >/dev/null 2>&1 || hfx_die "acquisition evidence is malformed: $candidate"
}

validate_outcomes_evidence() {
    local candidate=$1
    if [[ -f "$campaign_dir/state/selection.json" ]]; then
        "$JQ" -e --arg campaign "$campaign" \
            --slurpfile inventory "$campaign_dir/state/inventory.json" \
            --slurpfile selection "$campaign_dir/state/selection.json" '
          ($selection[0].basin_ids) as $selected |
          (($inventory[0] | keys) - $selected) as $unselected |
          . as $document |
          type == "object" and
          keys == ["attempted_basin_ids","campaign","excluded_basins","outcomes","schema_version","selected_basin_ids","unselected_basin_ids"] and
          .schema_version == 2 and .campaign == $campaign and
          .selected_basin_ids == $selected and .unselected_basin_ids == $unselected and
          (($selected + $unselected) | sort) == ($inventory[0] | keys) and
          .attempted_basin_ids == (.attempted_basin_ids | sort | unique) and
          (.attempted_basin_ids - $selected | length) == 0 and
          (.excluded_basins | type == "array" and all(
            type == "object" and keys == ["failure_reason","processing_basin_id"] and
            (.failure_reason | type == "string" and length > 0))) and
          ([.excluded_basins[].processing_basin_id] ==
            ([.excluded_basins[].processing_basin_id] | sort | unique)) and
          (([.excluded_basins[].processing_basin_id] - $selected) | length) == 0 and
          (.outcomes | type == "array" and all(
            type == "object" and
            keys == ["attempts","failure_reason","processing_basin_id","status"] and
            (.status == "pending" or .status == "running" or
              .status == "succeeded" or .status == "failed") and
            (.attempts | type == "number" and . == floor and . >= 0) and
            (.failure_reason == null or (.failure_reason | type == "string")))) and
          ([.outcomes[].processing_basin_id] == $selected) and
          all(.excluded_basins[];
            . as $excluded | any($document.outcomes[];
              .processing_basin_id == $excluded.processing_basin_id and
              .status == "failed" and
              .failure_reason == $excluded.failure_reason))
        ' "$candidate" >/dev/null 2>&1 ||
            hfx_die "outcomes evidence is malformed: $candidate"
        return
    fi
    "$JQ" -e --arg campaign "$campaign" '
      type == "object" and
      keys == ["attempted_basin_ids","campaign","excluded_basins","outcomes","schema_version"] and
      .schema_version == 1 and .campaign == $campaign and
      (.attempted_basin_ids | type == "array" and all(test("^[0-9]{10}$"))) and
      (.excluded_basins | type == "array" and
        all(type == "object" and keys == ["failure_reason","processing_basin_id"])) and
      (.outcomes | type == "array" and
        all(type == "object" and keys == ["attempts","failure_reason","processing_basin_id","status"])) and
      (.attempted_basin_ids == (.attempted_basin_ids | sort | unique)) and
      ([.excluded_basins[].processing_basin_id] == ([.excluded_basins[].processing_basin_id] | sort | unique)) and
      ([.outcomes[].processing_basin_id] == ([.outcomes[].processing_basin_id] | sort | unique))
    ' "$candidate" >/dev/null 2>&1 || hfx_die "outcomes evidence is malformed: $candidate"
}

validate_diagnostics_evidence() {
    local candidate=$1
    if [[ -f "$campaign_dir/state/selection.json" ]]; then
        "$JQ" -e --arg campaign "$campaign" \
            --slurpfile inventory "$campaign_dir/state/inventory.json" \
            --slurpfile selection "$campaign_dir/state/selection.json" '
          ($selection[0].basin_ids) as $selected |
          (($inventory[0] | keys) - $selected) as $unselected |
          type == "object" and
          keys == ["basins","campaign","schema_version","selected_basin_ids","unselected_basin_ids"] and
          .schema_version == 2 and .campaign == $campaign and
          .selected_basin_ids == $selected and .unselected_basin_ids == $unselected and
          (($selected + $unselected) | sort) == ($inventory[0] | keys) and
          ([.basins[].processing_basin_id] == $selected) and
          (.basins | all(type == "object" and
            keys == ["diagnostics","processing_basin_id","report_path","unavailable_reason"] and
            ((.diagnostics == null and .report_path == null and
              (.unavailable_reason | type == "string" and length > 0)) or
             ((.diagnostics | type == "object") and
              (.report_path | type == "string") and .unavailable_reason == null))))
        ' "$candidate" >/dev/null 2>&1 ||
            hfx_die "diagnostics evidence is malformed: $candidate"
        return
    fi
    "$JQ" -e --arg campaign "$campaign" '
      type == "object" and keys == ["basins","campaign","schema_version"] and
      .schema_version == 1 and .campaign == $campaign and
      (.basins | type == "array" and all(
        type == "object" and
        keys == ["diagnostics","processing_basin_id","report_path","unavailable_reason"] and
        (.processing_basin_id | test("^[0-9]{10}$")) and
        ((.diagnostics == null and .report_path == null and
          (.unavailable_reason | type == "string" and length > 0)) or
         ((.diagnostics | type == "object") and
          (.report_path | type == "string") and .unavailable_reason == null)))) and
      ([.basins[].processing_basin_id] == ([.basins[].processing_basin_id] | sort | unique))
    ' "$candidate" >/dev/null 2>&1 || hfx_die "diagnostics evidence is malformed: $candidate"
}

generate_evidence() {
    local evidence_dir=$campaign_dir/publication/evidence
    local states=$campaign_dir/state/tmp/.evidence-states.$$
    local basin_id
    local current
    local acquisition=$campaign_dir/state/tmp/.acquisition.json.$$
    local outcomes=$campaign_dir/state/tmp/.outcomes.json.$$
    local diagnostics=$campaign_dir/state/tmp/.diagnostics.json.$$

    acquire_campaign_lock
    validate_workspace_state
    if [[ -e "$evidence_dir" || -L "$evidence_dir" ]]; then
        [[ -d "$evidence_dir" && ! -L "$evidence_dir" ]] ||
            hfx_die "evidence path is not a safe directory: $evidence_dir"
    else
        "$MKDIR" "$evidence_dir" || hfx_die "could not create evidence directory: $evidence_dir"
    fi
    : >"$states"
    while IFS= read -r basin_id; do
        current=$campaign_dir/state/basins/$basin_id/current.json
        case $("$JQ" -r '.schema_version' "$current") in
            3|4) ;;
            *) hfx_die "legacy basin state requires compile rerun before evidence: $basin_id" ;;
        esac
        if "$JQ" -e '
          .stages.compile.status == "succeeded" and .stages.compile.diagnostic_report == null
        ' "$current" >/dev/null; then
            hfx_die "diagnostic state is incomplete for $basin_id; rerun compile"
        fi
        "$JQ" -cS '.' "$current" >>"$states"
    done < <(effective_basin_ids)

    if [[ -f "$campaign_dir/state/selection.json" ]]; then
        "$JQ" -csS --arg campaign "$campaign" \
            --slurpfile inventory "$campaign_dir/state/inventory.json" \
            --slurpfile selection "$campaign_dir/state/selection.json" '
          ($selection[0].basin_ids) as $selected |
          (($inventory[0] | keys) - $selected) as $unselected |
          {
            schema_version:2,
            campaign:$campaign,
            selected_basin_ids:$selected,
            unselected_basin_ids:$unselected,
            basins:map({
              processing_basin_id,
              products:{
                basins:(.stages.acquire_basins | {status,attempts,failure_reason,evidence}),
                streamnet:(.stages.acquire_streamnet | {status,attempts,failure_reason,evidence})
              }
            })
          }
        ' "$states" >"$acquisition"
        "$JQ" -csS --arg campaign "$campaign" \
            --slurpfile inventory "$campaign_dir/state/inventory.json" \
            --slurpfile selection "$campaign_dir/state/selection.json" '
          ($selection[0].basin_ids) as $selected |
          (($inventory[0] | keys) - $selected) as $unselected |
          {
            schema_version:2,
            campaign:$campaign,
            selected_basin_ids:$selected,
            unselected_basin_ids:$unselected,
            attempted_basin_ids:(map(select([.stages[].attempts] | any(. > 0))) |
              map(.processing_basin_id)),
            excluded_basins:(map(select(.stages.compile.status == "failed") |
              {processing_basin_id,failure_reason:.stages.compile.failure_reason})),
            outcomes:map({
              processing_basin_id,
              status:.stages.compile.status,
              attempts:.stages.compile.attempts,
              failure_reason:.stages.compile.failure_reason
            })
          }
        ' "$states" >"$outcomes"
        "$JQ" -csS --arg campaign "$campaign" \
            --slurpfile inventory "$campaign_dir/state/inventory.json" \
            --slurpfile selection "$campaign_dir/state/selection.json" '
          ($selection[0].basin_ids) as $selected |
          (($inventory[0] | keys) - $selected) as $unselected |
          {
            schema_version:2,
            campaign:$campaign,
            selected_basin_ids:$selected,
            unselected_basin_ids:$unselected,
            basins:map(
              if .stages.compile.diagnostic_report != null then {
                processing_basin_id,
                diagnostics:.stages.compile.diagnostic_report.diagnostics,
                report_path:.stages.compile.diagnostic_report.path,
                unavailable_reason:null
              } else {
                processing_basin_id,
                diagnostics:null,
                report_path:null,
                unavailable_reason:(.stages.compile.failure_reason // .stages.compile.status)
              } end
            )
          }
        ' "$states" >"$diagnostics"
    else
    "$JQ" -csS --arg campaign "$campaign" '
      map(select(
        ([.stages[].attempts] | any(. > 0)) or .stages.compile.status == "failed"
      )) |
      {
        schema_version:1,
        campaign:$campaign,
        basins:map({
          processing_basin_id,
          products:{
            basins:(.stages.acquire_basins | {status,attempts,failure_reason,evidence}),
            streamnet:(.stages.acquire_streamnet | {status,attempts,failure_reason,evidence})
          }
        })
      }
    ' "$states" >"$acquisition"
    "$JQ" -csS --arg campaign "$campaign" '
      . as $all |
      ($all | map(select([.stages[].attempts] | any(. > 0))) |
        map(.processing_basin_id)) as $attempted |
      ($all | map(select(
        ([.stages[].attempts] | any(. > 0)) or .stages.compile.status == "failed"
      ))) as $reportable |
      {
        schema_version:1,
        campaign:$campaign,
        attempted_basin_ids:$attempted,
        excluded_basins:($all | map(select(.stages.compile.status == "failed") |
          {processing_basin_id,failure_reason:.stages.compile.failure_reason})),
        outcomes:($reportable | map({
          processing_basin_id,
          status:.stages.compile.status,
          attempts:.stages.compile.attempts,
          failure_reason:.stages.compile.failure_reason
        }))
      }
    ' "$states" >"$outcomes"
    "$JQ" -csS --arg campaign "$campaign" '
      map(select(
        ([.stages[].attempts] | any(. > 0)) or .stages.compile.status == "failed"
      )) |
      {
        schema_version:1,
        campaign:$campaign,
        basins:map(
          if .stages.compile.diagnostic_report != null then {
            processing_basin_id,
            diagnostics:.stages.compile.diagnostic_report.diagnostics,
            report_path:.stages.compile.diagnostic_report.path,
            unavailable_reason:null
          } else {
            processing_basin_id,
            diagnostics:null,
            report_path:null,
            unavailable_reason:(.stages.compile.failure_reason // .stages.compile.status)
          } end
        )
      }
    ' "$states" >"$diagnostics"
    fi
    validate_acquisition_evidence "$acquisition"
    validate_outcomes_evidence "$outcomes"
    validate_diagnostics_evidence "$diagnostics"
    atomic_install "$acquisition" "$evidence_dir/acquisition.json" validate_acquisition_evidence
    atomic_install "$outcomes" "$evidence_dir/outcomes.json" validate_outcomes_evidence
    atomic_install "$diagnostics" "$evidence_dir/diagnostics.json" validate_diagnostics_evidence
    "$RM" -- "$states"
}

physical_file() {
    local path=$1
    local parent=${path%/*}
    local base=${path##*/}
    local physical_parent
    [[ "$path" == /* && "$base" != "$path" ]] || return 1
    physical_parent=$(cd -P -- "$parent" && pwd -P) || return 1
    printf '%s/%s\n' "$physical_parent" "$base"
}

validate_publication_json() {
    local candidate=$1
    "$JQ" -e --arg campaign "$campaign" '
      .prefix as $prefix |
      type == "object" and
      keys == ["bucket","citation","endpoint","notice","objects","out","prefix","region","report","schema_version"] and
      .schema_version == 1 and
      .endpoint == "https://fsn1.your-objectstorage.com" and
      .region == "fsn1" and
      .bucket == "pourpoint-hfx" and
      (.prefix | type == "string" and
        test("^scratch/tdx-hydro-" + $campaign + "/[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")) and
      ([.out,.report,.notice,.citation] |
        all(type == "string" and startswith("/"))) and
      (.objects | type == "array" and length > 0 and
        all(
          type == "object" and keys == ["bytes","key","source"] and
          (.bytes | type == "number" and . == floor and . > 0) and
          (.key | type == "string" and startswith($prefix + "/")) and
          (.source | type == "string" and startswith("/"))
        )) and
      ((.objects | map(.key)) as $keys |
        $keys == ($keys | sort) and
        ($keys | length) == ($keys | unique | length))
    ' "$candidate" >/dev/null 2>&1 || hfx_die "publication state is malformed: $candidate"
}

validate_publication_campaign_state() {
    [[ -d "$campaign_dir" && ! -L "$campaign_dir" ]] ||
        hfx_die "campaign path is not a safe directory: $campaign_dir"
    for required_dir in publication state state/locks state/tmp; do
        [[ -d "$campaign_dir/$required_dir" && ! -L "$campaign_dir/$required_dir" ]] ||
            hfx_die "required campaign directory is missing or unsafe: $campaign_dir/$required_dir"
    done
    validate_campaign_json "$campaign_dir/state/campaign.json"
    validate_inventory_file "$campaign_dir/state/inventory.json"
    "$JQ" -e -S --slurp '.[0] == .[1]' "$inventory_source" "$campaign_dir/state/inventory.json" >/dev/null 2>&1 ||
        hfx_die 'campaign inventory differs from the authoritative tracked crosswalk'
    if [[ -e "$campaign_dir/state/selection.json" || -L "$campaign_dir/state/selection.json" ]]; then
        [[ -f "$campaign_dir/state/selection.json" && ! -L "$campaign_dir/state/selection.json" ]] ||
            hfx_die "basin selection state is not a regular file: $campaign_dir/state/selection.json"
        validate_selection_file "$campaign_dir/state/selection.json"
    fi
}

list_remote_inventory() {
    local destination=$1
    local page=$campaign_dir/state/tmp/.aws-page.$$
    local entries=$campaign_dir/state/tmp/.aws-entries.$$
    local tokens=$campaign_dir/state/tmp/.aws-tokens.$$
    local continuation=
    local next=
    local truncated
    local page_count=0
    : >"$entries"
    : >"$tokens"
    while :; do
        page_count=$((page_count + 1))
        if [[ -z "$continuation" ]]; then
            "$AWS" s3api list-objects-v2 --bucket pourpoint-hfx --prefix "$scratch_prefix/" \
                --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --output json --no-paginate >"$page"
        else
            "$AWS" s3api list-objects-v2 --bucket pourpoint-hfx --prefix "$scratch_prefix/" \
                --continuation-token "$continuation" \
                --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --output json --no-paginate >"$page"
        fi
        "$JQ" -e '
          type == "object" and (.IsTruncated | type == "boolean") and
          ((.Contents // []) | type == "array" and all(
            type == "object" and
            (.Key | type == "string") and
            (.Size | type == "number" and . == floor and . >= 0)
          ))
        ' "$page" >/dev/null 2>&1 || hfx_die 'remote listing returned malformed JSON'
        "$JQ" -c '(.Contents // [])[] | {key:.Key,bytes:.Size}' "$page" >>"$entries"
        truncated=$("$JQ" -r '.IsTruncated' "$page")
        [[ "$truncated" == true ]] || break
        ((page_count < HFX_TDX_MAX_LIST_PAGES)) ||
            hfx_die 'remote listing exceeded 1000 pages'
        next=$("$JQ" -r 'if (.NextContinuationToken | type) == "string" then .NextContinuationToken else "" end' "$page")
        [[ -n "$next" && ! "$next" =~ [[:cntrl:]] ]] ||
            hfx_die 'remote listing omitted a valid continuation token'
        if "$GREP" -Fx -- "$next" "$tokens" >/dev/null 2>&1; then
            hfx_die 'remote listing repeated a continuation token'
        fi
        printf '%s\n' "$next" >>"$tokens"
        continuation=$next
    done
    "$JQ" -csS '
      sort_by(.key) |
      if (map(.key) | length) != (map(.key) | unique | length) then
        error("duplicate remote key")
      else . end
    ' "$entries" >"$destination" 2>/dev/null ||
        hfx_die 'remote listing contains duplicate keys'
    "$RM" -- "$page" "$entries" "$tokens"
}

publish_campaign() {
    local out_physical
    local report_physical
    local notice_physical
    local citation_physical
    local campaign_physical
    local ancestor_physical
    local campaign_marker
    local campaign_marker_root
    local private_path
    local private_physical
    local entry
    local relative
    local bytes
    local inventory=$campaign_dir/state/tmp/.publication-inventory.$$
    local objects=$campaign_dir/state/tmp/.publication-objects.$$
    local candidate=$campaign_dir/state/tmp/.publication.json.$$
    local contract=$campaign_dir/publication/current.json
    local remote=$campaign_dir/state/tmp/.remote.json.$$
    local missing=$campaign_dir/state/tmp/.missing.tsv.$$
    local campaign_markers=$campaign_dir/state/tmp/.publication-campaign-markers.$$
    local source
    local key
    local existing_canonical
    local requested_canonical

    acquire_campaign_lock
    validate_publication_campaign_state
    [[ "$scratch_prefix" =~ ^scratch/tdx-hydro-$campaign/[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] ||
        hfx_die 'scratch prefix must match the campaign-scoped scratch grammar'
    for entry in "$publication_out" "$publication_report" "$publication_notice" "$publication_citation"; do
        [[ "$entry" == /* && ! "$entry" =~ [[:cntrl:]] ]] ||
            hfx_die "publication paths must be absolute and contain no control characters: $entry"
    done
    [[ -d "$publication_out" && ! -L "$publication_out" ]] ||
        hfx_die "publication output is not a regular non-symlink directory: $publication_out"
    [[ -f "$publication_report" && ! -L "$publication_report" && -s "$publication_report" ]] ||
        hfx_die "publication report is not a nonempty regular non-symlink file: $publication_report"
    [[ -f "$publication_notice" && ! -L "$publication_notice" && -s "$publication_notice" ]] ||
        hfx_die "NOTICE is not a nonempty regular non-symlink file: $publication_notice"
    [[ -f "$publication_citation" && ! -L "$publication_citation" && -s "$publication_citation" ]] ||
        hfx_die "CITATION.txt is not a nonempty regular non-symlink file: $publication_citation"
    out_physical=$(cd -P -- "$publication_out" && pwd -P) ||
        hfx_die "could not resolve publication output: $publication_out"
    report_physical=$(physical_file "$publication_report") ||
        hfx_die "could not resolve publication report: $publication_report"
    notice_physical=$(physical_file "$publication_notice") ||
        hfx_die "could not resolve NOTICE: $publication_notice"
    citation_physical=$(physical_file "$publication_citation") ||
        hfx_die "could not resolve CITATION.txt: $publication_citation"
    case $report_physical in
        "$out_physical"|"$out_physical"/*) hfx_die 'publication report must be outside the dataset directory' ;;
    esac
    campaign_physical=$(cd -P -- "$campaign_dir" && pwd -P) ||
        hfx_die "could not resolve current campaign directory: $campaign_dir"
    if [[ -f "$out_physical/state/campaign.json" || -L "$out_physical/state/campaign.json" ]]; then
        hfx_die 'publication output must not be a campaign directory'
    fi
    ancestor_physical=$out_physical
    while [[ "$ancestor_physical" != / ]]; do
        ancestor_physical=${ancestor_physical%/*}
        [[ -n "$ancestor_physical" ]] || ancestor_physical=/
        if [[ -f "$ancestor_physical/state/campaign.json" ||
              -L "$ancestor_physical/state/campaign.json" ]]; then
            [[ "$ancestor_physical" == "$campaign_physical" ]] ||
                hfx_die "publication output must not be in another campaign directory: $ancestor_physical"
            break
        fi
    done
    "$FIND" "$out_physical" -mindepth 2 \
        -name campaign.json -path '*/state/campaign.json' \
        \( -type f -o -type l \) -print0 >"$campaign_markers" ||
        hfx_die "could not inspect publication output for campaign directories: $out_physical"
    while IFS= read -r -d '' campaign_marker; do
        campaign_marker_root=${campaign_marker%/state/campaign.json}
        [[ "$campaign_marker_root" == "$campaign_physical" ]] ||
            hfx_die "publication output must not contain another campaign directory: $campaign_marker_root"
    done <"$campaign_markers"
    "$RM" -- "$campaign_markers"
    case $campaign_physical in
        "$out_physical"|"$out_physical"/*)
            hfx_die 'publication output must not contain the private campaign directory'
            ;;
    esac
    for private_path in basin-outputs downloads reports state publication assembly/scratch; do
        private_physical=$(cd -P -- "$campaign_dir/$private_path" && pwd -P)
        case $out_physical in
            "$private_physical"|"$private_physical"/*)
                hfx_die "publication output must not be in the private campaign subtree: $private_path"
                ;;
        esac
        case $private_physical in
            "$out_physical"|"$out_physical"/*)
                hfx_die "publication output must not contain the private campaign subtree: $private_path"
                ;;
        esac
    done
    : >"$inventory"
    while IFS= read -r -d '' entry; do
        [[ ! -L "$entry" ]] || hfx_die "dataset contains a symlink: $entry"
        if [[ -d "$entry" ]]; then
            continue
        fi
        [[ -f "$entry" ]] || hfx_die "dataset contains a non-regular entry: $entry"
        relative=${entry#"$out_physical/"}
        [[ "$relative" != "$entry" && "$relative" != /* && ! "$relative" =~ [[:cntrl:]\\] ]] ||
            hfx_die "dataset contains an unsafe relative path: $relative"
        case /$relative/ in
            */./*|*/../*|*//* ) hfx_die "dataset contains an unsafe path component: $relative" ;;
        esac
        case $relative in
            NOTICE|CITATION.txt|build-report.json) hfx_die "dataset key collides with reserved root key: $relative" ;;
        esac
        bytes=$("$WC" -c <"$entry" | "$TR" -d '[:space:]')
        [[ "$bytes" =~ ^[0-9]+$ && "$bytes" != 0 ]] ||
            hfx_die "dataset files must be nonempty regular files: $entry"
        printf '%s\t%s\t%s\n' "$scratch_prefix/$relative" "$bytes" "$entry" >>"$inventory"
    done < <("$FIND" "$out_physical" -mindepth 1 -print0)
    [[ -s "$inventory" ]] || hfx_die 'publication dataset contains no files'
    printf '%s\t%s\t%s\n' "$scratch_prefix/CITATION.txt" "$("$WC" -c <"$citation_physical" | "$TR" -d '[:space:]')" "$citation_physical" >>"$inventory"
    printf '%s\t%s\t%s\n' "$scratch_prefix/NOTICE" "$("$WC" -c <"$notice_physical" | "$TR" -d '[:space:]')" "$notice_physical" >>"$inventory"
    printf '%s\t%s\t%s\n' "$scratch_prefix/build-report.json" "$("$WC" -c <"$report_physical" | "$TR" -d '[:space:]')" "$report_physical" >>"$inventory"
    LC_ALL=C "$SORT" -o "$inventory" "$inventory"
    "$JQ" -Rn '[inputs | split("\t") | {key:.[0],bytes:(.[1]|tonumber),source:.[2]}]' \
        <"$inventory" >"$objects"
    "$JQ" -cnS \
        --arg out "$out_physical" --arg report "$report_physical" \
        --arg notice "$notice_physical" --arg citation "$citation_physical" \
        --arg prefix "$scratch_prefix" --slurpfile objects "$objects" '{
          schema_version:1,
          endpoint:"https://fsn1.your-objectstorage.com",
          region:"fsn1",
          bucket:"pourpoint-hfx",
          prefix:$prefix,
          out:$out,
          report:$report,
          notice:$notice,
          citation:$citation,
          objects:$objects[0]
        }' >"$candidate"
    validate_publication_json "$candidate"
    if [[ -e "$contract" || -L "$contract" ]]; then
        [[ -f "$contract" && ! -L "$contract" ]] ||
            hfx_die "publication contract is not a regular file: $contract"
        validate_publication_json "$contract"
        existing_canonical=$("$JQ" -cS '.' "$contract")
        requested_canonical=$("$JQ" -cS '.' "$candidate")
        [[ "$existing_canonical" == "$requested_canonical" ]] ||
            hfx_die 'publication parameters or local inventory changed; use a new campaign'
        "$RM" -- "$candidate"
    else
        atomic_install "$candidate" "$contract" validate_publication_json
    fi
    AWS=$(resolve_command HFX_TDX_AWS aws)
    list_remote_inventory "$remote"
    "$JQ" -e --slurpfile expected "$contract" '
      . as $remote |
      ($expected[0].objects | map({key:.key,value:.bytes}) | from_entries) as $sizes |
      all($remote[]; . as $item |
        ($sizes | has($item.key)) and $sizes[$item.key] == $item.bytes)
    ' "$remote" >/dev/null || hfx_die 'remote inventory contains an unexpected key or wrong size'
    "$JQ" -r --slurpfile remote "$remote" '
      ($remote[0] | map(.key)) as $present |
      .objects[] as $object |
      select(($present | index($object.key)) == null) |
      [$object.source,$object.key] | @tsv
    ' "$contract" >"$missing"
    while IFS=$'\t' read -r source key; do
        [[ -n "$source" && -n "$key" ]] || continue
        "$AWS" s3 cp "$source" "s3://pourpoint-hfx/$key" \
            --endpoint-url https://fsn1.your-objectstorage.com --region fsn1 --only-show-errors
    done <"$missing"
    list_remote_inventory "$remote"
    "$JQ" -e --slurpfile remote "$remote" '
      (.objects | map({key,bytes})) == $remote[0]
    ' "$contract" >/dev/null || hfx_die 'final remote inventory does not exactly match publication contract'
    "$RM" -- "$inventory" "$objects" "$remote" "$missing"
}

evidence_bytes=
evidence_sha256=
evidence_layer=
verify_download() {
    local file=$1
    local token
    local hash_output
    local magic
    local ogr_output=$2
    local layer_count=0
    local line
    [[ -f "$file" && ! -L "$file" && -s "$file" ]] || return 1
    evidence_bytes=$("$WC" -c <"$file" | "$TR" -d '[:space:]') || return 1
    [[ "$evidence_bytes" =~ ^[0-9]+$ && "$evidence_bytes" != 0 ]] || return 1
    if [[ ${#evidence_bytes} -gt 19 ]] ||
        [[ ${#evidence_bytes} -eq 19 && "$evidence_bytes" > "$HFX_TDX_MAX_I64" ]]; then
        return 1
    fi
    hash_output=$("$SHA256SUM" "$file") || return 1
    token=${hash_output%%[[:space:]]*}
    [[ "$token" =~ ^[0-9a-f]{64}$ ]] || return 1
    evidence_sha256=$token
    magic=$("$OD" -An -tx1 -N16 "$file" | "$TR" -d '[:space:]') || return 1
    [[ "$magic" == "$HFX_TDX_SQLITE_MAGIC" ]] || return 1
    if ! "$OGRINFO" -ro -so "$file" >"$ogr_output" 2>&1; then
        return 1
    fi
    evidence_layer=
    while IFS= read -r line; do
        if [[ "$line" =~ ^[[:space:]]*[0-9]+:[[:space:]]+(.+)$ ]]; then
            layer_count=$((layer_count + 1))
            evidence_layer=${BASH_REMATCH[1]}
            evidence_layer=${evidence_layer%% \(*}
        fi
    done <"$ogr_output"
    [[ "$layer_count" -eq 1 && -n "$evidence_layer" ]] || return 1
    [[ ! "$evidence_layer" =~ [[:cntrl:]] ]] || return 1
}

evidence_json() {
    "$JQ" -cn --argjson bytes "$evidence_bytes" --arg sha256 "$evidence_sha256" \
        --arg magic "$HFX_TDX_SQLITE_MAGIC" --arg layer "$evidence_layer" \
        '{bytes:$bytes,sha256:$sha256,sqlite_identity:$magic,layer_name:$layer}'
}

fail_product() {
    local basin_id=$1
    local stage=$2
    local attempts=$3
    local reason=$4
    write_acquire_stage "$basin_id" "$stage" failed "$attempts" "$reason" null
}

is_positive_i64() {
    local value=$1
    [[ "$value" =~ ^[1-9][0-9]*$ ]] || return 1
    if [[ ${#value} -lt 19 ]]; then
        return 0
    fi
    [[ ${#value} -eq 19 ]] || return 1
    [[ "$value" < "$HFX_TDX_MAX_I64" || "$value" == "$HFX_TDX_MAX_I64" ]]
}

strong_etag_is_safe() {
    local value=$1
    [[ "$value" =~ ^\"[^\"]+\"$ && "$value" != W/* && "$value" != w/* &&
       ! "$value" =~ [[:cntrl:]] ]]
}

file_size_and_hash() {
    local file=$1
    local hash_output
    provenance_bytes=$("$WC" -c <"$file" | "$TR" -d '[:space:]') || return 1
    is_positive_i64 "$provenance_bytes" || return 1
    hash_output=$("$SHA256SUM" "$file") || return 1
    provenance_sha256=${hash_output%%[[:space:]]*}
    [[ "$provenance_sha256" =~ ^[0-9a-f]{64}$ ]]
}

validate_sidecar() {
    local sidecar=$1
    local partial=$2
    local basin_id=$3
    local product=$4
    local url=$5
    [[ -f "$sidecar" && ! -L "$sidecar" ]] || return 1
    "$JQ" -e --arg basin "$basin_id" --arg product "$product" --arg url "$url" '
      (keys | sort) == ["bytes","etag","processing_basin_id","product",
                       "remote_total_bytes","schema_version","sha256","url"] and
      .schema_version == 1 and .processing_basin_id == $basin and
      .product == $product and .url == $url and
      (.bytes | type == "number" and floor == . and . > 0 and . <= 9223372036854775807) and
      (.remote_total_bytes | type == "number" and floor == . and . > 0 and
       . <= 9223372036854775807) and
      .bytes <= .remote_total_bytes and
      (.sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
      (.etag | type == "string")
    ' "$sidecar" >/dev/null || return 1
    saved_bytes=$("$JQ" -r '.bytes' "$sidecar")
    saved_total=$("$JQ" -r '.remote_total_bytes' "$sidecar")
    saved_sha256=$("$JQ" -r '.sha256' "$sidecar")
    saved_etag=$("$JQ" -r '.etag' "$sidecar")
    strong_etag_is_safe "$saved_etag" || return 1
    file_size_and_hash "$partial" || return 1
    [[ "$provenance_bytes" == "$saved_bytes" && "$provenance_sha256" == "$saved_sha256" ]]
}

write_sidecar() {
    local sidecar=$1
    local sidecar_tmp=$2
    local basin_id=$3
    local product=$4
    local url=$5
    local etag=$6
    local bytes=$7
    local total=$8
    local sha256=$9
    strong_etag_is_safe "$etag" && is_positive_i64 "$bytes" &&
        is_positive_i64 "$total" && ((bytes <= total)) || return 1
    "$JQ" -cnS --arg basin "$basin_id" --arg product "$product" --arg url "$url" \
        --arg etag "$etag" --argjson bytes "$bytes" --argjson total "$total" \
        --arg sha256 "$sha256" '{
          schema_version:1,processing_basin_id:$basin,product:$product,url:$url,
          etag:$etag,bytes:$bytes,remote_total_bytes:$total,sha256:$sha256
        }' >"$sidecar_tmp" || return 1
    "$CHMOD" 0644 "$sidecar_tmp" && "$MV" "$sidecar_tmp" "$sidecar"
}

parse_final_headers() {
    local file=$1
    local line
    local value
    local header_etag_count
    local header_content_range_count
    local header_content_length_count
    header_status=
    header_etag=
    header_content_range=
    header_content_length=
    header_etag_count=0
    header_content_range_count=0
    header_content_length_count=0
    [[ -f "$file" && ! -L "$file" ]] || return 1
    while IFS= read -r line || [[ -n "$line" ]]; do
        line=${line%$'\r'}
        if [[ "$line" =~ ^HTTP/[0-9.]+[[:space:]]+([0-9][0-9][0-9])([[:space:]]|$) ]]; then
            header_status=${BASH_REMATCH[1]}
            header_etag=
            header_content_range=
            header_content_length=
            header_etag_count=0
            header_content_range_count=0
            header_content_length_count=0
        elif [[ -n "$header_status" ]]; then
            case $line in
                [Ee][Tt][Aa][Gg]:*)
                    header_etag_count=$((header_etag_count + 1))
                    value=${line#*:}; value=${value# }; header_etag=$value ;;
                [Cc][Oo][Nn][Tt][Ee][Nn][Tt]-[Rr][Aa][Nn][Gg][Ee]:*)
                    header_content_range_count=$((header_content_range_count + 1))
                    value=${line#*:}; value=${value# }; header_content_range=$value ;;
                [Cc][Oo][Nn][Tt][Ee][Nn][Tt]-[Ll][Ee][Nn][Gg][Tt][Hh]:*)
                    header_content_length_count=$((header_content_length_count + 1))
                    value=${line#*:}; value=${value# }; header_content_length=$value ;;
            esac
        fi
    done <"$file"
    [[ -n "$header_status" && "$header_etag_count" -le 1 &&
       "$header_content_range_count" -le 1 && "$header_content_length_count" -le 1 ]]
}

parse_transfer_stats() {
    local file=$1
    local line
    stats_http=
    stats_network=
    stats_time=
    stats_speed=
    [[ -f "$file" && ! -L "$file" ]] || return 1
    while IFS= read -r line; do
        case $line in
            http_status=*) stats_http=${line#*=} ;;
            network_bytes=*) stats_network=${line#*=} ;;
            time_total_seconds=*) stats_time=${line#*=} ;;
            average_bytes_per_second=*) stats_speed=${line#*=} ;;
            *) return 1 ;;
        esac
    done <"$file"
    [[ "$stats_http" =~ ^[0-9][0-9][0-9]$ &&
       "$stats_network" =~ ^[0-9]+([.][0-9]+)?$ &&
       "$stats_time" =~ ^[0-9]+([.][0-9]+)?$ &&
       "$stats_speed" =~ ^[0-9]+([.][0-9]+)?$ ]] || return 1
    stats_http=$((10#$stats_http))
}

validate_acquisition_report() {
    local report=$1
    local basin_id=$2
    local product=$3
    [[ -f "$report" && ! -L "$report" ]] || return 1
    "$JQ" -e --arg basin "$basin_id" --arg product "$product" '
      (keys | sort) == ["processing_basin_id","product","range_ignored_restart_count",
                       "resume_count","retry_count","schema_version","transfers"] and
      .schema_version == 1 and .processing_basin_id == $basin and .product == $product and
      ([.retry_count,.resume_count,.range_ignored_restart_count] |
       all(type == "number" and floor == . and . >= 0)) and
      (.transfers | type == "array") and
      all(.transfers[];
        (keys | sort) == ["attempt","average_bytes_per_second","http_status","mode",
                         "network_bytes","result","resume_offset_bytes","time_total_seconds"] and
        (.attempt | type == "number" and floor == . and . >= 1) and
        (.resume_offset_bytes | type == "number" and floor == . and . >= 0) and
        (.http_status | type == "number" and floor == . and . >= 0 and . <= 999) and
        (.network_bytes | type == "number" and . >= 0) and
        (.time_total_seconds | type == "number" and . >= 0) and
        (.average_bytes_per_second | type == "number" and . >= 0) and
        (.mode == "fresh" or .mode == "resume" or .mode == "range_ignored_restart") and
        (.result == "succeeded" or .result == "curl_failed" or
         .result == "integrity_failed" or .result == "range_ignored_restart"))
    ' "$report" >/dev/null
}

append_transfer_report() {
    local report=$1
    local report_tmp=$2
    local basin_id=$3
    local product=$4
    local attempt=$5
    local mode=$6
    local offset=$7
    local result=$8
    local ignored_increment=$9
    local existing
    if [[ -e "$report" || -L "$report" ]]; then
        validate_acquisition_report "$report" "$basin_id" "$product" || return 1
        existing=$report
    else
        existing=
    fi
    if [[ -n "$existing" ]]; then
        "$JQ" -cnS --slurpfile old "$existing" --argjson attempt "$attempt" \
            --arg mode "$mode" --argjson offset "$offset" --argjson status "$stats_http" \
            --argjson network "$stats_network" --argjson time "$stats_time" \
            --argjson speed "$stats_speed" --arg result "$result" \
            --argjson resumed "$([[ "$mode" == resume || "$mode" == range_ignored_restart ]] && printf 1 || printf 0)" \
            --argjson ignored "$ignored_increment" '
          $old[0] | .retry_count += 1 | .resume_count += $resumed |
          .range_ignored_restart_count += $ignored |
          .transfers += [{attempt:$attempt,mode:$mode,resume_offset_bytes:$offset,
            http_status:$status,network_bytes:$network,time_total_seconds:$time,
            average_bytes_per_second:$speed,result:$result}]
        ' >"$report_tmp" || return 1
    else
        "$JQ" -cnS --arg basin "$basin_id" --arg product "$product" \
            --argjson attempt "$attempt" --arg mode "$mode" --argjson offset "$offset" \
            --argjson status "$stats_http" --argjson network "$stats_network" \
            --argjson time "$stats_time" --argjson speed "$stats_speed" --arg result "$result" \
            --argjson resumed "$([[ "$mode" == resume || "$mode" == range_ignored_restart ]] && printf 1 || printf 0)" \
            --argjson ignored "$ignored_increment" '{
          schema_version:1,processing_basin_id:$basin,product:$product,retry_count:0,
          resume_count:$resumed,range_ignored_restart_count:$ignored,
          transfers:[{attempt:$attempt,mode:$mode,resume_offset_bytes:$offset,
            http_status:$status,network_bytes:$network,time_total_seconds:$time,
            average_bytes_per_second:$speed,result:$result}]
        }' >"$report_tmp" || return 1
    fi
    validate_acquisition_report "$report_tmp" "$basin_id" "$product" &&
        "$CHMOD" 0644 "$report_tmp" && "$MV" "$report_tmp" "$report"
}

emit_acquisition_summary() {
    local basin_id=$1
    local product=$2
    local status=$3
    local report=$4
    local retry=0 resume=0 ignored=0 network=0 time=0 speed=0
    if [[ -f "$report" && ! -L "$report" ]] && validate_acquisition_report "$report" "$basin_id" "$product"; then
        retry=$("$JQ" -r '.retry_count' "$report")
        resume=$("$JQ" -r '.resume_count' "$report")
        ignored=$("$JQ" -r '.range_ignored_restart_count' "$report")
        network=$("$JQ" -r 'if (.transfers|length)>0 then (.transfers | last | .network_bytes) else 0 end' "$report")
        time=$("$JQ" -r 'if (.transfers|length)>0 then (.transfers | last | .time_total_seconds) else 0 end' "$report")
        speed=$("$JQ" -r 'if (.transfers|length)>0 then (.transfers | last | .average_bytes_per_second) else 0 end' "$report")
    fi
    printf 'hfx: acquisition product=%s-%s status=%s retry_count=%s resume_count=%s range_ignored_restart_count=%s last_network_bytes=%s last_time_total_seconds=%s last_average_bytes_per_second=%s\n' \
        "$basin_id" "$product" "$status" "$retry" "$resume" "$ignored" "$network" "$time" "$speed" >&2
}

acquire_product() {
    local basin_id=$1
    local product=$2
    local stage=acquire_$product
    local current=$campaign_dir/state/basins/$basin_id/current.json
    local final=$campaign_dir/downloads/$basin_id-$product.gpkg
    local partial=$final.partial
    local sidecar=$partial.json
    local sidecar_tmp=$campaign_dir/downloads/.$basin_id-$product.gpkg.partial.json.tmp.$$
    local headers=$campaign_dir/state/tmp/.curl-headers.$basin_id.$product.$$
    local stats=$campaign_dir/state/tmp/.curl-stats.$basin_id.$product.$$
    local report=$campaign_dir/reports/$basin_id-$product-acquisition.json
    local report_tmp=$campaign_dir/reports/.$basin_id-$product-acquisition.json.tmp.$$
    local inspect=$campaign_dir/state/tmp/.ogr.$basin_id.$product.$$
    local persisted_status
    local attempts
    local expected
    local actual
    local mode=fresh
    local offset=0
    local curl_status
    local result
    local range_start range_last range_total
    local pre_bytes pre_sha256
    local curl_write_out='http_status=%{http_code}\nnetwork_bytes=%{size_download}\ntime_total_seconds=%{time_total}\naverage_bytes_per_second=%{speed_download}\n'
    local curl_args=()
    local url=https://earth-info.nga.mil/php/download.php?file=$basin_id-$product-gpkg
    persisted_status=$("$JQ" -r --arg stage "$stage" '.stages[$stage].status' "$current")
    attempts=$("$JQ" -r --arg stage "$stage" '.stages[$stage].attempts' "$current")

    if [[ -e "$report" || -L "$report" ]]; then
        if [[ ! -f "$report" || -L "$report" ]] ||
            ! validate_acquisition_report "$report" "$basin_id" "$product"; then
            fail_product "$basin_id" "$stage" "$attempts" 'acquisition report is unsafe or malformed; retained for inspection'
            emit_acquisition_summary "$basin_id" "$product" failed "$report"
            return
        fi
    fi

    if [[ -e "$final" || -L "$final" ]]; then
        if ! verify_download "$final" "$inspect"; then
            fail_product "$basin_id" "$stage" "$attempts" 'existing final file failed integrity verification; retained for inspection'
            "$RM" -f -- "$inspect"
            emit_acquisition_summary "$basin_id" "$product" failed "$report"
            return
        fi
        actual=$(evidence_json)
        if [[ "$persisted_status" == succeeded ]]; then
            expected=$("$JQ" -cS --arg stage "$stage" '.stages[$stage].evidence' "$current")
            actual=$(printf '%s\n' "$actual" | "$JQ" -cS '.')
            if [[ "$actual" != "$expected" ]]; then
                fail_product "$basin_id" "$stage" "$attempts" 'persisted evidence does not match final file; retained for inspection'
            fi
        else
            write_acquire_stage "$basin_id" "$stage" succeeded "$attempts" '' "$actual"
        fi
        "$RM" -f -- "$inspect"
        emit_acquisition_summary "$basin_id" "$product" reused "$report"
        return
    fi

    for candidate_path in "$partial" "$sidecar"; do
        if [[ -e "$candidate_path" || -L "$candidate_path" ]]; then
            if [[ ! -f "$candidate_path" || -L "$candidate_path" ]]; then
                fail_product "$basin_id" "$stage" "$attempts" 'partial provenance path is unsafe; retained without traversal'
                emit_acquisition_summary "$basin_id" "$product" failed "$report"
                return
            fi
        fi
    done

    if [[ -e "$partial" || -L "$partial" ]]; then
        if [[ -e "$sidecar" ]] &&
            validate_sidecar "$sidecar" "$partial" "$basin_id" "$product" "$url"; then
            mode=resume
            offset=$saved_bytes
            pre_bytes=$saved_bytes
            pre_sha256=$saved_sha256
        else
            "$RM" -f -- "$partial" "$sidecar"
        fi
    elif [[ -e "$sidecar" ]]; then
        "$RM" -- "$sidecar"
    fi

    attempts=$((attempts + 1))
    write_acquire_stage "$basin_id" "$stage" running "$attempts" '' null
    trap '"$RM" -f -- "$headers" "$stats"; exit 130' INT TERM
    while :; do
        curl_args=(--fail --show-error --location --connect-timeout 30
            --speed-limit 65536 --speed-time 60 --dump-header "$headers"
            --write-out "$curl_write_out" --output "$partial")
        if [[ "$mode" == resume ]]; then
            curl_args[${#curl_args[@]}]=--continue-at
            curl_args[${#curl_args[@]}]=-
            curl_args[${#curl_args[@]}]=--header
            curl_args[${#curl_args[@]}]="If-Range: $saved_etag"
        fi
        curl_args[${#curl_args[@]}]=$url
        curl_status=0
        "$CURL" ${curl_args[@]+"${curl_args[@]}"} >"$stats" || curl_status=$?
        parse_transfer_stats "$stats" || {
            stats_http=0; stats_network=0; stats_time=0; stats_speed=0;
        }
        parse_final_headers "$headers" || header_status=

        if [[ "$mode" == resume && "$header_status" == 200 &&
              -z "$header_content_range" ]]; then
            if ! file_size_and_hash "$partial" ||
                [[ "$provenance_bytes" != "$pre_bytes" || "$provenance_sha256" != "$pre_sha256" ]]; then
                append_transfer_report "$report" "$report_tmp" "$basin_id" "$product" \
                    "$attempts" resume "$offset" integrity_failed 0 || {
                        "$RM" -f -- "$headers" "$stats"
                        trap - INT TERM
                        return 1
                    }
                "$RM" -f -- "$partial" "$sidecar" "$headers" "$stats"
                trap - INT TERM
                fail_product "$basin_id" "$stage" "$attempts" 'partial changed during ignored-Range continuation'
                emit_acquisition_summary "$basin_id" "$product" failed "$report"
                return
            fi
            append_transfer_report "$report" "$report_tmp" "$basin_id" "$product" \
                "$attempts" range_ignored_restart "$offset" range_ignored_restart 1 || {
                    "$RM" -f -- "$headers" "$stats"
                    trap - INT TERM
                    return 1
                }
            "$RM" -- "$partial" "$sidecar"
            mode=fresh
            offset=0
            continue
        fi

        if [[ "$mode" == resume ]]; then
            if [[ "$header_status" =~ ^206$ &&
                  "$header_content_range" =~ ^bytes[[:space:]]+([0-9]+)-([0-9]+)/([0-9]+)$ ]]; then
                range_start=${BASH_REMATCH[1]}
                range_last=${BASH_REMATCH[2]}
                range_total=${BASH_REMATCH[3]}
            else
                range_start=
                range_total=
            fi
            if [[ "$range_start" != "$offset" || "$range_total" != "$saved_total" ||
                  "$header_etag" != "$saved_etag" ]]; then
                result=integrity_failed
                append_transfer_report "$report" "$report_tmp" "$basin_id" "$product" \
                    "$attempts" resume "$offset" "$result" 0 || {
                        "$RM" -f -- "$headers" "$stats"
                        trap - INT TERM
                        return 1
                    }
                "$RM" -f -- "$partial" "$sidecar" "$headers" "$stats"
                trap - INT TERM
                fail_product "$basin_id" "$stage" "$attempts" 'continuation response failed provenance verification'
                emit_acquisition_summary "$basin_id" "$product" failed "$report"
                return
            fi
            if ((curl_status != 0)); then
                if file_size_and_hash "$partial" && ((provenance_bytes <= range_total)); then
                    write_sidecar "$sidecar" "$sidecar_tmp" "$basin_id" "$product" "$url" \
                        "$saved_etag" "$provenance_bytes" "$range_total" "$provenance_sha256" || {
                            "$RM" -f -- "$headers" "$stats"
                            trap - INT TERM
                            return 1
                        }
                else
                    "$RM" -f -- "$partial" "$sidecar"
                fi
                result=curl_failed
            elif ! file_size_and_hash "$partial" ||
                [[ "$provenance_bytes" != "$range_total" || "$provenance_bytes" != "$saved_total" ]]; then
                "$RM" -f -- "$partial" "$sidecar"
                result=integrity_failed
            else
                result=succeeded
            fi
        else
            if ((curl_status != 0)); then
                result=curl_failed
                if [[ "$header_status" == 200 ]] && strong_etag_is_safe "$header_etag" &&
                    is_positive_i64 "$header_content_length" && file_size_and_hash "$partial" &&
                    ((provenance_bytes <= header_content_length)); then
                    write_sidecar "$sidecar" "$sidecar_tmp" "$basin_id" "$product" "$url" \
                        "$header_etag" "$provenance_bytes" "$header_content_length" "$provenance_sha256" || {
                            "$RM" -f -- "$headers" "$stats"
                            trap - INT TERM
                            return 1
                        }
                else
                    "$RM" -f -- "$partial" "$sidecar"
                fi
            elif [[ "$header_status" != 200 ]] || ! strong_etag_is_safe "$header_etag" ||
                ! is_positive_i64 "$header_content_length" || ! file_size_and_hash "$partial" ||
                [[ "$provenance_bytes" != "$header_content_length" ]]; then
                "$RM" -f -- "$partial" "$sidecar"
                result=integrity_failed
            else
                result=succeeded
            fi
        fi
        break
    done
    "$RM" -f -- "$headers" "$stats"
    trap - INT TERM
    if [[ "$result" != succeeded ]]; then
        append_transfer_report "$report" "$report_tmp" "$basin_id" "$product" \
            "$attempts" "$mode" "$offset" "$result" 0 || return 1
        fail_product "$basin_id" "$stage" "$attempts" \
            "$([[ "$result" == curl_failed ]] && printf 'transfer failed' || printf 'download provenance or size verification failed')"
        emit_acquisition_summary "$basin_id" "$product" failed "$report"
        return
    fi
    if ! verify_download "$partial" "$inspect"; then
        append_transfer_report "$report" "$report_tmp" "$basin_id" "$product" \
            "$attempts" "$mode" "$offset" integrity_failed 0 || return 1
        [[ -f "$partial" && ! -L "$partial" ]] && "$RM" -- "$partial"
        "$RM" -f -- "$sidecar"
        "$RM" -f -- "$inspect"
        fail_product "$basin_id" "$stage" "$attempts" 'download failed integrity verification'
        emit_acquisition_summary "$basin_id" "$product" failed "$report"
        return
    fi
    append_transfer_report "$report" "$report_tmp" "$basin_id" "$product" \
        "$attempts" "$mode" "$offset" succeeded 0 || return 1
    "$CHMOD" 0644 "$partial" || return 1
    "$MV" "$partial" "$final" || return 1
    if ! verify_download "$final" "$inspect"; then
        fail_product "$basin_id" "$stage" "$attempts" 'installed final failed integrity verification; retained for inspection'
        "$RM" -f -- "$inspect"
        return
    fi
    actual=$(evidence_json)
    "$RM" -f -- "$inspect" "$sidecar"
    write_acquire_stage "$basin_id" "$stage" succeeded "$attempts" '' "$actual"
    emit_acquisition_summary "$basin_id" "$product" succeeded "$report"
}

acquire_basin() {
    local basin_id=$1
    # Bash clears a parent-set EXIT trap in an & subshell. These resets remain
    # defense in depth so a worker can never satisfy campaign-lock re-entry.
    lock_owned=0
    takeover_owned=0
    if reconcile_reclaim_basin "$basin_id" false; then
        return
    fi
    acquire_product "$basin_id" basins
    acquire_product "$basin_id" streamnet
}

worker_pids=()
worker_failure=0
wait_oldest_worker() {
    local index
    local last
    if ! wait "${worker_pids[0]}"; then
        worker_failure=1
    fi
    last=$((${#worker_pids[@]} - 1))
    for ((index = 0; index < last; index++)); do
        worker_pids[$index]=${worker_pids[$((index + 1))]}
    done
    unset "worker_pids[$last]"
    if ((${#worker_pids[@]} == 0)) && [[ ${HFX_TDX_TEST_SIGNAL_AFTER_EMPTY_DRAIN-} == 1 ]]; then
        HFX_TDX_TEST_SIGNAL_AFTER_EMPTY_DRAIN=0
        kill -TERM "$$"
    fi
}

interrupt_acquisition() {
    local pid
    for pid in ${worker_pids[@]+"${worker_pids[@]}"}; do
        [[ "$pid" =~ ^[1-9][0-9]*$ ]] && kill -TERM "$pid" 2>/dev/null || :
    done
    while ((${#worker_pids[@]} > 0)); do
        wait_oldest_worker
    done
    exit 130
}

acquire_campaign() {
    local basin_id
    local locked_retention_policy
    acquire_campaign_lock
    validate_workspace_state
    locked_retention_policy=$("$JQ" -r '.retention.policy' "$campaign_dir/state/campaign.json")
    [[ "$locked_retention_policy" == "$acquire_retention_policy" ]] ||
        hfx_die 'campaign retention policy changed while acquiring the campaign lock'
    migrate_basin_states
    recover_running_stages true
    validate_workspace_state
    trap interrupt_acquisition INT TERM
    while IFS= read -r basin_id; do
        acquire_basin "$basin_id" &
        worker_pids[${#worker_pids[@]}]=$!
        if ((${#worker_pids[@]} >= max_parallel)); then
            wait_oldest_worker
        fi
    done < <(effective_basin_ids)
    while ((${#worker_pids[@]} > 0)); do
        wait_oldest_worker
    done
    trap - INT TERM
    ((worker_failure == 0)) || hfx_die 'an acquisition worker could not atomically record state'
    validate_workspace_state
    print_status
}

if (($# == 1)) && [[ "$1" == -h || "$1" == --help ]]; then
    usage
    exit 0
fi
(($# > 0)) || usage_error 'subcommand is required'
subcommand=$1
shift
case $subcommand in
    init|status|recover|acquire|compile|compile-basin|progress|pipeline|assemble|evidence|publish) ;;
    *) usage_error "unknown subcommand $subcommand" ;;
esac

campaign=
campaign_seen=0
workspace_root=/mnt/hfx/work
workspace_seen=0
available_memory_bytes=
available_disk_bytes=
retained_input_bytes=
peak_in_flight_download_bytes=
retained_basin_output_bytes=
assembly_memory_ceiling_bytes=
assembly_scratch_ceiling_bytes=
assembled_artifact_bytes=
active_compile_scratch_bytes=
filesystem_overhead_bytes=
sizing_seen=' '
retention_policy=retain-all-through-publication
retention_policy_seen=0
max_parallel=
max_parallel_seen=0
fabric_version=
fabric_version_seen=0
publication_out=
publication_out_seen=0
publication_report=
publication_report_seen=0
publication_notice=
publication_notice_seen=0
publication_citation=
publication_citation_seen=0
scratch_prefix=
scratch_prefix_seen=0
basin_ids=()

while (($# > 0)); do
    option=$1
    case $option in
        --campaign|--workspace-root|--basin|--retention-policy|--available-memory-bytes|--available-disk-bytes|--retained-input-bytes|--peak-in-flight-download-bytes|--retained-basin-output-bytes|--assembly-memory-ceiling-bytes|--assembly-scratch-ceiling-bytes|--assembled-artifact-bytes|--active-compile-scratch-bytes|--filesystem-overhead-bytes|--max-parallel|--fabric-version|--out|--report|--notice|--citation|--scratch-prefix)
            shift
            (($# > 0)) && [[ -n "$1" ]] || usage_error "option $option requires a value"
            if [[ "$1" == -* ]] &&
                ! [[ "$option" == --max-parallel && "$1" =~ ^-[0-9]+$ ]]; then
                usage_error "option $option requires a value"
            fi
            value=$1
            ;;
        -*) usage_error "unknown option $option" ;;
        *) usage_error "unexpected positional argument $option" ;;
    esac
    case $option in
        --campaign)
            ((campaign_seen == 0)) || usage_error 'option --campaign may not be repeated'
            campaign_seen=1
            campaign=$value
            ;;
        --workspace-root)
            ((workspace_seen == 0)) || usage_error 'option --workspace-root may not be repeated'
            workspace_seen=1
            workspace_root=$value
            ;;
        --basin)
            [[ "$subcommand" == init || "$subcommand" == compile-basin ]] ||
                usage_error 'option --basin is valid only for init or compile-basin'
            basin_ids[${#basin_ids[@]}]=$value
            ;;
        --retention-policy)
            [[ "$subcommand" == init ]] || usage_error 'option --retention-policy is valid only for init'
            ((retention_policy_seen == 0)) || usage_error 'option --retention-policy may not be repeated'
            retention_policy_seen=1
            retention_policy=$value
            ;;
        --max-parallel)
            [[ "$subcommand" == acquire || "$subcommand" == pipeline ]] ||
                usage_error 'option --max-parallel is valid only for acquire or pipeline'
            ((max_parallel_seen == 0)) || usage_error 'option --max-parallel may not be repeated'
            max_parallel_seen=1
            max_parallel=$value
            ;;
        --fabric-version)
            [[ "$subcommand" == compile || "$subcommand" == compile-basin || "$subcommand" == pipeline ]] ||
                usage_error 'option --fabric-version is valid only for compile, compile-basin, or pipeline'
            ((fabric_version_seen == 0)) || usage_error 'option --fabric-version may not be repeated'
            fabric_version_seen=1
            fabric_version=$value
            ;;
        --out)
            [[ "$subcommand" == publish ]] || usage_error 'option --out is valid only for publish'
            ((publication_out_seen == 0)) || usage_error 'option --out may not be repeated'
            publication_out_seen=1
            publication_out=$value
            ;;
        --report)
            [[ "$subcommand" == publish ]] || usage_error 'option --report is valid only for publish'
            ((publication_report_seen == 0)) || usage_error 'option --report may not be repeated'
            publication_report_seen=1
            publication_report=$value
            ;;
        --notice)
            [[ "$subcommand" == publish ]] || usage_error 'option --notice is valid only for publish'
            ((publication_notice_seen == 0)) || usage_error 'option --notice may not be repeated'
            publication_notice_seen=1
            publication_notice=$value
            ;;
        --citation)
            [[ "$subcommand" == publish ]] || usage_error 'option --citation is valid only for publish'
            ((publication_citation_seen == 0)) || usage_error 'option --citation may not be repeated'
            publication_citation_seen=1
            publication_citation=$value
            ;;
        --scratch-prefix)
            [[ "$subcommand" == publish ]] || usage_error 'option --scratch-prefix is valid only for publish'
            ((scratch_prefix_seen == 0)) || usage_error 'option --scratch-prefix may not be repeated'
            scratch_prefix_seen=1
            scratch_prefix=$value
            ;;
        *)
            [[ "$subcommand" == init ]] || usage_error "sizing option $option is valid only for init"
            case $sizing_seen in *" $option "*) usage_error "option $option may not be repeated" ;; esac
            sizing_seen="$sizing_seen$option "
            variable_name=${option#--}
            variable_name=${variable_name//-/_}
            eval "$variable_name=\$value"
            ;;
    esac
    shift
done

((campaign_seen == 1)) || usage_error 'option --campaign is required'
validate_campaign "$campaign"
validate_workspace_root "$workspace_root"

SCRIPT_DIR=$(cd -P -- "${BASH_SOURCE[0]%/*}" && pwd)
repo_root=$(cd -P -- "$SCRIPT_DIR/../.." && pwd)
inventory_source=$repo_root/$HFX_TDX_INVENTORY_SOURCE
JQ=$(resolve_command HFX_TDX_JQ jq)
MV=$(resolve_command HFX_TDX_MV mv)
MKDIR=$(resolve_command HFX_TDX_MKDIR mkdir)
RM=$(resolve_command HFX_TDX_RM rm)
CHMOD=$(resolve_command HFX_TDX_CHMOD chmod)
FIND=$(resolve_command HFX_TDX_FIND find)
WC=$(resolve_command HFX_TDX_WC wc)
TR=$(resolve_command HFX_TDX_TR tr)
PS=$(resolve_command HFX_TDX_PS ps)
validate_inventory_file "$inventory_source"

campaign_dir=$workspace_root/tdx-hydro-$campaign
trap 'release_takeover_guard; release_lock' EXIT

if [[ "$subcommand" == compile-basin ]]; then
    ((${#basin_ids[@]} > 0)) || usage_error 'option --basin is required for compile-basin'
    ((${#basin_ids[@]} == 1)) ||
        usage_error 'option --basin may be specified exactly once for compile-basin'
    [[ "${basin_ids[0]}" =~ ^[0-9]{10}$ ]] ||
        hfx_die "invalid processing basin ID '${basin_ids[0]}'; expected an authoritative 10-digit ID"
fi
if [[ "$subcommand" == compile || "$subcommand" == compile-basin || "$subcommand" == pipeline ]]; then
    ((fabric_version_seen == 1)) || usage_error 'option --fabric-version is required'
    [[ -n "$fabric_version" && "$fabric_version" != -* ]] ||
        usage_error 'option --fabric-version requires a non-empty non-option value'
    [[ ! "$fabric_version" =~ [[:cntrl:]] ]] ||
        usage_error 'option --fabric-version must not contain ASCII control characters'
fi

if [[ "$subcommand" == init ]]; then
    case $retention_policy in
        retain-all-through-publication)
            [[ -n "$retained_input_bytes" ]] || usage_error 'option --retained-input-bytes is required'
            [[ -z "$peak_in_flight_download_bytes" ]] ||
                usage_error 'option --peak-in-flight-download-bytes is incompatible with retention policy retain-all-through-publication'
            policy_input_name=retained_input_bytes
            ;;
        reclaim-inputs-after-terminal)
            [[ -n "$peak_in_flight_download_bytes" ]] ||
                usage_error 'option --peak-in-flight-download-bytes is required'
            [[ -z "$retained_input_bytes" ]] ||
                usage_error 'option --retained-input-bytes is incompatible with retention policy reclaim-inputs-after-terminal'
            policy_input_name=peak_in_flight_download_bytes
            ;;
        *) usage_error "invalid retention policy '$retention_policy'; expected retain-all-through-publication or reclaim-inputs-after-terminal" ;;
    esac
    for variable_name in available_memory_bytes available_disk_bytes retained_basin_output_bytes assembly_memory_ceiling_bytes assembly_scratch_ceiling_bytes assembled_artifact_bytes active_compile_scratch_bytes filesystem_overhead_bytes "$policy_input_name"; do
        eval "value=\${$variable_name-}"
        [[ -n "$value" ]] || usage_error "option --${variable_name//_/-} is required"
        value=$(normalize_positive_i64 "$variable_name" "$value")
        eval "$variable_name=\$value"
    done
    if [[ "$retention_policy" == reclaim-inputs-after-terminal ]] &&
        [[ "$peak_in_flight_download_bytes" != "$HFX_TDX_RECLAIM_PEAK_BYTES" ]]; then
        usage_error "option --peak-in-flight-download-bytes must equal $HFX_TDX_RECLAIM_PEAK_BYTES for retention policy reclaim-inputs-after-terminal"
    fi
    required_memory_bytes=$assembly_memory_ceiling_bytes
    assembly_peak_bytes=$assembly_scratch_ceiling_bytes
    if ((assembled_artifact_bytes > assembly_peak_bytes)); then
        assembly_peak_bytes=$assembled_artifact_bytes
    fi
    required_disk_bytes=0
    eval "policy_input_bytes=\${$policy_input_name}"
    required_disk_bytes=$(checked_add "$required_disk_bytes" "$policy_input_bytes")
    required_disk_bytes=$(checked_add "$required_disk_bytes" "$retained_basin_output_bytes")
    required_disk_bytes=$(checked_add "$required_disk_bytes" "$active_compile_scratch_bytes")
    required_disk_bytes=$(checked_add "$required_disk_bytes" "$assembly_peak_bytes")
    required_disk_bytes=$(checked_add "$required_disk_bytes" "$filesystem_overhead_bytes")
    ((available_memory_bytes >= required_memory_bytes)) ||
        hfx_die "insufficient memory: available $available_memory_bytes bytes; required $required_memory_bytes bytes"
    ((available_disk_bytes >= required_disk_bytes)) ||
        hfx_die "insufficient disk: available $available_disk_bytes bytes; required $required_disk_bytes bytes"
    initialize_campaign
elif [[ "$subcommand" == status ]]; then
    [[ -d "$campaign_dir" && ! -L "$campaign_dir" ]] || hfx_die "campaign does not exist safely: $campaign_dir"
    acquire_campaign_lock
    validate_workspace_state
    print_status
elif [[ "$subcommand" == progress ]]; then
    [[ -d "$campaign_dir" && ! -L "$campaign_dir" ]] || hfx_die "campaign does not exist safely: $campaign_dir"
    validate_workspace_state
    print_status
elif [[ "$subcommand" == recover ]]; then
    [[ -d "$campaign_dir" && ! -L "$campaign_dir" ]] || hfx_die "campaign does not exist safely: $campaign_dir"
    recover_campaign
elif [[ "$subcommand" == acquire || "$subcommand" == pipeline ]]; then
    ((max_parallel_seen == 1)) || usage_error 'option --max-parallel is required'
    [[ "$max_parallel" =~ ^[0-9]+$ ]] || usage_error 'option --max-parallel must be a base-10 integer from 1 through 62'
    ((max_parallel >= 1 && max_parallel <= 62)) ||
        usage_error 'option --max-parallel must be a base-10 integer from 1 through 62'
    [[ -d "$campaign_dir" && ! -L "$campaign_dir" ]] || hfx_die "campaign does not exist safely: $campaign_dir"
    validate_campaign_json "$campaign_dir/state/campaign.json"
    acquire_retention_policy=$("$JQ" -r '.retention.policy' "$campaign_dir/state/campaign.json")
    if [[ "$acquire_retention_policy" == reclaim-inputs-after-terminal ]]; then
        ((max_parallel >= 1 && max_parallel <= HFX_TDX_RECLAIM_MAX_PARALLEL)) ||
            usage_error "option --max-parallel must be a base-10 integer from 1 through $HFX_TDX_RECLAIM_MAX_PARALLEL for retention policy reclaim-inputs-after-terminal"
    fi
    if [[ "$subcommand" == acquire ]]; then
        CURL=$(resolve_command HFX_TDX_CURL curl)
        SHA256SUM=$(resolve_command HFX_TDX_SHA256SUM sha256sum)
        OD=$(resolve_command HFX_TDX_OD od)
        OGRINFO=$(resolve_command HFX_TDX_OGRINFO ogrinfo)
        acquire_campaign
    else
        [[ "$acquire_retention_policy" == reclaim-inputs-after-terminal ]] ||
            hfx_die 'pipeline requires retention policy reclaim-inputs-after-terminal'
        pipeline_campaign
    fi
elif [[ "$subcommand" == compile || "$subcommand" == compile-basin ]]; then
    [[ -d "$campaign_dir" && ! -L "$campaign_dir" ]] || hfx_die "campaign does not exist safely: $campaign_dir"
    ADAPTER_PYTHON=$(resolve_command HFX_TDX_ADAPTER_PYTHON "$HFX_TDX_DEFAULT_ADAPTER_PYTHON")
    ADAPTER_SCRIPT=${HFX_TDX_ADAPTER_SCRIPT-$repo_root/adapters/tdx-hydro/build_adapter.py}
    HFX=$(resolve_command HFX_TDX_HFX "$HFX_TDX_DEFAULT_HFX")
    if [[ "$subcommand" == compile-basin ]]; then
        compile_selected_basin "${basin_ids[0]}"
    else
        compile_campaign
    fi
elif [[ "$subcommand" == assemble ]]; then
    [[ -d "$campaign_dir" && ! -L "$campaign_dir" ]] || hfx_die "campaign does not exist safely: $campaign_dir"
    ADAPTER_PYTHON=$(resolve_command HFX_TDX_ADAPTER_PYTHON "$HFX_TDX_DEFAULT_ADAPTER_PYTHON")
    ADAPTER_SCRIPT=${HFX_TDX_ADAPTER_SCRIPT-$repo_root/adapters/tdx-hydro/build_adapter.py}
    HFX=$(resolve_command HFX_TDX_HFX "$HFX_TDX_DEFAULT_HFX")
    SORT=$(resolve_command HFX_TDX_SORT sort)
    assemble_campaign
elif [[ "$subcommand" == evidence ]]; then
    [[ -d "$campaign_dir" && ! -L "$campaign_dir" ]] || hfx_die "campaign does not exist safely: $campaign_dir"
    generate_evidence
elif [[ "$subcommand" == publish ]]; then
    ((publication_out_seen == 1)) || usage_error 'option --out is required'
    ((publication_report_seen == 1)) || usage_error 'option --report is required'
    ((publication_notice_seen == 1)) || usage_error 'option --notice is required'
    ((publication_citation_seen == 1)) || usage_error 'option --citation is required'
    ((scratch_prefix_seen == 1)) || usage_error 'option --scratch-prefix is required'
    [[ -d "$campaign_dir" && ! -L "$campaign_dir" ]] || hfx_die "campaign does not exist safely: $campaign_dir"
    SORT=$(resolve_command HFX_TDX_SORT sort)
    GREP=$(resolve_command HFX_TDX_GREP grep)
    publish_campaign
fi
