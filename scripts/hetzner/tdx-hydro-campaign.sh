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

hfx_log() {
    local message
    local IFS=' '
    message="$*"
    printf 'hfx: %s\n' "$message" >&2
}

usage() {
    printf '%s\n' \
        'Usage: tdx-hydro-campaign.sh init --campaign <id> [--workspace-root <path>] --available-memory-bytes <integer> --available-disk-bytes <integer> --retained-input-bytes <integer> --retained-basin-output-bytes <integer> --assembly-memory-ceiling-bytes <integer> --assembly-scratch-ceiling-bytes <integer> --assembled-artifact-bytes <integer>' \
        '       tdx-hydro-campaign.sh status --campaign <id> [--workspace-root <path>]' \
        '       tdx-hydro-campaign.sh recover --campaign <id> [--workspace-root <path>]' \
        '       tdx-hydro-campaign.sh acquire --campaign <id> [--workspace-root <path>] --max-parallel <integer>' \
        '       tdx-hydro-campaign.sh compile --campaign <id> [--workspace-root <path>] --fabric-version <value>'
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

validate_campaign_json() {
    local file=$1
    "$JQ" -e --arg campaign "$campaign" --argjson max_i64 "$HFX_TDX_MAX_I64" '
        type == "object" and
        (keys == ["campaign","inventory","retention","schema_version","sizing"]) and
        .schema_version == 1 and
        .campaign == $campaign and
        (.inventory | type == "object" and keys == ["count","source"] and
            .source == "adapters/tdx-hydro/data/tdx_header_numbers.json" and .count == 62) and
        (.retention | type == "object" and
            keys == ["policy","reclaim_inputs","retain_acquired_inputs","retain_basin_outputs","retain_external_reports"] and
            .policy == "retain-all-through-publication" and
            .reclaim_inputs == false and .retain_acquired_inputs == true and
            .retain_basin_outputs == true and .retain_external_reports == true) and
        (.sizing | type == "object" and
            keys == ["assembled_artifact_bytes","assembly_memory_ceiling_bytes","assembly_scratch_ceiling_bytes",
                     "available_disk_bytes","available_memory_bytes","required_disk_bytes","required_memory_bytes",
                     "retained_basin_output_bytes","retained_input_bytes"] and
            (to_entries | all(.value | type == "number" and . == floor and . > 0 and . <= $max_i64)) and
            .required_memory_bytes == .assembly_memory_ceiling_bytes and
            .required_disk_bytes == (
                .retained_input_bytes + .retained_basin_output_bytes +
                .assembly_scratch_ceiling_bytes + .assembled_artifact_bytes
            ) and
            .available_memory_bytes >= .required_memory_bytes and
            .available_disk_bytes >= .required_disk_bytes)
    ' "$file" >/dev/null 2>&1 || hfx_die "campaign state is malformed: $file"
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
        def valid_compile: valid_v1_stage;
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
        type == "object" and
        keys == ["processing_basin_id","schema_version","stages"] and
        (.schema_version == 1 or .schema_version == 2) and .processing_basin_id == $basin_id and
        (.stages | type == "object" and
            keys == ["acquire_basins","acquire_streamnet","compile"]) and
        (.stages.compile | valid_compile) and
        if .schema_version == 1 then
            (.stages.acquire_basins | valid_v1_stage) and
            (.stages.acquire_streamnet | valid_v1_stage)
        else
            (.stages.acquire_basins | valid_v2_acquire) and
            (.stages.acquire_streamnet | valid_v2_acquire)
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
release_takeover_guard() {
    if ((takeover_owned == 1)) && [[ -n "$takeover_path" && -d "$takeover_path" && ! -L "$takeover_path" ]] &&
        [[ -f "$takeover_path/owner.pid" && ! -L "$takeover_path/owner.pid" ]] &&
        [[ $(<"$takeover_path/owner.pid") == "$$" ]]; then
        "$RM" -r -- "$takeover_path"
    fi
    takeover_owned=0
}

release_lock() {
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

validate_workspace_state() {
    local expected_count
    local basin_id
    local basin_dir
    local actual_dirs
    local actual_files

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
    validate_inventory_file "$campaign_dir/state/inventory.json"
    "$JQ" -e -S --slurp '.[0] == .[1]' "$inventory_source" "$campaign_dir/state/inventory.json" >/dev/null 2>&1 ||
        hfx_die 'campaign inventory differs from the authoritative tracked crosswalk'
    expected_count=$("$JQ" -r 'length' "$campaign_dir/state/inventory.json")
    actual_dirs=$("$FIND" "$campaign_dir/state/basins" -mindepth 1 -maxdepth 1 -type d | "$WC" -l | "$TR" -d ' ')
    [[ "$actual_dirs" == "$expected_count" ]] || hfx_die "expected $expected_count basin directories; found $actual_dirs"
    actual_files=$("$FIND" "$campaign_dir/state/basins" -mindepth 2 -maxdepth 2 -type f -name current.json | "$WC" -l | "$TR" -d ' ')
    [[ "$actual_files" == "$expected_count" ]] || hfx_die "expected $expected_count basin state files; found $actual_files"
    while IFS= read -r basin_id; do
        basin_dir=$campaign_dir/state/basins/$basin_id
        [[ -d "$basin_dir" && ! -L "$basin_dir" ]] || hfx_die "basin directory is missing or unsafe: $basin_id"
        validate_basin_json "$basin_dir/current.json" "$basin_id"
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
        .sizing |
        "available_memory_bytes=\(.available_memory_bytes)",
        "available_disk_bytes=\(.available_disk_bytes)",
        "retained_input_bytes=\(.retained_input_bytes)",
        "retained_basin_output_bytes=\(.retained_basin_output_bytes)",
        "assembly_memory_ceiling_bytes=\(.assembly_memory_ceiling_bytes)",
        "assembly_scratch_ceiling_bytes=\(.assembly_scratch_ceiling_bytes)",
        "assembled_artifact_bytes=\(.assembled_artifact_bytes)",
        "required_memory_bytes=\(.required_memory_bytes)",
        "required_disk_bytes=\(.required_disk_bytes)"
    ' "$campaign_dir/state/campaign.json"
}

print_status() {
    local stage
    local status
    printf 'campaign=%s\n' "$campaign"
    printf 'inventory_count=62\n'
    print_sizing
    for stage in acquire_basins acquire_streamnet compile; do
        for status in pending running succeeded failed; do
            printf '%s_%s=' "$stage" "$status"
            "$JQ" -s --arg stage "$stage" --arg status "$status" \
                '[.[].stages[$stage].status | select(. == $status)] | length' \
                "$campaign_dir"/state/basins/*/current.json
        done
    done
}

initialize_campaign() {
    local created=0
    local basin_id
    local temporary
    local existing_canonical
    local requested_canonical

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

    temporary=$campaign_dir/state/.campaign.json.tmp.$$
    "$JQ" -n \
        --arg campaign "$campaign" \
        --argjson available_memory_bytes "$available_memory_bytes" \
        --argjson available_disk_bytes "$available_disk_bytes" \
        --argjson retained_input_bytes "$retained_input_bytes" \
        --argjson retained_basin_output_bytes "$retained_basin_output_bytes" \
        --argjson assembly_memory_ceiling_bytes "$assembly_memory_ceiling_bytes" \
        --argjson assembly_scratch_ceiling_bytes "$assembly_scratch_ceiling_bytes" \
        --argjson assembled_artifact_bytes "$assembled_artifact_bytes" \
        --argjson required_memory_bytes "$required_memory_bytes" \
        --argjson required_disk_bytes "$required_disk_bytes" '
        {
          schema_version: 1,
          campaign: $campaign,
          inventory: {source: "adapters/tdx-hydro/data/tdx_header_numbers.json", count: 62},
          retention: {
            policy: "retain-all-through-publication",
            reclaim_inputs: false,
            retain_acquired_inputs: true,
            retain_basin_outputs: true,
            retain_external_reports: true
          },
          sizing: {
            available_memory_bytes: $available_memory_bytes,
            available_disk_bytes: $available_disk_bytes,
            retained_input_bytes: $retained_input_bytes,
            retained_basin_output_bytes: $retained_basin_output_bytes,
            assembly_memory_ceiling_bytes: $assembly_memory_ceiling_bytes,
            assembly_scratch_ceiling_bytes: $assembly_scratch_ceiling_bytes,
            assembled_artifact_bytes: $assembled_artifact_bytes,
            required_memory_bytes: $required_memory_bytes,
            required_disk_bytes: $required_disk_bytes
          }
        }' >"$temporary"
    validate_campaign_json "$temporary"

    if ((created == 0)); then
        validate_workspace_state
        existing_canonical=$("$JQ" -cS '.' "$campaign_dir/state/campaign.json")
        requested_canonical=$("$JQ" -cS '.' "$temporary")
        if [[ "$existing_canonical" != "$requested_canonical" ]]; then
            "$RM" -- "$temporary" "$campaign_dir/state/.inventory.json.tmp.$$"
            hfx_die 'campaign parameters changed; use a new campaign ID'
        fi
        existing_canonical=$("$JQ" -cS '.' "$campaign_dir/state/inventory.json")
        requested_canonical=$("$JQ" -cS '.' "$campaign_dir/state/.inventory.json.tmp.$$")
        if [[ "$existing_canonical" != "$requested_canonical" ]]; then
            "$RM" -- "$temporary" "$campaign_dir/state/.inventory.json.tmp.$$"
            hfx_die 'campaign inventory changed; use a new campaign ID'
        fi
        "$RM" -- "$temporary" "$campaign_dir/state/.inventory.json.tmp.$$"
        print_status
        return
    fi

    atomic_install "$campaign_dir/state/.inventory.json.tmp.$$" "$campaign_dir/state/inventory.json" validate_inventory_file
    atomic_install "$temporary" "$campaign_dir/state/campaign.json" validate_campaign_json
    while IFS= read -r basin_id; do
        "$MKDIR" "$campaign_dir/state/basins/$basin_id"
        temporary=$campaign_dir/state/basins/$basin_id/.current.json.tmp.$$
        "$JQ" -n --arg basin_id "$basin_id" '{
          schema_version: 2,
          processing_basin_id: $basin_id,
          stages: {
            acquire_basins: {status: "pending", attempts: 0, failure_reason: null, evidence: null},
            acquire_streamnet: {status: "pending", attempts: 0, failure_reason: null, evidence: null},
            compile: {status: "pending", attempts: 0, failure_reason: null}
          }
        }' >"$temporary"
        atomic_install "$temporary" "$campaign_dir/state/basins/$basin_id/current.json" validate_basin_json "$basin_id"
    done < <("$JQ" -r 'keys[]' "$campaign_dir/state/inventory.json")
    validate_workspace_state
    print_status
}

migrate_basin_states() {
    local basin_id
    local current
    local temporary
    while IFS= read -r basin_id; do
        current=$campaign_dir/state/basins/$basin_id/current.json
        if [[ $("$JQ" -r '.schema_version' "$current") == 1 ]]; then
            temporary=$campaign_dir/state/basins/$basin_id/.current.json.tmp.$$
            "$JQ" '
                .schema_version = 2 |
                .stages.acquire_basins.evidence = null |
                .stages.acquire_streamnet.evidence = null |
                .stages.acquire_basins |=
                    if .status == "succeeded" then .status = "pending" else . end |
                .stages.acquire_streamnet |=
                    if .status == "succeeded" then .status = "pending" else . end
            ' "$current" >"$temporary"
            atomic_install "$temporary" "$current" validate_basin_json "$basin_id"
        fi
    done < <("$JQ" -r 'keys[]' "$campaign_dir/state/inventory.json")
}

recover_running_stages() {
    local acquisition_only=${1-false}
    local basin_id
    local current
    local temporary
    while IFS= read -r basin_id; do
        current=$campaign_dir/state/basins/$basin_id/current.json
        if "$JQ" -e '[.stages[].status] | any(. == "running")' "$current" >/dev/null; then
            temporary=$campaign_dir/state/basins/$basin_id/.current.json.tmp.$$
            "$JQ" --argjson acquisition_only "$acquisition_only" '
                .stages |= with_entries(
                    if .value.status == "running" and
                        (($acquisition_only | not) or .key == "acquire_basins" or .key == "acquire_streamnet") then
                        .value.status = "pending" |
                        .value.failure_reason = "interrupted before terminal state; reset by recover" |
                        if (.value | has("evidence")) then .value.evidence = null else . end
                    else .
                    end
                )
            ' "$current" >"$temporary"
            atomic_install "$temporary" "$current" validate_basin_json "$basin_id"
        fi
    done < <("$JQ" -r 'keys[]' "$campaign_dir/state/inventory.json")
}

recover_campaign() {
    acquire_campaign_lock
    validate_workspace_state
    recover_running_stages false
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
    local current=$campaign_dir/state/basins/$basin_id/current.json
    local temporary=$campaign_dir/state/basins/$basin_id/.current.json.tmp.$$
    "$JQ" --arg status "$status" --arg reason "$reason" --argjson attempts "$attempts" '
        .stages.compile.status = $status |
        .stages.compile.attempts = $attempts |
        .stages.compile.failure_reason = (if $reason == "" then null else $reason end)
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

verify_compile_artifacts() {
    local basin_id=$1
    local output=$2
    local report=$3
    local resolved_output
    [[ -d "$output" && ! -L "$output" ]] || return 1
    [[ -f "$report" && ! -L "$report" && -s "$report" ]] || return 1
    resolved_output=$(cd -P "$output" && pwd -P) || return 1
    "$JQ" -e --arg basin_id "$basin_id" --arg fabric_version "$fabric_version" \
        --arg dataset_root "$resolved_output" '
        .build_identity.processing_basin_id == $basin_id and
        .build_identity.fabric_name == "tdx_hydro" and
        .build_identity.fabric_version == $fabric_version and
        .build_identity.dataset_root == $dataset_root
    ' "$report" >/dev/null 2>&1 || return 1
    "$ADAPTER_PYTHON" "$ADAPTER_SCRIPT" validate "$output" --hfx-binary "$HFX"
}

compile_campaign() {
    local basin_id
    local current
    local acquire_basins_status
    local acquire_streamnet_status
    local compile_status
    local attempts
    local basins
    local streamnet
    local output
    local report

    acquire_campaign_lock
    validate_workspace_state
    establish_compile_contract
    migrate_basin_states
    recover_running_stages false
    validate_workspace_state

    while IFS= read -r basin_id; do
        current=$campaign_dir/state/basins/$basin_id/current.json
        acquire_basins_status=$("$JQ" -r '.stages.acquire_basins.status' "$current")
        acquire_streamnet_status=$("$JQ" -r '.stages.acquire_streamnet.status' "$current")
        compile_status=$("$JQ" -r '.stages.compile.status' "$current")
        attempts=$("$JQ" -r '.stages.compile.attempts' "$current")

        if [[ "$acquire_basins_status" != succeeded || "$acquire_streamnet_status" != succeeded ]]; then
            write_compile_stage "$basin_id" failed "$attempts" \
                'acquisition prerequisites are not both succeeded'
            continue
        fi

        basins=$campaign_dir/downloads/$basin_id-basins.gpkg
        streamnet=$campaign_dir/downloads/$basin_id-streamnet.gpkg
        output=$campaign_dir/basin-outputs/$basin_id
        report=$campaign_dir/reports/$basin_id-build-report.json

        if [[ "$compile_status" == succeeded ]]; then
            if verify_compile_artifacts "$basin_id" "$output" "$report"; then
                continue
            fi
            write_compile_stage "$basin_id" failed "$attempts" \
                'existing compile artifacts failed resume verification; retained for inspection'
            continue
        fi

        if [[ -e "$output" || -L "$output" || -e "$report" || -L "$report" ]]; then
            write_compile_stage "$basin_id" failed "$attempts" \
                'compile artifact path already exists; retained for inspection'
            continue
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
            write_compile_stage "$basin_id" failed "$attempts" 'adapter build failed'
            continue
        fi
        if ! verify_compile_artifacts "$basin_id" "$output" "$report"; then
            write_compile_stage "$basin_id" failed "$attempts" 'adapter validation failed'
            continue
        fi
        write_compile_stage "$basin_id" succeeded "$attempts" ''
    done < <("$JQ" -r 'keys[]' "$campaign_dir/state/inventory.json")

    validate_workspace_state
    print_status
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

acquire_product() {
    local basin_id=$1
    local product=$2
    local stage=acquire_$product
    local current=$campaign_dir/state/basins/$basin_id/current.json
    local final=$campaign_dir/downloads/$basin_id-$product.gpkg
    local partial=$final.partial
    local inspect=$campaign_dir/state/tmp/.ogr.$basin_id.$product.$$
    local persisted_status
    local attempts
    local expected
    local actual
    local url=https://earth-info.nga.mil/php/download.php?file=$basin_id-$product-gpkg
    persisted_status=$("$JQ" -r --arg stage "$stage" '.stages[$stage].status' "$current")
    attempts=$("$JQ" -r --arg stage "$stage" '.stages[$stage].attempts' "$current")

    if [[ -e "$final" || -L "$final" ]]; then
        if ! verify_download "$final" "$inspect"; then
            fail_product "$basin_id" "$stage" "$attempts" 'existing final file failed integrity verification; retained for inspection'
            "$RM" -f -- "$inspect"
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
        return
    fi

    if [[ -e "$partial" || -L "$partial" ]]; then
        if [[ -f "$partial" && ! -L "$partial" ]]; then
            "$RM" -- "$partial"
        else
            fail_product "$basin_id" "$stage" "$attempts" 'partial path is unsafe; retained without traversal'
            return
        fi
    fi

    attempts=$((attempts + 1))
    write_acquire_stage "$basin_id" "$stage" running "$attempts" '' null
    if ! "$CURL" --fail --show-error --location --connect-timeout 30 \
        --speed-limit 65536 --speed-time 60 --output "$partial" "$url"; then
        [[ ! -e "$partial" && ! -L "$partial" ]] ||
            { [[ -f "$partial" && ! -L "$partial" ]] && "$RM" -- "$partial"; }
        fail_product "$basin_id" "$stage" "$attempts" 'complete GET failed'
        return
    fi
    if ! verify_download "$partial" "$inspect"; then
        [[ -f "$partial" && ! -L "$partial" ]] && "$RM" -- "$partial"
        "$RM" -f -- "$inspect"
        fail_product "$basin_id" "$stage" "$attempts" 'download failed integrity verification'
        return
    fi
    "$CHMOD" 0644 "$partial" || return 1
    "$MV" "$partial" "$final" || return 1
    if ! verify_download "$final" "$inspect"; then
        fail_product "$basin_id" "$stage" "$attempts" 'installed final failed integrity verification; retained for inspection'
        "$RM" -f -- "$inspect"
        return
    fi
    actual=$(evidence_json)
    "$RM" -f -- "$inspect"
    write_acquire_stage "$basin_id" "$stage" succeeded "$attempts" '' "$actual"
}

acquire_basin() {
    local basin_id=$1
    lock_owned=0
    takeover_owned=0
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
    acquire_campaign_lock
    validate_workspace_state
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
    done < <("$JQ" -r 'keys[]' "$campaign_dir/state/inventory.json")
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
    init|status|recover|acquire|compile) ;;
    *) usage_error "unknown subcommand $subcommand" ;;
esac

campaign=
campaign_seen=0
workspace_root=/mnt/hfx/work
workspace_seen=0
available_memory_bytes=
available_disk_bytes=
retained_input_bytes=
retained_basin_output_bytes=
assembly_memory_ceiling_bytes=
assembly_scratch_ceiling_bytes=
assembled_artifact_bytes=
sizing_seen=' '
max_parallel=
max_parallel_seen=0
fabric_version=
fabric_version_seen=0

while (($# > 0)); do
    option=$1
    case $option in
        --campaign|--workspace-root|--available-memory-bytes|--available-disk-bytes|--retained-input-bytes|--retained-basin-output-bytes|--assembly-memory-ceiling-bytes|--assembly-scratch-ceiling-bytes|--assembled-artifact-bytes|--max-parallel|--fabric-version)
            shift
            (($# > 0)) && [[ -n "$1" && "$1" != -* ]] || usage_error "option $option requires a value"
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
        --max-parallel)
            [[ "$subcommand" == acquire ]] || usage_error 'option --max-parallel is valid only for acquire'
            ((max_parallel_seen == 0)) || usage_error 'option --max-parallel may not be repeated'
            max_parallel_seen=1
            max_parallel=$value
            ;;
        --fabric-version)
            [[ "$subcommand" == compile ]] || usage_error 'option --fabric-version is valid only for compile'
            ((fabric_version_seen == 0)) || usage_error 'option --fabric-version may not be repeated'
            fabric_version_seen=1
            fabric_version=$value
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

if [[ "$subcommand" == init ]]; then
    for variable_name in available_memory_bytes available_disk_bytes retained_input_bytes retained_basin_output_bytes assembly_memory_ceiling_bytes assembly_scratch_ceiling_bytes assembled_artifact_bytes; do
        eval "value=\${$variable_name-}"
        [[ -n "$value" ]] || usage_error "option --${variable_name//_/-} is required"
        value=$(normalize_positive_i64 "$variable_name" "$value")
        eval "$variable_name=\$value"
    done
    required_memory_bytes=$assembly_memory_ceiling_bytes
    required_disk_bytes=0
    required_disk_bytes=$(checked_add "$required_disk_bytes" "$retained_input_bytes")
    required_disk_bytes=$(checked_add "$required_disk_bytes" "$retained_basin_output_bytes")
    required_disk_bytes=$(checked_add "$required_disk_bytes" "$assembly_scratch_ceiling_bytes")
    required_disk_bytes=$(checked_add "$required_disk_bytes" "$assembled_artifact_bytes")
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
elif [[ "$subcommand" == recover ]]; then
    [[ -d "$campaign_dir" && ! -L "$campaign_dir" ]] || hfx_die "campaign does not exist safely: $campaign_dir"
    recover_campaign
elif [[ "$subcommand" == acquire ]]; then
    ((max_parallel_seen == 1)) || usage_error 'option --max-parallel is required'
    [[ "$max_parallel" =~ ^[0-9]+$ ]] || usage_error 'option --max-parallel must be a base-10 integer from 1 through 62'
    ((max_parallel >= 1 && max_parallel <= 62)) ||
        usage_error 'option --max-parallel must be a base-10 integer from 1 through 62'
    [[ -d "$campaign_dir" && ! -L "$campaign_dir" ]] || hfx_die "campaign does not exist safely: $campaign_dir"
    CURL=$(resolve_command HFX_TDX_CURL curl)
    SHA256SUM=$(resolve_command HFX_TDX_SHA256SUM sha256sum)
    OD=$(resolve_command HFX_TDX_OD od)
    OGRINFO=$(resolve_command HFX_TDX_OGRINFO ogrinfo)
    acquire_campaign
else
    ((fabric_version_seen == 1)) || usage_error 'option --fabric-version is required'
    [[ -n "$fabric_version" && "$fabric_version" != -* ]] ||
        usage_error 'option --fabric-version requires a non-empty non-option value'
    [[ ! "$fabric_version" =~ [[:cntrl:]] ]] ||
        usage_error 'option --fabric-version must not contain ASCII control characters'
    [[ -d "$campaign_dir" && ! -L "$campaign_dir" ]] || hfx_die "campaign does not exist safely: $campaign_dir"
    ADAPTER_PYTHON=$(resolve_command HFX_TDX_ADAPTER_PYTHON "$HFX_TDX_DEFAULT_ADAPTER_PYTHON")
    ADAPTER_SCRIPT=${HFX_TDX_ADAPTER_SCRIPT-$repo_root/adapters/tdx-hydro/build_adapter.py}
    HFX=$(resolve_command HFX_TDX_HFX "$HFX_TDX_DEFAULT_HFX")
    compile_campaign
fi
