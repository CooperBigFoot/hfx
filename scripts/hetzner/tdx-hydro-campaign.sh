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
        '       tdx-hydro-campaign.sh recover --campaign <id> [--workspace-root <path>]'
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

validate_basin_json() {
    local file=$1
    local basin_id=$2
    "$JQ" -e --arg basin_id "$basin_id" '
        def valid_stage:
            type == "object" and
            keys == ["attempts","failure_reason","status"] and
            (.status == "pending" or .status == "running" or .status == "succeeded" or .status == "failed") and
            (.attempts | type == "number" and . == floor and . >= 0) and
            (.failure_reason == null or (.failure_reason | type == "string"));
        type == "object" and
        keys == ["processing_basin_id","schema_version","stages"] and
        .schema_version == 1 and .processing_basin_id == $basin_id and
        (.stages | type == "object" and
            keys == ["acquire_basins","acquire_streamnet","compile"] and
            (.acquire_basins | valid_stage)) and
        (.stages.acquire_streamnet | valid_stage) and
        (.stages.compile | valid_stage)
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
release_lock() {
    if ((lock_owned == 1)) && [[ -n "$lock_path" && -d "$lock_path" && ! -L "$lock_path" ]] &&
        [[ -f "$lock_path/owner.pid" ]] && [[ $(<"$lock_path/owner.pid") == "$$" ]]; then
        "$RM" -r -- "$lock_path"
    fi
    lock_owned=0
}

acquire_campaign_lock() {
    local locks_dir=$campaign_dir/state/locks
    local owner=
    local stale=
    lock_path=$locks_dir/campaign.lock
    if "$MKDIR" "$lock_path" 2>/dev/null; then
        printf '%s\n' "$$" >"$lock_path/owner.pid"
        lock_owned=1
        return
    fi
    [[ -d "$lock_path" && ! -L "$lock_path" ]] || hfx_die "campaign lock is unsafe: $lock_path"
    if [[ -f "$lock_path/owner.pid" && ! -L "$lock_path/owner.pid" ]]; then
        owner=$(<"$lock_path/owner.pid")
    fi
    if [[ "$owner" =~ ^[0-9]+$ ]] && kill -0 "$owner" 2>/dev/null; then
        hfx_die "campaign lock is held by live PID $owner"
    fi
    stale=$locks_dir/.campaign.lock.stale.$$
    [[ ! -e "$stale" && ! -L "$stale" ]] || hfx_die "stale-lock destination already exists: $stale"
    "$MV" "$lock_path" "$stale" || hfx_die 'campaign lock changed during stale-lock takeover'
    if ! "$MKDIR" "$lock_path" 2>/dev/null; then
        hfx_die 'campaign lock was acquired concurrently'
    fi
    printf '%s\n' "$$" >"$lock_path/owner.pid"
    lock_owned=1
    [[ -d "$stale" && ! -L "$stale" ]] || hfx_die "renamed stale lock is unsafe: $stale"
    "$RM" -r -- "$stale"
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
          schema_version: 1,
          processing_basin_id: $basin_id,
          stages: {
            acquire_basins: {status: "pending", attempts: 0, failure_reason: null},
            acquire_streamnet: {status: "pending", attempts: 0, failure_reason: null},
            compile: {status: "pending", attempts: 0, failure_reason: null}
          }
        }' >"$temporary"
        atomic_install "$temporary" "$campaign_dir/state/basins/$basin_id/current.json" validate_basin_json "$basin_id"
    done < <("$JQ" -r 'keys[]' "$campaign_dir/state/inventory.json")
    validate_workspace_state
    print_status
}

recover_campaign() {
    local basin_id
    local current
    local temporary
    acquire_campaign_lock
    validate_workspace_state
    while IFS= read -r basin_id; do
        current=$campaign_dir/state/basins/$basin_id/current.json
        if "$JQ" -e '[.stages[].status] | any(. == "running")' "$current" >/dev/null; then
            temporary=$campaign_dir/state/basins/$basin_id/.current.json.tmp.$$
            "$JQ" '
                .stages |= with_entries(
                    if .value.status == "running" then
                        .value.status = "pending" |
                        .value.failure_reason = "interrupted before terminal state; reset by recover"
                    else .
                    end
                )
            ' "$current" >"$temporary"
            atomic_install "$temporary" "$current" validate_basin_json "$basin_id"
        fi
    done < <("$JQ" -r 'keys[]' "$campaign_dir/state/inventory.json")
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
    init|status|recover) ;;
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

while (($# > 0)); do
    option=$1
    case $option in
        --campaign|--workspace-root|--available-memory-bytes|--available-disk-bytes|--retained-input-bytes|--retained-basin-output-bytes|--assembly-memory-ceiling-bytes|--assembly-scratch-ceiling-bytes|--assembled-artifact-bytes)
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
validate_inventory_file "$inventory_source"

campaign_dir=$workspace_root/tdx-hydro-$campaign
trap release_lock EXIT

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
else
    [[ -d "$campaign_dir" && ! -L "$campaign_dir" ]] || hfx_die "campaign does not exist safely: $campaign_dir"
    recover_campaign
fi
