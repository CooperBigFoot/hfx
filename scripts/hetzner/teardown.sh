#!/usr/bin/env bash

SCRIPT_SOURCE=${BASH_SOURCE[0]}
while [[ -L "$SCRIPT_SOURCE" ]]; do
    SCRIPT_DIR=$(cd -P -- "$(dirname -- "$SCRIPT_SOURCE")" && pwd)
    SCRIPT_SOURCE=$(readlink -- "$SCRIPT_SOURCE")
    [[ "$SCRIPT_SOURCE" == /* ]] || SCRIPT_SOURCE=$SCRIPT_DIR/$SCRIPT_SOURCE
done
SCRIPT_DIR=$(cd -P -- "$(dirname -- "$SCRIPT_SOURCE")" && pwd)
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
set +x

usage() {
    cat <<'USAGE'
Usage: teardown.sh --campaign <id> [--keep-volume]

Options:
  --campaign <id>  required
  --keep-volume    retain the detached campaign volume
  -h, --help       print usage and exit 0
USAGE
}

usage_error() {
    usage >&2
    hfx_die "$@"
}

ownership_error() {
    local exact_name=$1
    local field=$2
    hfx_die "resource $exact_name failed campaign ownership validation for $field; refusing teardown; inspect the exact-name resource manually"
}

normalize_description() {
    local response=$1
    local exact_name=$2
    local normalized
    if ! normalized=$(jq -cse 'if length == 1 and (.[0] | type == "object") then .[0] else error("expected one object") end' <<<"$response" 2>/dev/null); then
        ownership_error "$exact_name" description
    fi
    DESCRIPTION_JSON=$normalized
}

validate_identity_and_labels() {
    local expected_id=$1
    local exact_name=$2
    local campaign=$3
    local role=$4

    if ! jq -e --argjson expected_id "$expected_id" '.id == $expected_id' <<<"$DESCRIPTION_JSON" >/dev/null 2>&1; then
        ownership_error "$exact_name" ID
    fi
    if ! jq -e --arg expected_name "$exact_name" '.name == $expected_name' <<<"$DESCRIPTION_JSON" >/dev/null 2>&1; then
        ownership_error "$exact_name" name
    fi
    if ! jq -e '.labels | type == "object"' <<<"$DESCRIPTION_JSON" >/dev/null 2>&1; then
        ownership_error "$exact_name" labels
    fi
    if ! jq -e --arg value campaign '.labels["hfx-managed"] == $value' <<<"$DESCRIPTION_JSON" >/dev/null 2>&1; then
        ownership_error "$exact_name" 'label hfx-managed'
    fi
    if ! jq -e --arg value "$campaign" '.labels["hfx-campaign"] == $value' <<<"$DESCRIPTION_JSON" >/dev/null 2>&1; then
        ownership_error "$exact_name" 'label hfx-campaign'
    fi
    if ! jq -e --arg value "$role" '.labels["hfx-role"] == $value' <<<"$DESCRIPTION_JSON" >/dev/null 2>&1; then
        ownership_error "$exact_name" 'label hfx-role'
    fi
}

describe_server() {
    local expected_id=$1
    local exact_name=$2
    local campaign=$3
    local response
    if ! response=$(hfx_hcloud server describe "$expected_id" -o json); then
        ownership_error "$exact_name" description
    fi
    normalize_description "$response" "$exact_name"
    validate_identity_and_labels "$expected_id" "$exact_name" "$campaign" server
}

describe_volume() {
    local expected_id=$1
    local exact_name=$2
    local campaign=$3
    local response
    if ! response=$(hfx_hcloud volume describe "$expected_id" -o json); then
        ownership_error "$exact_name" description
    fi
    normalize_description "$response" "$exact_name"
    validate_identity_and_labels "$expected_id" "$exact_name" "$campaign" data-volume
    if ! jq -e '.server == null or ((.server | type) == "number" and .server == (.server | floor))' <<<"$DESCRIPTION_JSON" >/dev/null 2>&1; then
        ownership_error "$exact_name" attachment
    fi
    if ! VALIDATED_VOLUME_SERVER=$(jq -er '.server | if . == null then "null" else tostring end' <<<"$DESCRIPTION_JSON" 2>/dev/null); then
        ownership_error "$exact_name" attachment
    fi
}

require_original_id() {
    local resource_kind=$1
    local exact_name=$2
    local original_id=$3
    local current_id
    if [[ "$resource_kind" == server ]]; then
        current_id=$(hfx_exact_server_id "$exact_name")
    else
        current_id=$(hfx_exact_volume_id "$exact_name")
    fi
    if [[ -z "$current_id" ]]; then
        RESOURCE_PRESENT=0
        return
    fi
    if [[ "$current_id" != "$original_id" ]]; then
        hfx_die "exact-name resource $exact_name changed from ID $original_id to $current_id; refusing teardown; inspect it manually"
    fi
    RESOURCE_PRESENT=1
}

non_campaign_attachment_error() {
    local attached_id=$1
    local volume_name=$2
    local server_name=$3
    local server_id=$4
    hfx_die "volume $volume_name is attached to server ID $attached_id, not campaign server ID $server_id; refusing to detach a non-campaign server; inspect the attachment and rerun"
}

wait_for_detach() {
    local volume_id=$1
    local volume_name=$2
    local server_name=$3
    local server_id=$4
    local attempt
    # Every bounded poll revalidates ownership before trusting attachment state.
    for ((attempt = 1; attempt <= 30; attempt++)); do
        describe_volume "$volume_id" "$volume_name" "$campaign"
        if [[ "$VALIDATED_VOLUME_SERVER" == null ]]; then
            return
        fi
        if [[ "$VALIDATED_VOLUME_SERVER" != "$server_id" ]]; then
            non_campaign_attachment_error "$VALIDATED_VOLUME_SERVER" "$volume_name" "$server_name" "$server_id"
        fi
        ((attempt < 30)) && sleep 2
    done
    hfx_die "volume $volume_name remained attached after 30 attempts over about 60 seconds; no resources were deleted; inspect the attachment and rerun"
}

wait_for_server_absence() {
    local server_id=$1
    local server_name=$2
    local attempt current_id
    for ((attempt = 1; attempt <= 30; attempt++)); do
        current_id=$(hfx_exact_server_id "$server_name")
        [[ -z "$current_id" ]] && return
        if [[ "$current_id" != "$server_id" ]]; then
            hfx_die "exact-name resource $server_name changed from ID $server_id to $current_id; refusing teardown; inspect it manually"
        fi
        ((attempt < 30)) && sleep 2
    done
    hfx_die "server $server_name remained present after 30 attempts over about 60 seconds; inspect it and rerun"
}

wait_for_volume_absence() {
    local volume_id=$1
    local volume_name=$2
    local attempt current_id
    for ((attempt = 1; attempt <= 30; attempt++)); do
        current_id=$(hfx_exact_volume_id "$volume_name")
        [[ -z "$current_id" ]] && return
        if [[ "$current_id" != "$volume_id" ]]; then
            hfx_die "exact-name resource $volume_name changed from ID $volume_id to $current_id; refusing teardown; inspect it manually"
        fi
        ((attempt < 30)) && sleep 2
    done
    hfx_die "volume $volume_name remained present after 30 attempts over about 60 seconds; server is absent; inspect the volume and rerun"
}

keep_volume_verification_error() {
    hfx_die "keep-volume verification failed for campaign $campaign; expected server absent and detached volume $volume_name retained as ID $volume_id; inspect only the campaign resources, then rerun"
}

verify_retained_volume() {
    local response normalized
    if ! response=$(hfx_hcloud volume describe "$volume_id" -o json); then
        keep_volume_verification_error
    fi
    if ! normalized=$(jq -cse 'if length == 1 and (.[0] | type == "object") then .[0] else error("expected one object") end' <<<"$response" 2>/dev/null); then
        keep_volume_verification_error
    fi
    if ! jq -e \
        --argjson expected_id "$volume_id" \
        --arg expected_name "$volume_name" \
        --arg expected_campaign "$campaign" \
        '.id == $expected_id
         and .name == $expected_name
         and (.labels | type == "object")
         and .labels["hfx-managed"] == "campaign"
         and .labels["hfx-campaign"] == $expected_campaign
         and .labels["hfx-role"] == "data-volume"
         and .server == null' <<<"$normalized" >/dev/null 2>&1; then
        keep_volume_verification_error
    fi
}

campaign=
keep_volume=0
campaign_seen=0
keep_volume_seen=0
argument_count=$#

while (($# > 0)); do
    case $1 in
        -h)
            ((argument_count == 1)) || usage_error '-h does not accept other arguments'
            usage
            exit 0
            ;;
        --help)
            ((argument_count == 1)) || usage_error '--help does not accept other arguments'
            usage
            exit 0
            ;;
        --campaign)
            ((campaign_seen == 0)) || usage_error 'option --campaign may not be repeated'
            campaign_seen=1
            shift
            (($# > 0)) && [[ -n "$1" && "$1" != -* ]] || usage_error 'option --campaign requires a value'
            campaign=$1
            ;;
        --keep-volume)
            ((keep_volume_seen == 0)) || usage_error 'option --keep-volume may not be repeated'
            keep_volume_seen=1
            keep_volume=1
            ;;
        -*)
            usage_error "unknown option $1; the only supported retention option is --keep-volume"
            ;;
        *)
            usage_error 'positional arguments are not accepted; use --campaign <id>'
            ;;
    esac
    shift
done

((campaign_seen == 1)) || usage_error 'option --campaign is required'
hfx_validate_campaign "$campaign"
server_name=$(hfx_server_name "$campaign")
volume_name=$(hfx_volume_name "$campaign")

hfx_authenticate
trap hfx_clear_auth EXIT

server_id=$(hfx_exact_server_id "$server_name")
volume_id=$(hfx_exact_volume_id "$volume_name")
volume_initially_present=0
[[ -z "$volume_id" ]] || volume_initially_present=1

# Validate the complete initial campaign state before making any mutation.
[[ -z "$server_id" ]] || describe_server "$server_id" "$server_name" "$campaign"
if [[ -n "$volume_id" ]]; then
    describe_volume "$volume_id" "$volume_name" "$campaign"
    initial_volume_server=$VALIDATED_VOLUME_SERVER
    if [[ "$initial_volume_server" != null ]]; then
        if [[ -z "$server_id" ]]; then
            hfx_die "volume $volume_name is attached to server ID $initial_volume_server, but campaign server $server_name is absent; detach it manually after confirming ownership, then rerun"
        fi
        if [[ "$initial_volume_server" != "$server_id" ]]; then
            non_campaign_attachment_error "$initial_volume_server" "$volume_name" "$server_name" "$server_id"
        fi
    fi
else
    initial_volume_server=null
fi

# Detach explicitly so server deletion can never implicitly decide volume fate.
if [[ -n "$volume_id" && "$initial_volume_server" != null ]]; then
    require_original_id volume "$volume_name" "$volume_id"
    if ((RESOURCE_PRESENT == 1)); then
        describe_volume "$volume_id" "$volume_name" "$campaign"
        if [[ "$VALIDATED_VOLUME_SERVER" == null ]]; then
            :
        elif [[ "$VALIDATED_VOLUME_SERVER" != "$server_id" ]]; then
            non_campaign_attachment_error "$VALIDATED_VOLUME_SERVER" "$volume_name" "$server_name" "$server_id"
        else
            hfx_log "detaching volume $volume_name from server $server_name"
            if ! hfx_hcloud volume detach "$volume_id" >/dev/null; then
                hfx_die "failed to detach volume $volume_name from server $server_name; no resources were deleted; inspect the attachment and rerun"
            fi
            wait_for_detach "$volume_id" "$volume_name" "$server_name" "$server_id"
        fi
    fi
fi

if [[ -n "$server_id" ]]; then
    require_original_id server "$server_name" "$server_id"
    if ((RESOURCE_PRESENT == 1)); then
        describe_server "$server_id" "$server_name" "$campaign"
        hfx_log "deleting server $server_name"
        if ! hfx_hcloud server delete "$server_id" >/dev/null; then
            hfx_die "failed to delete server $server_name; the campaign volume was not deleted; inspect the server and rerun"
        fi
        wait_for_server_absence "$server_id" "$server_name"
    fi
fi

if ((keep_volume == 0)) && [[ -n "$volume_id" ]]; then
    require_original_id volume "$volume_name" "$volume_id"
    if ((RESOURCE_PRESENT == 1)); then
        describe_volume "$volume_id" "$volume_name" "$campaign"
        if [[ "$VALIDATED_VOLUME_SERVER" != null ]]; then
            hfx_die "volume $volume_name is attached to server ID $VALIDATED_VOLUME_SERVER, but campaign server $server_name is absent; detach it manually after confirming ownership, then rerun"
        fi
        hfx_log "deleting volume $volume_name"
        if ! hfx_hcloud volume delete "$volume_id" >/dev/null; then
            hfx_die "failed to delete volume $volume_name; server is absent and volume is retained; inspect the volume and rerun"
        fi
        wait_for_volume_absence "$volume_id" "$volume_name"
    fi
fi

final_server_id=$(hfx_exact_server_id "$server_name")
final_volume_id=$(hfx_exact_volume_id "$volume_name")

if ((keep_volume == 0)); then
    if [[ -n "$final_server_id" || -n "$final_volume_id" ]]; then
        hfx_die "zero-footprint verification failed for campaign $campaign (server: ${final_server_id:-absent}, volume: ${final_volume_id:-absent}); inspect only $server_name and $volume_name, then rerun"
    fi
    hfx_log "campaign $campaign has zero Hetzner footprint: server $server_name absent; volume $volume_name absent"
elif ((volume_initially_present == 1)); then
    if [[ -n "$final_server_id" || "$final_volume_id" != "$volume_id" ]]; then
        keep_volume_verification_error
    fi
    verify_retained_volume
    hfx_log "campaign $campaign teardown complete: server $server_name absent; volume $volume_name retained and detached"
else
    if [[ -n "$final_server_id" || -n "$final_volume_id" ]]; then
        hfx_die "keep-volume verification failed for campaign $campaign; expected server absent and no campaign volume; inspect only the campaign resources, then rerun"
    fi
    hfx_log "campaign $campaign teardown complete: server $server_name absent; no campaign volume existed to retain"
fi
