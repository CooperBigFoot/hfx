#!/usr/bin/env bash

set -Eeuo pipefail
IFS=$'\n\t'
set +x

# Selects the only Hetzner Cloud project context lifecycle scripts may use.
readonly HFX_HCLOUD_CONTEXT=pourpoint
# Supplies the default Hetzner Cloud location for campaign resources.
readonly HFX_DEFAULT_LOCATION=fsn1
# Supplies the default Hetzner Cloud server type for campaign builders.
readonly HFX_DEFAULT_SERVER_TYPE=ccx33
# Supplies the default requested campaign data-volume size in gigabytes.
readonly HFX_DEFAULT_VOLUME_SIZE_GB=100
# Supplies the default registered workstation SSH key name.
readonly HFX_DEFAULT_SSH_KEY=nicolas-workstation
# Supplies the default named system image for campaign builders.
readonly HFX_DEFAULT_IMAGE=debian-12
# Prefixes every deterministic campaign resource name.
readonly HFX_RESOURCE_PREFIX=hfx-build
# Fixes the campaign data filesystem mount point on builders.
readonly HFX_MOUNT_POINT=/mnt/hfx
# Fixes the protected S3 credential destination on builders.
readonly HFX_REMOTE_S3_ENV=/etc/pourpoint-hfx.env

# Writes a prefixed diagnostic to stderr and terminates unsuccessfully.
hfx_die() {
    local message
    local IFS=' '
    message="$*"
    printf 'hfx: error: %s\n' "$message" >&2
    exit 1
}

# Writes a prefixed progress message to stderr while preserving stdout for results.
hfx_log() {
    local message
    local IFS=' '
    message="$*"
    printf 'hfx: %s\n' "$message" >&2
}

# Requires an executable command to be available on PATH.
hfx_require_command() {
    local name=${1-}
    [[ -n "$name" ]] || hfx_die 'command name is required'
    command -v -- "$name" >/dev/null 2>&1 || hfx_die "required command '$name' is not available on PATH"
}

# Validates a campaign identifier against the frozen campaign alphabet and length.
hfx_validate_campaign() {
    local campaign=${1-}
    [[ "$campaign" =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]] || hfx_die "invalid campaign '$campaign'; use 1-32 lowercase letters, digits, or hyphens, starting with a letter or digit"
}

# Validates a named option against the restricted infrastructure-name alphabet.
hfx_validate_name() {
    local field_label=${1-}
    local value=${2-}
    [[ -n "$field_label" ]] || hfx_die 'name field label is required'
    [[ "$value" =~ ^[A-Za-z0-9._-]+$ ]] || hfx_die "invalid $field_label; use only ASCII letters, digits, dot, underscore, or hyphen"
}

# Validates a named option as a digit-only base-10 integer greater than zero.
hfx_validate_positive_integer() {
    local field_label=${1-}
    local value=${2-}
    [[ -n "$field_label" ]] || hfx_die 'integer field label is required'
    [[ "$value" =~ ^[0-9]+$ ]] || hfx_die "invalid $field_label; expected a base-10 positive integer"
    [[ "$value" =~ [1-9] ]] || hfx_die "invalid $field_label; expected a value greater than zero"
}

# Prints the deterministic server name for a validated campaign identifier.
hfx_server_name() {
    local campaign=${1-}
    hfx_validate_campaign "$campaign"
    printf '%s-%s\n' "$HFX_RESOURCE_PREFIX" "$campaign"
}

# Prints the deterministic data-volume name for a validated campaign identifier.
hfx_volume_name() {
    local campaign=${1-}
    hfx_validate_campaign "$campaign"
    printf '%s-%s-data\n' "$HFX_RESOURCE_PREFIX" "$campaign"
}

# Retrieves the project token from macOS Keychain and exports workstation-only hcloud authentication.
hfx_authenticate() {
    local token
    set +x
    hfx_require_command hcloud
    hfx_require_command jq
    hfx_require_command security
    if ! token="$(security find-generic-password -s hetzner-cloud-pourpoint -a pourpoint-bootstrap -w 2>/dev/null)"; then
        hfx_die 'could not retrieve the Hetzner Cloud project token from macOS Keychain'
    fi
    [[ -n "$token" ]] || hfx_die 'macOS Keychain returned an empty Hetzner Cloud project token'
    export HCLOUD_TOKEN=$token
    export HCLOUD_CONTEXT=$HFX_HCLOUD_CONTEXT
    token=
    hfx_log "authenticated for Hetzner Cloud context $HFX_HCLOUD_CONTEXT"
}

# Removes the exported Hetzner Cloud token from the current shell environment.
hfx_clear_auth() {
    unset HCLOUD_TOKEN
}

# Invokes hcloud through the fixed project context after verifying authentication.
hfx_hcloud() {
    [[ -n "${HCLOUD_TOKEN:-}" ]] || hfx_die 'Hetzner Cloud authentication is not initialized'
    command hcloud --context "$HFX_HCLOUD_CONTEXT" "$@"
}

# Prints the sole numeric server ID matching an exact complete name, or nothing when absent.
hfx_exact_server_id() {
    local exact_name=${1-}
    local response matches count
    if ! response="$(hfx_hcloud server list -o json)"; then
        hfx_die 'failed to list servers for exact-name lookup'
    fi
    if ! matches="$(jq -cer --arg name "$exact_name" 'if type != "array" then error("expected resource array") elif (all(.[]; type == "object" and (.name | type == "string"))) | not then error("malformed resource") else [.[] | select(.name == $name)] end' <<<"$response")"; then
        hfx_die 'server lookup returned malformed JSON'
    fi
    count=$(jq -r 'length' <<<"$matches") || hfx_die 'could not count exact server matches'
    ((count <= 1)) || hfx_die "more than one server exactly matches '$exact_name'; have an operator inspect/remove the conflicting resource"
    if ((count == 1)); then
        jq -er '.[0].id | if type == "number" and . == floor then tostring else error("numeric ID required") end' <<<"$matches" || hfx_die "server '$exact_name' has a malformed ID"
    fi
}

# Prints the sole numeric volume ID matching an exact complete name, or nothing when absent.
hfx_exact_volume_id() {
    local exact_name=${1-}
    local response matches count
    if ! response="$(hfx_hcloud volume list -o json)"; then
        hfx_die 'failed to list volumes for exact-name lookup'
    fi
    if ! matches="$(jq -cer --arg name "$exact_name" 'if type != "array" then error("expected resource array") elif (all(.[]; type == "object" and (.name | type == "string"))) | not then error("malformed resource") else [.[] | select(.name == $name)] end' <<<"$response")"; then
        hfx_die 'volume lookup returned malformed JSON'
    fi
    count=$(jq -r 'length' <<<"$matches") || hfx_die 'could not count exact volume matches'
    ((count <= 1)) || hfx_die "more than one volume exactly matches '$exact_name'; have an operator inspect/remove the conflicting resource"
    if ((count == 1)); then
        jq -er '.[0].id | if type == "number" and . == floor then tostring else error("numeric ID required") end' <<<"$matches" || hfx_die "volume '$exact_name' has a malformed ID"
    fi
}

# Prints the sole numeric SSH-key ID matching an exact complete name, or nothing when absent.
hfx_exact_ssh_key_id() {
    local exact_name=${1-}
    local response matches count
    if ! response="$(hfx_hcloud ssh-key list -o json)"; then
        hfx_die 'failed to list SSH keys for exact-name lookup'
    fi
    if ! matches="$(jq -cer --arg name "$exact_name" 'if type != "array" then error("expected resource array") elif (all(.[]; type == "object" and (.name | type == "string"))) | not then error("malformed resource") else [.[] | select(.name == $name)] end' <<<"$response")"; then
        hfx_die 'SSH-key lookup returned malformed JSON'
    fi
    count=$(jq -r 'length' <<<"$matches") || hfx_die 'could not count exact SSH-key matches'
    ((count <= 1)) || hfx_die "more than one SSH key exactly matches '$exact_name'; correct --ssh-key or have an operator inspect/remove the conflicting key"
    if ((count == 1)); then
        jq -er '.[0].id | if type == "number" and . == floor then tostring else error("numeric ID required") end' <<<"$matches" || hfx_die "SSH key '$exact_name' has a malformed ID"
    fi
}
