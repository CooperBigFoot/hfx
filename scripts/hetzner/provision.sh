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
Usage: provision.sh --campaign <id> --s3-env-file <path> [options]

Options:
  --campaign <id>             required
  --server-type <type>        optional, default ccx33
  --volume-size-gb <integer>  optional, default 100
  --ssh-key <name>            optional, default nicolas-workstation
  --image <name>              optional, default debian-12
  --location <name>           optional, default fsn1
  --s3-env-file <path>        required
  -h, --help                  print usage and exit 0
USAGE
}

usage_error() {
    usage >&2
    hfx_die "$@"
}

resource_conflict() {
    local resource=$1
    local field=$2
    local observed=$3
    hfx_die "$resource conflicts on $field (observed: $observed); choose a different campaign ID, rerun with the original parameter, or have an operator inspect/remove the conflicting resource"
}

describe_server() {
    local server_id=$1
    local server_name=$2
    local response
    if ! response="$(hfx_hcloud server describe "$server_id" -o json)"; then
        resource_conflict "$server_name" 'server description' 'command failure'
    fi
    jq -ce 'select(type == "object")' <<<"$response" || resource_conflict "$server_name" 'server description' 'malformed JSON'
    jq -e --argjson expected_id "$server_id" '.id == $expected_id' <<<"$response" >/dev/null || resource_conflict "$server_name" ID "$(json_observed '.id' "$response")"
}

describe_volume() {
    local volume_id=$1
    local volume_name=$2
    local response
    if ! response="$(hfx_hcloud volume describe "$volume_id" -o json)"; then
        resource_conflict "$volume_name" 'volume description' 'command failure'
    fi
    jq -ce 'select(type == "object")' <<<"$response" || resource_conflict "$volume_name" 'volume description' 'malformed JSON'
    jq -e --argjson expected_id "$volume_id" '.id == $expected_id' <<<"$response" >/dev/null || resource_conflict "$volume_name" ID "$(json_observed '.id' "$response")"
}

json_observed() {
    local expression=$1
    local response=$2
    jq -r "$expression | if . == null then \"missing\" elif type == \"string\" or type == \"number\" or type == \"boolean\" then tostring else \"malformed\" end" <<<"$response" 2>/dev/null || printf 'malformed\n'
}

validate_server() {
    local response=$1
    local server_name=$2
    local expected_campaign=$3
    local expected_location=$4
    local expected_type=$5
    local expected_image=$6
    local expected_ssh_key=$7
    local field expected observed
    local -a label_fields=(hfx-managed hfx-campaign hfx-role hfx-location hfx-server-type hfx-image hfx-ssh-key)
    local -a label_values=(campaign "$expected_campaign" server "$expected_location" "$expected_type" "$expected_image" "$expected_ssh_key")

    if ! jq -e --arg value "$server_name" '.name == $value' <<<"$response" >/dev/null; then
        observed=$(json_observed '.name' "$response")
        resource_conflict "$server_name" name "$observed"
    fi
    jq -e '.id | type == "number" and . == floor' <<<"$response" >/dev/null || resource_conflict "$server_name" ID 'missing or malformed'
    jq -e 'type == "object" and (.labels | type == "object")' <<<"$response" >/dev/null || resource_conflict "$server_name" labels 'malformed or missing'
    for field in "${!label_fields[@]}"; do
        expected=${label_values[$field]}
        if ! jq -e --arg key "${label_fields[$field]}" --arg value "$expected" '.labels[$key] == $value' <<<"$response" >/dev/null; then
            observed=$(jq -r --arg key "${label_fields[$field]}" '.labels[$key] // "missing" | if type == "string" then . else "malformed" end' <<<"$response" 2>/dev/null || printf 'malformed')
            resource_conflict "$server_name" "label ${label_fields[$field]}" "$observed"
        fi
    done

    if ! jq -e --arg value "$expected_location" '.location.name == $value' <<<"$response" >/dev/null; then
        observed=$(json_observed '.location.name' "$response")
        resource_conflict "$server_name" location "$observed"
    fi
    if ! jq -e --arg value "$expected_type" '.server_type.name == $value' <<<"$response" >/dev/null; then
        observed=$(json_observed '.server_type.name' "$response")
        resource_conflict "$server_name" 'server type' "$observed"
    fi
    if jq -e '.image != null' <<<"$response" >/dev/null; then
        if ! jq -e --arg value "$expected_image" '.image.name == $value' <<<"$response" >/dev/null; then
            observed=$(json_observed '.image.name' "$response")
            resource_conflict "$server_name" image "$observed"
        fi
    fi
    jq -e '.status | type == "string" and length > 0' <<<"$response" >/dev/null || resource_conflict "$server_name" status 'missing or malformed'
}

validate_volume() {
    local response=$1
    local volume_name=$2
    local expected_campaign=$3
    local expected_location=$4
    local allowed_server_id=${5-}
    local field expected observed
    local -a label_fields=(hfx-managed hfx-campaign hfx-role hfx-location)
    local -a label_values=(campaign "$expected_campaign" data-volume "$expected_location")

    if ! jq -e --arg value "$volume_name" '.name == $value' <<<"$response" >/dev/null; then
        observed=$(json_observed '.name' "$response")
        resource_conflict "$volume_name" name "$observed"
    fi
    jq -e '.id | type == "number" and . == floor' <<<"$response" >/dev/null || resource_conflict "$volume_name" ID 'missing or malformed'
    jq -e 'type == "object" and (.labels | type == "object")' <<<"$response" >/dev/null || resource_conflict "$volume_name" labels 'malformed or missing'
    for field in "${!label_fields[@]}"; do
        expected=${label_values[$field]}
        if ! jq -e --arg key "${label_fields[$field]}" --arg value "$expected" '.labels[$key] == $value' <<<"$response" >/dev/null; then
            observed=$(jq -r --arg key "${label_fields[$field]}" '.labels[$key] // "missing" | if type == "string" then . else "malformed" end' <<<"$response" 2>/dev/null || printf 'malformed')
            resource_conflict "$volume_name" "label ${label_fields[$field]}" "$observed"
        fi
    done
    if ! jq -e --arg value "$expected_location" '.location.name == $value' <<<"$response" >/dev/null; then
        observed=$(json_observed '.location.name' "$response")
        resource_conflict "$volume_name" location "$observed"
    fi
    jq -e '.size | type == "number" and . == floor and . > 0' <<<"$response" >/dev/null || resource_conflict "$volume_name" size 'missing or malformed'
    jq -e '.server == null or (.server | type == "number" and . == floor)' <<<"$response" >/dev/null || resource_conflict "$volume_name" attachment 'malformed or multiple attachment state'
    if jq -e '.server != null' <<<"$response" >/dev/null; then
        observed=$(jq -r '.server | tostring' <<<"$response")
        [[ -n "$allowed_server_id" && "$observed" == "$allowed_server_id" ]] || resource_conflict "$volume_name" 'attached server ID' "$observed"
    fi
}

wait_for_running_server() {
    local server_id=$1
    local server_name=$2
    local response status attempt powered_on=0
    for ((attempt = 1; attempt <= 30; attempt++)); do
        response=$(describe_server "$server_id" "$server_name")
        status=$(json_observed '.status' "$response")
        case "$status" in
            running)
                printf '%s\n' "$response"
                return 0
                ;;
            off)
                if ((powered_on == 0)); then
                    hfx_log "powering on server $server_name"
                    hfx_hcloud server poweron "$server_id" >/dev/null || hfx_die "failed to power on server $server_name; inspect it and rerun"
                    powered_on=1
                fi
                ;;
        esac
        sleep 2
    done
    resource_conflict "$server_name" status "$status"
}

verify_attachment() {
    local volume_id=$1
    local volume_name=$2
    local server_id=$3
    local response observed
    response=$(describe_volume "$volume_id" "$volume_name")
    validate_volume "$response" "$volume_name" "$campaign" "$location" "$server_id"
    observed=$(json_observed '.server' "$response")
    [[ "$observed" == "$server_id" ]] || resource_conflict "$volume_name" 'attached server ID' "$observed"
    printf '%s\n' "$response"
}

run_remote_filesystem_setup() {
    local ip_address=$1
    local device=$2
    local mount_point=$3
    local device_q mount_q
    printf -v device_q '%q' "$device"
    printf -v mount_q '%q' "$mount_point"
    ssh "${ssh_options[@]}" "root@$ip_address" "bash -s -- $device_q $mount_q" <<'REMOTE_FILESYSTEM'
set -Eeuo pipefail
IFS=$'\n\t'
set +x
[[ $# -eq 2 ]] || { printf 'invalid filesystem setup arguments\n' >&2; exit 1; }
device=$1
mount_point=$2

for ((attempt = 1; attempt <= 60; attempt++)); do
    [[ -b "$device" ]] && break
    ((attempt < 60)) || { printf 'volume device did not become a block device\n' >&2; exit 1; }
    sleep 2
done

filesystem_type=
if filesystem_type=$(blkid -o value -s TYPE -- "$device" 2>/dev/null); then
    [[ -n "$filesystem_type" ]] || { printf 'blkid returned an empty filesystem type\n' >&2; exit 1; }
else
    blkid_status=$?
    [[ $blkid_status -eq 2 ]] || { printf 'blkid inspection failed\n' >&2; exit 1; }
fi

if [[ -z "$filesystem_type" ]]; then
    signatures=$(wipefs -n --noheadings --output TYPE -- "$device") || { printf 'wipefs inspection failed\n' >&2; exit 1; }
    [[ -z "${signatures//[[:space:]]/}" ]] || { printf 'unrecognized retained signature found; inspect the device manually\n' >&2; exit 1; }
    mkfs.ext4 -- "$device"
    filesystem_type=ext4
fi
[[ "$filesystem_type" == ext4 ]] || { printf 'unsupported retained filesystem; inspect the device manually and do not reformat it\n' >&2; exit 1; }

expected_major_minor=$(lsblk -dn -o MAJ:MIN -- "$device" | tr -d "[:space:]")
[[ -n "$expected_major_minor" ]] || { printf 'could not identify the expected block device\n' >&2; exit 1; }
mapfile -t device_targets < <(findmnt -rn -o TARGET,MAJ:MIN | awk -v expected="$expected_major_minor" '$2 == expected { print $1 }')
for device_target in "${device_targets[@]}"; do
    [[ "$device_target" == "$mount_point" ]] || { printf 'expected device is mounted at another location\n' >&2; exit 1; }
done
if findmnt -rn -M "$mount_point" >/dev/null 2>&1; then
    mounted_major_minor=$(findmnt -rn -M "$mount_point" -o MAJ:MIN)
    [[ "$mounted_major_minor" == "$expected_major_minor" ]] || { printf 'mount point is backed by another device\n' >&2; exit 1; }
else
    if [[ -d "$mount_point" ]] && [[ -n "$(find "$mount_point" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
        printf 'refusing to cover a nonempty unmounted directory\n' >&2
        exit 1
    fi
    mkdir -p -- "$mount_point"
fi

uuid=$(blkid -o value -s UUID -- "$device") || { printf 'could not obtain filesystem UUID\n' >&2; exit 1; }
[[ -n "$uuid" ]] || { printf 'filesystem UUID is empty\n' >&2; exit 1; }
[[ "$uuid" =~ ^[A-Fa-f0-9-]+$ ]] || { printf 'filesystem UUID is malformed\n' >&2; exit 1; }
fstab_entry="UUID=$uuid $mount_point ext4 defaults,nofail 0 2"
mapfile -t mount_entries < <(awk -v mount="$mount_point" '$1 !~ /^#/ && $2 == mount { print }' /etc/fstab)
if ((${#mount_entries[@]} == 0)); then
    printf '%s\n' "$fstab_entry" >>/etc/fstab
elif ((${#mount_entries[@]} == 1)) && [[ "${mount_entries[0]}" == "$fstab_entry" ]]; then
    :
else
    printf 'conflicting fstab entry for mount point; inspect it manually\n' >&2
    exit 1
fi

findmnt -rn -M "$mount_point" >/dev/null 2>&1 || mount -- "$mount_point"
mounted_major_minor=$(findmnt -rn -M "$mount_point" -o MAJ:MIN)
[[ "$mounted_major_minor" == "$expected_major_minor" ]] || { printf 'mounted filesystem is not backed by the expected device\n' >&2; exit 1; }
REMOTE_FILESYSTEM
}

run_remote_filesystem_growth() {
    local ip_address=$1
    local device=$2
    local mount_point=$3
    local requested_size=$4
    local device_q mount_q size_q
    printf -v device_q '%q' "$device"
    printf -v mount_q '%q' "$mount_point"
    printf -v size_q '%q' "$requested_size"
    ssh "${ssh_options[@]}" "root@$ip_address" "bash -s -- $device_q $mount_q $size_q" <<'REMOTE_GROWTH'
set -Eeuo pipefail
IFS=$'\n\t'
set +x
[[ $# -eq 3 ]] || { printf 'invalid filesystem growth arguments\n' >&2; exit 1; }
device=$1
mount_point=$2
requested_size_gb=$3
required_bytes=$((requested_size_gb * 1000000000))
for ((attempt = 1; attempt <= 60; attempt++)); do
    current_bytes=$(blockdev --getsize64 -- "$device") || { printf 'could not read block-device capacity\n' >&2; exit 1; }
    ((current_bytes >= required_bytes)) && break
    ((attempt < 60)) || { printf 'block device did not report its enlarged capacity\n' >&2; exit 1; }
    sleep 2
done
[[ "$(blkid -o value -s TYPE -- "$device")" == ext4 ]] || { printf 'filesystem is no longer ext4\n' >&2; exit 1; }
resize2fs "$device"
expected_major_minor=$(lsblk -dn -o MAJ:MIN -- "$device" | tr -d "[:space:]")
mounted_major_minor=$(findmnt -rn -M "$mount_point" -o MAJ:MIN)
[[ -n "$expected_major_minor" && "$mounted_major_minor" == "$expected_major_minor" ]] || { printf 'grown filesystem is not mounted at the expected mount point\n' >&2; exit 1; }
REMOTE_GROWTH
}

install_remote_credentials() {
    local ip_address=$1
    local source_path=$2
    local destination=$3
    local destination_q remote_program remote_program_q
    printf -v destination_q '%q' "$destination"
    remote_program='set -Eeuo pipefail
IFS=$'"'"'\n\t'"'"'
set +x
[[ $# -eq 1 ]] || { printf '"'"'invalid credential installation arguments\n'"'"' >&2; exit 1; }
destination=$1
umask 077
temporary=$(mktemp "${destination}.tmp.XXXXXX")
trap '"'"'rm -f -- "$temporary"'"'"' EXIT
if ! dd of="$temporary" status=none; then
    printf '"'"'failed to receive credential input\n'"'"' >&2
    exit 1
fi
install -o root -g root -m 600 -- "$temporary" "$destination"
metadata=$(stat -c '"'"'%U:%G %a'"'"' -- "$destination")
[[ "$metadata" == '"'"'root:root 600'"'"' ]] || { printf '"'"'credential metadata verification failed\n'"'"' >&2; exit 1; }'
    printf -v remote_program_q '%q' "$remote_program"
    if ! ssh "${ssh_options[@]}" "root@$ip_address" "bash -c $remote_program_q bash $destination_q" <"$source_path"; then
        hfx_die "credential installation failed from $source_path to $destination; correct the source or connectivity and rerun"
    fi
}

campaign=
server_type=$HFX_DEFAULT_SERVER_TYPE
volume_size_gb=$HFX_DEFAULT_VOLUME_SIZE_GB
ssh_key=$HFX_DEFAULT_SSH_KEY
image=$HFX_DEFAULT_IMAGE
location=$HFX_DEFAULT_LOCATION
s3_env_file=
seen_options=" "

while (($#)); do
    option=$1
    case "$option" in
        -h | --help)
            (($# == 1)) || usage_error "$option does not accept other arguments"
            usage
            exit 0
            ;;
        --campaign | --server-type | --volume-size-gb | --ssh-key | --image | --location | --s3-env-file)
            [[ "$seen_options" != *" $option "* ]] || usage_error "option $option may not be repeated"
            seen_options+="$option "
            (($# >= 2)) || usage_error "option $option requires a value"
            [[ -n "$2" && "$2" != --* && "$2" != -h ]] || usage_error "option $option requires a value"
            case "$option" in
                --campaign) campaign=$2 ;;
                --server-type) server_type=$2 ;;
                --volume-size-gb) volume_size_gb=$2 ;;
                --ssh-key) ssh_key=$2 ;;
                --image) image=$2 ;;
                --location) location=$2 ;;
                --s3-env-file) s3_env_file=$2 ;;
            esac
            shift 2
            ;;
        --*) usage_error "unknown option $option" ;;
        *) usage_error "positional arguments are not accepted" ;;
    esac
done

[[ -n "$campaign" ]] || usage_error 'option --campaign is required'
[[ -n "$s3_env_file" ]] || usage_error 'option --s3-env-file is required'
hfx_validate_campaign "$campaign"
hfx_validate_name '--server-type' "$server_type"
hfx_validate_positive_integer '--volume-size-gb' "$volume_size_gb"
hfx_validate_name '--ssh-key' "$ssh_key"
hfx_validate_name '--image' "$image"
hfx_validate_name '--location' "$location"
while [[ "$volume_size_gb" == 0* && ${#volume_size_gb} -gt 1 ]]; do
    volume_size_gb=${volume_size_gb#0}
done
[[ ! -L "$s3_env_file" ]] || hfx_die "--s3-env-file path is a symlink: $s3_env_file"
[[ -f "$s3_env_file" ]] || hfx_die "--s3-env-file must be a regular file: $s3_env_file"
[[ -r "$s3_env_file" ]] || hfx_die "--s3-env-file is not readable: $s3_env_file"
[[ -s "$s3_env_file" ]] || hfx_die "--s3-env-file is empty: $s3_env_file"
hfx_require_command ssh

server_name=$(hfx_server_name "$campaign")
volume_name=$(hfx_volume_name "$campaign")
readonly server_type volume_size_gb ssh_key image location s3_env_file

hfx_authenticate
trap hfx_clear_auth EXIT

hfx_hcloud location describe "$location" >/dev/null || hfx_die "invalid --location '$location'; choose an existing location"
hfx_hcloud server-type describe "$server_type" >/dev/null || hfx_die "invalid --server-type '$server_type'; choose an available type"

# Campaign server types are x86; cax* Arm types would need an architecture parameter, deliberately out of scope.
image_response=$(hfx_hcloud image list -o json) || hfx_die "could not validate --image '$image'"
image_matches=$(jq -cer --arg image "$image" 'if type != "array" then error("expected array") elif (all(.[]; type == "object" and (.name | type == "string") and (.type | type == "string"))) | not then error("malformed image") else [.[] | select(.name == $image and .type == "system" and .architecture == "x86")] end' <<<"$image_response") || hfx_die "malformed response while validating --image '$image'"
image_count=$(jq -r 'length' <<<"$image_matches") || hfx_die "could not count matches for --image '$image'"
((image_count == 1)) || hfx_die "--image '$image' must exactly identify one named x86 system image; correct --image"
jq -e '.[0].id | type == "number" and . == floor' <<<"$image_matches" >/dev/null || hfx_die "--image '$image' has a malformed ID; correct --image"

ssh_key_id=$(hfx_exact_ssh_key_id "$ssh_key")
[[ -n "$ssh_key_id" ]] || hfx_die "--ssh-key '$ssh_key' does not exactly match a registered key; correct --ssh-key"

server_id=$(hfx_exact_server_id "$server_name")
volume_id=$(hfx_exact_volume_id "$volume_name")
server_json=
volume_json=
if [[ -n "$server_id" ]]; then
    server_json=$(describe_server "$server_id" "$server_name")
    validate_server "$server_json" "$server_name" "$campaign" "$location" "$server_type" "$image" "$ssh_key"
fi
if [[ -n "$volume_id" ]]; then
    volume_json=$(describe_volume "$volume_id" "$volume_name")
    validate_volume "$volume_json" "$volume_name" "$campaign" "$location" "$server_id"
fi

if [[ -z "$server_id" ]]; then
    hfx_log "creating server $server_name"
    hfx_hcloud server create \
        --name "$server_name" \
        --type "$server_type" \
        --image "$image" \
        --location "$location" \
        --ssh-key "$ssh_key" \
        --label hfx-managed=campaign \
        --label "hfx-campaign=$campaign" \
        --label hfx-role=server \
        --label "hfx-location=$location" \
        --label "hfx-server-type=$server_type" \
        --label "hfx-image=$image" \
        --label "hfx-ssh-key=$ssh_key" >/dev/null || hfx_die "failed to create server $server_name; inspect the campaign and rerun"
    server_id=$(hfx_exact_server_id "$server_name")
    [[ -n "$server_id" ]] || hfx_die "server $server_name was not found after creation; inspect the campaign and rerun"
    server_json=$(describe_server "$server_id" "$server_name")
    validate_server "$server_json" "$server_name" "$campaign" "$location" "$server_type" "$image" "$ssh_key"
fi

if [[ -n "$volume_id" ]]; then
    volume_json=$(describe_volume "$volume_id" "$volume_name")
    validate_volume "$volume_json" "$volume_name" "$campaign" "$location" "$server_id"
fi

if [[ -z "$volume_id" ]]; then
    hfx_log "creating volume $volume_name"
    hfx_hcloud volume create \
        --name "$volume_name" \
        --size "$volume_size_gb" \
        --location "$location" \
        --label hfx-managed=campaign \
        --label "hfx-campaign=$campaign" \
        --label hfx-role=data-volume \
        --label "hfx-location=$location" >/dev/null || hfx_die "failed to create volume $volume_name; inspect the campaign and rerun"
    volume_id=$(hfx_exact_volume_id "$volume_name")
    [[ -n "$volume_id" ]] || hfx_die "volume $volume_name was not found after creation; inspect the campaign and rerun"
    volume_json=$(describe_volume "$volume_id" "$volume_name")
    validate_volume "$volume_json" "$volume_name" "$campaign" "$location" "$server_id"
fi

server_json=$(wait_for_running_server "$server_id" "$server_name")
validate_server "$server_json" "$server_name" "$campaign" "$location" "$server_type" "$image" "$ssh_key"

volume_json=$(describe_volume "$volume_id" "$volume_name")
validate_volume "$volume_json" "$volume_name" "$campaign" "$location" "$server_id"
attached_server=$(json_observed '.server' "$volume_json")
if [[ "$attached_server" == missing ]]; then
    hfx_log "attaching volume $volume_name to server $server_name"
    hfx_hcloud volume attach "$volume_id" --server "$server_id" >/dev/null || hfx_die "failed to attach volume $volume_name to server $server_name; inspect the attachment and rerun"
fi
volume_json=$(verify_attachment "$volume_id" "$volume_name" "$server_id")

server_json=$(describe_server "$server_id" "$server_name")
ip_address=$(jq -er '.public_net.ipv4.ip | select(type == "string") | select((split(".") | length) == 4 and all(split(".")[]; test("^[0-9]{1,3}$") and (tonumber >= 0 and tonumber <= 255)))' <<<"$server_json") || resource_conflict "$server_name" 'public IPv4 address' 'missing or malformed'

ssh_options=(-o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new)
ssh_ready=0
for ((attempt = 1; attempt <= 60; attempt++)); do
    if ssh "${ssh_options[@]}" "root@$ip_address" true; then
        ssh_ready=1
        break
    fi
    ((attempt < 60)) && sleep 5
done
((ssh_ready == 1)) || hfx_die "SSH readiness failed for server $server_name at $ip_address after 60 attempts; resources were retained, verify connectivity and rerun"

volume_json=$(verify_attachment "$volume_id" "$volume_name" "$server_id")
device=$(jq -er '.linux_device | select(type == "string" and length > 0)' <<<"$volume_json") || resource_conflict "$volume_name" linux_device 'missing, null, or malformed'
[[ "$device" =~ ^/dev/[A-Za-z0-9._+:-]+(/[A-Za-z0-9._+:-]+)*$ ]] || resource_conflict "$volume_name" linux_device 'invalid device path'
IFS='/' read -r -a device_components <<<"${device#/dev/}"
for component in "${device_components[@]}"; do
    [[ "$component" != . && "$component" != .. ]] || resource_conflict "$volume_name" linux_device 'invalid path component'
done

run_remote_filesystem_setup "$ip_address" "$device" "$HFX_MOUNT_POINT" || hfx_die "filesystem inspection or mount failed for $volume_name on $HFX_MOUNT_POINT; resources were retained, inspect the device and rerun"

volume_json=$(describe_volume "$volume_id" "$volume_name")
current_volume_size=$(jq -er '.size | if type == "number" and . == floor and . > 0 then tostring else error("positive integer size required") end' <<<"$volume_json") || resource_conflict "$volume_name" size 'missing or malformed'
if ((10#$current_volume_size < 10#$volume_size_gb)); then
    hfx_log "growing volume $volume_name from $current_volume_size GB to $volume_size_gb GB"
    hfx_hcloud volume resize "$volume_id" --size "$volume_size_gb" >/dev/null || hfx_die "cloud resize failed for $volume_name; resources were retained, inspect it and rerun"
    run_remote_filesystem_growth "$ip_address" "$device" "$HFX_MOUNT_POINT" "$volume_size_gb" || hfx_die "filesystem growth failed for $volume_name; resources were retained, inspect it and rerun"
    retained_volume_size=$volume_size_gb
elif ((10#$current_volume_size > 10#$volume_size_gb)); then
    retained_volume_size=$current_volume_size
    hfx_log "volume $volume_name is already $current_volume_size GB, exceeding the requested $volume_size_gb GB; retaining the larger size"
else
    retained_volume_size=$current_volume_size
fi

install_remote_credentials "$ip_address" "$s3_env_file" "$HFX_REMOTE_S3_ENV"

hfx_log "campaign: $campaign"
hfx_log "server: $server_name"
hfx_log "volume: $volume_name"
hfx_log "server IPv4: $ip_address"
hfx_log "volume size GB: $retained_volume_size"
hfx_log "mount point: $HFX_MOUNT_POINT"
hfx_log "credential path: $HFX_REMOTE_S3_ENV"
