#!/usr/bin/env bash

set -Eeuo pipefail
IFS=$'\n\t'
set +x

usage() {
    printf 'Usage: smoke.sh --campaign <id>\n'
}

smoke_die() {
    local message
    local IFS=' '
    message="$*"
    printf 'smoke: error: %s\n' "$message" >&2
    exit 1
}

usage_error() {
    usage >&2
    smoke_die "$@"
}

if [[ $# -eq 1 && ("$1" == -h || "$1" == --help) ]]; then
    usage
    exit 0
fi
[[ $# -eq 2 ]] || usage_error 'exactly --campaign <id> is required'
[[ "$1" == --campaign ]] || usage_error 'option --campaign is required'
[[ -n "$2" && "$2" != -* ]] || usage_error 'option --campaign requires a value'
campaign=$2
[[ "$campaign" =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]] || smoke_die "invalid campaign '$campaign'; use 1-32 lowercase letters, digits, or hyphens, starting with a letter or digit"

((EUID == 0)) || smoke_die 'root privileges are required to read protected credentials; run this through launch.sh after provisioning'
for command_name in curl aws stat od tr mv rm date chmod; do
    command -v -- "$command_name" >/dev/null 2>&1 || smoke_die "required command $command_name is unavailable; rerun bootstrap.sh and then rerun the smoke workload"
done

readonly download_directory=/mnt/hfx/work/downloads
readonly url=https://earth-info.nga.mil/php/download.php?file=7020000010-streamnet-gpkg
readonly expected_bytes=1676398592
readonly expected_magic=53514c69746520666f726d6174203300
readonly final_path=$download_directory/7020000010-streamnet-gpkg
readonly partial_path=$download_directory/7020000010-streamnet-gpkg.partial
readonly credential_path=/etc/pourpoint-hfx.env

[[ -d "$download_directory" && ! -L "$download_directory" ]] || smoke_die '/mnt/hfx/work/downloads is absent or unsafe; rerun bootstrap.sh after provisioning, then rerun the smoke workload'

observed_bytes=
verify_download() {
    local file=$1
    local observed_magic
    [[ -f "$file" && ! -L "$file" ]] || return 1
    observed_bytes=$(stat -c %s -- "$file") || return 1
    [[ "$observed_bytes" =~ ^[0-9]+$ && "$observed_bytes" -eq "$expected_bytes" ]] || return 2
    observed_magic=$(od -An -tx1 -N16 -- "$file" | tr -d '[:space:]') || return 1
    [[ "$observed_magic" == "$expected_magic" ]] || return 3
}

if [[ -e "$final_path" || -L "$final_path" ]]; then
    if [[ ! -f "$final_path" || -L "$final_path" ]]; then
        smoke_die "$final_path exists but is not a regular non-symlink file; remove or correct it, then rerun"
    fi
    verification_status=0
    verify_download "$final_path" || verification_status=$?
    case $verification_status in
        0)
            printf 'smoke: NGA download is already complete and verified at %s\n' "$final_path"
            ;;
        2)
            rm -- "$final_path"
            smoke_die "invalid NGA download removed: expected $expected_bytes bytes, observed $observed_bytes; rerun the smoke workload"
            ;;
        3)
            rm -- "$final_path"
            smoke_die 'invalid NGA download removed: invalid SQLite header; rerun the smoke workload'
            ;;
        *)
            smoke_die "could not verify $final_path; inspect the download directory and rerun"
            ;;
    esac
else
    if [[ -e "$partial_path" || -L "$partial_path" ]]; then
        if [[ -f "$partial_path" && ! -L "$partial_path" ]]; then
            rm -- "$partial_path"
            printf 'smoke: removed incomplete NGA partial download\n'
        else
            smoke_die "$partial_path exists but is not a regular non-symlink file; remove or correct it, then rerun"
        fi
    fi
    printf 'smoke: downloading pinned NGA stream network to %s\n' "$partial_path"
    if ! curl --fail --show-error --location \
        --retry 2 --retry-delay 5 --retry-all-errors \
        --connect-timeout 30 --max-time 3600 \
        --output "$partial_path" "$url"; then
        [[ -f "$partial_path" && ! -L "$partial_path" ]] && rm -- "$partial_path"
        smoke_die 'NGA download failed; the partial file was removed; rerun the smoke workload'
    fi
    verification_status=0
    verify_download "$partial_path" || verification_status=$?
    case $verification_status in
        0) ;;
        2)
            rm -- "$partial_path"
            smoke_die "NGA download size mismatch: expected $expected_bytes bytes, observed $observed_bytes; the partial file was removed; rerun"
            ;;
        3)
            rm -- "$partial_path"
            smoke_die 'NGA download has an invalid SQLite header; the partial file was removed; rerun'
            ;;
        *)
            [[ -f "$partial_path" && ! -L "$partial_path" ]] && rm -- "$partial_path"
            smoke_die 'NGA download could not be verified; the partial file was removed; inspect storage and rerun'
            ;;
    esac
    if ! mv -- "$partial_path" "$final_path"; then
        [[ -f "$partial_path" && ! -L "$partial_path" ]] && rm -- "$partial_path"
        smoke_die 'the verified NGA download could not be installed atomically; the partial file was removed; inspect storage and rerun'
    fi
    if ! chmod 0644 "$final_path"; then
        rm -- "$final_path"
        smoke_die 'the final NGA file mode could not be set and the file was removed; inspect storage and rerun'
    fi
    if ! verify_download "$final_path"; then
        rm -- "$final_path"
        smoke_die 'final NGA file failed verification after atomic installation and was removed; inspect storage and rerun'
    fi
    printf 'smoke: NGA download completed and verified at %s\n' "$final_path"
fi

if [[ ! -e "$credential_path" && ! -L "$credential_path" ]]; then
    smoke_die '/etc/pourpoint-hfx.env is absent; rerun provision.sh with --s3-env-file, then rerun the smoke workload'
fi
if [[ -L "$credential_path" || ! -f "$credential_path" || ! -s "$credential_path" ]] ||
    [[ "$(stat -c '%U:%G %a' -- "$credential_path" 2>/dev/null)" != 'root:root 600' ]]; then
    smoke_die "protected credential file is unsafe; after correcting the source, rerun provision.sh --campaign $campaign --s3-env-file <path>, then rerun the smoke workload; expected root:root mode 600"
fi

set -a
# shellcheck disable=SC1091
if ! source "$credential_path"; then
    set +a
    smoke_die '/etc/pourpoint-hfx.env could not be loaded; correct its shell assignments, reprovision it, and rerun'
fi
set +a
[[ ${AWS_ACCESS_KEY_ID+x} && -n "${AWS_ACCESS_KEY_ID-}" ]] || smoke_die 'AWS_ACCESS_KEY_ID is missing or empty; the provision source file must define it'
[[ ${AWS_SECRET_ACCESS_KEY+x} && -n "${AWS_SECRET_ACCESS_KEY-}" ]] || smoke_die 'AWS_SECRET_ACCESS_KEY is missing or empty; the provision source file must define it'

object_timestamp=$(date -u +%Y%m%dT%H%M%S%NZ)
object_key=smoke/$campaign-$object_timestamp.txt
if ! printf 'hfx lifecycle smoke\n' | aws s3 cp - "s3://pourpoint-hfx/$object_key" \
    --endpoint-url https://fsn1.your-objectstorage.com \
    --region fsn1 \
    --only-show-errors; then
    smoke_die "S3 upload failed for s3://pourpoint-hfx/$object_key at https://fsn1.your-objectstorage.com; inspect access and rerun"
fi

printf 'smoke: verified NGA file: %s\n' "$final_path"
printf 'smoke: verified NGA bytes: %s\n' "$expected_bytes"
printf 'smoke: uploaded: s3://pourpoint-hfx/%s\n' "$object_key"
printf 'smoke: success\n'
