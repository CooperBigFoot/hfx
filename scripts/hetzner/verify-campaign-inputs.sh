#!/usr/bin/env bash
# verify : (EvidenceRoot | CredentialFile | HcloudContext, Check) -> Pass | Refusal
#
# Proves the operator-side inputs of a campaign before any cloud mutation: the
# evidence root accepts writes, the opaque S3 credential file authenticates
# against the campaign bucket, and the Hetzner Cloud context resolves with
# read-only listings. Credential values are parsed into the environment of one
# subprocess with tracing off and are never printed, logged, or recorded.

set -Eeuo pipefail
IFS=$'\n\t'
set +x

SCRIPT_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/hetzner/common.sh
source "$SCRIPT_DIR/common.sh"

readonly HFX_CAMPAIGN_BUCKET=s3://pourpoint-hfx
readonly HFX_S3_ENDPOINT=https://fsn1.your-objectstorage.com
readonly HFX_S3_REGION=fsn1

usage() {
    cat <<'USAGE'
Usage: verify-campaign-inputs.sh --evidence-root <absolute-path> --check evidence-root-writable
       verify-campaign-inputs.sh --s3-env-file <absolute-path> --check credential-file-authenticates
       verify-campaign-inputs.sh --check hcloud-context-resolves

Options:
  --evidence-root <path>  directory that will receive campaign records
  --s3-env-file <path>    opaque file holding exactly AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
  --check <name>          one of the three checks above
  -h, --help              print usage and exit 0
USAGE
}

check_evidence_root_writable() {
    local requested=$1
    local resolved probe

    [[ -n "$requested" ]] || hfx_die '--evidence-root must not be empty'
    [[ "$requested" == /* ]] || hfx_die '--evidence-root must be absolute'
    [[ -d "$requested" ]] || hfx_die '--evidence-root must be an existing directory'
    [[ ! -L "$requested" ]] || hfx_die '--evidence-root must not be a symlink'
    resolved=$(cd -P -- "$requested" && pwd -P) || hfx_die '--evidence-root could not be resolved'
    printf 'evidence-root: %s\n' "$resolved"

    probe=$(mktemp "$resolved/.hfx-campaign-input-probe.XXXXXX") ||
        hfx_die '--evidence-root is not writable'
    rm -f -- "$probe" || hfx_die '--evidence-root probe could not be removed'
    printf '%s\n' 'evidence-root-writable: PASS'
}

# Reads exactly the two AWS assignments into the environment. Any other line, a
# duplicate, an empty value, or a value outside the credential alphabet refuses.
parse_credential_file() {
    local path=$1
    local line trimmed name value
    local access_seen=0
    local secret_seen=0

    while IFS= read -r line || [[ -n "$line" ]]; do
        trimmed=$line
        trimmed=${trimmed#"${trimmed%%[![:space:]]*}"}
        trimmed=${trimmed%"${trimmed##*[![:space:]]}"}
        [[ -n "$trimmed" && ${trimmed:0:1} != '#' ]] || continue
        if [[ "$trimmed" == export[[:space:]]* ]]; then
            trimmed=${trimmed#export}
            trimmed=${trimmed#"${trimmed%%[![:space:]]*}"}
        fi
        [[ "$trimmed" == *=* ]] || return 1
        name=${trimmed%%=*}
        name=${name%"${name##*[![:space:]]}"}
        value=${trimmed#*=}
        value=${value#"${value%%[![:space:]]*}"}
        value=${value%"${value##*[![:space:]]}"}
        [[ -n "$value" ]] || return 1
        if [[ ${#value} -ge 2 && ${value:0:1} == "'" && ${value: -1} == "'" ]]; then
            value=${value:1:${#value}-2}
        elif [[ ${#value} -ge 2 && ${value:0:1} == '"' && ${value: -1} == '"' ]]; then
            value=${value:1:${#value}-2}
        fi
        [[ -n "$value" && "$value" != *[!A-Za-z0-9_./+,:=@%-]* ]] || return 1
        case $name in
            AWS_ACCESS_KEY_ID)
                ((access_seen == 0)) || return 1
                AWS_ACCESS_KEY_ID=$value
                access_seen=1
                ;;
            AWS_SECRET_ACCESS_KEY)
                ((secret_seen == 0)) || return 1
                AWS_SECRET_ACCESS_KEY=$value
                secret_seen=1
                ;;
            *) return 1 ;;
        esac
    done <"$path"

    ((access_seen == 1 && secret_seen == 1)) || return 1
    export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
}

check_credential_file_authenticates() {
    local path=$1
    local status=0

    set +x
    unset AWS_SESSION_TOKEN AWS_SECURITY_TOKEN
    printf '%s\n' 'credential-file: present'
    [[ -n "$path" && "$path" == /* ]] || status=1
    [[ -f "$path" && ! -L "$path" && -s "$path" ]] || status=1
    if ((status == 0)); then
        parse_credential_file "$path" || status=1
    fi
    if ((status == 0)); then
        command -v -- aws >/dev/null 2>&1 || status=1
    fi
    if ((status == 0)); then
        AWS_PAGER= command aws s3 ls "$HFX_CAMPAIGN_BUCKET" \
            --endpoint-url "$HFX_S3_ENDPOINT" --region "$HFX_S3_REGION" \
            >/dev/null 2>&1 || status=1
    fi
    unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
    if ((status != 0)); then
        printf '%s\n' 'credential-file-authenticates: FAIL'
        exit 1
    fi
    printf '%s\n' 'credential-file-authenticates: PASS'
}

check_hcloud_context_resolves() {
    local active_context

    hfx_require_command hcloud
    active_context=$(command hcloud context active 2>/dev/null) ||
        hfx_die 'could not resolve the active hcloud context'
    [[ "$active_context" == "$HFX_HCLOUD_CONTEXT" ]] ||
        hfx_die "active hcloud context must be $HFX_HCLOUD_CONTEXT"
    hfx_authenticate
    hfx_hcloud server list >/dev/null || hfx_die 'read-only hcloud server listing failed'
    hfx_hcloud volume list >/dev/null || hfx_die 'read-only hcloud volume listing failed'
    hfx_clear_auth
    printf '%s\n' 'hcloud-context-resolves: PASS'
}

evidence_root=
s3_env_file=
check=
seen=' '
while (($#)); do
    case $1 in
        -h | --help)
            (($# == 1)) || hfx_die "$1 must be used alone"
            usage
            exit 0
            ;;
        --evidence-root | --s3-env-file | --check)
            [[ "$seen" != *" $1 "* ]] || hfx_die "option $1 may not be repeated"
            seen="$seen$1 "
            (($# >= 2)) || hfx_die "option $1 requires a value"
            case $1 in
                --evidence-root) evidence_root=$2 ;;
                --s3-env-file) s3_env_file=$2 ;;
                --check) check=$2 ;;
            esac
            shift 2
            ;;
        *) hfx_die "unknown argument: $1" ;;
    esac
done

[[ -n "$check" ]] || hfx_die '--check is required'
case $check in
    evidence-root-writable)
        [[ -z "$s3_env_file" ]] || hfx_die '--s3-env-file is not valid for this check'
        check_evidence_root_writable "$evidence_root"
        ;;
    credential-file-authenticates)
        [[ -z "$evidence_root" ]] || hfx_die '--evidence-root is not valid for this check'
        check_credential_file_authenticates "$s3_env_file"
        ;;
    hcloud-context-resolves)
        [[ -z "$evidence_root" && -z "$s3_env_file" ]] ||
            hfx_die 'path options are not valid for this check'
        check_hcloud_context_resolves
        ;;
    *) hfx_die "unknown check: $check" ;;
esac
