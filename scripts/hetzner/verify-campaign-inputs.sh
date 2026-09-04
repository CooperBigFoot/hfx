#!/usr/bin/env bash
# verify : (EvidenceRoot | CredentialFile | HcloudContext, Check) -> Pass | Refusal
#
# Proves the operator-side inputs of a campaign before any cloud mutation: the
# evidence root accepts writes, the opaque S3 credential file authenticates
# against the campaign bucket, the Hetzner Cloud context resolves with
# read-only listings, and the installed hcloud CLI emits the JSON shape the
# runbook's identity projections read. Credential values are parsed into the
# environment of one subprocess with tracing off and are never printed, logged,
# or recorded.

set -Eeuo pipefail
IFS=$'\n\t'
set +x

SCRIPT_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/hetzner/common.sh
source "$SCRIPT_DIR/common.sh"

readonly HFX_CAMPAIGN_BUCKET=s3://pourpoint-hfx
readonly HFX_S3_ENDPOINT=https://fsn1.your-objectstorage.com
readonly HFX_S3_REGION=fsn1
# The standing out-of-scope server whose read-only description witnesses the
# installed hcloud JSON shape before any campaign resource exists.
readonly HFX_SHAPE_WITNESS_SERVER=pourpoint-web-1
readonly HFX_IDENTITY_PROJECTION=$SCRIPT_DIR/hcloud-identity.jq
# The project quota is 8 dedicated cores, so the campaign server type must use all of them.
readonly HFX_DEDICATED_CORE_QUOTA=8

usage() {
    cat <<'USAGE'
Usage: verify-campaign-inputs.sh --evidence-root <absolute-path> --check evidence-root-writable
       verify-campaign-inputs.sh --s3-env-file <absolute-path> --check credential-file-authenticates
       verify-campaign-inputs.sh --check hcloud-context-resolves
       verify-campaign-inputs.sh --check hcloud-json-shape

Options:
  --evidence-root <path>  directory that will receive campaign records
  --s3-env-file <path>    opaque file holding exactly AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
  --check <name>          one of the four checks above

hcloud-json-shape projects read-only descriptions of the standing server
pourpoint-web-1, the campaign server type, and the campaign location through
scripts/hetzner/hcloud-identity.jq, the same projection the runbook applies
after provisioning, and refuses when any projected field is null or mistyped.
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

require_active_hcloud_context() {
    local active_context

    hfx_require_command hcloud
    active_context=$(command hcloud context active 2>/dev/null) ||
        hfx_die 'could not resolve the active hcloud context'
    [[ "$active_context" == "$HFX_HCLOUD_CONTEXT" ]] ||
        hfx_die "active hcloud context must be $HFX_HCLOUD_CONTEXT"
}

check_hcloud_context_resolves() {
    require_active_hcloud_context
    hfx_authenticate
    hfx_hcloud server list >/dev/null || hfx_die 'read-only hcloud server listing failed'
    hfx_hcloud volume list >/dev/null || hfx_die 'read-only hcloud volume listing failed'
    hfx_clear_auth
    printf '%s\n' 'hcloud-context-resolves: PASS'
}

# Describes one resource read-only and projects it through the tracked identity
# projection; a null or mistyped field is a shape refusal, never a value.
project_hcloud_identity() {
    local kind=$1
    shift
    local response identity

    response=$(hfx_hcloud "$@" -o json) || hfx_die "read-only hcloud $kind description failed"
    if ! identity=$(jq -c --arg kind "$kind" -f "$HFX_IDENTITY_PROJECTION" <<<"$response" 2>&1); then
        printf 'hcloud-json-shape: FAIL (%s)\n' "$identity" >&2
        hfx_die "installed hcloud emits a $kind JSON shape that hcloud-identity.jq cannot project; refuse to provision"
    fi
    printf '%s\n' "$identity"
}

check_hcloud_json_shape() {
    local server server_type location

    hfx_require_command jq
    [[ -f "$HFX_IDENTITY_PROJECTION" && ! -L "$HFX_IDENTITY_PROJECTION" ]] ||
        hfx_die 'hcloud-identity.jq is missing beside this script'
    require_active_hcloud_context
    hfx_authenticate
    printf 'hcloud-version: %s\n' "$(command hcloud version 2>/dev/null | head -n 1)"

    server=$(project_hcloud_identity server server describe "$HFX_SHAPE_WITNESS_SERVER")
    jq -e --arg name "$HFX_SHAPE_WITNESS_SERVER" --arg location "$HFX_DEFAULT_LOCATION" \
        '.name == $name and .location == $location and (.id | . == floor)' <<<"$server" >/dev/null ||
        hfx_die "projected identity of $HFX_SHAPE_WITNESS_SERVER carries unexpected values"
    printf 'server-identity: %s\n' "$server"

    server_type=$(project_hcloud_identity server-type server-type describe "$HFX_DEFAULT_SERVER_TYPE")
    jq -e --arg name "$HFX_DEFAULT_SERVER_TYPE" --arg location "$HFX_DEFAULT_LOCATION" --argjson cores "$HFX_DEDICATED_CORE_QUOTA" \
        '.name == $name and .cores == $cores and .cpu_type == "dedicated" and .architecture == "x86" and (.locations | index($location) != null)' \
        <<<"$server_type" >/dev/null ||
        hfx_die "server type $HFX_DEFAULT_SERVER_TYPE is not a $HFX_DEDICATED_CORE_QUOTA-core dedicated x86 type offered in $HFX_DEFAULT_LOCATION"
    printf 'server-type-identity: %s\n' "$server_type"

    location=$(project_hcloud_identity location location describe "$HFX_DEFAULT_LOCATION")
    jq -e --arg name "$HFX_DEFAULT_LOCATION" '.name == $name' <<<"$location" >/dev/null ||
        hfx_die "location $HFX_DEFAULT_LOCATION did not describe itself"
    printf 'location-identity: %s\n' "$location"

    hfx_clear_auth
    printf '%s\n' 'hcloud-json-shape: PASS'
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
    hcloud-json-shape)
        [[ -z "$evidence_root" && -z "$s3_env_file" ]] ||
            hfx_die 'path options are not valid for this check'
        check_hcloud_json_shape
        ;;
    *) hfx_die "unknown check: $check" ;;
esac
