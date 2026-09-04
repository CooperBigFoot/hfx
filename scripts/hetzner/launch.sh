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
Usage:
  launch.sh --campaign <id> start --workload <name> -- <command> [argument ...]
  launch.sh --campaign <id> attach --workload <name>
  launch.sh --campaign <id> tail [--log <basename>]
  launch.sh --campaign <id> status --workload <name>

Options:
  --campaign <id>   required campaign identifier
  --workload <name> required for start, attach, and status
  --log <basename>  optional exact log basename for tail
  -h, --help        print usage and exit 0
USAGE
}

usage_error() {
    usage >&2
    hfx_die "$@"
}

identity_error() {
    local field=$1
    hfx_die "campaign server $server_name failed identity validation for $field; refusing launch; inspect it and rerun provision"
}

validate_workload() {
    local value=$1
    [[ "$value" =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]] || hfx_die "invalid workload '$value'; use 1-32 lowercase letters, digits, or hyphens, starting with a letter or digit"
}

[[ $# -gt 0 ]] || usage_error 'option --campaign is required'
if [[ "$1" == -h || "$1" == --help ]]; then
    (($# == 1)) || usage_error "$1 does not accept other arguments"
    usage
    exit 0
fi
[[ "$1" == --campaign ]] || usage_error 'option --campaign must precede the subcommand'
shift
[[ $# -gt 0 && -n "$1" && "$1" != -* ]] || usage_error 'option --campaign requires a value'
campaign=$1
shift
[[ $# -gt 0 ]] || usage_error 'one subcommand is required'
subcommand=$1
shift

workload=
log_basename=
command_argv=()
case $subcommand in
    start)
        [[ $# -gt 0 && "$1" == --workload ]] || usage_error 'start requires --workload <name>'
        shift
        [[ $# -gt 0 && -n "$1" && "$1" != -* ]] || usage_error 'option --workload requires a value'
        workload=$1
        shift
        [[ $# -gt 0 && "$1" == -- ]] || usage_error 'start requires -- before the command'
        shift
        [[ $# -gt 0 && -n "$1" ]] || usage_error 'start requires a nonempty command after --'
        command_argv=("$@")
        ;;
    attach | status)
        [[ $# -gt 0 && "$1" == --workload ]] || usage_error "$subcommand requires --workload <name>"
        shift
        [[ $# -gt 0 && -n "$1" && "$1" != -* ]] || usage_error 'option --workload requires a value'
        workload=$1
        shift
        (($# == 0)) || usage_error "$subcommand accepts no arguments after --workload <name>"
        ;;
    tail)
        if (($# > 0)); then
            [[ "$1" == --log ]] || usage_error 'tail accepts only --log <basename>'
            shift
            [[ $# -gt 0 && -n "$1" && "$1" != -* ]] || usage_error 'option --log requires a value'
            log_basename=$1
            shift
            (($# == 0)) || usage_error 'tail accepts at most one --log <basename> pair'
        fi
        ;;
    *)
        usage_error "unknown subcommand $subcommand"
        ;;
esac

hfx_validate_campaign "$campaign"
if [[ -n "$workload" ]]; then
    validate_workload "$workload"
fi
if [[ -n "$log_basename" ]]; then
    workload_pattern='[a-z0-9][a-z0-9-]{0,31}'
    timestamp_pattern='[0-9]{8}T[0-9]{6}Z'
    if [[ ! "$log_basename" =~ ^hfx-${campaign}-${workload_pattern}(-${timestamp_pattern})?\.log$ ]]; then
        hfx_die "invalid log basename '$log_basename'; name a canonical or timestamped log for campaign $campaign"
    fi
fi

hfx_require_command ssh
hfx_authenticate
trap hfx_clear_auth EXIT

server_name=$(hfx_server_name "$campaign")
server_id=$(hfx_exact_server_id "$server_name")
[[ -n "$server_id" ]] || hfx_die "campaign server $server_name is absent; run provision.sh --campaign $campaign with its required provisioning options, then rerun launch"

if ! description=$(hfx_hcloud server describe "$server_id" -o json); then
    identity_error description
fi
if ! description=$(jq -cse 'if length == 1 and (.[0] | type == "object") then .[0] else error("expected one object") end' <<<"$description" 2>/dev/null); then
    identity_error description
fi
if ! jq -e --argjson expected_id "$server_id" '.id | type == "number" and . == floor and . == $expected_id' <<<"$description" >/dev/null 2>&1; then
    identity_error ID
fi
if ! jq -e --arg expected_name "$server_name" '.name == $expected_name' <<<"$description" >/dev/null 2>&1; then
    identity_error name
fi
if ! jq -e '.labels | type == "object"' <<<"$description" >/dev/null 2>&1; then
    identity_error labels
fi
if ! jq -e --arg expected_value campaign '.labels["hfx-managed"] == $expected_value' <<<"$description" >/dev/null 2>&1; then
    identity_error 'label hfx-managed'
fi
if ! jq -e --arg expected_value "$campaign" '.labels["hfx-campaign"] == $expected_value' <<<"$description" >/dev/null 2>&1; then
    identity_error 'label hfx-campaign'
fi
if ! jq -e --arg expected_value server '.labels["hfx-role"] == $expected_value' <<<"$description" >/dev/null 2>&1; then
    identity_error 'label hfx-role'
fi
if ! ip_address=$(jq -er '.public_net.ipv4.ip | select(type == "string") | select((split(".") | length) == 4 and all(split(".")[]; test("^[0-9]{1,3}$") and (tonumber >= 0 and tonumber <= 255)))' <<<"$description" 2>/dev/null); then
    identity_error 'public IPv4 address'
fi

hfx_clear_auth
trap - EXIT

ssh_options=(-o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new)

run_remote() {
    local argument argument_q
    local quoted_arguments=()
    for argument in "$@"; do
        printf -v argument_q '%q' "$argument"
        quoted_arguments+=("$argument_q")
    done
    ssh "${ssh_options[@]}" "root@$ip_address" bash -s -- "${quoted_arguments[@]}"
}

case $subcommand in
    start)
        if ! run_remote "$campaign" "$workload" "${command_argv[@]}" <<'REMOTE_START'
set -Eeuo pipefail
IFS=$'\n\t'
set +x
[[ $# -ge 3 && -n "$3" ]] || { printf 'launch: invalid remote start arguments\n' >&2; exit 1; }
campaign=$1
workload=$2
shift 2
[[ "$campaign" =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]] || { printf 'launch: invalid remote campaign\n' >&2; exit 1; }
[[ "$workload" =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]] || { printf 'launch: invalid remote workload\n' >&2; exit 1; }
logs_directory=/mnt/hfx/logs
[[ -d "$logs_directory" && ! -L "$logs_directory" && "$(stat -c '%U:%G' -- "$logs_directory" 2>/dev/null)" == root:root ]] || { printf 'launch: /mnt/hfx/logs is not the root-owned bootstrap log directory; rerun bootstrap.sh\n' >&2; exit 1; }
command -v tmux >/dev/null 2>&1 || { printf 'launch: tmux is unavailable; rerun bootstrap.sh\n' >&2; exit 1; }
session=hfx-$campaign-$workload
canonical_log=$logs_directory/hfx-$campaign-$workload.log
if tmux has-session -t "=$session" 2>/dev/null; then
    printf 'launch: session %s already exists; attach it or wait for it to finish\n' "$session" >&2
    exit 1
fi
timestamp=$(date -u +%Y%m%dT%H%M%SZ)
run_log=$logs_directory/hfx-$campaign-$workload-$timestamp.log
[[ ! -e "$run_log" && ! -L "$run_log" ]] || { printf 'launch: timestamped run log %s already exists; rerun start\n' "$run_log" >&2; exit 1; }
canonical_log_existed=0
if [[ -e "$canonical_log" || -L "$canonical_log" ]]; then
    canonical_log_existed=1
    [[ -f "$canonical_log" && ! -L "$canonical_log" ]] || { printf 'launch: canonical log %s is not a regular file\n' "$canonical_log" >&2; exit 1; }
fi
install -o root -g root -m 0644 /dev/null "$canonical_log"
install -o root -g root -m 0644 /dev/null "$run_log"
runner='canonical_log=$1
run_log=$2
shift 2
exec > >(tee -a -- "$canonical_log" "$run_log") 2>&1
tee_pid=$!
printf "launch: started at %s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf "launch: command:"
printf " %q" "$@"
printf "\n"
set +e
"$@"
command_status=$?
set -e
printf "launch: finished at %s with exit %d\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$command_status"
exec >&- 2>&-
wait "$tee_pid"
exit "$command_status"'
if ! tmux new-session -d -s "$session" -- bash -c "$runner" bash "$canonical_log" "$run_log" "$@"; then
    if tmux has-session -t "=$session" 2>/dev/null; then
        failure_message="launch: session $session already exists; attach it or wait for it to finish"
    else
        failure_message="launch: failed to create session $session"
    fi
    ((canonical_log_existed == 1)) || { [[ -s "$canonical_log" ]] || rm -f -- "$canonical_log"; }
    [[ ! -s "$run_log" ]] && rm -f -- "$run_log"
    printf '%s\n' "$failure_message" >&2
    exit 1
fi
printf 'launch: session %s\n' "$session"
printf 'launch: canonical log %s\n' "$canonical_log"
printf 'launch: timestamped run log %s\n' "$run_log"
REMOTE_START
        then
            hfx_die "start failed for workload $workload on campaign server $server_name; inspect the remote diagnostic and rerun launch"
        fi
        ;;
    attach)
        session="hfx-$campaign-$workload"
        printf -v exact_session_q '%q' "=$session"
        if ! ssh -t "${ssh_options[@]}" "root@$ip_address" "tmux attach-session -t $exact_session_q"; then
            hfx_die "attach failed for session $session; verify bootstrap completed and the session is running"
        fi
        ;;
    tail)
        if ! run_remote "$campaign" "$log_basename" <<'REMOTE_TAIL'
set -Eeuo pipefail
IFS=$'\n\t'
set +x
[[ $# -eq 2 ]] || { printf 'launch: invalid remote tail arguments\n' >&2; exit 1; }
campaign=$1
basename=$2
[[ "$campaign" =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]] || { printf 'launch: invalid remote campaign\n' >&2; exit 1; }
logs_directory=/mnt/hfx/logs
[[ -d "$logs_directory" && ! -L "$logs_directory" && "$(stat -c '%U:%G' -- "$logs_directory" 2>/dev/null)" == root:root ]] || { printf 'launch: /mnt/hfx/logs is not the root-owned bootstrap log directory; rerun bootstrap.sh\n' >&2; exit 1; }
command -v tmux >/dev/null 2>&1 || { printf 'launch: tmux is unavailable; rerun bootstrap.sh\n' >&2; exit 1; }
if [[ -z "$basename" ]]; then
    newest=$(find "$logs_directory" -maxdepth 1 -type f ! -lname '*' -name "hfx-$campaign-*-????????T??????Z.log" -printf '%T@ %f\0' |
        sort -z -nr | {
            found=0
            while IFS= read -r -d '' candidate; do
                candidate=${candidate#* }
                if ((found == 0)) && [[ "$candidate" =~ ^hfx-${campaign}-[a-z0-9][a-z0-9-]{0,31}-[0-9]{8}T[0-9]{6}Z\.log$ ]]; then
                    printf '%s' "$candidate"
                    found=1
                fi
            done
        })
    [[ -n "$newest" ]] || { printf 'launch: no timestamped logs found for campaign %s\n' "$campaign" >&2; exit 1; }
    basename=$newest
else
    [[ "$basename" =~ ^hfx-${campaign}-[a-z0-9][a-z0-9-]{0,31}(-[0-9]{8}T[0-9]{6}Z)?\.log$ ]] || { printf 'launch: invalid remote log basename\n' >&2; exit 1; }
fi
log_path=$logs_directory/$basename
[[ -f "$log_path" && ! -L "$log_path" ]] || { printf 'launch: log %s is not a regular campaign log\n' "$basename" >&2; exit 1; }
tail -n 50 -f -- "$log_path"
REMOTE_TAIL
        then
            hfx_die "tail failed for campaign $campaign; inspect the remote diagnostic and rerun launch"
        fi
        ;;
    status)
        remote_status=0
        run_remote "$campaign" "$workload" <<'REMOTE_STATUS' || remote_status=$?
set -Eeuo pipefail
IFS=$'\n\t'
set +x
[[ $# -eq 2 ]] || { printf 'launch: invalid remote status arguments\n' >&2; exit 1; }
campaign=$1
workload=$2
[[ "$campaign" =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]] || { printf 'launch: invalid remote campaign\n' >&2; exit 1; }
[[ "$workload" =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]] || { printf 'launch: invalid remote workload\n' >&2; exit 1; }
logs_directory=/mnt/hfx/logs
[[ -d "$logs_directory" && ! -L "$logs_directory" && "$(stat -c '%U:%G' -- "$logs_directory" 2>/dev/null)" == root:root ]] || { printf 'launch: /mnt/hfx/logs is not the root-owned bootstrap log directory; rerun bootstrap.sh\n' >&2; exit 1; }
command -v tmux >/dev/null 2>&1 || { printf 'launch: tmux is unavailable; rerun bootstrap.sh\n' >&2; exit 1; }
session=hfx-$campaign-$workload
canonical_log=$logs_directory/hfx-$campaign-$workload.log
state=3
if tmux has-session -t "=$session" 2>/dev/null; then
    printf 'launch: session %s is running\n' "$session"
    state=0
else
    printf 'launch: session %s is not running\n' "$session"
fi
if [[ -f "$canonical_log" && ! -L "$canonical_log" ]]; then
    printf 'launch: recent log: %s\n' "$canonical_log"
    tail -n 20 -- "$canonical_log"
else
    printf 'launch: no canonical log exists\n'
fi
exit "$state"
REMOTE_STATUS
        case $remote_status in
            0) ;;
            3) exit 3 ;;
            *) hfx_die "status failed for workload $workload on campaign server $server_name; inspect the remote diagnostic and rerun launch" ;;
        esac
        ;;
esac
