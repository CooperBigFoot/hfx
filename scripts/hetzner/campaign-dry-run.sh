#!/usr/bin/env bash
# dry-run : (CommittedRunbook, RehearsalContract, Workstation) -> LifecycleResult | Refusal
#
# Executes the entire composed campaign driver locally, at zero cloud cost, on
# the tiny synthetic corpus: every fence of sections 4 to 16 runs for real, with
# the cloud and the VM replaced by shims. ssh, scp, and rsync remap the VM paths
# onto a scratch VM root and run `bash -s` scripts with the same argument
# joining the real ssh performs; hcloud answers from a state file seeded with
# the recorded fixtures; aws reads and writes a directory standing in for the
# bucket; curl answers the price preflight from a fixture; fallocate, mkswap,
# swapon, free, and /proc/meminfo keep a swap table so the converge fence's swap
# post-condition reads what the fence created; provision.sh and
# bootstrap.sh are replaced by stubs that lay out the VM root; launch.sh,
# teardown.sh, tmux, the campaign runner, the adapter, and hfx run unchanged.
# The dry run exercises the committed HEAD: it clones the repository, so
# uncommitted edits are not part of it.

set -Eeuo pipefail
IFS=$'\n\t'
set +x

SCRIPT_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/hetzner/common.sh
source "$SCRIPT_DIR/common.sh"
repo_root=$(cd -P -- "$SCRIPT_DIR/../.." && pwd)

usage() {
    cat <<'USAGE'
Usage: campaign-dry-run.sh [--work <absolute-dir>] [--record <absolute-json-path>] [--keep] [--lifecycles <n>]

Options:
  --work <dir>     scratch directory (default: a fresh mktemp directory)
  --record <path>  write the dry-run result record there when the run passes
  --keep           keep the scratch directory on success (it is always kept on failure)
  --lifecycles <n> run the composed driver n times in the same evidence root (default 1); each
                   later lifecycle gets a fresh shimmed VM and a cleared extension scratch prefix,
                   as a rerun after a failed rehearsal does, and every lifecycle must pass
  -h, --help       print usage and exit 0

Exit status: 0 when the composed driver ran every fence to lifecycle-result.json
with result passed and zero footprint; 1 otherwise.
USAGE
}

work=
record=
keep=0
lifecycles=1
while (($#)); do
    case $1 in
        -h | --help) usage; exit 0 ;;
        --work | --record | --lifecycles)
            (($# >= 2)) || hfx_die "option $1 requires a value"
            case $1 in
                --work) work=$2 ;;
                --record) record=$2 ;;
                --lifecycles) lifecycles=$2 ;;
            esac
            shift 2
            ;;
        --keep) keep=1; shift ;;
        *) hfx_die "unknown argument: $1" ;;
    esac
done
[[ -z "$work" || "$work" == /* ]] || hfx_die '--work must be an absolute path'
[[ -z "$record" || "$record" == /* ]] || hfx_die '--record must be an absolute path'
[[ "$lifecycles" =~ ^[1-9][0-9]*$ ]] || hfx_die '--lifecycles must be a positive integer'
for command in git jq uv tmux rsync ogrinfo gdal-config sha256sum python3; do hfx_require_command "$command"; done
hfx_binary=$repo_root/target/release/hfx
[[ -x "$hfx_binary" ]] || hfx_die "release hfx binary is missing at $hfx_binary; run cargo build --release -p hfx-cli"
[[ "$(rsync --version | sed -n 1p)" =~ ^rsync\ +version\ 3\.[1-9] ]] || hfx_die 'GNU rsync 3.1 or later must be first on PATH'
head_ref=$(git -C "$repo_root" rev-parse HEAD)

if [[ -z "$work" ]]; then
    work=$(mktemp -d "${TMPDIR:-/tmp}/hfx-campaign-dry-run.XXXXXX")
else
    mkdir -p -- "$work"
    [[ -z "$(find "$work" -mindepth 1 -maxdepth 1 -print -quit)" ]] || hfx_die "--work directory is not empty: $work"
fi
export DRY_WORK=$work
export DRY_VMROOT=$work/vm
export DRY_BUCKET=$work/bucket
export DRY_HCLOUD_STATE=$work/hcloud-state.json
export DRY_TMUX_SOCKET="hfx-dry-run-$(basename -- "$work" | tr -c 'A-Za-z0-9\n' '-')"
export DRY_VM_BIN=$work/vm-bin
export DRY_WS_BIN=$work/bin
export DRY_BARE=$work/bare.git
export DRY_LOG=$work/dry-run.log
export DRY_FIXTURES=$repo_root/scripts/hetzner/fixtures
# Fault injection for the dry-run test: a nonzero status makes the mkswap shim fail with it.
export DRY_MKSWAP_STATUS=${DRY_MKSWAP_STATUS:-0}
[[ "$DRY_MKSWAP_STATUS" =~ ^[0-9]+$ ]] || hfx_die 'DRY_MKSWAP_STATUS must be a non-negative integer'
mkdir -p "$DRY_VMROOT" "$DRY_BUCKET" "$DRY_VM_BIN" "$DRY_WS_BIN" "$work/evidence"
hfx_log "scratch: $work"

finish() {
    local status=$?
    tmux -L "$DRY_TMUX_SOCKET" kill-server >/dev/null 2>&1 || true
    if ((status == 0)) && ((keep == 0)); then
        rm -rf -- "$work"
    else
        hfx_log "scratch kept at $work"
    fi
}
trap finish EXIT

# ---------------------------------------------------------------- repository clones
hfx_log "cloning committed HEAD $head_ref"
git clone -q --bare -- "$repo_root" "$DRY_BARE"
git -C "$DRY_BARE" update-ref refs/heads/main "$head_ref"
git clone -q -- "$DRY_BARE" "$work/workstation"
git -C "$work/workstation" checkout -q --detach origin/main
[[ "$(git -C "$work/workstation" rev-parse HEAD)" == "$head_ref" ]] || hfx_die 'workstation clone is not at HEAD'
[[ "$(git -C "$work/workstation" rev-parse origin/main)" == "$head_ref" ]] || hfx_die 'workstation clone origin/main is not HEAD'
mkdir -p "$work/workstation/target/release"
cp -- "$hfx_binary" "$work/workstation/target/release/hfx"
venv_python=$(cd "$work/workstation" && uv run --frozen --project adapters/tdx-hydro python -c 'import sys; print(sys.executable)')
[[ -x "$venv_python" ]] || hfx_die 'adapter environment python is not executable'

# ---------------------------------------------------------------- VM root and VM-side shims
write_meminfo() {
    # SwapTotal follows the swap table the swapon shim maintains, so the fence 8 post-condition reads real state.
    local swap_kib=0 kib
    if [[ -f "$DRY_VMROOT/swap-table.txt" ]]; then
        while read -r _ kib; do swap_kib=$((swap_kib + kib)); done <"$DRY_VMROOT/swap-table.txt"
    fi
    printf 'MemTotal:        4000000 kB\nMemFree:         3500000 kB\nMemAvailable:    3600000 kB\nSwapTotal:      %8d kB\nSwapFree:       %8d kB\n' "$swap_kib" "$swap_kib" >"$DRY_VMROOT/proc/meminfo"
}
# A freshly provisioned VM: empty work and log trees, the adapter environment, no swap, no checkout.
lay_out_vm_root() {
    mkdir -p "$DRY_VMROOT/mnt/hfx/work/downloads" "$DRY_VMROOT/mnt/hfx/logs" "$DRY_VMROOT/root/.ssh" "$DRY_VMROOT/root/.cargo/bin" \
        "$DRY_VMROOT/opt" "$DRY_VMROOT/etc" "$DRY_VMROOT/proc"
    # The whole environment directory is linked so Python finds pyvenv.cfg beside the invoked path.
    ln -s "$(cd -P -- "$(dirname -- "$venv_python")/.." && pwd)" "$DRY_VMROOT/opt/hfx-geo"
    ln -s "$DRY_VM_BIN/cargo" "$DRY_VMROOT/root/.cargo/bin/cargo"
    write_meminfo
}
lay_out_vm_root

write_shim() {
    local path=$1
    cat >"$path"
    chmod +x "$path"
}

# Rewrites every absolute VM path that a fence names onto the scratch VM root.
cat >"$work/rewrite-paths.sed" <<EOF
s#/mnt/hfx#$DRY_VMROOT/mnt/hfx#g
s#/root/#$DRY_VMROOT/root/#g
s#/opt/hfx-geo#$DRY_VMROOT/opt/hfx-geo#g
s#/etc/pourpoint-hfx\.env#$DRY_VMROOT/etc/pourpoint-hfx.env#g
s#\([[:space:]]\)/swapfile#\1$DRY_VMROOT/swapfile#g
s#/proc/meminfo#$DRY_VMROOT/proc/meminfo#g
EOF

write_shim "$DRY_VM_BIN/stat" <<'SHIM'
#!/usr/bin/env bash
# GNU stat -c forms the fences and launch.sh use, mapped onto BSD stat.
if [[ ${1-} == -c ]]; then
    format=$2; shift 2
    [[ ${1-} == -- ]] && shift
    for path in "$@"; do
        case $format in
            '%s') /usr/bin/stat -f %z -- "$path" ;;
            '%U:%G %a') printf 'root:root %s\n' "$(/usr/bin/stat -f %Lp -- "$path")" ;;
            '%U:%G') printf 'root:root\n' ;;
            *) printf 'dry-run stat: unsupported format %s\n' "$format" >&2; exit 1 ;;
        esac
    done
    exit 0
fi
exec /usr/bin/stat "$@"
SHIM
write_shim "$DRY_VM_BIN/sha256sum" <<'SHIM'
#!/usr/bin/env bash
# GNU sha256sum reads a `-c` manifest from stdin; the macOS binary does not, so map onto shasum.
exec shasum -a 256 "$@"
SHIM
write_shim "$DRY_VM_BIN/df" <<'SHIM'
#!/usr/bin/env bash
# `df -B1 --output=avail PATH` on the VM; the scratch root reports a large free space.
printf 'Avail\n400000000000\n'
SHIM
write_shim "$DRY_VM_BIN/du" <<'SHIM'
#!/usr/bin/env bash
if [[ ${1-} == -sb ]]; then
    shift
    kilobytes=$(/usr/bin/du -sk "$@" | cut -f1)
    printf '%s\t%s\n' "$((kilobytes * 1024))" "$*"
    exit 0
fi
exec /usr/bin/du "$@"
SHIM
write_shim "$DRY_VM_BIN/install" <<'SHIM'
#!/usr/bin/env bash
# launch.sh installs root-owned logs; the scratch VM has no root, so drop the ownership options.
args=()
while (($#)); do
    case $1 in
        -o | -g) shift 2 ;;
        *) args+=("$1"); shift ;;
    esac
done
exec /usr/bin/install "${args[@]}"
SHIM
# Swap: fallocate creates a sparse file of the requested size, mkswap stamps a signature sidecar
# whose size follows the kernel's model (whole 4 KiB pages, one page for the header), swapon
# requires the signature and appends the file to the swap table that meminfo and free report.
write_shim "$DRY_VM_BIN/fallocate" <<'SHIM'
#!/usr/bin/env bash
set -Eeuo pipefail
[[ ${1-} == -l && ${2-} =~ ^[0-9]+$ && -n ${3-} ]] || { printf 'dry-run fallocate: unsupported: %s\n' "$*" >&2; exit 1; }
: >"$3"
dd if=/dev/null of="$3" bs=1 seek="$2" count=0 2>/dev/null
SHIM
write_shim "$DRY_VM_BIN/mkswap" <<'SHIM'
#!/usr/bin/env bash
set -Eeuo pipefail
if [[ "${DRY_MKSWAP_STATUS:-0}" != 0 ]]; then printf 'dry-run mkswap: injected failure on %s\n' "${1-}" >&2; exit "$DRY_MKSWAP_STATUS"; fi
path=${1-}
[[ -f "$path" ]] || { printf 'mkswap: cannot open %s: No such file or directory\n' "$path" >&2; exit 1; }
pages=$(( $(/usr/bin/stat -f %z -- "$path") / 4096 ))
((pages >= 10)) || { printf 'mkswap: error: swap area needs to be at least 40 KiB\n' >&2; exit 1; }
printf '%s\n' "$(( (pages - 1) * 4 ))" >"$path.dry-run-swap-kib"
printf 'Setting up swapspace version 1, size = %s KiB (dry run)\n' "$(( (pages - 1) * 4 ))"
SHIM
write_shim "$DRY_VM_BIN/swapon" <<'SHIM'
#!/usr/bin/env bash
set -Eeuo pipefail
table=$DRY_VMROOT/swap-table.txt
if [[ ${1-} == --show* ]]; then
    columns=; headings=1; unit=1024
    while (($#)); do
        case $1 in
            --show) ;;
            --show=*) columns=${1#--show=} ;;
            --noheadings) headings=0 ;;
            --bytes) unit=1024 ;;
            *) printf 'dry-run swapon: unsupported: %s\n' "$*" >&2; exit 1 ;;
        esac
        shift
    done
    [[ -f "$table" ]] || exit 0
    if [[ "$columns" == NAME ]]; then
        cut -d ' ' -f1 -- "$table"
    else
        ((headings == 0)) || printf 'NAME TYPE SIZE USED PRIO\n'
        priority=-2
        while read -r path kib; do printf '%s file %s 0 %s\n' "$path" "$((kib * unit))" "$priority"; priority=$((priority - 1)); done <"$table"
    fi
    exit 0
fi
path=${1-}
[[ -f "$path.dry-run-swap-kib" ]] || { printf 'swapon: %s: read swap header failed\n' "$path" >&2; exit 255; }
printf '%s %s\n' "$path" "$(cat "$path.dry-run-swap-kib")" >>"$table"
swap_kib=0
while read -r _ kib; do swap_kib=$((swap_kib + kib)); done <"$table"
printf 'MemTotal:        4000000 kB\nMemFree:         3500000 kB\nMemAvailable:    3600000 kB\nSwapTotal:      %8d kB\nSwapFree:       %8d kB\n' "$swap_kib" "$swap_kib" >"$DRY_VMROOT/proc/meminfo"
SHIM
write_shim "$DRY_VM_BIN/free" <<'SHIM'
#!/usr/bin/env bash
swap_kib=0
if [[ -f "$DRY_VMROOT/swap-table.txt" ]]; then while read -r _ kib; do swap_kib=$((swap_kib + kib)); done <"$DRY_VMROOT/swap-table.txt"; fi
printf '               total        used        free\nMem:      4000000000   400000000  3600000000\nSwap:    %11d           0 %11d\n' "$((swap_kib * 1024))" "$((swap_kib * 1024))"
SHIM
write_shim "$DRY_VM_BIN/dmesg" <<'SHIM'
#!/usr/bin/env bash
exit 0
SHIM
write_shim "$DRY_VM_BIN/cargo" <<'SHIM'
#!/usr/bin/env bash
# The release binary is placed by the bootstrap stub; the converge rebuild is a no-op here.
printf '    Finished `release` profile [optimized] target(s) (dry run)\n'
SHIM
write_shim "$DRY_VM_BIN/tmux" <<'SHIM'
#!/usr/bin/env bash
exec /opt/homebrew/bin/tmux -L "$DRY_TMUX_SOCKET" "$@"
SHIM
[[ -x /opt/homebrew/bin/tmux ]] || sed -i '' "s#/opt/homebrew/bin/tmux#$(command -v tmux)#" "$DRY_VM_BIN/tmux"

# The bucket stand-in: s3://pourpoint-hfx/<key> is $DRY_BUCKET/<key>.
write_shim "$DRY_VM_BIN/aws" <<'SHIM'
#!/usr/bin/env bash
set -Eeuo pipefail
bucket_root=${DRY_BUCKET:?}
if [[ ${1-} == --version ]]; then printf 'aws-cli/2.0.0 (dry run)\n'; exit 0; fi
[[ ${1-} == s3 ]] || { printf 'dry-run aws: unsupported: %s\n' "$*" >&2; exit 1; }
shift
verb=$1; shift
positional=()
recursive=0
while (($#)); do
    case $1 in
        --endpoint-url | --region) shift 2 ;;
        --only-show-errors) shift ;;
        --recursive) recursive=1; shift ;;
        *) positional+=("$1"); shift ;;
    esac
done
to_local() { local key=${1#s3://pourpoint-hfx/}; printf '%s/%s' "$bucket_root" "$key"; }
case $verb in
    ls)
        target=${positional[0]-s3://pourpoint-hfx/}
        local_target=$(to_local "$target")
        if ((recursive)); then
            prefix=${target#s3://pourpoint-hfx/}
            [[ -d "$local_target" ]] || exit 0
            (cd "$bucket_root" && find "${prefix%/}" -type f | LC_ALL=C sort | while IFS= read -r file; do
                printf '2026-09-04 22:00:00 %10d %s\n' "$(/usr/bin/stat -f %z -- "$file")" "$file"
            done)
        else
            [[ -e "$local_target" ]] || exit 0
            if [[ -d "$local_target" ]]; then
                (cd "$local_target" && for entry in *; do
                    [[ -e "$entry" ]] || continue
                    if [[ -d "$entry" ]]; then printf '                           PRE %s/\n' "$entry"; else printf '2026-09-04 22:00:00 %10d %s\n' "$(/usr/bin/stat -f %z -- "$entry")" "$entry"; fi
                done)
            else
                printf '2026-09-04 22:00:00 %10d %s\n' "$(/usr/bin/stat -f %z -- "$local_target")" "$(basename -- "$local_target")"
            fi
        fi
        ;;
    cp)
        source=${positional[0]}; destination=${positional[1]}
        if [[ "$source" == s3://* ]]; then source=$(to_local "$source"); fi
        if [[ "$destination" == s3://* ]]; then destination=$(to_local "$destination"); fi
        if [[ "$destination" == - ]]; then cat -- "$source"; exit 0; fi
        if ((recursive)); then
            mkdir -p -- "$destination"
            rsync -a -- "${source%/}/" "${destination%/}/"
        else
            mkdir -p -- "$(dirname -- "$destination")"
            cp -- "$source" "$destination"
        fi
        ;;
    *) printf 'dry-run aws: unsupported verb %s\n' "$verb" >&2; exit 1 ;;
esac
SHIM

# ---------------------------------------------------------------- workstation shims
write_shim "$DRY_WS_BIN/security" <<'SHIM'
#!/usr/bin/env bash
printf 'DRY-RUN-HCLOUD-TOKEN\n'
SHIM
write_shim "$DRY_WS_BIN/curl" <<'SHIM'
#!/usr/bin/env bash
cat >/dev/null
cat -- "$DRY_FIXTURES/pricing-fsn1.json"
SHIM
cp -- "$DRY_VM_BIN/aws" "$DRY_WS_BIN/aws"

# hcloud: a state file seeded with the recorded fixtures; describe, list, detach, delete, and create.
fixtures=$repo_root/scripts/hetzner/fixtures
jq -n --slurpfile web "$fixtures/hcloud/server-describe-pourpoint-web-1.json" '{servers: [$web[0]], volumes: [], next_id: 900000}' >"$DRY_HCLOUD_STATE"
write_shim "$DRY_WS_BIN/hcloud" <<'SHIM'
#!/usr/bin/env bash
set -Eeuo pipefail
state=${DRY_HCLOUD_STATE:?}
fixtures=${DRY_FIXTURES:?}
args=()
while (($#)); do
    case $1 in
        --context) shift 2 ;;
        -o) shift 2 ;;
        *) args+=("$1"); shift ;;
    esac
done
set -- "${args[@]}"
select_server() { jq -e --arg key "$1" '.servers[] | select(.name == $key or (.id | tostring) == $key)' "$state"; }
select_volume() { jq -e --arg key "$1" '.volumes[] | select(.name == $key or (.id | tostring) == $key)' "$state"; }
case "$1 $2" in
    'context active') printf 'pourpoint\n' ;;
    'server list') jq '.servers' "$state" ;;
    'volume list') jq '.volumes' "$state" ;;
    'server describe') select_server "$3" >/dev/null 2>&1 || { printf 'hcloud: server not found: %s\n' "$3" >&2; exit 1; }; select_server "$3" ;;
    'volume describe') select_volume "$3" >/dev/null 2>&1 || { printf 'hcloud: volume not found: %s\n' "$3" >&2; exit 1; }; select_volume "$3" ;;
    'server-type describe') cat -- "$fixtures/hcloud/server-type-describe-ccx33.json" ;;
    'location describe') cat -- "$fixtures/hcloud/location-describe-fsn1.json" ;;
    'volume detach')
        jq --arg key "$3" '(.volumes[] | select((.id | tostring) == $key) | .server) = null
            | (.servers[] | select(.volumes != null) | .volumes) |= map(select((. | tostring) != $key))' "$state" >"$state.tmp" && mv "$state.tmp" "$state" ;;
    'server delete') jq --arg key "$3" '.servers |= map(select((.id | tostring) != $key))' "$state" >"$state.tmp" && mv "$state.tmp" "$state" ;;
    'volume delete') jq --arg key "$3" '.volumes |= map(select((.id | tostring) != $key))' "$state" >"$state.tmp" && mv "$state.tmp" "$state" ;;
    *) printf 'dry-run hcloud: unsupported: %s\n' "$*" >&2; exit 1 ;;
esac
SHIM

# ssh: joins the remote command into one string, re-splits it through bash -c, remaps the VM paths in
# the command and in the script on stdin, and runs it in the scratch VM with the VM-side shims first.
write_shim "$DRY_WS_BIN/ssh" <<'SHIM'
#!/usr/bin/env bash
set -Eeuo pipefail
while (($#)); do
    case $1 in
        -o) shift 2 ;;
        -*) shift ;;
        *) break ;;
    esac
done
shift   # root@host
joined="$*"
command=$(printf '%s' "$joined" | sed -f "$DRY_WORK/rewrite-paths.sed")
script=""
if [[ ! -t 0 ]]; then script=$(cat | sed -f "$DRY_WORK/rewrite-paths.sed"); fi
export PATH="$DRY_VM_BIN:$PATH"
export HOME="$DRY_VMROOT/root"
export HFX_TDX_ADAPTER_PYTHON="$DRY_VMROOT/opt/hfx-geo/bin/python"
export HFX_TDX_HFX="$DRY_VMROOT/root/hfx/target/release/hfx"
cd "$DRY_VMROOT/root"
if [[ -z "$command" ]]; then exit 0; fi
printf '%s\n' "$script" | bash -c "$command"
SHIM
write_shim "$DRY_WS_BIN/scp" <<'SHIM'
#!/usr/bin/env bash
set -Eeuo pipefail
paths=()
while (($#)); do
    case $1 in
        -o) shift 2 ;;
        -*) shift ;;
        *) paths+=("$1"); shift ;;
    esac
done
map() { if [[ "$1" == root@*:* ]]; then printf '%s' "${1#*:}" | sed -f "$DRY_WORK/rewrite-paths.sed"; else printf '%s' "$1"; fi; }
destination=$(map "${paths[${#paths[@]}-1]}")
unset 'paths[${#paths[@]}-1]'
for source in "${paths[@]}"; do
    mapped=$(map "$source")
    for expanded in $mapped; do cp -- "$expanded" "$destination"; done
done
SHIM
write_shim "$DRY_WS_BIN/rsync" <<'SHIM'
#!/usr/bin/env bash
set -Eeuo pipefail
args=()
if [[ ${1-} == --version ]]; then exec /opt/homebrew/bin/rsync "$@"; fi
while (($#)); do
    case $1 in
        -e) shift 2 ;;
        root@*:*) args+=("$(printf '%s' "${1#*:}" | sed -f "$DRY_WORK/rewrite-paths.sed")"); shift ;;
        *) args+=("$1"); shift ;;
    esac
done
exec /opt/homebrew/bin/rsync "${args[@]}"
SHIM
[[ -x /opt/homebrew/bin/rsync ]] || sed -i '' "s#/opt/homebrew/bin/rsync#$(command -v rsync)#g" "$DRY_WS_BIN/rsync"

# ---------------------------------------------------------------- provision and bootstrap stubs
cat >"$work/workstation/scripts/hetzner/provision.sh" <<'STUB'
#!/usr/bin/env bash
# dry-run stand-in: records the campaign server and volume in the hcloud state and lays out the VM.
set -Eeuo pipefail
campaign=; s3_env_file=; server_type=; volume_size=; location=
while (($#)); do
    case $1 in
        --campaign) campaign=$2; shift 2 ;;
        --s3-env-file) s3_env_file=$2; shift 2 ;;
        --server-type) server_type=$2; shift 2 ;;
        --volume-size-gb) volume_size=$2; shift 2 ;;
        --location) location=$2; shift 2 ;;
        *) shift ;;
    esac
done
state=${DRY_HCLOUD_STATE:?}
printf 'hfx: authenticated for Hetzner Cloud context pourpoint\n' >&2
if ! jq -e --arg name "hfx-build-$campaign" '.servers[] | select(.name == $name)' "$state" >/dev/null; then
    printf 'hfx: creating server hfx-build-%s\n' "$campaign" >&2
    jq --arg campaign "$campaign" --arg type "$server_type" --arg location "$location" --argjson size "$volume_size" '
        .next_id as $server_id | (.next_id + 1) as $volume_id
        | .servers += [{id: $server_id, name: ("hfx-build-" + $campaign), status: "running",
            server_type: {name: $type}, location: {name: $location}, datacenter: null, image: {name: "debian-12"},
            public_net: {ipv4: {ip: "127.0.0.1"}}, volumes: [$volume_id],
            labels: {"hfx-managed": "campaign", "hfx-campaign": $campaign, "hfx-role": "server", "hfx-location": $location, "hfx-server-type": $type, "hfx-image": "debian-12", "hfx-ssh-key": "nicolas-workstation"}}]
        | .volumes += [{id: $volume_id, name: ("hfx-build-" + $campaign + "-data"), size: $size, location: {name: $location}, server: $server_id, linux_device: "/dev/sdb",
            labels: {"hfx-managed": "campaign", "hfx-campaign": $campaign, "hfx-role": "data-volume", "hfx-location": $location}}]
        | .next_id += 2' "$state" >"$state.tmp" && mv "$state.tmp" "$state"
    printf 'hfx: creating volume hfx-build-%s-data\n' "$campaign" >&2
fi
mkdir -p "$DRY_VMROOT/mnt/hfx/work" "$DRY_VMROOT/mnt/hfx/logs" "$DRY_VMROOT/etc"
install -m 600 -- "$s3_env_file" "$DRY_VMROOT/etc/pourpoint-hfx.env"
printf 'hfx: campaign: %s\nhfx: server: hfx-build-%s\nhfx: volume: hfx-build-%s-data\nhfx: server IPv4: 127.0.0.1\nhfx: volume size GB: %s\nhfx: mount point: /mnt/hfx\nhfx: credential path: /etc/pourpoint-hfx.env\n' "$campaign" "$campaign" "$campaign" "$volume_size" >&2
STUB
cat >"$work/workstation/scripts/hetzner/bootstrap.sh" <<'STUB'
#!/usr/bin/env bash
# dry-run stand-in: converges the VM checkout from the bare repository and places the release binary.
set -Eeuo pipefail
campaign=
while (($#)); do case $1 in --campaign) campaign=$2; shift 2 ;; *) shift ;; esac; done
mkdir -p "$DRY_VMROOT/mnt/hfx/work/downloads" "$DRY_VMROOT/mnt/hfx/logs"
if [[ ! -e "$DRY_VMROOT/root/hfx" ]]; then
    git clone -q -- "$DRY_BARE" "$DRY_VMROOT/root/hfx"
    git -C "$DRY_VMROOT/root/hfx" checkout -q --detach origin/main
fi
mkdir -p "$DRY_VMROOT/root/hfx/target/release"
cp -- "$DRY_WORK/workstation/target/release/hfx" "$DRY_VMROOT/root/hfx/target/release/hfx"
printf 'bootstrap: campaign %s bootstrap complete\n' "$campaign"
STUB
chmod +x "$work/workstation/scripts/hetzner/provision.sh" "$work/workstation/scripts/hetzner/bootstrap.sh"

# ---------------------------------------------------------------- rehearsal evidence root and bucket
printf 'DRY-RUN-APPROVAL-FIXTURE; not a maintainer record\n' >"$work/evidence/provisioner-transfer-approval.txt"
printf 'AWS_ACCESS_KEY_ID=DRYRUNACCESSKEY\nAWS_SECRET_ACCESS_KEY=DRYRUNSECRETKEY\n' >"$work/s3.env"
chmod 600 "$work/s3.env"
hfx_log 'preparing the rehearsal inputs from the synthetic corpus'
(cd "$work/workstation" && bash scripts/hetzner/prepare-rehearsal-campaign.sh --evidence-root "$work/evidence" --s3-env-file "$work/s3.env" --skip-upload) >"$work/prepare.log" 2>&1 ||
    { cat "$work/prepare.log" >&2; hfx_die 'rehearsal preparation failed'; }
baseline_prefix=$(jq -r '.baseline.prefix' "$work/evidence/rehearsal-campaign-contract.resolved.json")
baseline_key=${baseline_prefix#s3://pourpoint-hfx/}
mkdir -p "$DRY_BUCKET/${baseline_key%/}" "$DRY_BUCKET/${baseline_key%dataset/}evidence/state"
cp -R -- "$work/evidence/off-vm/baseline/dataset/." "$DRY_BUCKET/${baseline_key%/}/"
cp -- "$work/evidence/off-vm/baseline/evidence-assembly.json" "$DRY_BUCKET/${baseline_key%dataset/}evidence/state/assembly.json"
# The dry run is itself the precondition it would otherwise have to prove.
jq '.requires_passing_dry_run = false' "$work/evidence/rehearsal-campaign-contract.resolved.json" >"$work/evidence/dry-run-contract.json"

# ---------------------------------------------------------------- compose and run the driver
hfx_log 'composing the full driver'
(cd "$work/workstation" && python3 scripts/hetzner/compose-campaign-driver.py --mode full --out "$work/evidence/composed") >/dev/null
[[ "$(grep -c ': IDENTICAL$' "$work/evidence/composed/fence-diff-proof.txt")" == 25 ]] || hfx_die 'composed driver proof is not 25 identical fences'
evidence=$work/evidence/campaign-rehearsal
extension_key=$(jq -r '.extension_scratch_prefix' "$work/evidence/dry-run-contract.json")
baseline_top=${baseline_key#"$extension_key"}; baseline_top=${baseline_top%%/*}
[[ -n "$baseline_top" && "$baseline_key" == "$extension_key"* ]] || hfx_die 'the rehearsal baseline prefix is not under the extension scratch prefix'

# require_passing_lifecycle : () -> () | Refusal; the checks every lifecycle must satisfy.
require_passing_lifecycle() {
    local milestone
    [[ -f "$evidence/lifecycle-result.json" ]] || hfx_die 'lifecycle-result.json was not written'
    jq -e '.result == "passed" and .strict_validation == "passed" and .zero_footprint == true' "$evidence/lifecycle-result.json" >/dev/null ||
        hfx_die "lifecycle result is not a pass: $(jq -c . "$evidence/lifecycle-result.json")"
    grep -qF 'has zero Hetzner footprint' "$evidence/teardown.log" || hfx_die 'teardown log lacks the zero-footprint line'
    [[ "$(jq '.servers | map(select(.name != "pourpoint-web-1")) | length' "$DRY_HCLOUD_STATE")" == 0 ]] || hfx_die 'a campaign server remains in the hcloud state'
    [[ "$(jq '.volumes | length' "$DRY_HCLOUD_STATE")" == 0 ]] || hfx_die 'a campaign volume remains in the hcloud state'
    for milestone in 01-preflight-passed 02-provisioned 03-corpus-verified-on-vm 04-control-gates-passed 05-compiles-done 06-assembly-done 07-preserved 08-validation-classified 09-torn-down; do
        [[ -f "$evidence/milestones/$milestone" ]] || hfx_die "milestone $milestone was not reached"
    done
    [[ ! -e "$evidence/gate-transport-failures.log" ]] ||
        hfx_die "gate transport failures were recorded although the price shim never fails: $(tr '\n' ' ' <"$evidence/gate-transport-failures.log")"
    [[ -s "$evidence/observed-swap-total-bytes.txt" ]] || hfx_die 'the converge fence did not preserve observed-swap-total-bytes.txt'
    [[ -d "$evidence/control-reference" ]] || hfx_die 'the VM-built reference control was not preserved under the campaign evidence directory'
    [[ ! -e "$work/evidence/off-vm/control-builds" ]] || hfx_die 'the reference control was written into the shared off-vm inputs'
}

for lifecycle in $(seq 1 "$lifecycles"); do
    if ((lifecycle > 1)); then
        # A rerun after a failed rehearsal provisions a fresh VM and the maintainer clears the extension
        # scratch prefix by hand; the baseline prefix under it is read-only and stays.
        hfx_log "lifecycle $lifecycle: fresh shimmed VM; clearing the extension scratch prefix except the baseline"
        # The pulled baseline is carried onto the fresh VM root, as a resume at the baseline stage finds it on the
        # volume: the idempotent pull of fence 18 must keep every object and copy none.
        carried_baseline=$work/carried-baseline
        rm -rf -- "$carried_baseline"
        mv -- "$DRY_VMROOT/mnt/hfx/work/baseline" "$carried_baseline"
        rm -rf -- "$DRY_VMROOT"
        lay_out_vm_root
        mkdir -p -- "$DRY_VMROOT/mnt/hfx/work"
        mv -- "$carried_baseline" "$DRY_VMROOT/mnt/hfx/work/baseline"
        baseline_manifest_before=$(/usr/bin/stat -f '%m %i' -- "$DRY_VMROOT/mnt/hfx/work/baseline/dataset/manifest.json")
        for entry in "$DRY_BUCKET/${extension_key%/}"/*; do
            [[ -e "$entry" ]] || continue
            [[ "$(basename -- "$entry")" == "$baseline_top" ]] || rm -rf -- "$entry"
        done
        DRY_LOG=$work/dry-run-lifecycle-$lifecycle.log
    fi
    hfx_log "lifecycle $lifecycle of $lifecycles: running the composed driver end to end (every fence, shimmed cloud and VM)"
    driver_status=0
    (
        cd "$work/workstation"
        export PATH="$DRY_WS_BIN:$PATH"
        export HFX_CAMPAIGN_EVIDENCE=$work/evidence
        export HFX_CAMPAIGN_CONTRACT=$work/evidence/dry-run-contract.json
        export POLL_SECONDS=${DRY_POLL_SECONDS:-3}
        printf '%s\n' "$work/s3.env" | bash "$work/evidence/composed/campaign-driver.sh"
    ) >"$DRY_LOG" 2>&1 || driver_status=$?
    if ((driver_status != 0)); then
        printf '%s\n' '--- last driver lines ---' >&2
        tail -n 40 "$DRY_LOG" >&2
        hfx_die "lifecycle $lifecycle: composed driver exited $driver_status; log at $DRY_LOG"
    fi
    require_passing_lifecycle
    baseline_object_count=$(jq -r '.baseline.object_count' "$work/evidence/dry-run-contract.json")
    if ((lifecycle > 1)); then
        grep -qx "baseline_objects_kept=$baseline_object_count baseline_objects_pulled=0" "$evidence/baseline-pull.log" ||
            hfx_die "lifecycle $lifecycle: the baseline pull did not keep every already-pulled object: $(grep 'baseline_objects_' "$evidence/baseline-pull.log")"
        [[ "$(/usr/bin/stat -f '%m %i' -- "$DRY_VMROOT/mnt/hfx/work/baseline/dataset/manifest.json")" == "$baseline_manifest_before" ]] ||
            hfx_die "lifecycle $lifecycle: the baseline pull re-copied an object that was already on the volume"
    else
        grep -qx "baseline_objects_kept=0 baseline_objects_pulled=$baseline_object_count" "$evidence/baseline-pull.log" ||
            hfx_die "lifecycle $lifecycle: the baseline pull did not pull every object onto the empty volume: $(grep 'baseline_objects_' "$evidence/baseline-pull.log")"
    fi
    hfx_log "lifecycle $lifecycle passed: $(jq -c '{campaign, strict_validation, result}' "$evidence/lifecycle-result.json")"
done
if ((lifecycles > 1)); then
    [[ "$(find "$work/evidence" -mindepth 1 -maxdepth 1 -type d -name 'campaign-rehearsal-superseded-*' | wc -l | tr -d ' ')" == "$((lifecycles - 1))" ]] ||
        hfx_die 'each earlier lifecycle must have been superseded in place'
fi
hfx_log "dry run passed: $(jq -c '{campaign, strict_validation, result}' "$evidence/lifecycle-result.json")"
hfx_log "control gates: $(jq -c '.control_gates' "$evidence/lifecycle-result.json")"
hfx_log "compiled basins: $(tr '\n' ' ' <"$evidence/compiled-absent-basins.txt")"
if [[ -n "$record" ]]; then
    mkdir -p -- "$(dirname -- "$record")"
    jq -n --arg ref "$head_ref" --arg finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --slurpfile result "$evidence/lifecycle-result.json" \
        '{schema_version: 1, kind: "campaign-dry-run", ground_truth_ref: $ref, finished_at: $finished, result: "passed", lifecycle_result: $result[0]}' >"$record"
    hfx_log "record: $record"
fi
