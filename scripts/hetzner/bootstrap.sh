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

# Frozen boundary: bootstrap does not touch /etc/pourpoint-hfx.env.

usage() {
    cat <<'USAGE'
Usage: bootstrap.sh --campaign <id>

Options:
  --campaign <id>  required
  -h, --help       print usage and exit 0
USAGE
}

usage_error() {
    usage >&2
    hfx_die "$@"
}

identity_error() {
    local field=$1
    hfx_die "campaign server $server_name failed identity validation for $field; refusing bootstrap; inspect it and rerun provision"
}

campaign=
campaign_seen=0
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
        -*)
            usage_error "unknown option $1"
            ;;
        *)
            usage_error 'positional arguments are not accepted; use --campaign <id>'
            ;;
    esac
    shift
done

((campaign_seen == 1)) || usage_error 'option --campaign is required'
hfx_validate_campaign "$campaign"
hfx_require_command ssh

hfx_authenticate
trap hfx_clear_auth EXIT

server_name=$(hfx_server_name "$campaign")
server_id=$(hfx_exact_server_id "$server_name")
[[ -n "$server_id" ]] || hfx_die "campaign server $server_name is absent; run provision.sh --campaign $campaign with its required provisioning options, then rerun bootstrap"

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

printf -v campaign_q '%q' "$campaign"
ssh_options=(-o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new)
if ! ssh "${ssh_options[@]}" "root@$ip_address" "bash -s -- $campaign_q" <<'REMOTE_BOOTSTRAP'
set -Eeuo pipefail
IFS=$'\n\t'
set +x
[[ $# -eq 1 ]] || { printf 'bootstrap: invalid remote arguments\n' >&2; exit 1; }
campaign=$1

# FROZEN REMOTE CONTRACT FOR M3-S2
# mount point:             /mnt/hfx
# workspace root:          /mnt/hfx/work
# download directory:      /mnt/hfx/work/downloads
# logs directory:          /mnt/hfx/logs
# source checkout:         /root/hfx
# built CLI:               /root/hfx/target/release/hfx
# Python geo environment:  /opt/hfx-geo
# uv executable:           /usr/local/bin/uv
# rustup executable:       /root/.cargo/bin/rustup
# cargo executable:        /root/.cargo/bin/cargo
# repository URL:          https://github.com/CooperBigFoot/hfx.git
# repository branch:       origin/main
# tmux session format:     hfx-<campaign>-<workload>
# M3-S2 smoke session:     hfx-<campaign>-smoke
# log format:              /mnt/hfx/logs/hfx-<campaign>-<workload>.log
# M3-S2 smoke log:         /mnt/hfx/logs/hfx-<campaign>-smoke.log
# Generic workloads use 1-32 lowercase letters, digits, or hyphens, beginning
# with a letter or digit. M3-S2 uses the literal workload "smoke".

readonly MOUNT_POINT=/mnt/hfx
readonly WORKSPACE_ROOT=/mnt/hfx/work
readonly DOWNLOAD_DIRECTORY=/mnt/hfx/work/downloads
readonly LOGS_DIRECTORY=/mnt/hfx/logs
readonly BUILT_CLI=/root/hfx/target/release/hfx
readonly UV_EXECUTABLE=/usr/local/bin/uv
readonly RUSTUP_EXECUTABLE=/root/.cargo/bin/rustup
readonly CARGO_EXECUTABLE=/root/.cargo/bin/cargo
readonly UV_VERSION=0.8.22
readonly UV_INSTALLER_URL=https://astral.sh/uv/0.8.22/install.sh
readonly PYTHON_VERSION=3.12.11
readonly RUSTUP_VERSION=1.28.2
readonly RUSTUP_INIT_URL=https://static.rust-lang.org/rustup/archive/1.28.2/x86_64-unknown-linux-gnu/rustup-init
readonly RUST_TOOLCHAIN=1.88.0
readonly HFX_REPOSITORY_URL=https://github.com/CooperBigFoot/hfx.git
readonly HFX_REPOSITORY_BRANCH=main
readonly HFX_REPOSITORY_DIR=/root/hfx
readonly HFX_GEO_VENV=/opt/hfx-geo

bootstrap_log() {
    printf 'bootstrap: %s\n' "$*"
}

bootstrap_die() {
    printf 'bootstrap: %s\n' "$*" >&2
    exit 1
}

verify_mount() {
    local target source options
    target=$(findmnt -rn -M "$MOUNT_POINT" -o TARGET 2>/dev/null) || return 1
    source=$(findmnt -rn -M "$MOUNT_POINT" -o SOURCE 2>/dev/null) || return 1
    options=$(findmnt -rn -M "$MOUNT_POINT" -o OPTIONS 2>/dev/null) || return 1
    [[ "$target" == "$MOUNT_POINT" && "$source" == /dev/* && -b "$source" ]] || return 1
    [[ ",$options," == *,rw,* ]] || return 1
}

verify_mount || bootstrap_die '/mnt/hfx is not a mounted read-write campaign volume; run provision.sh to completion, inspect the attachment, and rerun bootstrap'
install -d -o root -g root -m 0755 "$WORKSPACE_ROOT" "$DOWNLOAD_DIRECTORY" "$LOGS_DIRECTORY" || bootstrap_die 'failed to create campaign volume directories; inspect the mount and rerun bootstrap'

temporary_directory=$(mktemp -d) || bootstrap_die 'failed to create a private installer directory; inspect temporary storage and rerun bootstrap'
trap 'rm -rf -- "$temporary_directory"' EXIT

retry_apt() {
    local attempt
    for ((attempt = 1; attempt <= 3; attempt++)); do
        bootstrap_log "apt attempt $attempt of 3"
        if DEBIAN_FRONTEND=noninteractive "$@"; then
            return 0
        fi
        ((attempt < 3)) && sleep 10
    done
    return 1
}

campaign_command_names=(
    aws
    jq
    mv
    mkdir
    rm
    chmod
    find
    wc
    tr
    ps
    curl
    sha256sum
    od
    ogrinfo
    sort
    grep
)

apt_packages=(
    awscli
    build-essential
    ca-certificates
    clang
    curl
    gdal-bin
    git
    jq
    libclang-dev
    libgdal-dev
    libgeos-dev
    libproj-dev
    libsqlite3-dev
    pkg-config
    procps
    proj-bin
    proj-data
    tmux
)
missing_packages=()
for package in "${apt_packages[@]}"; do
    status=$(dpkg-query -W -f='${Status}' -- "$package" 2>/dev/null || true)
    [[ "$status" == 'install ok installed' ]] || missing_packages+=("$package")
done
if ((${#missing_packages[@]} == 0)); then
    bootstrap_log 'all apt packages are already installed; skipping apt update and install'
else
    bootstrap_log "installing ${#missing_packages[@]} missing apt packages"
    retry_apt apt-get -o Acquire::Retries=5 -o DPkg::Lock::Timeout=120 update || bootstrap_die 'apt update failed after three attempts; inspect Debian repository access and rerun bootstrap'
    retry_apt apt-get -o Acquire::Retries=5 -o DPkg::Lock::Timeout=120 install -y --no-install-recommends "${missing_packages[@]}" || bootstrap_die 'apt install failed after three attempts; inspect package manager output and rerun bootstrap'
fi
for package in "${apt_packages[@]}"; do
    status=$(dpkg-query -W -f='${Status}' -- "$package" 2>/dev/null || true)
    [[ "$status" == 'install ok installed' ]] || bootstrap_die "required apt package $package is not installed; inspect package manager output and rerun bootstrap"
done
for command_name in "${campaign_command_names[@]}" tmux git gdal-config clang pkg-config; do
    command -v -- "$command_name" >/dev/null 2>&1 || bootstrap_die "required command $command_name is unavailable after package installation; inspect packages and rerun bootstrap"
done

if [[ -x "$UV_EXECUTABLE" ]] && [[ "$($UV_EXECUTABLE --version | head -n 1)" == "uv $UV_VERSION" ]]; then
    bootstrap_log "uv $UV_VERSION is already installed"
else
    uv_installer=$temporary_directory/uv-installer.sh
    bootstrap_log "installing uv $UV_VERSION"
    curl --fail --show-error --silent --location --retry 5 --retry-delay 2 --retry-all-errors "$UV_INSTALLER_URL" -o "$uv_installer" || bootstrap_die 'uv installer download failed; inspect network access and rerun bootstrap'
    env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh "$uv_installer" || bootstrap_die 'uv installation failed; inspect installer output and rerun bootstrap'
fi
[[ -x "$UV_EXECUTABLE" ]] && [[ "$($UV_EXECUTABLE --version | head -n 1)" == "uv $UV_VERSION" ]] || bootstrap_die "uv version verification failed; expected uv $UV_VERSION at $UV_EXECUTABLE"

if ! managed_python=$(/usr/local/bin/uv python find 3.12.11 2>/dev/null); then
    bootstrap_log "installing Python $PYTHON_VERSION"
    /usr/local/bin/uv python install 3.12.11 || bootstrap_die 'managed Python installation failed; inspect uv output and rerun bootstrap'
    managed_python=$(/usr/local/bin/uv python find 3.12.11) || bootstrap_die 'managed Python was not found after installation; inspect uv state and rerun bootstrap'
else
    bootstrap_log "Python $PYTHON_VERSION is already managed by uv"
fi
[[ -x "$managed_python" ]] || bootstrap_die 'uv returned a non-executable managed Python path; inspect uv state and rerun bootstrap'
[[ "$($managed_python -c 'import platform; print(platform.python_version())')" == "$PYTHON_VERSION" ]] || bootstrap_die "managed Python version verification failed; expected $PYTHON_VERSION"

if [[ ! -e "$HFX_GEO_VENV" ]]; then
    bootstrap_log "creating geo environment $HFX_GEO_VENV"
    /usr/local/bin/uv venv --python 3.12.11 /opt/hfx-geo || bootstrap_die 'geo environment creation failed; inspect uv output and rerun bootstrap'
elif [[ ! -x "$HFX_GEO_VENV/bin/python" ]]; then
    bootstrap_die "$HFX_GEO_VENV exists without an executable bin/python; inspect and remove this bootstrap-owned path before rerunning"
elif [[ "$($HFX_GEO_VENV/bin/python -c 'import platform; print(platform.python_version())')" != "$PYTHON_VERSION" ]]; then
    bootstrap_die "$HFX_GEO_VENV uses the wrong Python version; inspect and remove this bootstrap-owned path before rerunning"
fi
[[ -x "$HFX_GEO_VENV/bin/python" ]] || bootstrap_die 'geo environment Python is not executable after creation; inspect uv output and rerun bootstrap'

verify_geo_packages() {
    "$HFX_GEO_VENV/bin/python" <<'PYTHON_VERIFY'
import importlib
import importlib.metadata

packages = {
    "geopandas": ("geopandas", "1.1.3"),
    "geoparquet-io": ("geoparquet_io", "1.0.0b2"),
    "numpy": ("numpy", "2.4.6"),
    "pandas": ("pandas", "3.0.3"),
    "polars": ("polars", "1.41.1"),
    "pyarrow": ("pyarrow", "22.0.0"),
    "pyogrio": ("pyogrio", "0.12.1"),
    "rasterio": ("rasterio", "1.5.0"),
    "rio-cogeo": ("rio_cogeo", "7.0.2"),
    "shapely": ("shapely", "2.1.2"),
}
modules = {}
for distribution, (module_name, expected_version) in packages.items():
    modules[module_name] = importlib.import_module(module_name)
    actual_version = importlib.metadata.version(distribution)
    if actual_version != expected_version:
        raise SystemExit(
            f"{distribution} version mismatch: expected {expected_version}, got {actual_version}"
        )
print(f"bootstrap: pyogrio GDAL {modules['pyogrio'].__gdal_version__}")
print(f"bootstrap: rasterio GDAL {modules['rasterio'].__gdal_version__}")
PYTHON_VERIFY
}

if verify_geo_packages; then
    bootstrap_log 'exact geo package set is already installed'
else
    bootstrap_log 'installing exact geo package set'
    /usr/local/bin/uv pip install --python /opt/hfx-geo/bin/python \
        geopandas==1.1.3 geoparquet-io==1.0.0b2 numpy==2.4.6 pandas==3.0.3 \
        polars==1.41.1 pyarrow==22.0.0 pyogrio==0.12.1 rasterio==1.5.0 \
        rio-cogeo==7.0.2 shapely==2.1.2 || bootstrap_die 'geo package installation failed; inspect uv output and rerun bootstrap'
    verify_geo_packages || bootstrap_die 'geo package verification failed after installation; inspect package output and rerun bootstrap'
fi

[[ "$(uname -m)" == x86_64 ]] || bootstrap_die 'unexpected machine architecture; this bootstrap requires the provisioned x86_64 Debian 12 server'
if [[ -x "$RUSTUP_EXECUTABLE" ]] && [[ "$($RUSTUP_EXECUTABLE --version | head -n 1)" == "rustup $RUSTUP_VERSION "* ]]; then
    bootstrap_log "rustup $RUSTUP_VERSION is already installed"
else
    rustup_init=$temporary_directory/rustup-init
    bootstrap_log "installing rustup $RUSTUP_VERSION"
    curl --fail --show-error --silent --location --retry 5 --retry-delay 2 --retry-all-errors "$RUSTUP_INIT_URL" -o "$rustup_init" || bootstrap_die 'rustup-init download failed; inspect network access and rerun bootstrap'
    chmod 0700 "$rustup_init" || bootstrap_die 'failed to make rustup-init executable; inspect temporary storage and rerun bootstrap'
    "$rustup_init" -y --no-modify-path --profile minimal --default-toolchain "$RUST_TOOLCHAIN" || bootstrap_die 'rustup installation failed; inspect installer output and rerun bootstrap'
fi
[[ -x "$RUSTUP_EXECUTABLE" ]] && [[ "$($RUSTUP_EXECUTABLE --version | head -n 1)" == "rustup $RUSTUP_VERSION "* ]] || bootstrap_die "rustup version verification failed; expected $RUSTUP_VERSION"

if ! $RUSTUP_EXECUTABLE toolchain list | awk -v version="$RUST_TOOLCHAIN" '$1 == version || $1 == version "-x86_64-unknown-linux-gnu" { found = 1 } END { exit !found }'; then
    bootstrap_log "installing Rust toolchain $RUST_TOOLCHAIN"
    /root/.cargo/bin/rustup toolchain install 1.88.0 --profile minimal || bootstrap_die 'Rust toolchain installation failed; inspect rustup output and rerun bootstrap'
else
    bootstrap_log "Rust toolchain $RUST_TOOLCHAIN is already installed"
fi
if ! $RUSTUP_EXECUTABLE default | awk -v version="$RUST_TOOLCHAIN" 'NR == 1 && ($1 == version || $1 == version "-x86_64-unknown-linux-gnu") { found = 1 } END { exit !found }'; then
    /root/.cargo/bin/rustup default 1.88.0 || bootstrap_die 'setting the default Rust toolchain failed; inspect rustup output and rerun bootstrap'
fi
[[ "$(/root/.cargo/bin/rustc --version)" == "rustc $RUST_TOOLCHAIN "* ]] || bootstrap_die "rustc version verification failed; expected $RUST_TOOLCHAIN"
[[ "$($CARGO_EXECUTABLE --version)" == "cargo $RUST_TOOLCHAIN "* ]] || bootstrap_die "cargo version verification failed; expected $RUST_TOOLCHAIN"

if [[ -e "$HFX_REPOSITORY_DIR/.git" ]]; then
    git -C "$HFX_REPOSITORY_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1 || bootstrap_die "$HFX_REPOSITORY_DIR is not a Git worktree; inspect the unexpected path before rerunning bootstrap"
    origin_url=$(git -C "$HFX_REPOSITORY_DIR" remote get-url origin 2>/dev/null || true)
    if [[ "$origin_url" != "$HFX_REPOSITORY_URL" ]]; then
        if git -C "$HFX_REPOSITORY_DIR" remote get-url origin >/dev/null 2>&1; then
            git -C "$HFX_REPOSITORY_DIR" remote set-url origin "$HFX_REPOSITORY_URL" || bootstrap_die 'failed to restore the pinned anonymous origin URL; inspect the checkout and rerun bootstrap'
        else
            git -C "$HFX_REPOSITORY_DIR" remote add origin "$HFX_REPOSITORY_URL" || bootstrap_die 'failed to restore the pinned anonymous origin URL; inspect the checkout and rerun bootstrap'
        fi
    fi
    bootstrap_log 'fetching origin/main'
    git -C /root/hfx fetch --prune origin main || bootstrap_die 'anonymous Git fetch failed; inspect network and repository state, then rerun bootstrap'
elif [[ ! -e "$HFX_REPOSITORY_DIR" ]]; then
    bootstrap_log 'cloning origin/main'
    git clone --origin origin --branch main --single-branch https://github.com/CooperBigFoot/hfx.git /root/hfx || bootstrap_die 'anonymous Git clone failed; inspect network access and rerun bootstrap'
else
    bootstrap_die "$HFX_REPOSITORY_DIR exists but is not a Git worktree; inspect the unexpected path before rerunning bootstrap"
fi
git -C "$HFX_REPOSITORY_DIR" rev-parse --verify refs/remotes/origin/main >/dev/null 2>&1 || bootstrap_die 'origin/main does not resolve; inspect the anonymous checkout and rerun bootstrap'
git -C /root/hfx reset --hard origin/main || bootstrap_die 'checkout convergence to origin/main failed; inspect the checkout and rerun bootstrap'

bootstrap_log 'building the release hfx CLI'
cd /root/hfx || bootstrap_die 'failed to enter the source checkout; inspect the checkout and rerun bootstrap'
/root/.cargo/bin/cargo build --release -p hfx-cli || bootstrap_die 'release hfx CLI build failed; inspect Cargo output and rerun bootstrap'
[[ -x "$BUILT_CLI" ]] || bootstrap_die "release hfx CLI is not executable at $BUILT_CLI; inspect build output and rerun bootstrap"
"$BUILT_CLI" --version || bootstrap_die 'release hfx CLI version check failed; inspect the binary and rerun bootstrap'

verify_mount || bootstrap_die '/mnt/hfx is not a mounted read-write campaign volume; run provision.sh to completion, inspect the attachment, and rerun bootstrap'
for directory in "$WORKSPACE_ROOT" "$DOWNLOAD_DIRECTORY" "$LOGS_DIRECTORY"; do
    [[ -d "$directory" ]] || bootstrap_die "required volume directory $directory is absent; inspect the volume and rerun bootstrap"
done
for command_name in "${campaign_command_names[@]}" tmux gdal-config; do
    command -v -- "$command_name" >/dev/null 2>&1 || bootstrap_die "required command $command_name failed final verification; rerun bootstrap"
done
[[ "$($UV_EXECUTABLE --version | head -n 1)" == "uv $UV_VERSION" ]] || bootstrap_die 'uv failed final version verification; rerun bootstrap'
managed_python=$($UV_EXECUTABLE python find "$PYTHON_VERSION") || bootstrap_die 'managed Python failed final verification; rerun bootstrap'
[[ "$($managed_python -c 'import platform; print(platform.python_version())')" == "$PYTHON_VERSION" ]] || bootstrap_die 'managed Python failed final version verification; rerun bootstrap'
[[ -x "$HFX_GEO_VENV/bin/python" ]] || bootstrap_die 'geo environment Python failed final executable verification; rerun bootstrap'
[[ "$($HFX_GEO_VENV/bin/python -c 'import platform; print(platform.python_version())')" == "$PYTHON_VERSION" ]] || bootstrap_die 'geo environment failed final Python verification; rerun bootstrap'
verify_geo_packages || bootstrap_die 'geo packages failed final verification; rerun bootstrap'
[[ "$($RUSTUP_EXECUTABLE --version | head -n 1)" == "rustup $RUSTUP_VERSION "* ]] || bootstrap_die 'rustup failed final version verification; rerun bootstrap'
[[ "$(/root/.cargo/bin/rustc --version)" == "rustc $RUST_TOOLCHAIN "* ]] || bootstrap_die 'rustc failed final version verification; rerun bootstrap'
[[ "$($CARGO_EXECUTABLE --version)" == "cargo $RUST_TOOLCHAIN "* ]] || bootstrap_die 'cargo failed final version verification; rerun bootstrap'
[[ "$(git -C "$HFX_REPOSITORY_DIR" remote get-url origin)" == "$HFX_REPOSITORY_URL" ]] || bootstrap_die 'Git origin failed final anonymous URL verification; rerun bootstrap'
[[ "$(git -C "$HFX_REPOSITORY_DIR" rev-parse HEAD)" == "$(git -C "$HFX_REPOSITORY_DIR" rev-parse refs/remotes/origin/main)" ]] || bootstrap_die 'checkout HEAD does not equal origin/main; rerun bootstrap'
[[ -x "$BUILT_CLI" ]] || bootstrap_die 'release hfx CLI failed final executable verification; rerun bootstrap'
"$BUILT_CLI" --version || bootstrap_die 'release hfx CLI failed final verification; rerun bootstrap'

bootstrap_log "mount point: $MOUNT_POINT"
bootstrap_log "workspace root: $WORKSPACE_ROOT"
bootstrap_log "download directory: $DOWNLOAD_DIRECTORY"
bootstrap_log "logs directory: $LOGS_DIRECTORY"
bootstrap_log "source checkout: $HFX_REPOSITORY_DIR"
bootstrap_log "built CLI: $BUILT_CLI"
bootstrap_log "Python geo environment: $HFX_GEO_VENV"
bootstrap_log "$($UV_EXECUTABLE --version | head -n 1)"
bootstrap_log "Python $($managed_python -c 'import platform; print(platform.python_version())')"
bootstrap_log "$(tmux -V)"
bootstrap_log "$(aws --version 2>&1)"
bootstrap_log "$(git --version)"
bootstrap_log "$(curl --version | head -n 1)"
bootstrap_log "GDAL $(gdal-config --version)"
bootstrap_log "$(clang --version | head -n 1)"
bootstrap_log "pkg-config $(pkg-config --version)"
bootstrap_log "$($RUSTUP_EXECUTABLE --version | head -n 1)"
bootstrap_log "$(/root/.cargo/bin/rustc --version)"
bootstrap_log "$($CARGO_EXECUTABLE --version)"
bootstrap_log "$($BUILT_CLI --version)"
bootstrap_log "campaign $campaign bootstrap complete"
REMOTE_BOOTSTRAP
then
    hfx_die "bootstrap failed on campaign server $server_name at $ip_address; inspect the live output, correct the cause, and rerun bootstrap"
fi
