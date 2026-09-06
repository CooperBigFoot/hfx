#!/usr/bin/env bash
# prepare : (RehearsalContract, EvidenceRoot, S3EnvFile) -> RehearsalEvidenceRoot + ResolvedContract
#
# Builds everything the compile runbook's fences read as preserved inputs, from
# tiny synthetic data, so the rehearsal lifecycle can run every fence for real:
# the synthetic corpus and its SHA-256 manifest, the tiny baseline fabric
# assembled from the roster basins and uploaded to the rehearsal baseline
# prefix, and the resolved rehearsal contract with every DERIVED value filled
# in. The reference control and the adjudication record are built and derived
# on the VM by runbook section 10 (contract control_reference
# vm-planetary-build), never here, so no workstation float result becomes a
# byte reference. It provisions nothing, never names the production baseline
# prefix, and never creates the approval record. With --re-resolve it only
# regenerates the resolved contract from the tracked record, carrying over the
# values derived at preparation, for an evidence root whose resolved contract
# predates fields added to the tracked record since.

set -Eeuo pipefail
IFS=$'\n\t'
set +x

SCRIPT_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/hetzner/common.sh
source "$SCRIPT_DIR/common.sh"
repo_root=$(cd -P -- "$SCRIPT_DIR/../.." && pwd)

readonly HFX_REHEARSAL_CONTRACT_REL=scripts/hetzner/rehearsal-campaign-contract.json
readonly HFX_S3_ENDPOINT=https://fsn1.your-objectstorage.com
readonly HFX_S3_REGION=fsn1

usage() {
    cat <<'USAGE'
Usage: prepare-rehearsal-campaign.sh --evidence-root <absolute-path> --s3-env-file <absolute-path> [--skip-upload]
       prepare-rehearsal-campaign.sh --evidence-root <absolute-path> --re-resolve

Options:
  --evidence-root <path>  rehearsal evidence root; must already hold provisioner-transfer-approval.txt
  --s3-env-file <path>    opaque file holding AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (never printed)
  --skip-upload           build everything but do not upload the baseline (dry run; the resolved
                          contract is still written so the driver's preflight can be rehearsed)
  --re-resolve            regenerate rehearsal-campaign-contract.resolved.json from the tracked
                          record, keeping the values derived at preparation and checking them
                          against the corpus and baseline on disk; builds and uploads nothing
  -h, --help              print usage and exit 0
USAGE
}

evidence_root=
s3_env_file=
skip_upload=0
re_resolve=0
while (($#)); do
    case $1 in
        -h | --help)
            usage
            exit 0
            ;;
        --evidence-root | --s3-env-file)
            (($# >= 2)) || hfx_die "option $1 requires a value"
            case $1 in
                --evidence-root) evidence_root=$2 ;;
                --s3-env-file) s3_env_file=$2 ;;
            esac
            shift 2
            ;;
        --skip-upload)
            skip_upload=1
            shift
            ;;
        --re-resolve)
            re_resolve=1
            shift
            ;;
        *) hfx_die "unknown argument: $1" ;;
    esac
done
[[ -n "$evidence_root" && "$evidence_root" == /* ]] || hfx_die '--evidence-root must be an absolute path'
[[ -d "$evidence_root" && ! -L "$evidence_root" ]] || hfx_die '--evidence-root must be an existing non-symlink directory'
approval=$evidence_root/provisioner-transfer-approval.txt
[[ -f "$approval" && ! -L "$approval" && -s "$approval" ]] ||
    hfx_die 'the rehearsal evidence root must already hold provisioner-transfer-approval.txt; this tool never creates it'
if ((re_resolve == 0)); then
    [[ -n "$s3_env_file" && "$s3_env_file" == /* ]] || hfx_die '--s3-env-file must be an absolute path'
    [[ -f "$s3_env_file" && ! -L "$s3_env_file" && -s "$s3_env_file" ]] || hfx_die '--s3-env-file must be a nonempty regular file'
else
    [[ -z "$s3_env_file" && "$skip_upload" == 0 ]] || hfx_die '--re-resolve takes no --s3-env-file or --skip-upload; it builds and uploads nothing'
fi
for command in jq uv git shasum aws python3; do hfx_require_command "$command"; done

contract=$repo_root/$HFX_REHEARSAL_CONTRACT_REL
git -C "$repo_root" ls-files --error-unmatch -- "$HFX_REHEARSAL_CONTRACT_REL" >/dev/null 2>&1 || hfx_die 'rehearsal contract is not tracked'
campaign=$(jq -er '.campaign' "$contract")
fabric_version=$(jq -er '.fabric_version' "$contract")
manifest_name=$(jq -er '.source_corpus.manifest' "$contract")
baseline_prefix=$(jq -er '.baseline.prefix' "$contract")
roster_ids=()
while IFS= read -r roster_id; do roster_ids+=("$roster_id"); done < <(jq -r '.baseline.basin_ids[]' "$contract")
((${#roster_ids[@]} >= 1)) || hfx_die 'the rehearsal baseline roster is empty'
jq -e '.control_reference == "vm-planetary-build" and .control_digests == "DERIVED-ON-VM" and .control_adjudication_record == "DERIVED-ON-VM"' "$contract" >/dev/null ||
    hfx_die 'the rehearsal contract must build its reference control on the VM'
[[ "$baseline_prefix" == "s3://pourpoint-hfx/scratch/tdx-hydro-$campaign/"* ]] || hfx_die 'rehearsal baseline prefix is outside the rehearsal scratch namespace'
[[ "$baseline_prefix" != *tdx-m5-planetary* ]] || hfx_die 'rehearsal baseline prefix names the production baseline'
selected_ids=()
while IFS= read -r selected_id; do selected_ids+=("$selected_id"); done < <(jq -r '.absent_basins[], .control_basin, .baseline.basin_ids[]' "$contract" | sort -u)

corpus_dir=$evidence_root/off-vm/acquired-source
baseline_dir=$evidence_root/off-vm/baseline
resolved=$evidence_root/rehearsal-campaign-contract.resolved.json

measure_corpus_bytes() { find "$corpus_dir" -type f -name '*.gpkg' -exec stat -f '%z' {} + | { total=0; while read -r size; do [[ "$size" =~ ^[0-9]+$ ]] || exit 1; total=$((total + size)); done; printf '%s\n' "$total"; }; }
measure_corpus_files() { find "$corpus_dir" -type f -name '*.gpkg' | wc -l | tr -d ' '; }
measure_baseline_bytes() { find "$baseline_dir/dataset" -type f -exec stat -f '%z' {} + | { total=0; while read -r size; do [[ "$size" =~ ^[0-9]+$ ]] || exit 1; total=$((total + size)); done; printf '%s\n' "$total"; }; }
measure_baseline_objects() { find "$baseline_dir/dataset" -type f | wc -l | tr -d ' '; }

# write_resolved_contract : (corpus_bytes, baseline_bytes, baseline_objects) -> resolved contract file
write_resolved_contract() {
    jq --argjson corpus_bytes "$1" --argjson baseline_bytes "$2" --argjson baseline_objects "$3" '
        .source_corpus.total_bytes = $corpus_bytes
        | .baseline.exported_bytes = $baseline_bytes
        | .baseline.object_count = $baseline_objects
        | del(.derived_at_preparation)
        | .resolved_from = "scripts/hetzner/rehearsal-campaign-contract.json"
    ' "$contract" > "$resolved"
    jq -e '[paths(. == "DERIVED")] == []' "$resolved" >/dev/null || hfx_die 'a DERIVED value remains unresolved'
}

if ((re_resolve == 1)); then
    [[ -f "$resolved" && ! -L "$resolved" ]] || hfx_die "no resolved contract to re-resolve at $resolved; prepare the root first"
    jq -e --arg campaign "$campaign" '.campaign == $campaign and .resolved_from == "scripts/hetzner/rehearsal-campaign-contract.json"' "$resolved" >/dev/null ||
        hfx_die "$resolved was not resolved from the tracked rehearsal contract for campaign $campaign"
    carried=()
    while IFS= read -r derived_path; do
        value=$(jq -r --arg path "$derived_path" 'getpath($path | split(".")) | if type == "number" then tostring else empty end' "$resolved")
        [[ -n "$value" ]] || hfx_die "$resolved carries no derived value at $derived_path; prepare a fresh root instead"
        carried+=("$value")
    done < <(jq -r '.derived_at_preparation[]' "$contract")
    ((${#carried[@]} == 3)) || hfx_die 'the tracked contract must derive exactly source_corpus.total_bytes, baseline.exported_bytes, and baseline.object_count'
    [[ -d "$corpus_dir" && -d "$baseline_dir/dataset" ]] || hfx_die 'the evidence root lacks the corpus or the baseline dataset; prepare a fresh root instead'
    [[ "$(measure_corpus_bytes)" == "${carried[0]}" && "$(measure_corpus_files)" == "$(jq -r '.source_corpus.file_count' "$contract")" ]] ||
        hfx_die 'the corpus on disk disagrees with the recorded derived values; prepare a fresh root instead'
    [[ "$(measure_baseline_bytes)" == "${carried[1]}" && "$(measure_baseline_objects)" == "${carried[2]}" ]] ||
        hfx_die 'the baseline on disk disagrees with the recorded derived values; prepare a fresh root instead'
    superseded="$resolved.superseded-$(date -u +%Y%m%dT%H%M%SZ)"
    [[ ! -e "$superseded" ]] || hfx_die "refusing to overwrite $superseded"
    mv -- "$resolved" "$superseded"
    write_resolved_contract "${carried[0]}" "${carried[1]}" "${carried[2]}"
    hfx_log "re-resolved contract: $resolved (previous copy kept at $superseded)"
    hfx_log "run the driver with HFX_CAMPAIGN_CONTRACT=$resolved HFX_CAMPAIGN_EVIDENCE=$evidence_root"
    exit 0
fi

[[ ! -e "$corpus_dir" && ! -e "$evidence_root/off-vm/control-builds" && ! -e "$baseline_dir" ]] ||
    hfx_die 'the rehearsal evidence root already holds off-vm inputs; use a fresh root'
mkdir -p -- "$corpus_dir" "$baseline_dir"

hfx_binary=$repo_root/target/release/hfx
[[ -x "$hfx_binary" ]] || hfx_die "release hfx binary is missing at $hfx_binary; run cargo build --release -p hfx-cli"
adapter_run() { HFX_BINARY=$hfx_binary uv run --frozen --project "$repo_root/adapters/tdx-hydro" python "$@"; }

hfx_log 'synthesizing the rehearsal corpus'
synth_args=()
for id in "${selected_ids[@]}"; do synth_args+=(--basin "$id"); done
adapter_run "$repo_root/adapters/tdx-hydro/synthesize_rehearsal_corpus.py" --out "$corpus_dir" --manifest "$manifest_name" "${synth_args[@]}"
mv -- "$corpus_dir/$manifest_name" "$evidence_root/$manifest_name"
(cd "$corpus_dir" && shasum -a 256 -c "$evidence_root/$manifest_name") >/dev/null
corpus_bytes=$(measure_corpus_bytes)
corpus_files=$(measure_corpus_files)
[[ "$corpus_files" == "$(jq -r '.source_corpus.file_count' "$contract")" ]] || hfx_die 'synthesized corpus file count differs from the contract'

hfx_log 'compiling the roster basins and assembling the tiny baseline'
mkdir -p -- "$baseline_dir/compiled"
assemble_inputs=()
for roster_basin in "${roster_ids[@]}"; do
    adapter_run "$repo_root/adapters/tdx-hydro/build_adapter.py" build \
        --basins "$corpus_dir/$roster_basin-basins.gpkg" --streamnet "$corpus_dir/$roster_basin-streamnet.gpkg" \
        --out "$baseline_dir/compiled/$roster_basin" --report "$baseline_dir/compiled/$roster_basin-build-report.json" \
        --processing-basin-id "$roster_basin" --fabric-version "$fabric_version"
    assemble_inputs+=(--input "$baseline_dir/compiled/$roster_basin")
done
adapter_run "$repo_root/adapters/tdx-hydro/build_adapter.py" assemble "${assemble_inputs[@]}" --out "$baseline_dir/dataset"
adapter_run "$repo_root/adapters/tdx-hydro/build_adapter.py" validate "$baseline_dir/dataset" --hfx-binary "$hfx_binary"
jq -e --arg region "$(jq -r '.baseline.region' "$contract")" --argjson units "$(jq -r '.baseline.unit_count' "$contract")" --arg fabric "$fabric_version" \
    '.region == $region and .unit_count == $units and .format_version == "0.3.0" and .fabric_version == $fabric and .fabric_name == "tdx_hydro"' \
    "$baseline_dir/dataset/manifest.json" >/dev/null || hfx_die 'assembled baseline manifest disagrees with the rehearsal contract'
jq -n --argjson roster "$(jq -c '.baseline.basin_ids | sort | unique' "$contract")" '{schema_version: 1, status: "succeeded", input_basin_ids: $roster}' > "$baseline_dir/evidence-assembly.json"
baseline_bytes=$(measure_baseline_bytes)
baseline_objects=$(measure_baseline_objects)

if ((skip_upload == 0)); then
    hfx_log "uploading the tiny baseline to $baseline_prefix"
    (
        set +x
        set -a
        # shellcheck disable=SC1090
        source "$s3_env_file"
        set +a
        listing=$(aws s3 ls "$baseline_prefix" --endpoint-url "$HFX_S3_ENDPOINT" --region "$HFX_S3_REGION" 2>/dev/null || true)
        [[ -z "$listing" ]] || hfx_die 'rehearsal baseline prefix already holds objects; choose a fresh rehearsal or clear it by hand'
        aws s3 cp "$baseline_dir/dataset/" "$baseline_prefix" --recursive --endpoint-url "$HFX_S3_ENDPOINT" --region "$HFX_S3_REGION" --only-show-errors
        aws s3 cp "$baseline_dir/evidence-assembly.json" "${baseline_prefix%dataset/}evidence/state/assembly.json" --endpoint-url "$HFX_S3_ENDPOINT" --region "$HFX_S3_REGION" --only-show-errors
        aws s3 ls "$baseline_prefix" --recursive --endpoint-url "$HFX_S3_ENDPOINT" --region "$HFX_S3_REGION" > "$baseline_dir/remote-listing.txt"
    )
    [[ "$(grep -c . "$baseline_dir/remote-listing.txt")" == "$baseline_objects" ]] || hfx_die 'uploaded baseline object count differs from the local dataset'
else
    hfx_log 'skipping the baseline upload (--skip-upload)'
fi

hfx_log 'writing the resolved rehearsal contract'
write_resolved_contract "$corpus_bytes" "$baseline_bytes" "$baseline_objects"

hfx_log "corpus: $corpus_files files, $corpus_bytes bytes; baseline: $baseline_objects objects, $baseline_bytes bytes"
hfx_log "resolved contract: $evidence_root/rehearsal-campaign-contract.resolved.json"
hfx_log "run the driver with HFX_CAMPAIGN_CONTRACT=$evidence_root/rehearsal-campaign-contract.resolved.json HFX_CAMPAIGN_EVIDENCE=$evidence_root"
