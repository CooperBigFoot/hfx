#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
set +x

# Covers the --re-resolve mode of prepare-rehearsal-campaign.sh: a resolved contract written by an
# older preparation lacks fields added to the tracked record since (rehearsal run 4's setup lacked
# requires_passing_dry_run); re-resolving regenerates it from the tracked record, keeps the values
# derived at preparation, and refuses when those values are missing or disagree with the root.
# The full preparation (synthesis, compile, assembly) runs inside test-campaign-dry-run.sh.
SCRIPT_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
tool=$SCRIPT_DIR/prepare-rehearsal-campaign.sh
contract=$SCRIPT_DIR/rehearsal-campaign-contract.json
tmp=$(mktemp -d "${TMPDIR:-/tmp}/hfx-prepare-rehearsal-test.XXXXXX")
cleanup() { rm -rf -- "$tmp"; }
trap cleanup EXIT
stdout=$tmp/stdout
stderr=$tmp/stderr
passed=0
die() { printf 'test-prepare-rehearsal-campaign: error: %s\n' "$1" >&2; exit 1; }
pass() { passed=$((passed + 1)); printf 'ok %d - %s\n' "$passed" "$1"; }
assert_contains() { grep -F -- "$2" "$1" >/dev/null || die "missing '$2' in $1"; }
run() { bash "$tool" "$@" >"$stdout" 2>"$stderr"; }
expect_refusal() {
    local needle=$1; shift
    if run "$@"; then die "unexpected success: $*"; fi
    assert_contains "$stderr" "$needle"
}

# An evidence root with the corpus and baseline shapes the derived values are measured from.
corpus_bytes=0
lay_out_root() {
    local root=$1 id
    mkdir -p -- "$root/off-vm/acquired-source" "$root/off-vm/baseline/dataset"
    printf 'TEST-APPROVAL-FIXTURE; not a maintainer record\n' >"$root/provisioner-transfer-approval.txt"
    while IFS= read -r id; do
        printf '%s-basins\n' "$id" >"$root/off-vm/acquired-source/$id-basins.gpkg"
        printf '%s-streamnet-longer\n' "$id" >"$root/off-vm/acquired-source/$id-streamnet.gpkg"
    done < <(jq -r '.absent_basins[], .control_basin, .baseline.basin_ids[]' "$contract" | sort -u)
    printf 'manifest\n' >"$root/off-vm/baseline/dataset/manifest.json"
    printf 'units-parquet-bytes\n' >"$root/off-vm/baseline/dataset/units.parquet"
    printf 'graph\n' >"$root/off-vm/baseline/dataset/graph.parquet"
}
lay_out_root "$tmp/root"
corpus_bytes=$(find "$tmp/root/off-vm/acquired-source" -type f -name '*.gpkg' -exec stat -f '%z' {} + | awk '{s+=$1} END {print s}')
corpus_files=$(find "$tmp/root/off-vm/acquired-source" -type f -name '*.gpkg' | wc -l | tr -d ' ')
[[ "$corpus_files" == "$(jq -r '.source_corpus.file_count' "$contract")" ]] || die 'fixture corpus file count differs from the contract'
baseline_bytes=$(find "$tmp/root/off-vm/baseline/dataset" -type f -exec stat -f '%z' {} + | awk '{s+=$1} END {print s}')
baseline_objects=3
# A resolved contract as an older preparation wrote it: derived values filled, requires_passing_dry_run absent.
stale_resolve() {
    jq --argjson corpus "$corpus_bytes" --argjson bytes "$baseline_bytes" --argjson objects "$baseline_objects" '
        .source_corpus.total_bytes = $corpus | .baseline.exported_bytes = $bytes | .baseline.object_count = $objects
        | del(.derived_at_preparation) | del(.requires_passing_dry_run)
        | .resolved_from = "scripts/hetzner/rehearsal-campaign-contract.json"' "$contract"
}
stale_resolve >"$tmp/root/rehearsal-campaign-contract.resolved.json"
jq -e 'has("requires_passing_dry_run") | not' "$tmp/root/rehearsal-campaign-contract.resolved.json" >/dev/null || die 'stale fixture still carries the field'

run --evidence-root "$tmp/root" --re-resolve || die "re-resolve failed: $(cat "$stderr")"
resolved=$tmp/root/rehearsal-campaign-contract.resolved.json
jq -e '.requires_passing_dry_run == true' "$resolved" >/dev/null || die 're-resolved contract lacks requires_passing_dry_run'
jq -e --argjson corpus "$corpus_bytes" --argjson bytes "$baseline_bytes" --argjson objects "$baseline_objects" \
    '.source_corpus.total_bytes == $corpus and .baseline.exported_bytes == $bytes and .baseline.object_count == $objects' "$resolved" >/dev/null ||
    die 're-resolved contract lost the derived values'
jq -e '[paths(. == "DERIVED")] == [] and (has("derived_at_preparation") | not) and .resolved_from == "scripts/hetzner/rehearsal-campaign-contract.json"' "$resolved" >/dev/null ||
    die 're-resolved contract is not fully resolved'
diff <(jq -S 'del(.derived_at_preparation) | keys' "$contract") <(jq -S 'del(.resolved_from) | keys' "$resolved") >/dev/null ||
    die 're-resolved contract keys differ from the tracked record'
[[ $(ls "$tmp/root" | grep -c '^rehearsal-campaign-contract\.resolved\.json\.superseded-[0-9TZ]*$') == 1 ]] || die 'the previous resolved contract was not kept'
assert_contains "$stderr" 're-resolved contract:'
[[ ! -e "$tmp/root/off-vm/control-builds" ]] || die 're-resolve created off-vm inputs'
pass 're-resolve regenerates the resolved contract from the tracked record, keeps the derived values, and keeps the previous copy'

# The composed driver's first fence refuses the stale contract up front and accepts the re-resolved one.
mkdir -p "$tmp/fence1/evidence" "$tmp/fence1/composed"
stale_resolve >"$tmp/fence1/stale.json"
python3 "$SCRIPT_DIR/compose-campaign-driver.py" --mode full --out "$tmp/fence1/composed" >/dev/null
printf 'AWS_ACCESS_KEY_ID=TEST\nAWS_SECRET_ACCESS_KEY=TEST\n' >"$tmp/fence1/s3.env"
fence1_status=0
(cd "$SCRIPT_DIR/../.." && HFX_CAMPAIGN_EVIDENCE=$tmp/fence1/evidence HFX_CAMPAIGN_CONTRACT=$tmp/fence1/stale.json \
    bash "$tmp/fence1/composed/fence-proof/driver-01.sh" <<<"$tmp/fence1/s3.env") >"$stdout" 2>"$stderr" || fence1_status=$?
[[ "$fence1_status" -ne 0 ]] || die 'fence 1 accepted a stale resolved contract'
assert_contains "$stderr" "resolved contract $tmp/fence1/stale.json lacks requires_passing_dry_run"
assert_contains "$stderr" '--re-resolve'
[[ ! -e "$tmp/fence1/evidence/campaign-rehearsal" ]] || die 'fence 1 created the evidence directory before refusing the stale contract'
(cd "$SCRIPT_DIR/../.." && HFX_CAMPAIGN_EVIDENCE=$tmp/fence1/evidence HFX_CAMPAIGN_CONTRACT=$resolved \
    bash "$tmp/fence1/composed/fence-proof/driver-01.sh" <<<"$tmp/fence1/s3.env") >"$stdout" 2>"$stderr" || die "fence 1 refused the re-resolved contract: $(cat "$stderr")"
pass 'the driver refuses a stale resolved contract up front, naming the file and the missing field, and accepts the re-resolved one'

mkdir -p "$tmp/unprepared"
printf 'TEST-APPROVAL-FIXTURE; not a maintainer record\n' >"$tmp/unprepared/provisioner-transfer-approval.txt"
expect_refusal 'no resolved contract to re-resolve' --evidence-root "$tmp/unprepared" --re-resolve
expect_refusal '--re-resolve takes no --s3-env-file' --evidence-root "$tmp/root" --re-resolve --s3-env-file "$tmp/fence1/s3.env"
lay_out_root "$tmp/no-derived"
stale_resolve | jq 'del(.baseline.object_count)' >"$tmp/no-derived/rehearsal-campaign-contract.resolved.json"
expect_refusal 'carries no derived value at baseline.object_count' --evidence-root "$tmp/no-derived" --re-resolve
lay_out_root "$tmp/drift"
stale_resolve >"$tmp/drift/rehearsal-campaign-contract.resolved.json"
printf 'extra bytes\n' >>"$tmp/drift/off-vm/acquired-source/$(jq -r '.control_basin' "$contract")-basins.gpkg"
expect_refusal 'the corpus on disk disagrees with the recorded derived values' --evidence-root "$tmp/drift" --re-resolve
lay_out_root "$tmp/other"
stale_resolve | jq '.campaign = "other-campaign"' >"$tmp/other/rehearsal-campaign-contract.resolved.json"
expect_refusal 'was not resolved from the tracked rehearsal contract' --evidence-root "$tmp/other" --re-resolve
pass 're-resolve refuses a missing resolved contract, extra options, a missing derived value, a drifted corpus, and another campaign'

printf '1..%d\n' "$passed"
