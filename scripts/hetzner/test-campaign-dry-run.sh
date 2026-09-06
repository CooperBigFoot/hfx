#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
set +x

# Runs the local dry run of the whole composed campaign driver and requires its
# record. This is the gate that must pass before any rehearsal or production
# lifecycle; it needs the release hfx binary, uv, tmux, and GDAL on the workstation.
SCRIPT_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -P -- "$SCRIPT_DIR/../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/hfx-campaign-dry-run-test.XXXXXX")
cleanup() { rm -rf -- "$tmp"; }
trap cleanup EXIT
passed=0
die() { printf 'test-campaign-dry-run: error: %s\n' "$1" >&2; exit 1; }
pass() { passed=$((passed + 1)); printf 'ok %d - %s\n' "$passed" "$1"; }

[[ -x "$repo_root/target/release/hfx" ]] || die 'build the release hfx binary first: cargo build --release -p hfx-cli'
bash "$SCRIPT_DIR/campaign-dry-run.sh" --work "$tmp/work" --record "$tmp/record/campaign-dry-run-result.json" --keep --lifecycles 2 >"$tmp/dry-run.out" 2>&1 ||
    { tail -n 60 "$tmp/dry-run.out" >&2; die 'campaign dry run failed'; }
grep -q 'dry run passed' "$tmp/dry-run.out" || die 'dry run did not report a pass'
jq -e '.result == "passed" and .lifecycle_result.strict_validation == "passed" and .lifecycle_result.zero_footprint == true' \
    "$tmp/record/campaign-dry-run-result.json" >/dev/null || die 'dry-run record is not a pass'
[[ $(jq -r '.ground_truth_ref' "$tmp/record/campaign-dry-run-result.json") == $(git -C "$repo_root" rev-parse HEAD) ]] || die 'dry-run record ref is not HEAD'
"$SCRIPT_DIR/verify-compile-runbook.sh" --evidence-root "$tmp/record" --check dry-run-passed >/dev/null || die 'verifier does not accept the dry-run record'
pass 'the whole composed driver runs locally to a passing lifecycle result with zero footprint, and the verifier accepts its record'

# The second lifecycle ran in the same evidence root: the first campaign directory was superseded in place,
# and the VM-built reference control lives under each campaign directory, so the second run's fence 2
# found no previous reference (a shared off-vm/control-builds/planetary/<control> would have refused it).
control_id=$(jq -r '.control_basin' "$repo_root/scripts/hetzner/rehearsal-campaign-contract.json")
grep -q 'lifecycle 2 passed' "$tmp/dry-run.out" || die 'the second lifecycle did not pass'
[[ $(find "$tmp/work/evidence" -mindepth 1 -maxdepth 1 -type d -name 'campaign-rehearsal-superseded-*' | wc -l | tr -d ' ') == 1 ]] ||
    die 'the first lifecycle was not superseded in place'
superseded=$(find "$tmp/work/evidence" -mindepth 1 -maxdepth 1 -type d -name 'campaign-rehearsal-superseded-*')
extension_key=$(jq -r '.extension_scratch_prefix' "$tmp/work/evidence/dry-run-contract.json")
for dir in "$superseded" "$tmp/work/evidence/campaign-rehearsal"; do
    [[ ! -e "$dir/control-reference" && ! -e "$dir/off-vm/campaign/basin-outputs" && ! -e "$dir/off-vm/control-builds" && ! -e "$dir/off-vm/extension" ]] ||
        die "dataset bytes were written to the workstation under $dir"
    [[ -s "$dir/verify-control-builds.txt" && -s "$dir/verify-basin-outputs.txt" && -s "$dir/verify-source-corpus.txt" && -s "$dir/verify-extension.txt" ]] ||
        die "bucket read-back verifications are missing under $dir"
    jq -e '.result == "passed"' "$dir/lifecycle-result.json" >/dev/null || die "lifecycle under $dir did not pass"
done
[[ -f "$tmp/work/bucket/${extension_key%/}/control-reference/$control_id/manifest.json" ]] || die 'the VM-built reference control is not in the bucket'
[[ -n "$(find "$tmp/work/bucket/${extension_key%/}/source-corpus" -type f -name '*.gpkg' 2>/dev/null)" ]] || die 'the source corpus is not in the bucket'
[[ ! -e "$tmp/work/evidence/off-vm/control-builds" ]] || die 'a reference control was written into the shared off-vm inputs'
object_count=$(jq -r '.baseline.object_count' "$tmp/work/evidence/dry-run-contract.json")
[[ "$object_count" =~ ^[1-9][0-9]*$ ]] || die 'the resolved dry-run contract carries no baseline object count'
grep -qx "baseline_objects_kept=0 baseline_objects_pulled=$object_count" "$superseded/baseline-pull.log" || die 'the first lifecycle did not pull every baseline object'
grep -qx "baseline_objects_kept=$object_count baseline_objects_pulled=0" "$tmp/work/evidence/campaign-rehearsal/baseline-pull.log" ||
    die 'the second lifecycle re-copied the baseline that the volume already held'
pass 'a second lifecycle in the same evidence root passes with its own reference control after the first was superseded, and its baseline pull copies nothing'

# The converge fence's swap post-condition read the swap table the shims built: SwapTotal must equal
# the contract's root plus volume swap bytes under the shim's page model (whole 4 KiB pages, one page
# per file for the header), so a wrong sizing or a skipped swapon fails the dry run.
evidence=$tmp/work/evidence/campaign-rehearsal
contract=$repo_root/scripts/hetzner/rehearsal-campaign-contract.json
root_swap_bytes=$(( 400000000000 - $(jq -r '.workload_sizing.root_disk_reserve_bytes' "$contract") ))
root_swap_bytes_max=$(jq -r '.workload_sizing.root_swap_bytes_max' "$contract")
((root_swap_bytes <= root_swap_bytes_max)) || root_swap_bytes=$root_swap_bytes_max
volume_swap_bytes=$(jq -r '.workload_sizing.volume_swap_bytes' "$contract")
expected_swap_total=$(( (root_swap_bytes / 4096 - 1) * 4096 + (volume_swap_bytes / 4096 - 1) * 4096 ))
[[ $(cat "$evidence/observed-swap-total-bytes.txt") == "$expected_swap_total" ]] ||
    die "observed swap total $(cat "$evidence/observed-swap-total-bytes.txt") differs from the expected $expected_swap_total"
grep -q "^swap_total_bytes=$expected_swap_total expected_swap_bytes=$((root_swap_bytes + volume_swap_bytes))\$" "$evidence/converge.log" ||
    die 'converge.log does not carry the swap post-condition line'
grep -q 'observed-swap-total-bytes=' "$evidence/OPERATOR-LOG.md" || die 'the operator log does not record the swap total'
[[ ! -e "$evidence/gate-transport-failures.log" ]] || die 'the passing dry run recorded a gate transport failure'
[[ $(ls "$evidence" | grep -c '^gate-compile-monitor-[0-9TZ]*-[0-9]\{4\}\.json$') -ge 2 ]] || die 'the compile monitor did not write two sequence-numbered gate records'
pass 'the swap post-condition saw the contract sizing through the swap shims, every gate record is distinct, and no transport failure was recorded'

# A failed mkswap must end the dry run at the converge fence: under errexit the old `a && b && c` chain
# skipped swapon and carried on. The injected failure keeps the scratch directory for inspection.
mutation_status=0
DRY_MKSWAP_STATUS=1 bash "$SCRIPT_DIR/campaign-dry-run.sh" --work "$tmp/mutated" >"$tmp/mutated.out" 2>&1 || mutation_status=$?
[[ "$mutation_status" -ne 0 ]] || die 'the dry run passed although mkswap failed'
grep -q 'dry-run mkswap: injected failure' "$tmp/mutated/dry-run.log" || die 'the injected mkswap failure did not reach the converge log'
mutated_evidence=$tmp/mutated/evidence/campaign-rehearsal
! grep -q 'swap_total_bytes=' "$mutated_evidence/converge.log" || die 'the converge fence reached the swap post-condition after mkswap failed'
[[ ! -e "$mutated_evidence/milestones/03-corpus-verified-on-vm" ]] || die 'the driver went on past converge after mkswap failed'
[[ ! -e "$tmp/mutated/evidence/campaign-rehearsal/lifecycle-result.json" ]] || die 'a lifecycle result was written after mkswap failed'
grep -q 'zero Hetzner footprint' "$mutated_evidence/teardown.log" || die 'the exit trap did not tear down after the converge failure'
pass 'a failed mkswap stops the converge fence, writes no lifecycle result, and still tears down'
printf '1..%d\n' "$passed"
