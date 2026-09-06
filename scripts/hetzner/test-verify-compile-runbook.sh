#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
set +x

SCRIPT_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -P -- "$SCRIPT_DIR/../.." && pwd)
verifier=$SCRIPT_DIR/verify-compile-runbook.sh
runbook=$SCRIPT_DIR/RUNBOOK-tdx-hydro-seven-basin-compile.md
tmp=$(mktemp -d "${TMPDIR:-/tmp}/hfx-verify-compile-runbook-test.XXXXXX")
cleanup() { rm -rf -- "$tmp"; }
trap cleanup EXIT
stdout=$tmp/stdout
stderr=$tmp/stderr
passed=0

die() { printf 'test-verify-compile-runbook: error: %s\n' "$1" >&2; exit 1; }
pass() { passed=$((passed + 1)); printf 'ok %d - %s\n' "$passed" "$1"; }
assert_contains() { grep -F -- "$2" "$1" >/dev/null || die "missing '$2' in $1"; }
expect_pass() {
    "$verifier" "$@" >"$stdout" 2>"$stderr" || die "unexpected failure: $* ($(cat "$stderr"))"
}
expect_failure() {
    if "$verifier" "$@" >"$stdout" 2>"$stderr"; then
        die "unexpected success: $*"
    fi
    [[ -s "$stderr" ]] || die "no diagnostic for: $*"
}

[[ -f "$runbook" ]] || die "runbook is missing: $runbook"
git -C "$repo_root" ls-files --error-unmatch -- scripts/hetzner/RUNBOOK-tdx-hydro-seven-basin-compile.md >/dev/null 2>&1 ||
    die 'runbook must be tracked before the verifier can run against the checkout'

hotpatch_available=1
git -C "$repo_root" cat-file -e 'bde61149d3fefc5e3f30435bf7ed3d0bb32a519c^{commit}' 2>/dev/null || hotpatch_available=0
git -C "$repo_root" cat-file -e '43a98aff8c15a1a196f47b10217ad2f5553b6611^{commit}' 2>/dev/null || hotpatch_available=0

for check in scope-permits-compilation ceilings-and-kill-switches control-digests-are-pinned control-adjudication-is-pinned baseline-is-pinned rehearsal-record-is-pinned; do
    expect_pass --check "$check"
    assert_contains "$stdout" "PASS $check"
done
pass 'tracked runbook passes the scope, ceiling, digest, adjudication, baseline, and rehearsal-record checks'

if ((hotpatch_available == 1)); then
    expect_pass --check control-hotpatch-is-pinned
    assert_contains "$stdout" 'PASS control-hotpatch-is-pinned'
    pass 'tracked runbook pins the control ARG_MAX hotpatch against real Git objects'
else
    passed=$((passed + 1))
    printf 'ok %d - control hotpatch objects # SKIP pinned commits are unavailable in this clone\n' "$passed"
fi

if git -C "$repo_root" cat-file -e '69747055bcb1876d9d1fad48c60f5cae6a24ea60^{commit}' 2>/dev/null &&
    git -C "$repo_root" merge-base --is-ancestor 69747055bcb1876d9d1fad48c60f5cae6a24ea60 HEAD 2>/dev/null; then
    expect_pass --check authority-is-current
    assert_contains "$stdout" 'PASS authority-is-current'
    pass 'authority ref is an ancestor of HEAD and names the vision section'
else
    passed=$((passed + 1))
    printf 'ok %d - authority ref # SKIP authority commit is not an ancestor of this checkout\n' "$passed"
fi

expect_failure --check unknown-check
assert_contains "$stderr" 'unknown check'
expect_failure
assert_contains "$stderr" '--check is required'
expect_failure --check scope-permits-compilation --check scope-permits-compilation
assert_contains "$stderr" 'may not be repeated'
expect_failure --check --evidence-root
assert_contains "$stderr" 'requires a value'
pass 'argument errors refuse with a diagnostic'

mkdir "$tmp/evidence" "$tmp/empty-evidence"
expect_failure --check approval-is-a-precondition
assert_contains "$stderr" 'evidence root is required'
expect_failure --evidence-root relative/path --check approval-is-a-precondition
assert_contains "$stderr" 'evidence root must be absolute'
expect_failure --evidence-root "$tmp/evidence" --check approval-is-a-precondition
assert_contains "$stderr" 'approval record must be a regular non-symlink file'
: >"$tmp/empty-evidence/provisioner-transfer-approval.txt"
expect_failure --evidence-root "$tmp/empty-evidence" --check approval-is-a-precondition
assert_contains "$stderr" 'approval record must be non-empty'
printf '%s\n' 'TEST-APPROVAL-FIXTURE-DO-NOT-PRINT' >"$tmp/evidence/provisioner-transfer-approval.txt"
expect_pass --evidence-root "$tmp/evidence" --check approval-is-a-precondition
assert_contains "$stdout" 'PASS approval-is-a-precondition'
! grep -r -F 'TEST-APPROVAL-FIXTURE-DO-NOT-PRINT' "$stdout" "$stderr" >/dev/null || die 'approval record contents leaked'
ln -s "$tmp/evidence" "$tmp/evidence-link"
expect_failure --evidence-root "$tmp/evidence-link" --check approval-is-a-precondition
assert_contains "$stderr" 'evidence root must be an existing non-symlink directory'
mkdir "$tmp/link-evidence"
ln -s "$tmp/evidence/provisioner-transfer-approval.txt" "$tmp/link-evidence/provisioner-transfer-approval.txt"
expect_failure --evidence-root "$tmp/link-evidence" --check approval-is-a-precondition
assert_contains "$stderr" 'approval record must be a regular non-symlink file'
pass 'approval precondition tests only existence, regular-file-ness, and non-emptiness'

mutate() {
    local name=$1
    local perl_expression=$2
    perl -0pe "$perl_expression" "$runbook" >"$tmp/$name.md"
    printf '%s\n' "$tmp/$name.md"
}

mutated=$(mutate no-contract 's/<!-- BEGIN COMPILE CAMPAIGN CONTRACT\n.*?\nEND COMPILE CAMPAIGN CONTRACT -->\n//s')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'missing or repeated compile campaign contract'
mutated=$(mutate malformed-contract 's/"schema": 5,/"schema": 5,,/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'malformed compile campaign contract'
mutated=$(mutate duplicate-key 's/"schema": 5,/"schema": 5,\n  "schema": 5,/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'duplicate contract key'
pass 'a missing, malformed, or duplicate-key contract refuses'

mutated=$(mutate wrong-campaign 's/"campaign": "seven-basin-extension"/"campaign": "tdx-m5-seven-compile"/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'wrong campaign identity'
mutated=$(mutate two-lifecycles 's/"lifecycles_authorized": 1/"lifecycles_authorized": 2/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'exactly one lifecycle'
mutated=$(mutate third-consumed 's/"workload_dispatched": true}\n/"workload_dispatched": true},\n      {"date": "2026-09-05", "cause": "x", "workload_dispatched": true}\n/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'lifecycle ledger does not record exactly the two consumed 2026-09-04 lifecycles'
mutated=$(mutate two-authorized 's/("current_authority": \{[^}]*"lifecycles": )1,/${1}2,/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'current lifecycle authority is not the 2026-09-04 maintainer decision for one lifecycle'
mutated=$(mutate pending-record 's/("current_authority": \{[^}]*"record": )"[^"]*"/${1}"PR-URL-PENDING"/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'current lifecycle authority does not name its record'
mutated=$(mutate unconditional-authority 's/("current_authority": \{[^}]*"precondition": )"[^"]*"/${1}"none"/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'current lifecycle authority is not conditional on a passing rehearsal record'
mutated=$(mutate rehearsal-not-required 's/"requires_passing_rehearsal": true/"requires_passing_rehearsal": false/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'production contract must require a passing rehearsal'
mutated=$(mutate gate-phase-dropped 's/"pre-preservation": 6\}/"pre-preserve": 6}/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'gate reserves are not pinned for every gate phase'
mutated=$(mutate one-only-removed 's/The 2026-09-04 authorization covers one lifecycle only\. //')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'runbook omits required operational text'
mutated=$(mutate extra-basin 's/"6020000010"\]/"6020000010", "9020000010"]/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'seven-basin scope is not exact'
mutated=$(mutate extra-act 's/"exact-resource-teardown"\],/"exact-resource-teardown", "delete-baseline"],/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'permitted acts are incomplete or overbroad'
mutated=$(mutate web-in-scope 's/"out_of_scope_resources": \["pourpoint-web-1"\]/"out_of_scope_resources": []/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'pourpoint-web-1 is not declared out of scope'
mutated=$(mutate phrase-removed 's/Only the named campaign server and volume may be mutated\. //')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'runbook omits required operational text'
pass 'scope drift in identity, lifecycles, basins, acts, or prose refuses'

mutated=$(mutate laptop-corpus 's/"corpus_source": "workstation-rsync"/"corpus_source": "laptop"/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'corpus_source must be workstation-rsync or bucket'
mutated=$(mutate bigger-server 's/"server_type": "ccx33"/"server_type": "ccx43"/')
expect_failure --runbook "$mutated" --check ceilings-and-kill-switches
assert_contains "$stderr" 'server type is absent or changed'
mutated=$(mutate longer-clock 's/"wall_clock_ceiling_hours": 72/"wall_clock_ceiling_hours": 96/')
expect_failure --runbook "$mutated" --check ceilings-and-kill-switches
assert_contains "$stderr" 'wall-clock ceiling is absent or changed'
mutated=$(mutate higher-cost 's/"gross_cost_ceiling_eur": 40\.0/"gross_cost_ceiling_eur": 60.0/')
expect_failure --runbook "$mutated" --check ceilings-and-kill-switches
assert_contains "$stderr" 'gross cost ceiling is absent or changed'
mutated=$(mutate bigger-volume 's/"volume_size_gb": 600/"volume_size_gb": 1000/')
expect_failure --runbook "$mutated" --check ceilings-and-kill-switches
assert_contains "$stderr" 'volume size is absent or changed'
mutated=$(mutate keep-volume-allowed 's/`--keep-volume` is forbidden\./`--keep-volume` is allowed./')
expect_failure --runbook "$mutated" --check ceilings-and-kill-switches
assert_contains "$stderr" 'runbook omits required operational text'
mutated=$(mutate passing-oom 's/An interrupted or OOM-killed validation is recorded as `incomplete`, never as passing\./Validation is recorded as passing./')
expect_failure --runbook "$mutated" --check ceilings-and-kill-switches
assert_contains "$stderr" 'runbook omits required operational text'
pass 'ceiling, retention, and validation-truth drift refuses'

mutated=$(mutate approval-unpinned 's/"approval_record": "provisioner-transfer-approval\.txt"/"approval_record": "approval.txt"/')
expect_failure --runbook "$mutated" --evidence-root "$tmp/evidence" --check approval-is-a-precondition
assert_contains "$stderr" 'approval record is not pinned'
pass 'an unpinned approval record refuses'

mutated=$(mutate wrong-blob 's/"post_patch_blob": "d227920a7ac0ab98ffcc80aac2c72a5dfc9c2429"/"post_patch_blob": "0000000000000000000000000000000000000000"/')
expect_failure --runbook "$mutated" --check control-hotpatch-is-pinned
assert_contains "$stderr" 'control ARG_MAX hotpatch is not pinned'
mutated=$(mutate wrong-digest 's/"graph\.parquet": "fb64ce0fa941f244841ffc5eeed4f2057ea65262a0183a1ac1e81c67380e6cc5"/"graph.parquet": "0000000000000000000000000000000000000000000000000000000000000000"/')
expect_failure --runbook "$mutated" --check control-digests-are-pinned
assert_contains "$stderr" 'preserved planetary control digests are not pinned'
adjudication_record=$SCRIPT_DIR/seven-basin-control-adjudication.json
mutate_record() {
    local name=$1
    local jq_expression=$2
    jq "$jq_expression" "$adjudication_record" >"$tmp/$name.json"
    printf '%s\n' "$tmp/$name.json"
}
mutated=$(mutate record-unpinned 's|"control_adjudication_record": "scripts/hetzner/seven-basin-control-adjudication.json"|"control_adjudication_record": "scripts/hetzner/other.json"|')
expect_failure --runbook "$mutated" --check control-adjudication-is-pinned
assert_contains "$stderr" 'control adjudication record is not pinned'
expect_failure --adjudication-record "$tmp/absent.json" --check control-adjudication-is-pinned
assert_contains "$stderr" 'not a nonempty regular file'
expect_failure --adjudication-record relative.json --check control-adjudication-is-pinned
assert_contains "$stderr" '--adjudication-record must be an absolute path'
printf '{\n' >"$tmp/broken.json"
expect_failure --adjudication-record "$tmp/broken.json" --check control-adjudication-is-pinned
assert_contains "$stderr" 'malformed control adjudication record'
jq '.status = "PENDING polarity PR"' "$adjudication_record" >"$tmp/pending.json"
expect_failure --adjudication-record "$tmp/pending.json" --check control-adjudication-is-pinned
assert_contains "$stderr" 'PENDING placeholder'
for mutation in '.outlet_differences += 1' '.downstream_differences = 1' '.unit_count = 1' \
    '.processing_basin_id = "1020018110"' '.planetary_revision = "0000000000000000000000000000000000000000"' \
    '.adjudication.date = "2026-09-03"' '.corrected_orientation_digest = .planetary_orientation_digest' \
    '.outlet_difference_native_linknos = []' '.outlet_difference_native_linknos |= (. + [.[0]])' \
    '.outlet_difference_native_linknos |= reverse' '.max_shift_deg = 0' '.differences_by_class = {}'; do
    record=$(mutate_record inconsistent "$mutation")
    expect_failure --adjudication-record "$record" --check control-adjudication-is-pinned
    assert_contains "$stderr" 'control adjudication record is incomplete or inconsistent'
done
mutated=$(mutate adjudication-phrase-removed 's/The set of units whose outlet differs must equal exactly the adjudicated set pinned in the tracked record\. //')
expect_failure --runbook "$mutated" --check control-adjudication-is-pinned
assert_contains "$stderr" 'runbook omits required operational text'
expect_pass --adjudication-record "$adjudication_record" --check control-adjudication-is-pinned
pass 'an unpinned, missing, malformed, pending, inconsistent, or undocumented control adjudication record refuses'

mutated=$(mutate wrong-baseline 's/"unit_count": 12748154/"unit_count": 12748155/')
expect_failure --runbook "$mutated" --check baseline-is-pinned
assert_contains "$stderr" 'baseline artifact is not pinned'
mutated=$(mutate roster-swapped 's/"9020000010"\n    \]/"9020000011"\n    ]/')
expect_failure --runbook "$mutated" --check baseline-is-pinned
assert_contains "$stderr" 'baseline roster digest does not match the partial-region suffix'
mutated=$(mutate baseline-prefix-written 's|"extension_scratch_prefix": "scratch/tdx-hydro-seven-basin-extension/"|"extension_scratch_prefix": "scratch/tdx-hydro-tdx-m5-planetary/"|')
expect_failure --runbook "$mutated" --check baseline-is-pinned
assert_contains "$stderr" 'extension scratch prefix is not pinned'
pass 'hotpatch, control digest, baseline, and extension prefix drift refuses'

mutated=$(mutate rehearsal-ceiling-reached 's/"estimated_cost_eur": 0\.01/"estimated_cost_eur": 1.0/')
expect_failure --runbook "$mutated" --check rehearsal-record-is-pinned
assert_contains "$stderr" 'cumulative estimated rehearsal spend has reached the rehearsal ceiling'
mutated=$(mutate rehearsal-run-incomplete 's/"cause": "ssh-remote-argument-flattening", //')
expect_failure --runbook "$mutated" --check rehearsal-record-is-pinned
assert_contains "$stderr" 'a rehearsal run is neither a failure with its cause nor a pass'
mutated=$(mutate rehearsal-pass-unpinned 's/"ground_truth_ref": "a8d4ef29b9fd95d465cbff27eefc239a5717c283", "result": "passed"/"result": "passed"/')
expect_failure --runbook "$mutated" --check rehearsal-record-is-pinned
assert_contains "$stderr" 'a rehearsal run is neither a failure with its cause nor a pass'
mutated=$(mutate rehearsal-pass-with-cause 's/"result": "passed", "strict_validation": "passed"/"cause": "none", "result": "passed", "strict_validation": "passed"/')
expect_failure --runbook "$mutated" --check rehearsal-record-is-pinned
assert_contains "$stderr" 'a rehearsal run is neither a failure with its cause nor a pass'
mutated=$(mutate rehearsal-pass-differences 's/"outlet_differences": 0\}/"outlet_differences": 3}/')
expect_failure --runbook "$mutated" --check rehearsal-record-is-pinned
assert_contains "$stderr" 'a rehearsal run is neither a failure with its cause nor a pass'
mutated=$(mutate rehearsal-rerun-rule-removed 's/"reruns": "[^"]*",/"reruns": "never",/')
expect_failure --runbook "$mutated" --check rehearsal-record-is-pinned
assert_contains "$stderr" 'rehearsal authority lacks a cumulative ceiling, the rerun rule, or a complete run ledger'
mutated=$(mutate rehearsal-record-pending 's/("rehearsal_authority": \{[^}]*"record": )"[^"]*"/${1}"RECORD-URL"/')
expect_failure --runbook "$mutated" --check rehearsal-record-is-pinned
assert_contains "$stderr" 'rehearsal authority does not name its record'
rehearsal_contract=$SCRIPT_DIR/rehearsal-campaign-contract.json
mutate_rehearsal() {
    local name=$1
    local jq_expression=$2
    jq "$jq_expression" "$rehearsal_contract" >"$tmp/$name.json"
    printf '%s\n' "$tmp/$name.json"
}
for mutation in '.source_corpus.file_count = 6' '.baseline.basin_ids = ["4020050470"] | .baseline.basin_count = 1' \
    '.baseline.basin_ids = ["7020000010", "4020050470"]' '.control_reference = "preserved-off-vm"' \
    '.control_digests = {"manifest.json": "0000000000000000000000000000000000000000000000000000000000000000"}' \
    '.absent_basins += ["4020050470"]' '.server_type = "ccx33"' '.gross_cost_ceiling_eur = 2' \
    '.baseline.prefix = "s3://pourpoint-hfx/scratch/tdx-hydro-tdx-m5-planetary/planetary-hfx-v0-3-0/dataset/"'; do
    record=$(mutate_rehearsal inconsistent-rehearsal "$mutation")
    expect_failure --rehearsal-contract "$record" --check rehearsal-record-is-pinned
    assert_contains "$stderr" 'rehearsal contract is inconsistent'
done
record=$(mutate_rehearsal roster-digest-wrong '.baseline.roster_digest_prefix = "000000000000"')
expect_failure --rehearsal-contract "$record" --check rehearsal-record-is-pinned
assert_contains "$stderr" 'rehearsal roster digest prefix does not match its roster'
expect_pass --rehearsal-contract "$rehearsal_contract" --check rehearsal-record-is-pinned
pass 'a rehearsal contract whose corpus count, roster, control reference, ceilings, or prefixes drift refuses'
mutated=$(mutate rehearsal-phrase-removed 's/The rehearsal never names the production baseline prefix\.//')
expect_failure --runbook "$mutated" --check rehearsal-record-is-pinned
assert_contains "$stderr" 'runbook omits required operational text'
mkdir "$tmp/rehearsal-evidence" "$tmp/rehearsal-evidence/campaign-rehearsal"
expect_failure --evidence-root "$tmp/rehearsal-evidence" --check rehearsal-passed
assert_contains "$stderr" 'rehearsal lifecycle result is missing'
head_ref=$(git -C "$repo_root" rev-parse HEAD)
write_result() {
    jq -n --arg ref "$head_ref" --arg result "$1" --arg validation "$2" '{schema_version: 1, campaign: "campaign-rehearsal", ground_truth_ref: $ref,
        provisioning_request_epoch: 1788526261, finished_at: "2026-09-05T00:00:00Z", strict_validation: $validation, zero_footprint: true,
        control_gates: {planetary_versus_preserved: "created-at-only", corrected_adjudicated_comparison: "accepted"}, result: $result}' \
        >"$tmp/rehearsal-evidence/campaign-rehearsal/lifecycle-result.json"
}
write_result not-passed incomplete
expect_failure --evidence-root "$tmp/rehearsal-evidence" --check rehearsal-passed
assert_contains "$stderr" 'rehearsal lifecycle result is not a passing record'
jq '.ground_truth_ref = "0000000000000000000000000000000000000000"' "$tmp/rehearsal-evidence/campaign-rehearsal/lifecycle-result.json" >"$tmp/result-old-ref.json"
mv "$tmp/result-old-ref.json" "$tmp/rehearsal-evidence/campaign-rehearsal/lifecycle-result.json"
write_result_old() { jq '.result = "passed" | .strict_validation = "passed" | .ground_truth_ref = "0000000000000000000000000000000000000000"' "$tmp/rehearsal-evidence/campaign-rehearsal/lifecycle-result.json" >"$tmp/r.json" && mv "$tmp/r.json" "$tmp/rehearsal-evidence/campaign-rehearsal/lifecycle-result.json"; }
write_result_old
expect_failure --evidence-root "$tmp/rehearsal-evidence" --check rehearsal-passed
assert_contains "$stderr" 'does not descend from the runner fix'
write_result passed passed
if git -C "$repo_root" merge-base --is-ancestor 0ffa2d048ce5d748c0ab4c71fbe6f5862478107d HEAD 2>/dev/null; then
    expect_pass --evidence-root "$tmp/rehearsal-evidence" --check rehearsal-passed
    assert_contains "$stdout" 'PASS rehearsal-passed'
    pass 'a rehearsal result passes only when it records passed validation, zero footprint, and a ref carrying the repaired runner'
else
    passed=$((passed + 1))
    printf 'ok %d - rehearsal result # SKIP runner fix commit is not an ancestor of this checkout\n' "$passed"
fi
mkdir "$tmp/dry-evidence"
expect_failure --evidence-root "$tmp/dry-evidence" --check dry-run-passed
assert_contains "$stderr" 'campaign dry-run result is missing'
jq -n --arg ref "$head_ref" '{schema_version: 1, kind: "campaign-dry-run", ground_truth_ref: $ref, finished_at: "2026-09-05T00:00:00Z", result: "passed",
    lifecycle_result: {result: "passed", strict_validation: "passed", zero_footprint: true}}' >"$tmp/dry-evidence/campaign-dry-run-result.json"
expect_pass --evidence-root "$tmp/dry-evidence" --check dry-run-passed
assert_contains "$stdout" 'PASS dry-run-passed'
jq '.ground_truth_ref = "0000000000000000000000000000000000000000"' "$tmp/dry-evidence/campaign-dry-run-result.json" >"$tmp/dry-old.json" && mv "$tmp/dry-old.json" "$tmp/dry-evidence/campaign-dry-run-result.json"
expect_failure --evidence-root "$tmp/dry-evidence" --check dry-run-passed
assert_contains "$stderr" 'recorded at another ref than HEAD'
jq --arg ref "$head_ref" '.ground_truth_ref = $ref | .lifecycle_result.strict_validation = "incomplete"' "$tmp/dry-evidence/campaign-dry-run-result.json" >"$tmp/dry-bad.json" && mv "$tmp/dry-bad.json" "$tmp/dry-evidence/campaign-dry-run-result.json"
expect_failure --evidence-root "$tmp/dry-evidence" --check dry-run-passed
assert_contains "$stderr" 'campaign dry-run result is not a passing record'
pass 'the dry-run precondition requires a passing record at HEAD'
mutated=$(mutate short-authority 's/"authority_ref": "69747055bcb1876d9d1fad48c60f5cae6a24ea60"/"authority_ref": "6974705"/')
expect_failure --runbook "$mutated" --check authority-is-current
assert_contains "$stderr" 'authority ref is not a full commit hash'
mutated=$(mutate unknown-authority 's/"authority_ref": "69747055bcb1876d9d1fad48c60f5cae6a24ea60"/"authority_ref": "ffffffffffffffffffffffffffffffffffffffff"/')
expect_failure --runbook "$mutated" --check authority-is-current
assert_contains "$stderr" 'authority ref is unavailable'
mutated=$(mutate non-vision-authority 's|"authority_document": "planning/visions/2026-09-03-close-the-seven-basin-coverage-gap.md"|"authority_document": "scripts/hetzner/README.md"|')
expect_failure --runbook "$mutated" --check authority-is-current
assert_contains "$stderr" 'authority document is not a vision'
pass 'a short, unknown, or non-vision authority refuses'

expect_failure --runbook relative.md --check scope-permits-compilation
assert_contains "$stderr" '--runbook must be an absolute path'
ln -s "$runbook" "$tmp/runbook-link.md"
expect_failure --runbook "$tmp/runbook-link.md" --check scope-permits-compilation
assert_contains "$stderr" 'not a regular file'
pass 'a relative or symlinked runbook path refuses'

# The runbook shell sets IFS to newline and tab; the absent-basin list is built from
# the contract into an array so loops enumerate seven ids instead of one space-joined word.
grep -q -E '^ABSENT_IDS=\(\)$' "$runbook" || die 'runbook does not define ABSENT_IDS as an array'
cat >"$tmp/enumerate.sh" <<'ENUMERATE'
set -Eeuo pipefail
IFS=$'\n\t'
CAMPAIGN_CONTRACT_JSON=$(sed -n '/^<!-- BEGIN COMPILE CAMPAIGN CONTRACT$/,/^END COMPILE CAMPAIGN CONTRACT -->$/p' "$1" | sed '1d;$d')
contract_value() { jq -er "$1" <<<"$CAMPAIGN_CONTRACT_JSON"; }
ABSENT_IDS=()
while IFS= read -r absent_id; do ABSENT_IDS+=("$absent_id"); done < <(contract_value '.absent_basins[]')
for id in "${ABSENT_IDS[@]}"; do printf '%s\n' "$id"; done
ENUMERATE
enumerated=$(bash "$tmp/enumerate.sh" "$runbook")
[[ $(printf '%s\n' "$enumerated" | wc -l | tr -d ' ') == 7 ]] || die 'ABSENT_IDS did not enumerate seven ids under the runbook IFS'
[[ $(printf '%s\n' "$enumerated" | sort -u | grep -c -E '^[0-9]{10}$') == 7 ]] || die 'ABSENT_IDS entries are not seven distinct ten-digit ids'
! grep -E -- '\$ABSENT_IDS([^\[]|$)' "$runbook" >/dev/null || die 'runbook still expands ABSENT_IDS as a scalar'
pass 'the absent-basin array enumerates seven ids under the runbook IFS and is never expanded as a scalar'

printf '1..%d\n' "$passed"
printf 'test-verify-compile-runbook: all %d cases passed\n' "$passed"
