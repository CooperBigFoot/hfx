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

for check in scope-permits-compilation ceilings-and-kill-switches control-digests-are-pinned control-adjudication-is-pinned baseline-is-pinned; do
    expect_pass --check "$check"
    assert_contains "$stdout" "PASS $check"
done
pass 'tracked runbook passes the scope, ceiling, digest, adjudication, and baseline checks'

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
mutated=$(mutate malformed-contract 's/"schema": 4,/"schema": 4,,/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'malformed compile campaign contract'
mutated=$(mutate duplicate-key 's/"schema": 4,/"schema": 4,\n  "schema": 4,/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'duplicate contract key'
pass 'a missing, malformed, or duplicate-key contract refuses'

mutated=$(mutate wrong-campaign 's/"campaign": "seven-basin-extension"/"campaign": "tdx-m5-seven-compile"/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'wrong campaign identity'
mutated=$(mutate two-lifecycles 's/"lifecycles_authorized": 1/"lifecycles_authorized": 2/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'exactly one lifecycle'
mutated=$(mutate second-consumed 's/"workload_dispatched": false}\n/"workload_dispatched": false},\n      {"date": "2026-09-05", "cause": "x", "workload_dispatched": true}\n/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'lifecycle ledger does not record exactly the consumed 2026-09-04 lifecycle'
mutated=$(mutate two-authorized 's/"lifecycles": 1,/"lifecycles": 2,/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'current lifecycle authority is not the 2026-09-04 maintainer decision for one lifecycle'
mutated=$(mutate pending-record 's/"record": "[^"]*"/"record": "PR-URL-PENDING"/')
expect_failure --runbook "$mutated" --check scope-permits-compilation
assert_contains "$stderr" 'current lifecycle authority does not name its record'
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
mutated=$(mutate baseline-prefix-written 's|"extension_scratch_prefix": "scratch/tdx-hydro-seven-basin-extension/"|"extension_scratch_prefix": "scratch/tdx-hydro-tdx-m5-planetary/"|')
expect_failure --runbook "$mutated" --check baseline-is-pinned
assert_contains "$stderr" 'extension scratch prefix is not pinned'
pass 'hotpatch, control digest, baseline, and extension prefix drift refuses'

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

# The runbook shell sets IFS to newline and tab; the absent-basin list must be an
# array so loops enumerate seven ids instead of one space-joined word.
array_line=$(grep -E '^ABSENT_IDS=\(' "$runbook") || die 'runbook does not define ABSENT_IDS as an array'
enumerated=$(bash -c "set -Eeuo pipefail; IFS=\$'\n\t'; $array_line; for id in \"\${ABSENT_IDS[@]}\"; do printf '%s\n' \"\$id\"; done")
[[ $(printf '%s\n' "$enumerated" | wc -l | tr -d ' ') == 7 ]] || die 'ABSENT_IDS did not enumerate seven ids under the runbook IFS'
[[ $(printf '%s\n' "$enumerated" | sort -u | grep -c -E '^[0-9]{10}$') == 7 ]] || die 'ABSENT_IDS entries are not seven distinct ten-digit ids'
! grep -E -- '\$ABSENT_IDS([^\[]|$)' "$runbook" >/dev/null || die 'runbook still expands ABSENT_IDS as a scalar'
pass 'the absent-basin array enumerates seven ids under the runbook IFS and is never expanded as a scalar'

printf '1..%d\n' "$passed"
printf 'test-verify-compile-runbook: all %d cases passed\n' "$passed"
