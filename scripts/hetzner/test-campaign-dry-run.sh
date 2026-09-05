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
bash "$SCRIPT_DIR/campaign-dry-run.sh" --work "$tmp/work" --record "$tmp/record/campaign-dry-run-result.json" >"$tmp/dry-run.out" 2>&1 ||
    { tail -n 60 "$tmp/dry-run.out" >&2; die 'campaign dry run failed'; }
grep -q 'dry run passed' "$tmp/dry-run.out" || die 'dry run did not report a pass'
jq -e '.result == "passed" and .lifecycle_result.strict_validation == "passed" and .lifecycle_result.zero_footprint == true' \
    "$tmp/record/campaign-dry-run-result.json" >/dev/null || die 'dry-run record is not a pass'
[[ $(jq -r '.ground_truth_ref' "$tmp/record/campaign-dry-run-result.json") == $(git -C "$repo_root" rev-parse HEAD) ]] || die 'dry-run record ref is not HEAD'
"$SCRIPT_DIR/verify-compile-runbook.sh" --evidence-root "$tmp/record" --check dry-run-passed >/dev/null || die 'verifier does not accept the dry-run record'
pass 'the whole composed driver runs locally to a passing lifecycle result with zero footprint, and the verifier accepts its record'
printf '1..%d\n' "$passed"
