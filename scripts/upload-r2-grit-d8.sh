#!/usr/bin/env bash
# Human-fired, authority-driven manifest publication and rollback runbook.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEFAULT_AUTHORITY_ROOT="$REPO_ROOT/hosting/grit-hfx-v0.3.0"
CANONICAL_VERIFIER="$DEFAULT_AUTHORITY_ROOT/verify-authority.py"

BUCKET="basin-delineations-public"
TARGET_PREFIX="grit/hfx-v0.3.0/"
TARGET_KEY="grit/hfx-v0.3.0/manifest.json"
CANDIDATE_REL="manifest.json"
FORMER_REL="manifest.former.json"
CANDIDATE_BYTES="1426"
CANDIDATE_SHA256="02339ff92cbfd1d2ea57bb5332cb843b98115cd7a7395f64c14fac78d2ed643c"
FORMER_BYTES="1132"
FORMER_SHA256="0935a7bc09b7c2636786082fd9fd9a669ea1b32c6e2e4d92cb3f8da531c083c4"
INVENTORY_BYTES="8252"
INVENTORY_SHA256="86c25402910af4c0050e97910c0eff966060cbe50503f9cb24bb44bb77ff402a"
PUBLISH_CONFIRMATION="PUBLISH GRIT HFX V0.3.0 CANDIDATE MANIFEST"
ROLLBACK_CONFIRMATION="ROLL BACK GRIT HFX V0.3.0 FORMER MANIFEST"

MODE="dry-run"
MODE_FLAGS=0
ACTION=""
ACTION_FLAGS=0
AUTHORITY_ROOT="$DEFAULT_AUTHORITY_ROOT"
PROFILE="upstream-r2"
REMOTE_COPY_CALLS=0
OPS=()

usage() {
  printf 'usage: scripts/upload-r2-grit-d8.sh [--dry-run | --self-test | --execute] [--publish-manifest | --rollback-manifest] [--authority <dir>] [--profile <name>]\n'
  printf '  --dry-run           verify authority and print one manifest operation with zero AWS calls (default)\n'
  printf '  --self-test         falsify authority, identity, scope, ordering, action, and confirmation guards offline\n'
  printf '  --execute           human-only mutation mode; requires exactly one explicit action flag\n'
  printf '  --publish-manifest  select the candidate manifest (implicit only for dry-run)\n'
  printf '  --rollback-manifest select the byte-identical former manifest\n'
  printf '  --authority         authority-package directory (default: hosting/grit-hfx-v0.3.0)\n'
  printf '  --profile           AWS profile for a future human execution (default: upstream-r2)\n'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      MODE="dry-run"
      MODE_FLAGS=$((MODE_FLAGS + 1))
      shift
      ;;
    --self-test)
      MODE="self-test"
      MODE_FLAGS=$((MODE_FLAGS + 1))
      shift
      ;;
    --execute)
      MODE="execute"
      MODE_FLAGS=$((MODE_FLAGS + 1))
      shift
      ;;
    --publish-manifest)
      ACTION="publish"
      ACTION_FLAGS=$((ACTION_FLAGS + 1))
      shift
      ;;
    --rollback-manifest)
      ACTION="rollback"
      ACTION_FLAGS=$((ACTION_FLAGS + 1))
      shift
      ;;
    --authority)
      if [[ $# -lt 2 || "$2" == --* ]]; then
        usage >&2
        exit 1
      fi
      AUTHORITY_ROOT="${2%/}"
      shift 2
      ;;
    --profile)
      if [[ $# -lt 2 || "$2" == --* ]]; then
        usage >&2
        exit 1
      fi
      PROFILE="$2"
      shift 2
      ;;
    *)
      usage >&2
      exit 1
      ;;
  esac
done

mode_action_valid() {
  local mode="$1" action_count="$2"
  case "$mode" in
    dry-run) [[ "$action_count" -le 1 ]] ;;
    execute) [[ "$action_count" -eq 1 ]] ;;
    self-test) [[ "$action_count" -eq 0 ]] ;;
    *) return 1 ;;
  esac
}

if [[ "$MODE_FLAGS" -gt 1 ]] || ! mode_action_valid "$MODE" "$ACTION_FLAGS"; then
  usage >&2
  exit 1
fi
if [[ ! -d "$AUTHORITY_ROOT" ]]; then
  printf 'FATAL: authority path is not a directory: %s\n' "$AUTHORITY_ROOT" >&2
  exit 1
fi
if [[ "$MODE" == "dry-run" && -z "$ACTION" ]]; then
  ACTION="publish"
fi

verify_authority() {
  local inventory="$AUTHORITY_ROOT/identity-inventory.json"
  local measured_bytes measured_sha256

  python3 "$CANONICAL_VERIFIER" --root "$AUTHORITY_ROOT" >/dev/null || return 1
  measured_bytes="$(wc -c < "$inventory" | tr -d ' ')"
  measured_sha256="$(shasum -a 256 "$inventory" | awk '{print $1}')"
  if [[ "$measured_bytes" != "$INVENTORY_BYTES" ]]; then
    printf 'FATAL: identity inventory bytes drifted: expected %s, got %s\n' "$INVENTORY_BYTES" "$measured_bytes" >&2
    return 1
  fi
  if [[ "$measured_sha256" != "$INVENTORY_SHA256" ]]; then
    printf 'FATAL: identity inventory sha256 drifted: expected %s, got %s\n' "$INVENTORY_SHA256" "$measured_sha256" >&2
    return 1
  fi
}

build_ops() {
  local action="$1"
  local -n output_ops="$2"
  case "$action" in
    publish)
      output_ops=("$AUTHORITY_ROOT/$CANDIDATE_REL|$TARGET_KEY|application/json|$CANDIDATE_BYTES|$CANDIDATE_SHA256")
      ;;
    rollback)
      output_ops=("$AUTHORITY_ROOT/$FORMER_REL|$TARGET_KEY|application/json|$FORMER_BYTES|$FORMER_SHA256")
      ;;
    *)
      printf 'FATAL: unknown action: %s\n' "$action" >&2
      return 1
      ;;
  esac
}

guard_ops() {
  local action="$1"
  local -n guarded_ops="$2"
  local source destination content_type bytes sha256 expected_source expected_bytes expected_sha256

  local _idx _last _dest
  _last=$(( ${#guarded_ops[@]} - 1 ))
  for _idx in "${!guarded_ops[@]}"; do
    IFS='|' read -r _ _dest _ _ _ <<<"${guarded_ops[$_idx]}"
    if [[ "$_idx" -ne "$_last" && "$_dest" == */manifest.json ]]; then
      printf 'FATAL: manifest is not the final operation\n' >&2
      return 1
    fi
    if [[ "$_idx" -eq "$_last" && "$_dest" != */manifest.json ]]; then
      printf 'FATAL: final destination is not manifest.json\n' >&2
      return 1
    fi
  done

  if [[ ${#guarded_ops[@]} -ne 1 ]]; then
    printf 'FATAL: manifest plan must contain exactly one operation\n' >&2
    return 1
  fi
  IFS='|' read -r source destination content_type bytes sha256 <<<"${guarded_ops[0]}"
  case "$action" in
    publish)
      expected_source="$CANDIDATE_REL"
      expected_bytes="$CANDIDATE_BYTES"
      expected_sha256="$CANDIDATE_SHA256"
      ;;
    rollback)
      expected_source="$FORMER_REL"
      expected_bytes="$FORMER_BYTES"
      expected_sha256="$FORMER_SHA256"
      ;;
    *)
      printf 'FATAL: unknown action: %s\n' "$action" >&2
      return 1
      ;;
  esac
  if [[ "$destination" != "$TARGET_KEY" ]]; then
    printf 'FATAL: destination must equal %s exactly: %s\n' "$TARGET_KEY" "$destination" >&2
    return 1
  fi
  if [[ "$content_type" != "application/json" ]]; then
    printf 'FATAL: manifest content type must be application/json\n' >&2
    return 1
  fi
  if [[ "${source##*/}" != "$expected_source" || "$bytes" != "$expected_bytes" || "$sha256" != "$expected_sha256" ]]; then
    printf 'FATAL: selected manifest source or identity does not match action %s\n' "$action" >&2
    return 1
  fi
}

confirmation_matches() {
  local action="$1" supplied="$2"
  case "$action" in
    publish) [[ "$supplied" == "$PUBLISH_CONFIRMATION" ]] ;;
    rollback) [[ "$supplied" == "$ROLLBACK_CONFIRMATION" ]] ;;
    *) return 1 ;;
  esac
}

authority_display() {
  if [[ "$AUTHORITY_ROOT" == "$DEFAULT_AUTHORITY_ROOT" ]]; then
    printf 'hosting/grit-hfx-v0.3.0'
  else
    printf '%s' "$AUTHORITY_ROOT"
  fi
}

print_plan() {
  local action="$1" source destination content_type bytes sha256 label
  IFS='|' read -r source destination content_type bytes sha256 <<<"${OPS[0]}"
  if [[ "$action" == "publish" ]]; then
    label="publication"
  else
    label="rollback"
  fi
  printf 'GRIT HFX v0.3.0 %s dry-run (zero AWS calls made)\n' "$label"
  printf 'authority: %s\n' "$(authority_display)"
  printf 'candidate: manifest.json 1426 bytes sha256 %s\n' "$CANDIDATE_SHA256"
  printf 'former: manifest.former.json 1132 bytes sha256 %s\n' "$FORMER_SHA256"
  printf 'prerequisites: 7 exact inventory entries; COG live evidence is Content-Length plus ETag only\n'
  printf 'flow_dir: Content-Length 50686516478 ETag "bc48d1013cf6908fb44c325dd2ad10ab-1511"; historical recorded sha256 eace32b63c4bc09e8172f03cce6dacfbf09a86c6b51c42b50c6cccd498d4d656 (not live-verified)\n'
  printf 'flow_acc: Content-Length 205069870081 ETag "49eab3942a26036aa49e72ea33a1b724-6112"; historical recorded sha256 30f16ba3238085289d87e72f3386fa152da7e9b56063f5d610422d20a79fc98b (not live-verified)\n'
  printf 'PLAN 1/1 (manifest only; manifest last): %s/%s -> s3://%s/%s %s\n' "$(authority_display)" "${source##*/}" "$BUCKET" "$destination" "$content_type"
  printf '1 operation; zero AWS calls; zero mutations.\n'
}

run_dry_run() {
  REMOTE_COPY_CALLS=0
  verify_authority
  build_ops "$ACTION" OPS
  guard_ops "$ACTION" OPS
  print_plan "$ACTION"
  if [[ "$REMOTE_COPY_CALLS" -ne 0 ]]; then
    printf 'FATAL: dry-run made a remote-copy call\n' >&2
    return 1
  fi
}

remote_copy() {
  local source="$1"
  REMOTE_COPY_CALLS=$((REMOTE_COPY_CALLS + 1))
  aws s3 cp "$source" "s3://$BUCKET/$TARGET_KEY" --profile "$PROFILE" --content-type application/json
}

run_execute() {
  local source destination content_type bytes sha256 approval expected_confirmation
  verify_authority
  build_ops "$ACTION" OPS
  guard_ops "$ACTION" OPS
  IFS='|' read -r source destination content_type bytes sha256 <<<"${OPS[0]}"
  printf '%s -> s3://%s/%s %s bytes sha256 %s\n' "$source" "$BUCKET" "$destination" "$bytes" "$sha256"
  if [[ "$ACTION" == "publish" ]]; then
    expected_confirmation="$PUBLISH_CONFIRMATION"
  else
    expected_confirmation="$ROLLBACK_CONFIRMATION"
  fi
  printf 'Type %s to continue: ' "$expected_confirmation"
  read -r approval
  if ! confirmation_matches "$ACTION" "$approval"; then
    printf 'aborted before manifest mutation\n' >&2
    return 1
  fi
  verify_authority
  guard_ops "$ACTION" OPS
  remote_copy "$source"
}

flip_one_byte() {
  python3 - "$1" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
data = bytearray(path.read_bytes())
data[0] ^= 1
path.write_bytes(data)
PY
}

run_self_test() {
  local test_root rollback_op
  local valid candidate_bad successor_bad cog_extra notice_extra manifest_first wrong_publish wrong_rollback
  test_root="$(mktemp -d /tmp/hfx-m1-s2-self-test.XXXXXX)"
  SELF_TEST_ROOT="$test_root"
  trap 'if [[ -n "${SELF_TEST_ROOT:-}" && "$SELF_TEST_ROOT" == /tmp/hfx-m1-s2-self-test.* ]]; then rm -rf -- "$SELF_TEST_ROOT"; fi' EXIT

  printf 'GRIT HFX v0.3.0 manifest runbook self-test\n'
  verify_authority
  printf 'PASS: canonical authority package accepted\n'

  cp -R "$DEFAULT_AUTHORITY_ROOT" "$test_root/candidate"
  flip_one_byte "$test_root/candidate/manifest.json"
  if (AUTHORITY_ROOT="$test_root/candidate"; verify_authority) >/dev/null 2>"$test_root/candidate.err"; then
    printf 'FAIL: candidate manifest byte drift accepted\n' >&2
    return 1
  fi
  grep -Eq 'candidate|manifest.json.*identity|identity.*manifest.json' "$test_root/candidate.err"
  printf 'PASS: candidate manifest byte drift rejected\n'

  cp -R "$DEFAULT_AUTHORITY_ROOT" "$test_root/former"
  flip_one_byte "$test_root/former/manifest.former.json"
  if (AUTHORITY_ROOT="$test_root/former"; verify_authority) >/dev/null 2>"$test_root/former.err"; then
    printf 'FAIL: former manifest byte drift accepted\n' >&2
    return 1
  fi
  grep -Eq 'former|manifest.former.json.*identity|identity.*manifest.former.json' "$test_root/former.err"
  printf 'PASS: former manifest byte drift rejected\n'

  cp -R "$DEFAULT_AUTHORITY_ROOT" "$test_root/inventory"
  flip_one_byte "$test_root/inventory/identity-inventory.json"
  if (AUTHORITY_ROOT="$test_root/inventory"; verify_authority) >/dev/null 2>"$test_root/inventory.err"; then
    printf 'FAIL: prerequisite identity inventory drift accepted\n' >&2
    return 1
  fi
  grep -Fq 'identity inventory bytes drifted' "$test_root/inventory.err"
  printf 'PASS: prerequisite identity inventory drift rejected before planning\n'

  valid=("$DEFAULT_AUTHORITY_ROOT/$CANDIDATE_REL|$TARGET_KEY|application/json|$CANDIDATE_BYTES|$CANDIDATE_SHA256")
  guard_ops publish valid
  printf 'PASS: exact target accepts grit/hfx-v0.3.0/manifest.json\n'

  candidate_bad=("$DEFAULT_AUTHORITY_ROOT/$CANDIDATE_REL|grit/hfx-v0.3.0/not-manifest.json|application/json|$CANDIDATE_BYTES|$CANDIDATE_SHA256")
  successor_bad=("$DEFAULT_AUTHORITY_ROOT/$CANDIDATE_REL|grit/hfx-v0.3.1/manifest.json|application/json|$CANDIDATE_BYTES|$CANDIDATE_SHA256")
  if guard_ops publish candidate_bad 2>/dev/null || guard_ops publish successor_bad 2>/dev/null; then
    printf 'FAIL: invalid destination accepted\n' >&2
    return 1
  fi
  printf 'PASS: non-manifest and literal successor-prefix negative-fixture destinations rejected\n'

  cog_extra=("${valid[0]}" "$DEFAULT_AUTHORITY_ROOT/aux/d8/flow_dir.tif|grit/hfx-v0.3.0/aux/d8/flow_dir.tif|image/tiff|50686516478|eace32b63c4bc09e8172f03cce6dacfbf09a86c6b51c42b50c6cccd498d4d656")
  notice_extra=("${valid[0]}" "$DEFAULT_AUTHORITY_ROOT/NOTICE|grit/hfx-v0.3.0/NOTICE|text/plain|1454|eac224bf0b70b1494e5abd89f80079d665150ea744a2f730593f7216ca223db3")
  if guard_ops publish cog_extra 2>/dev/null || guard_ops publish notice_extra 2>/dev/null; then
    printf 'FAIL: extra operation accepted\n' >&2
    return 1
  fi
  printf 'PASS: extra COG and attribution operations rejected\n'

  manifest_first=("${valid[0]}" "$DEFAULT_AUTHORITY_ROOT/NOTICE|grit/hfx-v0.3.0/NOTICE|text/plain|1454|eac224bf0b70b1494e5abd89f80079d665150ea744a2f730593f7216ca223db3")
  if guard_ops publish manifest_first 2>"$test_root/ordering.err"; then
    printf 'FAIL: non-final manifest ordering accepted\n' >&2
    return 1
  fi
  grep -Fq 'manifest is not the final operation' "$test_root/ordering.err"
  printf 'PASS: non-final manifest ordering rejected\n'

  wrong_publish=("$DEFAULT_AUTHORITY_ROOT/$FORMER_REL|$TARGET_KEY|application/json|$FORMER_BYTES|$FORMER_SHA256")
  if guard_ops publish wrong_publish 2>/dev/null; then
    printf 'FAIL: rollback source accepted for publication\n' >&2
    return 1
  fi
  printf 'PASS: publication source substitution rejected\n'

  wrong_rollback=("$DEFAULT_AUTHORITY_ROOT/$CANDIDATE_REL|$TARGET_KEY|application/json|$CANDIDATE_BYTES|$CANDIDATE_SHA256")
  if guard_ops rollback wrong_rollback 2>/dev/null; then
    printf 'FAIL: publication source accepted for rollback\n' >&2
    return 1
  fi
  printf 'PASS: rollback source substitution rejected\n'

  ACTION="publish"
  run_dry_run >"$test_root/dry-run.out"
  if [[ "$REMOTE_COPY_CALLS" -ne 0 ]]; then
    printf 'FAIL: default dry-run made a remote-copy call\n' >&2
    return 1
  fi
  printf 'PASS: default dry-run made zero remote-copy calls\n'

  if mode_action_valid execute 0 || mode_action_valid execute 2 || mode_action_valid dry-run 2 || mode_action_valid self-test 1; then
    printf 'FAIL: invalid execute/action combination accepted\n' >&2
    return 1
  fi
  mode_action_valid execute 1 && mode_action_valid dry-run 0 && mode_action_valid dry-run 1 && mode_action_valid self-test 0
  printf 'PASS: execute and action combinations require exactly one explicit action\n'

  if confirmation_matches publish "" || confirmation_matches publish "$ROLLBACK_CONFIRMATION" || confirmation_matches publish "PUBLISH GRIT HFX V0.3.0 CANDIDATE MANIFESX"; then
    printf 'FAIL: invalid publication confirmation accepted\n' >&2
    return 1
  fi
  confirmation_matches publish "$PUBLISH_CONFIRMATION"
  printf 'PASS: publication confirmation rejects rollback, empty, and corrupt phrases\n'

  if confirmation_matches rollback "" || confirmation_matches rollback "$PUBLISH_CONFIRMATION" || confirmation_matches rollback "ROLL BACK GRIT HFX V0.3.0 FORMER MANIFESX"; then
    printf 'FAIL: invalid rollback confirmation accepted\n' >&2
    return 1
  fi
  confirmation_matches rollback "$ROLLBACK_CONFIRMATION"
  printf 'PASS: rollback confirmation rejects publication, empty, and corrupt phrases\n'

  verify_authority
  build_ops rollback rollback_op
  guard_ops rollback rollback_op
  IFS='|' read -r rollback_source rollback_destination rollback_content_type rollback_bytes rollback_sha256 <<<"${rollback_op[0]}"
  [[ "${rollback_source##*/}" == "$FORMER_REL" && "$rollback_destination" == "$TARGET_KEY" && "$rollback_bytes" == "$FORMER_BYTES" && "$rollback_sha256" == "$FORMER_SHA256" ]]
  printf 'PASS: rollback selects 1132 bytes sha256 %s\n' "$FORMER_SHA256"
  printf 'self-test OK\n'
}

case "$MODE" in
  self-test) run_self_test ;;
  dry-run) run_dry_run ;;
  execute) run_execute ;;
esac
