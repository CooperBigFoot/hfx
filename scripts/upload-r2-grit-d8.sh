#!/usr/bin/env bash
# Human-fired runbook for amending grit/hfx-v0.3.0/ in place with the
# planetary GRIT D8 raster pair and its attribution objects.
#
# Default --dry-run makes zero AWS calls. The input directory is enumerated at
# runtime and intersected with the exact ALLOW_LIST below. The current
# workstation directory
# /Users/nicolaslazaro/Desktop/work/grit-d8-staging/grit-hfx-v0.3.0/
# is an example --staging argument only. It is neither a default nor a
# hardcoded input.
#
# The raster and attribution objects may stage early because no published
# manifest references them. Publishing manifest.json requires a v2-capable
# pourpoint release from ticket #45. The manifest is the atomic reader gate and
# is always the final upload.
#
# This script uses individual copy operations only and has no remote deletion
# behavior. A human must select --execute and pass the typed phase confirmation.
# Manifest publication also requires --publish-manifest and its distinct typed
# confirmation.

set -euo pipefail

# ============================================================================
# READER-GATED IN-PLACE PUBLISH TARGET -- ONE LINE.
TARGET_PREFIX="grit/hfx-v0.3.0/"
# ============================================================================

usage() {
  printf 'usage: %s [--dry-run | --execute | --self-test] [--publish-manifest] [--staging <dir>] [--profile <name>]\n' "${BASH_SOURCE[0]}"
  printf '  --dry-run          print all six operations without any AWS calls (default)\n'
  printf '  --execute          stage allowed objects after typed confirmation (human only)\n'
  printf '  --self-test        run offline scope, membership, gate, and enumeration checks\n'
  printf '  --publish-manifest permit the final manifest phase with --execute after ticket #45\n'
  printf '  --staging          local directory holding the six authored objects\n'
  printf '  --profile          AWS profile to use (default: upstream-r2)\n'
}

MODE="dry-run"
PROFILE="upstream-r2"
STAGING=""
PUBLISH_MANIFEST=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      MODE="dry-run"
      shift
      ;;
    --execute)
      MODE="execute"
      shift
      ;;
    --self-test)
      MODE="self-test"
      shift
      ;;
    --publish-manifest)
      PUBLISH_MANIFEST=1
      shift
      ;;
    --staging)
      if [[ $# -lt 2 || "$2" == --* ]]; then
        usage >&2
        exit 1
      fi
      STAGING="${2%/}"
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

BUCKET="basin-delineations-public"

ALLOW_LIST=(
  "aux/d8/flow_dir.tif"
  "aux/d8/flow_acc.tif"
  "NOTICE"
  "CITATION.txt"
  "README.md"
  "manifest.json"
)

GUARD_RE="^$(printf '%s' "$TARGET_PREFIX" | sed 's/\./\\./g')"

content_type_for() {
  case "$1" in
    *.json) printf 'application/json' ;;
    *) printf 'application/octet-stream' ;;
  esac
}

assert_keys_in_scope() {
  local key
  for key in "$@"; do
    if [[ ! "$key" =~ $GUARD_RE ]]; then
      printf 'FATAL: destination key %s is outside the target prefix %s\n' "$key" "$TARGET_PREFIX" >&2
      exit 1
    fi
  done
}

is_allowed() {
  local candidate="$1" entry
  for entry in "${ALLOW_LIST[@]}"; do
    if [[ "$candidate" == "$entry" ]]; then
      return 0
    fi
  done
  return 1
}

classify_paths() {
  local relpath
  OPS=()
  SKIPPED=()
  for relpath in "$@"; do
    if is_allowed "$relpath"; then
      OPS+=("$relpath|${TARGET_PREFIX}${relpath}")
    else
      SKIPPED+=("$relpath")
    fi
  done
}

build_ops() {
  local path relpath
  local paths=()

  if [[ -z "$STAGING" ]]; then
    printf 'FATAL: --staging <dir> is required for mode %s\n' "$MODE" >&2
    exit 1
  fi
  if [[ ! -d "$STAGING" ]]; then
    printf 'FATAL: staging dir not found: %s\n' "$STAGING" >&2
    exit 1
  fi

  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    relpath="${path#"$STAGING"/}"
    paths+=("$relpath")
  done < <(find "$STAGING" -type f | sort)
  classify_paths "${paths[@]}"
}

guard_ops() {
  local op relpath dest
  local dests=()

  for op in "${OPS[@]}"; do
    IFS='|' read -r relpath dest <<<"$op"
    dests+=("$dest")
  done
  if [[ ${#dests[@]} -eq 0 ]]; then
    printf 'FATAL: no allowed upload candidates found under %s\n' "$STAGING" >&2
    exit 1
  fi
  assert_keys_in_scope "${dests[@]}"
}

can_publish_manifest() {
  local mode="$1" publish_flag="$2"
  [[ "$mode" == "execute" && "$publish_flag" -eq 1 ]]
}

run_self_test() {
  local entry op relpath dest
  local synthetic=(
    "CITATION.txt"
    "NOTICE"
    "README.md"
    "aux/d8/flow_acc.tif"
    "aux/d8/flow_dir.tif"
    "manifest.json"
    "secrets.txt"
  )

  if (assert_keys_in_scope "${TARGET_PREFIX}manifest.json" "${TARGET_PREFIX}aux/d8/flow_dir.tif") 2>/dev/null; then
    printf 'PASS: scope accepts keys beneath TARGET_PREFIX\n'
  else
    printf 'FAIL: scope rejected an in-prefix key\n' >&2
    exit 1
  fi
  for entry in "merit/0.2.0/manifest.json" "grit/1.0.0/manifest.json"; do
    if (assert_keys_in_scope "$entry") 2>/dev/null; then
      printf 'FAIL: scope accepted out-of-prefix destination %s\n' "$entry" >&2
      exit 1
    fi
    printf 'PASS: scope out-of-prefix destination aborts: %s\n' "$entry"
  done

  for entry in "${ALLOW_LIST[@]}"; do
    if ! is_allowed "$entry"; then
      printf 'FAIL: membership rejected %s\n' "$entry" >&2
      exit 1
    fi
  done
  for entry in "catchments.parquet" "secrets.txt"; do
    if is_allowed "$entry"; then
      printf 'FAIL: membership accepted %s\n' "$entry" >&2
      exit 1
    fi
  done
  printf 'PASS: membership accepts six exact paths and rejects unrelated paths\n'

  if can_publish_manifest "dry-run" 1 || can_publish_manifest "execute" 0 || can_publish_manifest "dry-run" 0; then
    printf 'FAIL: manifest gate accepted an incomplete flag combination\n' >&2
    exit 1
  fi
  if ! can_publish_manifest "execute" 1; then
    printf 'FAIL: manifest gate rejected execute plus publish flag\n' >&2
    exit 1
  fi
  printf 'PASS: gating requires execute plus publish-manifest\n'

  classify_paths "${synthetic[@]}"
  if [[ ${#OPS[@]} -ne 6 || ${#SKIPPED[@]} -ne 1 || "${SKIPPED[0]}" != "secrets.txt" ]]; then
    printf 'FAIL: enumeration classification counts or skipped path differ\n' >&2
    exit 1
  fi
  for op in "${OPS[@]}"; do
    IFS='|' read -r relpath dest <<<"$op"
    assert_keys_in_scope "$dest"
  done
  IFS='|' read -r relpath dest <<<"${OPS[5]}"
  if [[ "$relpath" != "manifest.json" ]]; then
    printf 'FAIL: enumeration did not leave manifest in the final publish phase\n' >&2
    exit 1
  fi
  printf 'PASS: enumeration yields six deterministic operations, one skip, and manifest last\n'
  printf 'self-test OK\n'
}

run_dry_run() {
  local op relpath dest

  build_ops
  guard_ops

  printf 'upload-r2-grit-d8.sh dry-run (zero AWS calls made):\n'
  printf 'target prefix: %s   profile: %s   staging: %s\n' "$TARGET_PREFIX" "$PROFILE" "$STAGING"

  for op in "${OPS[@]}"; do
    IFS='|' read -r relpath dest <<<"$op"
    [[ "$relpath" == "manifest.json" ]] && continue
    assert_keys_in_scope "$dest"
    printf 'DRY-RUN: aws s3 cp %s/%s s3://%s/%s\n' "$STAGING" "$relpath" "$BUCKET" "$dest"
  done
  for op in "${OPS[@]}"; do
    IFS='|' read -r relpath dest <<<"$op"
    [[ "$relpath" == "manifest.json" ]] || continue
    assert_keys_in_scope "$dest"
    printf 'DRY-RUN: aws s3 cp %s/%s s3://%s/%s   GATED: requires --execute --publish-manifest after ticket #45 release\n' "$STAGING" "$relpath" "$BUCKET" "$dest"
  done

  for relpath in "${SKIPPED[@]}"; do
    printf 'SKIP: %s\n' "$relpath"
  done
  printf '%d operations; zero uploads. Manifest publication requires --execute --publish-manifest.\n' "${#OPS[@]}"
}

run_execute() {
  local op relpath dest content approval
  local stage_count=0

  build_ops
  guard_ops

  printf 'Stage these non-manifest objects under %s with profile %s:\n' "$TARGET_PREFIX" "$PROFILE"
  for op in "${OPS[@]}"; do
    IFS='|' read -r relpath dest <<<"$op"
    [[ "$relpath" == "manifest.json" ]] && continue
    assert_keys_in_scope "$dest"
    printf '  %s/%s -> s3://%s/%s\n' "$STAGING" "$relpath" "$BUCKET" "$dest"
    stage_count=$((stage_count + 1))
  done
  if [[ "$stage_count" -eq 0 ]]; then
    printf 'FATAL: no non-manifest objects are available to stage\n' >&2
    exit 1
  fi
  printf 'Type STAGE GRIT D8 OBJECTS to continue: '
  read -r approval
  if [[ "$approval" != "STAGE GRIT D8 OBJECTS" ]]; then
    printf 'aborted before object staging\n' >&2
    exit 1
  fi

  for op in "${OPS[@]}"; do
    IFS='|' read -r relpath dest <<<"$op"
    [[ "$relpath" == "manifest.json" ]] && continue
    content="$(content_type_for "$relpath")"
    assert_keys_in_scope "$dest"
    aws s3 cp --profile "$PROFILE" --content-type "$content" \
      "$STAGING/$relpath" "s3://$BUCKET/$dest"
  done

  if ! can_publish_manifest "$MODE" "$PUBLISH_MANIFEST"; then
    printf 'manifest.json remains gated; use --execute --publish-manifest only after the ticket #45 release.\n'
    return
  fi

  printf 'READER GATE: ticket #45 must have produced a v2-capable pourpoint release.\n'
  printf 'The manifest is the atomic reader-visible switch and will be uploaded last:\n'
  for op in "${OPS[@]}"; do
    IFS='|' read -r relpath dest <<<"$op"
    [[ "$relpath" == "manifest.json" ]] || continue
    assert_keys_in_scope "$dest"
    printf '  %s/%s -> s3://%s/%s\n' "$STAGING" "$relpath" "$BUCKET" "$dest"
  done
  printf 'Type PUBLISH GRIT D8 MANIFEST AFTER TICKET 45 RELEASE to continue: '
  read -r approval
  if [[ "$approval" != "PUBLISH GRIT D8 MANIFEST AFTER TICKET 45 RELEASE" ]]; then
    printf 'aborted before manifest publication\n' >&2
    exit 1
  fi

  for op in "${OPS[@]}"; do
    IFS='|' read -r relpath dest <<<"$op"
    [[ "$relpath" == "manifest.json" ]] || continue
    content="$(content_type_for "$relpath")"
    assert_keys_in_scope "$dest"
    aws s3 cp --profile "$PROFILE" --content-type "$content" \
      "$STAGING/$relpath" "s3://$BUCKET/$dest"
  done
}

if [[ "$PUBLISH_MANIFEST" -eq 1 && "$MODE" != "execute" ]]; then
  printf 'FATAL: --publish-manifest requires --execute\n' >&2
  exit 1
fi

case "$MODE" in
  self-test)
    run_self_test
    ;;
  dry-run)
    run_dry_run
    ;;
  execute)
    run_execute
    ;;
esac
