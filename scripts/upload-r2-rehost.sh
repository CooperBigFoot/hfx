#!/usr/bin/env bash
# s13 GRIT 2.0.0 re-host runbook (feeds human step s15; OD-8 prefix policy in
# planning/milestone-alden-feedback/pending-human-decisions.md).
# DO NOT EXECUTE WITHOUT HUMAN APPROVAL -- fired only by a human via --execute.
#
# Re-uploads the GRIT 2.0.0 dataset rebuilt with the HFX v0.3.0 GeoParquet bbox
# covering struct (manifest format_version 0.3.0, validated by hfx toolkit
# 0.4.0). It PUBLISHES the 5 data objects under the NEW additive prefix
# grit/hfx-v0.3.0/ (OD-8 chosen: new prefix, not in-place). This deletes
# nothing: the legacy grit/2.0.0/ dataset (format_version 0.2.1) stays live for
# clients pinned to pyshed 0.2.4. Because this is a real dataset publish, the
# manifest is uploaded LAST so a reader never sees a 0.3.0 manifest pointing at
# not-yet-uploaded data.
#
# Upload set (5 relative paths, enumerated from --staging at RUNTIME and
# intersected with ALLOW_LIST):
#   catchments.parquet
#   graph.parquet
#   aux/snap_segments.parquet
#   aux/snap_reaches.parquet
#   manifest.json            (uploaded LAST)
#
# This script only uploads. It leaves the attribution objects (NOTICE /
# CITATION.txt / README.md, published by CP-1) untouched, uses no external sync
# utility, and enumerates the staging dir at RUNTIME (no hardcoded path list).
# Default mode is --dry-run, which makes zero aws calls. The endpoint is baked
# into ~/.aws/config for the default profile, so no endpoint flag is used.
#
# Invariants (kept greppable by the absence of trigger strings): this file
# contains NO prohibited sync-tool name and NO `aws s3` removal subcommand, so
# the register greps stay empty.

set -euo pipefail

# ============================================================================
# OD-8 CHANGE POINT -- ONE LINE.
# OD-8 decision (Nik -- see planning/milestone-alden-feedback/pending-human-decisions.md):
# publish the 0.3.0 build under the NEW additive prefix grit/hfx-v0.3.0/. The
# legacy grit/2.0.0/ dataset (format_version 0.2.1) stays live and is NOT touched
# by this script. To target a different prefix, change ONLY the line below.
# The scope guard regex is DERIVED from this constant, so it tracks whatever
# prefix you choose; nothing else needs to change.
TARGET_PREFIX="grit/hfx-v0.3.0/"
# ============================================================================

usage() {
  printf 'usage: %s [--dry-run | --execute | --self-test] [--staging <dir>] [--profile <name>]\n' "${BASH_SOURCE[0]}"
  printf '  --dry-run    print the upload operations without any aws calls (default)\n'
  printf '  --execute    perform the uploads after a typed confirmation (human only)\n'
  printf '  --self-test  run the offline scope-guard unit checks (no aws, no staging)\n'
  printf '  --staging    local dir holding the rebuilt grit-2.0.0 dataset (required for dry-run/execute)\n'
  printf '  --profile    aws profile to use (default: upstream-r2)\n'
}

MODE="dry-run"
PROFILE="upstream-r2"
STAGING=""

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
    --staging)
      if [[ $# -lt 2 ]]; then
        usage >&2
        exit 1
      fi
      STAGING="${2%/}"
      shift 2
      ;;
    --profile)
      if [[ $# -lt 2 ]]; then
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

# The ONLY relative paths this script may ever upload. Nothing else, ever.
# The attribution objects (NOTICE, CITATION.txt, README.md) are intentionally
# ABSENT -- they belong to CP-1 (upload-r2-attribution.sh) and stay untouched.
ALLOW_LIST=(
  "catchments.parquet"
  "graph.parquet"
  "aux/snap_segments.parquet"
  "aux/snap_reaches.parquet"
  "manifest.json"
)

# Scope guard regex, DERIVED from TARGET_PREFIX (dot-escaped, anchored). With the
# default TARGET_PREFIX this is exactly  ^grit/hfx-v0\.3\.0/  -- the prefix outside
# of which no key may ever be written.
GUARD_RE="^$(printf '%s' "$TARGET_PREFIX" | sed 's/\./\\./g')"

content_type_for() {
  case "$1" in
    *.json) printf 'application/json' ;;
    *) printf 'application/octet-stream' ;;
  esac
}

# UNCONDITIONAL GUARD (used in every mode): abort if ANY destination key falls
# outside TARGET_PREFIX. Present even though dest keys are built from
# TARGET_PREFIX by construction -- defense in depth, and the unit exercised by
# --self-test. Uses `exit 1` so --self-test can capture it in a subshell.
assert_keys_in_scope() {
  local key
  for key in "$@"; do
    if [[ ! "$key" =~ $GUARD_RE ]]; then
      printf 'FATAL: destination key %s is outside the target prefix %s\n' "$key" "$TARGET_PREFIX" >&2
      exit 1
    fi
  done
}

# Enumerate --staging at RUNTIME; emit candidate ops (relpath|destkey) for files
# whose relative path is in ALLOW_LIST. Everything else is recorded in SKIPPED.
# Sets globals OPS and SKIPPED.
build_ops() {
  local path relpath allowed entry

  if [[ -z "$STAGING" ]]; then
    printf 'FATAL: --staging <dir> is required for mode %s\n' "$MODE" >&2
    exit 1
  fi
  if [[ ! -d "$STAGING" ]]; then
    printf 'FATAL: staging dir not found: %s\n' "$STAGING" >&2
    exit 1
  fi

  OPS=()
  SKIPPED=()
  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    relpath="${path#"$STAGING"/}"
    allowed=0
    for entry in "${ALLOW_LIST[@]}"; do
      if [[ "$relpath" == "$entry" ]]; then
        allowed=1
        break
      fi
    done
    if [[ "$allowed" -eq 1 ]]; then
      OPS+=("$relpath|${TARGET_PREFIX}${relpath}")
    else
      SKIPPED+=("$relpath")
    fi
  done < <(find "$STAGING" -type f | sort)
}

# Collect dest keys from OPS and assert they are all in scope; abort if no
# candidates were found.
guard_ops() {
  local op relpath dest
  local dests=()

  if [[ ${#OPS[@]} -gt 0 ]]; then
    for op in "${OPS[@]}"; do
      IFS='|' read -r relpath dest <<<"$op"
      dests+=("$dest")
    done
  fi
  if [[ ${#dests[@]} -eq 0 ]]; then
    printf 'FATAL: no upload candidates found under %s (expected the 5 grit/hfx-v0.3.0 data objects)\n' "$STAGING" >&2
    exit 1
  fi
  assert_keys_in_scope "${dests[@]}"
}

run_self_test() {
  printf 'self-test: scope guard = %s\n' "$GUARD_RE"

  if ( assert_keys_in_scope "${TARGET_PREFIX}manifest.json" "${TARGET_PREFIX}catchments.parquet" ) 2>/dev/null; then
    printf '  PASS: in-scope keys accepted\n'
  else
    printf '  FAIL: in-scope keys were rejected\n' >&2
    exit 1
  fi

  if ( assert_keys_in_scope "merit/0.2.0/manifest.json" ) 2>/dev/null; then
    printf '  FAIL: out-of-scope key merit/0.2.0/manifest.json was accepted\n' >&2
    exit 1
  else
    printf '  PASS: out-of-scope key merit/0.2.0/manifest.json aborted\n'
  fi

  if ( assert_keys_in_scope "grit/1.0.0/manifest.json" ) 2>/dev/null; then
    printf '  FAIL: out-of-scope key grit/1.0.0/manifest.json was accepted\n' >&2
    exit 1
  else
    printf '  PASS: out-of-scope key grit/1.0.0/manifest.json aborted\n'
  fi

  printf 'self-test OK\n'
}

run_dry_run() {
  local op relpath dest

  build_ops
  guard_ops

  printf 'upload-r2-rehost.sh dry-run (zero aws calls made):\n'
  printf 'target prefix: %s   profile: %s   staging: %s\n' "$TARGET_PREFIX" "$PROFILE" "$STAGING"

  # Data objects first.
  for op in "${OPS[@]}"; do
    IFS='|' read -r relpath dest <<<"$op"
    [[ "$relpath" == "manifest.json" ]] && continue
    printf 'DRY-RUN: aws s3 cp %s/%s s3://%s/%s\n' "$STAGING" "$relpath" "$BUCKET" "$dest"
  done
  # Manifest LAST.
  for op in "${OPS[@]}"; do
    IFS='|' read -r relpath dest <<<"$op"
    [[ "$relpath" == "manifest.json" ]] || continue
    printf 'DRY-RUN: aws s3 cp %s/%s s3://%s/%s   (manifest uploaded LAST)\n' "$STAGING" "$relpath" "$BUCKET" "$dest"
  done

  if [[ ${#SKIPPED[@]} -gt 0 ]]; then
    printf -- '--- skipped (not in upload allow-list; e.g. attribution objects) ---\n'
    for relpath in "${SKIPPED[@]}"; do
      printf 'SKIP: %s\n' "$relpath"
    done
  fi
  printf '%d operations; nothing uploaded. Re-run with --execute to upload.\n' "${#OPS[@]}"
}

run_execute() {
  local op relpath dest content approval

  build_ops
  guard_ops

  printf 'About to OVERWRITE %d data objects under %s with profile %s:\n' "${#OPS[@]}" "$TARGET_PREFIX" "$PROFILE"
  for op in "${OPS[@]}"; do
    IFS='|' read -r relpath dest <<<"$op"
    printf '  %s/%s -> s3://%s/%s\n' "$STAGING" "$relpath" "$BUCKET" "$dest"
  done
  printf 'Type REHOST GRIT 2.0.0 COVERING to continue: '
  read -r approval
  if [[ "$approval" != "REHOST GRIT 2.0.0 COVERING" ]]; then
    printf 'aborted\n' >&2
    exit 1
  fi

  # Data objects first.
  for op in "${OPS[@]}"; do
    IFS='|' read -r relpath dest <<<"$op"
    [[ "$relpath" == "manifest.json" ]] && continue
    content="$(content_type_for "$relpath")"
    aws s3 cp --profile "$PROFILE" --content-type "$content" \
      "$STAGING/$relpath" "s3://$BUCKET/$dest"
  done
  # Manifest LAST.
  for op in "${OPS[@]}"; do
    IFS='|' read -r relpath dest <<<"$op"
    [[ "$relpath" == "manifest.json" ]] || continue
    content="$(content_type_for "$relpath")"
    aws s3 cp --profile "$PROFILE" --content-type "$content" \
      "$STAGING/$relpath" "s3://$BUCKET/$dest"
  done
}

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
