#!/usr/bin/env bash
# CP-1 runbook (G1 in planning/human-gated-actions.md).
# DO NOT EXECUTE WITHOUT HUMAN APPROVAL — fired only by a human via --execute.
#
# Uploads exactly 4 attribution objects to the public R2 bucket:
# 2 new keys (NOTICE, CITATION.txt) + 2 README overwrites (grit/2.0.0/README.md
# and the bucket-root README.md). This is NOT a dataset publish: the
# manifest-last convention is honored by not touching manifest.json at all.
# That sentence and the PROTECTED_KEYS guard below are the only places
# manifest.json appears in this script; no upload operation references it.
#
# Default mode is --dry-run, which makes zero aws calls. The endpoint is baked
# into ~/.aws/config for the default profile, so no endpoint flag is needed.

set -euo pipefail

usage() {
  printf 'usage: %s [--dry-run | --execute] [--profile <name>]\n' "${BASH_SOURCE[0]}"
  printf '  --dry-run   print the 4 upload operations without any aws calls (default)\n'
  printf '  --execute   perform the 4 uploads after a typed confirmation\n'
  printf '  --profile   aws profile to use (default: upstream-r2)\n'
}

MODE="dry-run"
PROFILE="upstream-r2"

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

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUCKET="basin-delineations-public"

# The complete operation set: local_path|dest_key|content_type.
# Ops 1-2 create new keys. Op 3 OVERWRITES the pre-existing 1,707 B
# grit/2.0.0/README.md (original archived at
# hosting/archive/grit-2.0.0-README-pre-campaign.md). Op 4 OVERWRITES the
# 61 B bucket-root README.md (original archived at
# hosting/archive/root-README-pre-campaign.md).
OPERATIONS=(
  "hosting/grit-2.0.0/NOTICE|grit/2.0.0/NOTICE|text/plain"
  "hosting/grit-2.0.0/CITATION.txt|grit/2.0.0/CITATION.txt|text/plain"
  "hosting/grit-2.0.0/README.md|grit/2.0.0/README.md|text/markdown"
  "hosting/README.md|README.md|text/markdown"
)

# The 5 grit/2.0.0 data objects this script must never write to.
PROTECTED_KEYS=(
  "grit/2.0.0/manifest.json"
  "grit/2.0.0/catchments.parquet"
  "grit/2.0.0/graph.parquet"
  "grit/2.0.0/aux/snap_segments.parquet"
  "grit/2.0.0/aux/snap_reaches.parquet"
)

# Guard — runs in BOTH modes: refuse any destination that collides with a
# protected data object, and require every local source file to exist.
run_guard() {
  local op src dest protected

  for op in "${OPERATIONS[@]}"; do
    IFS='|' read -r src dest _ <<<"$op"
    for protected in "${PROTECTED_KEYS[@]}"; do
      if [[ "$dest" == "$protected" ]]; then
        printf 'FATAL: destination key %s is a protected grit/2.0.0 data object\n' "$dest" >&2
        exit 1
      fi
    done
    if [[ ! -f "$REPO_ROOT/$src" ]]; then
      printf 'FATAL: missing local source file: %s\n' "$REPO_ROOT/$src" >&2
      exit 1
    fi
  done
}

run_dry_run() {
  local op src dest

  run_guard
  printf 'upload-r2-attribution.sh dry-run (zero aws calls made):\n'
  for op in "${OPERATIONS[@]}"; do
    IFS='|' read -r src dest _ <<<"$op"
    printf 'DRY-RUN: aws s3 cp %s s3://%s/%s\n' "$REPO_ROOT/$src" "$BUCKET" "$dest"
  done
  printf '%d operations; nothing uploaded. Re-run with --execute to upload.\n' "${#OPERATIONS[@]}"
}

run_execute() {
  local op src dest content_type approval

  printf 'About to upload %d attribution objects with profile %s:\n' "${#OPERATIONS[@]}" "$PROFILE"
  for op in "${OPERATIONS[@]}"; do
    IFS='|' read -r src dest _ <<<"$op"
    printf '  %s -> s3://%s/%s\n' "$REPO_ROOT/$src" "$BUCKET" "$dest"
  done
  printf 'Type UPLOAD GRIT ATTRIBUTION to continue: '
  read -r approval
  if [[ "$approval" != "UPLOAD GRIT ATTRIBUTION" ]]; then
    printf 'aborted\n' >&2
    exit 1
  fi

  run_guard
  for op in "${OPERATIONS[@]}"; do
    IFS='|' read -r src dest content_type <<<"$op"
    aws s3 cp --profile "$PROFILE" --content-type "$content_type" \
      "$REPO_ROOT/$src" "s3://$BUCKET/$dest"
  done
}

if [[ "$MODE" == "dry-run" ]]; then
  run_dry_run
else
  run_execute
fi
