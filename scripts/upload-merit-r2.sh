#!/usr/bin/env bash
# DO NOT EXECUTE WITHOUT HUMAN APPROVAL
#
# Runbook script for publishing the MERIT HFX 0.1.0 bundle to the public R2
# bucket. The manifest is uploaded last so clients only see the dataset after
# the large artifacts are already present.

set -euo pipefail

PROFILE="upstream-r2"
ENDPOINT_URL="https://7179d2d9b122c136aae4e8374004d46b.r2.cloudflarestorage.com"
DEST_PREFIX="s3://basin-delineations-public/merit-basins/0.1.0/"
SOURCE_ROOT="${HFX_ROOT:-$HOME/Desktop/merit-hfx/global/hfx}"

if [[ "${HFX_UPLOAD_INSIDE_CAFFEINATE:-}" != "1" ]]; then
  export HFX_UPLOAD_INSIDE_CAFFEINATE=1
  exec caffeinate -i -s "$BASH" "$0" "$@"
fi

require_file() {
  local name="$1"

  if [[ ! -f "$SOURCE_ROOT/$name" ]]; then
    printf 'missing required file: %s\n' "$SOURCE_ROOT/$name" >&2
    exit 1
  fi
}

upload_artifact() {
  local name="$1"
  local content_type="$2"

  aws s3 cp \
    --profile "$PROFILE" \
    --endpoint-url "$ENDPOINT_URL" \
    --exclude manifest.json \
    --content-type "$content_type" \
    "$SOURCE_ROOT/$name" \
    "$DEST_PREFIX$name"
}

upload_manifest() {
  aws s3 cp \
    --profile "$PROFILE" \
    --endpoint-url "$ENDPOINT_URL" \
    --content-type application/json \
    "$SOURCE_ROOT/manifest.json" \
    "${DEST_PREFIX}manifest.json"
}

print_parity_check() {
  printf '\nBulk upload complete. Compare local and remote totals before publishing manifest:\n'
  printf '  du -sb %q/\n' "$SOURCE_ROOT"
  printf '  aws s3 ls --recursive --human-readable --summarize %s --profile %s --endpoint-url %s\n' \
    "$DEST_PREFIX" "$PROFILE" "$ENDPOINT_URL"
}

print_final_check() {
  printf '\nFinal public manifest check:\n'
  printf '  curl -sI https://basin-delineations-public.upstream.tech/merit-basins/0.1.0/manifest.json\n'
}

main() {
  local approval
  local publish_approval

  printf 'This will upload MERIT HFX 0.1.0 artifacts from:\n  %s\n' "$SOURCE_ROOT"
  printf 'Destination:\n  %s\n\n' "$DEST_PREFIX"
  printf 'Type UPLOAD MERIT 0.1.0 to continue: '
  read -r approval
  if [[ "$approval" != "UPLOAD MERIT 0.1.0" ]]; then
    printf 'aborted\n' >&2
    exit 1
  fi

  require_file graph.arrow
  require_file snap.parquet
  require_file catchments.parquet
  require_file flow_dir.tif
  require_file flow_acc.tif
  require_file manifest.json

  upload_artifact graph.arrow application/vnd.apache.arrow.file
  upload_artifact snap.parquet application/vnd.apache.parquet
  upload_artifact catchments.parquet application/vnd.apache.parquet
  upload_artifact flow_dir.tif image/tiff
  upload_artifact flow_acc.tif image/tiff

  print_parity_check

  printf '\nAfter parity check passes, type PUBLISH MANIFEST to publish manifest.json: '
  read -r publish_approval
  if [[ "$publish_approval" != "PUBLISH MANIFEST" ]]; then
    printf 'manifest upload skipped\n' >&2
    exit 1
  fi

  upload_manifest
  print_final_check
}

main "$@"
