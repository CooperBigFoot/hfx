#!/usr/bin/env bash
# CP-2 runbook (G2 in planning/human-gated-actions.md).
# DO NOT EXECUTE WITHOUT HUMAN APPROVAL — fired only by a human via --execute.
#
# IRREVERSIBLE: deletes the legacy R2 prefixes grit/1.0.0/, merit-basins/0.1.0/
# and merit/0.2.0/ (~153 GB / 137 objects). Deletion candidates are enumerated
# at runtime (never hardcoded) and intersected with the allow-list below; an
# unconditional guard aborts if any candidate falls under grit/2.0.0/.
#
# Default mode is --dry-run, which only lists candidates (read-only). The
# endpoint is baked into ~/.aws/config for the default profile, so no endpoint
# flag is needed.

set -euo pipefail

usage() {
  printf 'usage: %s [--dry-run | --execute] [--profile <name>]\n' "${BASH_SOURCE[0]}"
  printf '  --dry-run   list deletion candidates and per-prefix counts (default)\n'
  printf '  --execute   delete candidates after a per-prefix typed confirmation\n'
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

BUCKET="basin-delineations-public"

# The ONLY prefixes this script may ever delete from. Nothing else, ever.
ALLOW_LIST=(
  "grit/1.0.0/"
  "merit-basins/0.1.0/"
  "merit/0.2.0/"
)

# Runtime enumeration (both modes) — never a hardcoded object list.
ENUMERATED_KEYS="$(aws s3 ls "s3://$BUCKET/" --recursive --profile "$PROFILE" | awk '{print $4}')"

# Candidates = enumerated keys ∩ allow-list (string prefix match).
CANDIDATES=()
while IFS= read -r key; do
  [[ -z "$key" ]] && continue
  for prefix in "${ALLOW_LIST[@]}"; do
    if [[ "$key" == "$prefix"* ]]; then
      CANDIDATES+=("$key")
      break
    fi
  done
done <<<"$ENUMERATED_KEYS"

# UNCONDITIONAL GUARD — independent of the allow-list, both modes, before any
# candidate printing or deletion: grit/2.0.0/ is untouchable by construction.
VIOLATIONS=""
if [[ ${#CANDIDATES[@]} -gt 0 ]]; then
  for key in "${CANDIDATES[@]}"; do
    if [[ "$key" =~ ^grit/2\.0\.0/ ]]; then
      VIOLATIONS="$VIOLATIONS$key"$'\n'
    fi
  done
fi
if [[ -n "$VIOLATIONS" ]]; then
  printf 'FATAL: deletion candidate(s) under protected prefix grit/2.0.0/:\n' >&2
  printf '%s' "$VIOLATIONS" >&2
  exit 1
fi

prefix_count() {
  local prefix="$1" key count=0

  if [[ ${#CANDIDATES[@]} -gt 0 ]]; then
    for key in "${CANDIDATES[@]}"; do
      if [[ "$key" == "$prefix"* ]]; then
        count=$((count + 1))
      fi
    done
  fi
  printf '%d' "$count"
}

run_dry_run() {
  local key prefix total=0

  if [[ ${#CANDIDATES[@]} -gt 0 ]]; then
    for key in "${CANDIDATES[@]}"; do
      printf '%s\n' "$key"
    done
  fi
  printf -- '--- per-prefix counts ---\n'
  for prefix in "${ALLOW_LIST[@]}"; do
    printf '%s: %s objects\n' "$prefix" "$(prefix_count "$prefix")"
    total=$((total + $(prefix_count "$prefix")))
  done
  printf 'total: %d objects (dry-run; nothing deleted)\n' "$total"
}

run_execute() {
  local prefix key approval count

  for prefix in "${ALLOW_LIST[@]}"; do
    count="$(prefix_count "$prefix")"
    printf '%s: %s objects to delete\n' "$prefix" "$count"
    printf 'Type DELETE %s to delete this prefix: ' "$prefix"
    read -r approval
    if [[ "$approval" != "DELETE $prefix" ]]; then
      printf 'aborted: confirmation mismatch for %s\n' "$prefix" >&2
      exit 1
    fi
    if [[ ${#CANDIDATES[@]} -gt 0 ]]; then
      for key in "${CANDIDATES[@]}"; do
        if [[ "$key" == "$prefix"* ]]; then
          aws s3 rm "s3://$BUCKET/$key" --profile "$PROFILE"
        fi
      done
    fi
  done
}

if [[ "$MODE" == "dry-run" ]]; then
  run_dry_run
else
  run_execute
fi
