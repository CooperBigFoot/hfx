#!/usr/bin/env bash
# compare : (LeftDatasetTree, RightDatasetTree, ExpectedDigests?) -> Identical | CreatedAtOnly | Different
#
# Compares two HFX dataset trees file by file with SHA-256. Every regular file
# must exist on both sides with the same relative path and digest. manifest.json
# may differ only in its embedded created_at when the caller allows it, because
# the adapter stamps the build time into the manifest. The verdict JSON goes to
# stdout; diagnostics go to stderr. Nothing is written or deleted.

set -Eeuo pipefail
IFS=$'\n\t'
set +x

SCRIPT_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/hetzner/common.sh
source "$SCRIPT_DIR/common.sh"

usage() {
    cat <<'USAGE'
Usage: compare-dataset-trees.sh --left <dataset-root> --right <dataset-root>
                                [--expected-sha256 <json>] [--allow-created-at-difference]

Options:
  --left <dataset-root>          reference dataset tree (absolute path)
  --right <dataset-root>         candidate dataset tree (absolute path)
  --expected-sha256 <json>       object of relative path to SHA-256 that the left
                                 tree must match exactly
  --allow-created-at-difference  accept a manifest.json that differs only in created_at
  -h, --help                     print usage and exit 0

Exit status: 0 when the verdict is identical, or created-at-only with the allowance;
1 otherwise.
USAGE
}

hfx_require_command jq
hfx_require_command find

sha256_of() {
    local file=$1
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum -- "$file" | cut -c1-64
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 -- "$file" | cut -c1-64
    else
        hfx_die 'neither sha256sum nor shasum is available on PATH'
    fi
}

validate_tree_root() {
    local label=$1
    local path=$2
    [[ -n "$path" ]] || hfx_die "--$label is required"
    [[ "$path" == /* ]] || hfx_die "--$label must be an absolute path: $path"
    [[ -d "$path" && ! -L "$path" ]] || hfx_die "--$label is not a regular directory: $path"
    cd -P -- "$path" && pwd -P
}

# Prints "relative-path<TAB>sha256" for every regular file below the root, sorted by path.
digest_tree() {
    local root=$1
    local entry
    local relative
    while IFS= read -r -d '' entry; do
        [[ ! -L "$entry" ]] || hfx_die "dataset tree contains a symlink: $entry"
        [[ -f "$entry" ]] || hfx_die "dataset tree contains a non-regular entry: $entry"
        relative=${entry#"$root/"}
        [[ "$relative" != "$entry" && ! "$relative" =~ [[:cntrl:]] ]] ||
            hfx_die "dataset tree contains an unsafe relative path: $entry"
        printf '%s\t%s\n' "$relative" "$(sha256_of "$entry")"
    done < <(find "$root" -mindepth 1 \( -type f -o -type l -o ! -type d \) -print0) |
        LC_ALL=C sort -t $'\t' -k1,1
}

manifest_matches_without_created_at() {
    local left=$1
    local right=$2
    local left_canonical
    local right_canonical
    left_canonical=$(jq -cS 'del(.created_at)' "$left" 2>/dev/null) || return 1
    right_canonical=$(jq -cS 'del(.created_at)' "$right" 2>/dev/null) || return 1
    jq -e 'has("created_at")' "$left" >/dev/null 2>&1 || return 1
    jq -e 'has("created_at")' "$right" >/dev/null 2>&1 || return 1
    [[ "$left_canonical" == "$right_canonical" ]]
}

left=
right=
expected=
allow_created_at=0
while (($#)); do
    case $1 in
        -h | --help)
            usage
            exit 0
            ;;
        --left | --right | --expected-sha256)
            (($# >= 2)) || hfx_die "option $1 requires a value"
            case $1 in
                --left) [[ -z "$left" ]] || hfx_die 'option --left may not be repeated'; left=$2 ;;
                --right) [[ -z "$right" ]] || hfx_die 'option --right may not be repeated'; right=$2 ;;
                --expected-sha256) [[ -z "$expected" ]] || hfx_die 'option --expected-sha256 may not be repeated'; expected=$2 ;;
            esac
            shift 2
            ;;
        --allow-created-at-difference)
            allow_created_at=1
            shift
            ;;
        *) hfx_die "unknown argument: $1" ;;
    esac
done

left=$(validate_tree_root left "$left")
right=$(validate_tree_root right "$right")
[[ "$left" != "$right" ]] || hfx_die 'left and right resolve to the same directory'
if [[ -n "$expected" ]]; then
    [[ "$expected" == /* ]] || hfx_die "--expected-sha256 must be an absolute path: $expected"
    [[ -f "$expected" && ! -L "$expected" && -s "$expected" ]] ||
        hfx_die "--expected-sha256 is not a nonempty regular file: $expected"
    jq -e 'type == "object" and length > 0 and all(.[]; type == "string" and test("^[0-9a-f]{64}$"))' \
        "$expected" >/dev/null 2>&1 || hfx_die "expected digest record is malformed: $expected"
fi

left_digests=$(digest_tree "$left")
right_digests=$(digest_tree "$right")
[[ -n "$left_digests" ]] || hfx_die "left dataset tree contains no files: $left"
[[ -n "$right_digests" ]] || hfx_die "right dataset tree contains no files: $right"

files_json=$(
    jq -n -R --rawfile left_text <(printf '%s\n' "$left_digests") \
        --rawfile right_text <(printf '%s\n' "$right_digests") '
        def rows($text): [$text | split("\n") | .[] | select(length > 0) | split("\t") | {path: .[0], sha256: .[1]}];
        (rows($left_text) | map({key: .path, value: .sha256}) | from_entries) as $l |
        (rows($right_text) | map({key: .path, value: .sha256}) | from_entries) as $r |
        (($l | keys) + ($r | keys) | unique) as $paths |
        [$paths[] | {
            path: .,
            left_sha256: ($l[.] // null),
            right_sha256: ($r[.] // null),
            verdict: (if ($l[.] == null) or ($r[.] == null) then "missing"
                      elif $l[.] == $r[.] then "identical"
                      else "different" end)
        }]'
)

# Re-classify a manifest.json that differs only in created_at.
manifest_verdict=$(jq -r '.[] | select(.path == "manifest.json") | .verdict' <<<"$files_json")
if [[ "$manifest_verdict" == different ]] &&
    manifest_matches_without_created_at "$left/manifest.json" "$right/manifest.json"; then
    files_json=$(jq '(.[] | select(.path == "manifest.json") | .verdict) = "created-at-only"' <<<"$files_json")
fi

expected_match=null
if [[ -n "$expected" ]]; then
    expected_match=$(
        jq --slurpfile expected "$expected" '
            (map(select(.left_sha256 != null) | {key: .path, value: .left_sha256}) | from_entries) as $left |
            ($expected[0] == $left)
        ' <<<"$files_json"
    )
fi

verdict=$(
    jq -r --argjson allow "$allow_created_at" '
        map(.verdict) as $verdicts |
        if any($verdicts[]; . == "missing" or . == "different") then "different"
        elif any($verdicts[]; . == "created-at-only") then
            (if $allow == 1 then "created-at-only" else "different" end)
        else "identical" end
    ' <<<"$files_json"
)

jq -n --arg left "$left" --arg right "$right" --arg verdict "$verdict" \
    --argjson allow "$allow_created_at" --argjson expected_match "$expected_match" \
    --argjson files "$files_json" '{
        schema_version: 1,
        left: $left,
        right: $right,
        created_at_difference_allowed: ($allow == 1),
        left_matches_expected_sha256: $expected_match,
        verdict: $verdict,
        files: $files
    }'

if [[ -n "$expected" && "$expected_match" != true ]]; then
    hfx_die 'left dataset tree does not match the expected SHA-256 record'
fi
case $verdict in
    identical) hfx_log 'dataset trees are byte-identical' ;;
    created-at-only) hfx_log 'dataset trees are identical except manifest.json created_at' ;;
    *) hfx_die 'dataset trees differ' ;;
esac
