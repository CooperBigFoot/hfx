#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
set +x

SCRIPT_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
runner=$SCRIPT_DIR/compare-dataset-trees.sh
tmp=$(mktemp -d "${TMPDIR:-/tmp}/hfx-compare-dataset-trees-test.XXXXXX")
cleanup() { rm -rf -- "$tmp"; }
trap cleanup EXIT
stdout=$tmp/stdout
stderr=$tmp/stderr
passed=0

die() { printf 'test-compare-dataset-trees: error: %s\n' "$1" >&2; exit 1; }
pass() { passed=$((passed + 1)); printf 'ok %d - %s\n' "$passed" "$1"; }
assert_contains() { grep -F -- "$2" "$1" >/dev/null || die "missing '$2' in $1"; }
expect_status() {
    local expected=$1
    shift
    local status=0
    "$runner" "$@" >"$stdout" 2>"$stderr" || status=$?
    [[ "$status" -eq "$expected" ]] || die "expected exit $expected, observed $status for: $* ($(cat "$stderr"))"
}
sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum -- "$1" | cut -c1-64; else shasum -a 256 -- "$1" | cut -c1-64; fi
}
make_tree() {
    local root=$1
    local created_at=$2
    mkdir -p "$root/aux"
    printf 'catchments-bytes\n' >"$root/catchments.parquet"
    printf 'graph-bytes\n' >"$root/graph.parquet"
    printf 'stems-bytes\n' >"$root/aux/snap_stems.parquet"
    printf '{"format_version":"0.3.0","region":"7020000010","unit_count":331263,"created_at":"%s","bbox":[1,2,3,4]}\n' "$created_at" >"$root/manifest.json"
}

make_tree "$tmp/reference" '2026-08-19T14:03:32.603740+00:00'
cp -R "$tmp/reference" "$tmp/identical"
make_tree "$tmp/rebuilt" '2026-09-10T00:00:00.000000+00:00'
jq -n --arg a "$(sha256_of "$tmp/reference/aux/snap_stems.parquet")" --arg c "$(sha256_of "$tmp/reference/catchments.parquet")" \
    --arg g "$(sha256_of "$tmp/reference/graph.parquet")" --arg m "$(sha256_of "$tmp/reference/manifest.json")" \
    '{"aux/snap_stems.parquet":$a,"catchments.parquet":$c,"graph.parquet":$g,"manifest.json":$m}' >"$tmp/expected.json"

expect_status 0 --left "$tmp/reference" --right "$tmp/identical" --expected-sha256 "$tmp/expected.json"
jq -e '.verdict == "identical" and .left_matches_expected_sha256 == true and .created_at_difference_allowed == false and
    (.files | length == 4) and all(.files[]; .verdict == "identical") and
    (.files | map(.path)) == ["aux/snap_stems.parquet","catchments.parquet","graph.parquet","manifest.json"]' "$stdout" >/dev/null ||
    die 'identical trees were not reported as identical'
assert_contains "$stderr" 'dataset trees are byte-identical'
pass 'byte-identical trees report identical and match the expected digest record'

expect_status 1 --left "$tmp/reference" --right "$tmp/rebuilt"
jq -e '.verdict == "different" and (.files[] | select(.path == "manifest.json") | .verdict == "created-at-only")' "$stdout" >/dev/null ||
    die 'created_at-only difference was not classified'
assert_contains "$stderr" 'dataset trees differ'
expect_status 0 --left "$tmp/reference" --right "$tmp/rebuilt" --allow-created-at-difference --expected-sha256 "$tmp/expected.json"
jq -e '.verdict == "created-at-only" and .created_at_difference_allowed == true and .left_matches_expected_sha256 == true and
    all(.files[] | select(.path != "manifest.json"); .verdict == "identical")' "$stdout" >/dev/null ||
    die 'created_at-only difference was not accepted with the allowance'
assert_contains "$stderr" 'identical except manifest.json created_at'
pass 'a manifest that differs only in created_at is refused by default and accepted with the allowance'

cp -R "$tmp/rebuilt" "$tmp/content-drift"
printf 'graph-bytes-changed\n' >"$tmp/content-drift/graph.parquet"
expect_status 1 --left "$tmp/reference" --right "$tmp/content-drift" --allow-created-at-difference
jq -e '.verdict == "different" and (.files[] | select(.path == "graph.parquet") | .verdict == "different")' "$stdout" >/dev/null ||
    die 'a data file difference was not reported'
cp -R "$tmp/reference" "$tmp/manifest-drift"
jq '.unit_count = 1 | .created_at = "2026-09-10T00:00:00+00:00"' "$tmp/reference/manifest.json" >"$tmp/manifest-drift/manifest.json"
expect_status 1 --left "$tmp/reference" --right "$tmp/manifest-drift" --allow-created-at-difference
jq -e '.files[] | select(.path == "manifest.json") | .verdict == "different"' "$stdout" >/dev/null ||
    die 'a manifest field difference hid behind created_at'
pass 'a data file or manifest field difference is different even with the allowance'

cp -R "$tmp/reference" "$tmp/missing-file"
rm "$tmp/missing-file/aux/snap_stems.parquet"
expect_status 1 --left "$tmp/reference" --right "$tmp/missing-file"
jq -e '.verdict == "different" and (.files[] | select(.path == "aux/snap_stems.parquet") | .verdict == "missing" and .right_sha256 == null)' "$stdout" >/dev/null ||
    die 'a missing file was not reported'
cp -R "$tmp/reference" "$tmp/extra-file"
printf 'extra\n' >"$tmp/extra-file/extra.parquet"
expect_status 1 --left "$tmp/reference" --right "$tmp/extra-file"
jq -e '.files[] | select(.path == "extra.parquet") | .verdict == "missing" and .left_sha256 == null' "$stdout" >/dev/null ||
    die 'an extra file was not reported'
pass 'a missing or extra file on either side is different'

jq '.["graph.parquet"] = "0000000000000000000000000000000000000000000000000000000000000000"' "$tmp/expected.json" >"$tmp/wrong-expected.json"
expect_status 1 --left "$tmp/reference" --right "$tmp/identical" --expected-sha256 "$tmp/wrong-expected.json"
jq -e '.verdict == "identical" and .left_matches_expected_sha256 == false' "$stdout" >/dev/null || die 'expected mismatch not reported'
assert_contains "$stderr" 'does not match the expected SHA-256 record'
printf '{"graph.parquet":"short"}\n' >"$tmp/bad-expected.json"
expect_status 1 --left "$tmp/reference" --right "$tmp/identical" --expected-sha256 "$tmp/bad-expected.json"
assert_contains "$stderr" 'expected digest record is malformed'
pass 'the reference tree must match the expected digest record exactly'

cp -R "$tmp/reference" "$tmp/symlinked"
ln -s "$tmp/reference/graph.parquet" "$tmp/symlinked/link.parquet"
expect_status 1 --left "$tmp/reference" --right "$tmp/symlinked"
assert_contains "$stderr" 'contains a symlink'
expect_status 1 --left "$tmp/reference" --right "$tmp/reference"
assert_contains "$stderr" 'resolve to the same directory'
expect_status 1 --left relative --right "$tmp/reference"
assert_contains "$stderr" '--left must be an absolute path'
expect_status 1 --left "$tmp/reference"
assert_contains "$stderr" '--right is required'
mkdir "$tmp/empty"
expect_status 1 --left "$tmp/reference" --right "$tmp/empty"
assert_contains "$stderr" 'contains no files'
expect_status 1 --left "$tmp/reference" --right "$tmp/identical" --bogus
assert_contains "$stderr" 'unknown argument'
pass 'symlinks, aliased roots, relative paths, empty trees, and unknown arguments refuse'

printf '1..%d\n' "$passed"
printf 'test-compare-dataset-trees: all %d cases passed\n' "$passed"
