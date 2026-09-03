#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
set +x

SCRIPT_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
runner=$SCRIPT_DIR/verify-campaign-inputs.sh
tmp=$(mktemp -d "${TMPDIR:-/tmp}/hfx-campaign-inputs-test.XXXXXX")
cleanup() { rm -rf -- "$tmp"; }
trap cleanup EXIT
mkdir "$tmp/bin" "$tmp/evidence"
stdout=$tmp/stdout
stderr=$tmp/stderr
passed=0

die() { printf 'test-verify-campaign-inputs: error: %s\n' "$1" >&2; exit 1; }
pass() { passed=$((passed + 1)); printf 'ok %d - %s\n' "$passed" "$1"; }
expect_failure() {
    if "$runner" "$@" >"$stdout" 2>"$stderr"; then
        die "unexpected success: $*"
    fi
}
assert_contains() { grep -F -- "$2" "$1" >/dev/null || die "missing '$2' in $1"; }
assert_not_contains() { ! grep -F -- "$2" "$1" >/dev/null || die "secret '$2' leaked in $1"; }

# Poison every real cloud client so a regression cannot reach the network.
for poison in hcloud aws curl ssh security; do
    cat >"$tmp/bin/$poison" <<MOCK
#!/usr/bin/env bash
printf '%s\n' "\$*" >>"$tmp/poison-$poison.log"
exit 1
MOCK
    chmod +x "$tmp/bin/$poison"
done

expect_failure --evidence-root '' --check evidence-root-writable
assert_contains "$stderr" '--evidence-root must not be empty'
expect_failure --evidence-root relative --check evidence-root-writable
assert_contains "$stderr" '--evidence-root must be absolute'
"$runner" --evidence-root "$tmp/evidence" --check evidence-root-writable >"$stdout"
resolved_evidence=$(cd -P -- "$tmp/evidence" && pwd -P)
assert_contains "$stdout" "evidence-root: $resolved_evidence"
assert_contains "$stdout" 'evidence-root-writable: PASS'
[[ -z $(find "$tmp/evidence" -mindepth 1 -print -quit) ]] || die 'evidence probe was retained'
ln -s "$tmp/evidence" "$tmp/evidence-link"
expect_failure --evidence-root "$tmp/evidence-link" --check evidence-root-writable
assert_contains "$stderr" '--evidence-root must not be a symlink'
expect_failure --evidence-root "$tmp/evidence" --s3-env-file "$tmp/x" --check evidence-root-writable
assert_contains "$stderr" '--s3-env-file is not valid for this check'
pass 'evidence root must be an absolute, existing, non-symlink, writable directory and the probe is removed'

expect_failure --s3-env-file '' --check credential-file-authenticates
assert_contains "$stdout" 'credential-file-authenticates: FAIL'
expect_failure --s3-env-file "$tmp/missing.env" --check credential-file-authenticates
assert_contains "$stdout" 'credential-file-authenticates: FAIL'

access='TEST-ACCESS-DO-NOT-LOG'
secret='TEST-SECRET-DO-NOT-LOG/+'
printf 'AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\n' "$access" "$secret" >"$tmp/credentials.env"
cat >"$tmp/bin/aws" <<'MOCK'
#!/usr/bin/env bash
[[ ${AWS_ACCESS_KEY_ID-} == TEST-ACCESS-DO-NOT-LOG ]]
[[ ${AWS_SECRET_ACCESS_KEY-} == TEST-SECRET-DO-NOT-LOG/+ ]]
[[ $* == 's3 ls s3://pourpoint-hfx --endpoint-url https://fsn1.your-objectstorage.com --region fsn1' ]]
printf 'mock output with %s and %s\n' "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY"
MOCK
chmod +x "$tmp/bin/aws"
PATH="$tmp/bin:$PATH" "$runner" --s3-env-file "$tmp/credentials.env" \
    --check credential-file-authenticates >"$stdout" 2>"$stderr"
assert_contains "$stdout" 'credential-file: present'
assert_not_contains "$stdout" "$tmp/credentials.env"
assert_not_contains "$stderr" "$tmp/credentials.env"
assert_contains "$stdout" 'credential-file-authenticates: PASS'
assert_not_contains "$stdout" "$access"
assert_not_contains "$stdout" "$secret"
assert_not_contains "$stderr" "$access"
assert_not_contains "$stderr" "$secret"
pass 'a well-formed credential file authenticates through one read-only listing without leaking values'

cat >"$tmp/bin/aws" <<'MOCK'
#!/usr/bin/env bash
[[ -z ${AWS_SESSION_TOKEN-} ]] || exit 1
MOCK
chmod +x "$tmp/bin/aws"
AWS_SESSION_TOKEN=STALE-SESSION-TOKEN PATH="$tmp/bin:$PATH" \
    "$runner" --s3-env-file "$tmp/credentials.env" \
    --check credential-file-authenticates >"$stdout" 2>"$stderr"
assert_contains "$stdout" 'credential-file-authenticates: PASS'
pass 'a stale session token in the environment is cleared before the listing'

printf 'AWS_ACCESS_KEY_ID=%s\necho pwned >%s\nAWS_SECRET_ACCESS_KEY=%s\n' \
    "$access" "$tmp/executed" "$secret" >"$tmp/unsafe.env"
PATH="$tmp/bin:$PATH" expect_failure --s3-env-file "$tmp/unsafe.env" --check credential-file-authenticates
[[ ! -e "$tmp/executed" ]] || die 'credential file content was executed'
assert_contains "$stdout" 'credential-file-authenticates: FAIL'
printf 'AWS_ACCESS_KEY_ID=%s\nAWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\n' "$access" "$access" "$secret" >"$tmp/duplicate.env"
PATH="$tmp/bin:$PATH" expect_failure --s3-env-file "$tmp/duplicate.env" --check credential-file-authenticates
printf 'AWS_ACCESS_KEY_ID=%s\n' "$access" >"$tmp/partial.env"
PATH="$tmp/bin:$PATH" expect_failure --s3-env-file "$tmp/partial.env" --check credential-file-authenticates
ln -s "$tmp/credentials.env" "$tmp/credentials-link"
PATH="$tmp/bin:$PATH" expect_failure --s3-env-file "$tmp/credentials-link" --check credential-file-authenticates
[[ ! -e "$tmp/poison-curl.log" && ! -e "$tmp/poison-ssh.log" ]] || die 'credential check reached a poisoned client'
pass 'unsafe, duplicate, partial, or symlinked credential files refuse without execution'

cat >"$tmp/bin/security" <<'MOCK'
#!/usr/bin/env bash
[[ $* == 'find-generic-password -s hetzner-cloud-pourpoint -a pourpoint-bootstrap -w' ]] || exit 1
printf '%s\n' 'TEST-HCLOUD-TOKEN-DO-NOT-LOG'
MOCK
cat >"$tmp/bin/hcloud" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$HFX_TEST_HCLOUD_CALLS"
if [[ $* == 'context active' ]]; then
    printf '%s\n' "${HFX_TEST_ACTIVE_CONTEXT:-pourpoint}"
elif [[ $* == '--context pourpoint server list' || $* == '--context pourpoint volume list' ]]; then
    [[ ${HCLOUD_TOKEN-} == TEST-HCLOUD-TOKEN-DO-NOT-LOG ]] || exit 1
else
    exit 1
fi
MOCK
chmod +x "$tmp/bin/security" "$tmp/bin/hcloud"
export HFX_TEST_HCLOUD_CALLS=$tmp/hcloud-calls
: >"$HFX_TEST_HCLOUD_CALLS"
PATH="$tmp/bin:$PATH" "$runner" --check hcloud-context-resolves >"$stdout" 2>"$stderr"
assert_contains "$stdout" 'hcloud-context-resolves: PASS'
assert_not_contains "$stdout" 'TEST-HCLOUD-TOKEN-DO-NOT-LOG'
assert_not_contains "$stderr" 'TEST-HCLOUD-TOKEN-DO-NOT-LOG'
diff -u <(printf '%s\n' 'context active' '--context pourpoint server list' '--context pourpoint volume list') \
    "$HFX_TEST_HCLOUD_CALLS" || die 'hcloud calls were not the expected read-only operations'
pass 'the hcloud check authenticates from the Keychain and performs only read-only listings'

: >"$HFX_TEST_HCLOUD_CALLS"
HFX_TEST_ACTIVE_CONTEXT=other PATH="$tmp/bin:$PATH" expect_failure --check hcloud-context-resolves
assert_contains "$stderr" 'active hcloud context must be pourpoint'
[[ $(wc -l <"$HFX_TEST_HCLOUD_CALLS" | tr -d ' ') == 1 ]] || die 'wrong context still listed resources'
PATH="$tmp/bin:$PATH" expect_failure --evidence-root "$tmp/evidence" --check hcloud-context-resolves
assert_contains "$stderr" 'path options are not valid for this check'
pass 'a wrong active context refuses before any listing'

expect_failure --check unknown
assert_contains "$stderr" 'unknown check: unknown'
expect_failure --check evidence-root-writable --check evidence-root-writable
assert_contains "$stderr" 'may not be repeated'
pass 'unknown or repeated options refuse'

printf '1..%d\n' "$passed"
printf 'test-verify-campaign-inputs: all %d cases passed\n' "$passed"
