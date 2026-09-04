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

fixtures=$SCRIPT_DIR/fixtures/hcloud
projection=$SCRIPT_DIR/hcloud-identity.jq
[[ -f "$projection" ]] || die 'hcloud-identity.jq is missing'
for fixture in server-describe-pourpoint-web-1 server-describe-legacy-datacenter server-type-describe-ccx33 location-describe-fsn1; do
    [[ -f "$fixtures/$fixture.json" ]] || die "fixture $fixture.json is missing"
done

# The projection is the single source of truth the runbook applies after provisioning.
[[ $(jq -c --arg kind server -f "$projection" "$fixtures/server-describe-pourpoint-web-1.json") == \
    '{"id":1,"name":"pourpoint-web-1","server_type":"cx33","location":"fsn1","volumes":[]}' ]] ||
    die 'server projection of the recorded hcloud v1.66.0 shape is wrong'
[[ $(jq -c --arg kind server-type -f "$projection" "$fixtures/server-type-describe-ccx33.json") == \
    '{"name":"ccx33","cores":8,"cpu_type":"dedicated","architecture":"x86","locations":["fsn1","nbg1","hel1","ash","hil","sin"]}' ]] ||
    die 'server-type projection of the recorded hcloud v1.66.0 shape is wrong'
[[ $(jq -c --arg kind location -f "$projection" "$fixtures/location-describe-fsn1.json") == '{"name":"fsn1","network_zone":"eu-central"}' ]] ||
    die 'location projection of the recorded hcloud v1.66.0 shape is wrong'
attached_volume='{"id":7,"name":"hfx-build-x-data","size":600,"location":{"id":1,"name":"fsn1"},"server":3,"linux_device":"/dev/disk/by-id/scsi-0HC_Volume_7"}'
[[ $(jq -c --arg kind volume -f "$projection" <<<"$attached_volume") == '{"id":7,"name":"hfx-build-x-data","size":600,"location":"fsn1","server":3}' ]] ||
    die 'volume projection is wrong'
if jq -c --arg kind server -f "$projection" "$fixtures/server-describe-legacy-datacenter.json" >"$stdout" 2>"$stderr"; then
    die 'legacy .datacenter shape projected without refusal'
fi
assert_contains "$stderr" 'hcloud server field location.name is null, expected string'
if jq -c --arg kind volume -f "$projection" <<<"${attached_volume/\"server\":3/\"server\":null}" >"$stdout" 2>"$stderr"; then
    die 'detached volume projected without refusal'
fi
assert_contains "$stderr" 'hcloud volume field server is null, expected number'
if jq -c --arg kind server -f "$projection" <<<'{"id":"1","name":"x"}' >"$stdout" 2>"$stderr"; then
    die 'string id projected without refusal'
fi
assert_contains "$stderr" 'hcloud server field id is string, expected number'
if jq -c --arg kind server -f "$projection" <<<'[]' >"$stdout" 2>"$stderr"; then
    die 'array projected without refusal'
fi
assert_contains "$stderr" 'hcloud server description is array, expected object'
if jq -c --arg kind image -f "$projection" <<<'{}' >"$stdout" 2>"$stderr"; then
    die 'unknown kind projected without refusal'
fi
assert_contains "$stderr" 'unknown hcloud identity kind image'
pass 'hcloud-identity.jq projects the recorded hcloud v1.66.0 shape and refuses null, mistyped, legacy, and unknown shapes'

cat >"$tmp/bin/hcloud" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$HFX_TEST_HCLOUD_CALLS"
if [[ $* == 'context active' ]]; then
    printf '%s\n' "${HFX_TEST_ACTIVE_CONTEXT:-pourpoint}"
elif [[ $* == 'version' ]]; then
    printf '%s\n' 'hcloud v1.66.0'
elif [[ $* == '--context pourpoint server describe pourpoint-web-1 -o json' ]]; then
    [[ ${HCLOUD_TOKEN-} == TEST-HCLOUD-TOKEN-DO-NOT-LOG ]] || exit 1
    cat "$HFX_TEST_SERVER_FIXTURE"
elif [[ $* == '--context pourpoint server-type describe ccx33 -o json' ]]; then
    [[ ${HCLOUD_TOKEN-} == TEST-HCLOUD-TOKEN-DO-NOT-LOG ]] || exit 1
    cat "$HFX_TEST_FIXTURES/server-type-describe-ccx33.json"
elif [[ $* == '--context pourpoint location describe fsn1 -o json' ]]; then
    [[ ${HCLOUD_TOKEN-} == TEST-HCLOUD-TOKEN-DO-NOT-LOG ]] || exit 1
    cat "$HFX_TEST_FIXTURES/location-describe-fsn1.json"
else
    exit 1
fi
MOCK
chmod +x "$tmp/bin/hcloud"
export HFX_TEST_FIXTURES=$fixtures
: >"$HFX_TEST_HCLOUD_CALLS"
HFX_TEST_SERVER_FIXTURE=$fixtures/server-describe-pourpoint-web-1.json PATH="$tmp/bin:$PATH" \
    "$runner" --check hcloud-json-shape >"$stdout" 2>"$stderr"
assert_contains "$stdout" 'hcloud-version: hcloud v1.66.0'
assert_contains "$stdout" 'server-identity: {"id":1,"name":"pourpoint-web-1","server_type":"cx33","location":"fsn1","volumes":[]}'
assert_contains "$stdout" 'server-type-identity: {"name":"ccx33","cores":8,"cpu_type":"dedicated"'
assert_contains "$stdout" 'location-identity: {"name":"fsn1","network_zone":"eu-central"}'
assert_contains "$stdout" 'hcloud-json-shape: PASS'
assert_not_contains "$stdout" 'TEST-HCLOUD-TOKEN-DO-NOT-LOG'
assert_not_contains "$stderr" 'TEST-HCLOUD-TOKEN-DO-NOT-LOG'
diff -u <(printf '%s\n' 'context active' 'version' \
    '--context pourpoint server describe pourpoint-web-1 -o json' \
    '--context pourpoint server-type describe ccx33 -o json' \
    '--context pourpoint location describe fsn1 -o json') \
    "$HFX_TEST_HCLOUD_CALLS" || die 'shape check performed calls other than the expected read-only describes'
pass 'the shape check proves the projection against read-only describes of the standing server, server type, and location'

: >"$HFX_TEST_HCLOUD_CALLS"
HFX_TEST_SERVER_FIXTURE=$fixtures/server-describe-legacy-datacenter.json PATH="$tmp/bin:$PATH" \
    expect_failure --check hcloud-json-shape
assert_contains "$stderr" 'hcloud-json-shape: FAIL ('
assert_contains "$stderr" 'hcloud server field location.name is null, expected string'
assert_contains "$stderr" 'installed hcloud emits a server JSON shape that hcloud-identity.jq cannot project; refuse to provision'
[[ $(wc -l <"$HFX_TEST_HCLOUD_CALLS" | tr -d ' ') == 3 ]] || die 'shape refusal did not stop at the first mismatched description'
pass 'a legacy .datacenter server shape refuses before provisioning'

: >"$HFX_TEST_HCLOUD_CALLS"
HFX_TEST_ACTIVE_CONTEXT=other HFX_TEST_SERVER_FIXTURE=$fixtures/server-describe-pourpoint-web-1.json PATH="$tmp/bin:$PATH" \
    expect_failure --check hcloud-json-shape
assert_contains "$stderr" 'active hcloud context must be pourpoint'
[[ $(wc -l <"$HFX_TEST_HCLOUD_CALLS" | tr -d ' ') == 1 ]] || die 'wrong context still described resources'
PATH="$tmp/bin:$PATH" expect_failure --evidence-root "$tmp/evidence" --check hcloud-json-shape
assert_contains "$stderr" 'path options are not valid for this check'
pass 'the shape check refuses a wrong context before any description'

expect_failure --check unknown
assert_contains "$stderr" 'unknown check: unknown'
expect_failure --check evidence-root-writable --check evidence-root-writable
assert_contains "$stderr" 'may not be repeated'
pass 'unknown or repeated options refuse'

printf '1..%d\n' "$passed"
printf 'test-verify-campaign-inputs: all %d cases passed\n' "$passed"
