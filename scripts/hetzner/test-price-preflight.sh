#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
set +x

SCRIPT_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
runner=$SCRIPT_DIR/price-preflight.sh
tmp=$(mktemp -d "${TMPDIR:-/tmp}/hfx-price-preflight-test.XXXXXX")
cleanup() { rm -rf -- "$tmp"; }
trap cleanup EXIT
mkdir "$tmp/bin" "$tmp/records"
stdout=$tmp/stdout
stderr=$tmp/stderr
passed=0
token='TEST-HCLOUD-TOKEN-DO-NOT-LOG'

die() { printf 'test-price-preflight: error: %s\n' "$1" >&2; exit 1; }
pass() { passed=$((passed + 1)); printf 'ok %d - %s\n' "$passed" "$1"; }
assert_contains() { grep -F -- "$2" "$1" >/dev/null || die "missing '$2' in $1"; }
assert_not_contains() { ! grep -F -- "$2" "$1" >/dev/null || die "secret '$2' leaked in $1"; }
run() { PATH="$tmp/bin:$PATH" "$runner" "$@" >"$stdout" 2>"$stderr"; }
expect_status() {
    local expected=$1
    shift
    local status=0
    run "$@" || status=$?
    [[ "$status" -eq "$expected" ]] || die "expected exit $expected, observed $status for: $* ($(cat "$stderr"))"
}

# Keychain and price source fakes. The curl fake records argv and reads the
# config from stdin, so the test can prove the token never travels through argv.
cat >"$tmp/bin/security" <<'MOCK'
#!/usr/bin/env bash
[[ $* == 'find-generic-password -s hetzner-cloud-pourpoint -a pourpoint-bootstrap -w' ]] || exit 1
printf '%s\n' 'TEST-HCLOUD-TOKEN-DO-NOT-LOG'
MOCK
cat >"$tmp/bin/curl" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$*" >"$HFX_TEST_CURL_ARGV"
config=$(cat)
[[ "$config" == *'header = "Authorization: Bearer TEST-HCLOUD-TOKEN-DO-NOT-LOG"'* ]] || exit 22
[[ "$config" == *'url = "https://api.hetzner.cloud/v1/pricing"'* ]] || exit 22
[[ -z ${HFX_TEST_CURL_FAIL-} ]] || exit 22
cat "$HFX_TEST_PRICE_FIXTURE"
MOCK
for poison in hcloud aws ssh; do
    printf '#!/usr/bin/env bash\nexit 1\n' >"$tmp/bin/$poison"
done
chmod +x "$tmp/bin/security" "$tmp/bin/curl" "$tmp/bin/hcloud" "$tmp/bin/aws" "$tmp/bin/ssh"
export HFX_TEST_CURL_ARGV=$tmp/curl-argv
export HFX_TEST_PRICE_FIXTURE=$tmp/pricing.json

# Shape of https://api.hetzner.cloud/v1/pricing with the values observed on 2026-08-19.
cat >"$HFX_TEST_PRICE_FIXTURE" <<'JSON'
{
  "pricing": {
    "currency": "EUR",
    "vat_rate": "8.100000",
    "primary_ips": [
      {"type": "ipv4", "prices": [
        {"location": "fsn1", "price_hourly": {"net": "0.0008000000", "gross": "0.0008648000"}, "price_monthly": {"net": "0.5000000000", "gross": "0.5405000000"}},
        {"location": "nbg1", "price_hourly": {"net": "0.0008000000", "gross": "0.0008648000"}, "price_monthly": {"net": "0.5000000000", "gross": "0.5405000000"}}
      ]},
      {"type": "ipv6", "prices": [{"location": "fsn1", "price_hourly": {"net": "0.0000000000", "gross": "0.0000000000"}, "price_monthly": {"net": "0.0000000000", "gross": "0.0000000000"}}]}
    ],
    "server_types": [
      {"id": 1, "name": "cx33", "prices": [{"location": "fsn1", "price_hourly": {"net": "0.0100000000", "gross": "0.0108100000"}, "price_monthly": {"net": "6.0000000000", "gross": "6.4860000000"}, "included_traffic": 21990232555520, "price_per_tb_traffic": {"net": "1.0000000000", "gross": "1.0810000000"}}]},
      {"id": 2, "name": "ccx33", "prices": [
        {"location": "fsn1", "price_hourly": {"net": "0.2219000000", "gross": "0.2398739000"}, "price_monthly": {"net": "138.0000000000", "gross": "149.1780000000"}, "included_traffic": 32985348833280, "price_per_tb_traffic": {"net": "1.0000000000", "gross": "1.0810000000"}},
        {"location": "ash", "price_hourly": {"net": "0.2500000000", "gross": "0.2702500000"}, "price_monthly": {"net": "150.0000000000", "gross": "162.1500000000"}, "included_traffic": 1099511627776, "price_per_tb_traffic": {"net": "1.0000000000", "gross": "1.0810000000"}}
      ]}
    ],
    "volume": {"price_per_gb_month": {"net": "0.0572000000", "gross": "0.0618332000"}}
  }
}
JSON

expect_status 0 --hours 72 --billable-outbound-bytes 400000000000 --ceiling-eur 40.00 --out "$tmp/records/preflight.json"
assert_contains "$stdout" 'decision=permit'
assert_contains "$stdout" "price_record=$tmp/records/preflight.json"
assert_not_contains "$stdout" "$token"
assert_not_contains "$stderr" "$token"
assert_not_contains "$HFX_TEST_CURL_ARGV" "$token"
assert_not_contains "$tmp/records/preflight.json" "$token"
grep -F -- '--config -' "$HFX_TEST_CURL_ARGV" >/dev/null || die 'curl did not read its config from stdin'
jq -e '
  .schema_version == 2 and .currency == "EUR" and .vat_included == 1 and .vat_rate_percent == "8.100000" and
  .server_type == "ccx33" and .location == "fsn1" and .volume_size_gb == 600 and .wall_clock_ceiling_hours == 72 and
  .gross_server_eur_per_hour == "0.2398739000" and .gross_volume_eur_per_gb_month == "0.0618332000" and
  .gross_ipv4_eur_per_hour == "0.0008648000" and .included_outbound_bytes == 32985348833280 and
  .gross_outbound_eur_per_unit == "1.0810000000" and .outbound_billing_unit_bytes == 1000000000000 and
  .billable_outbound_estimate_bytes == 400000000000 and .outbound_overage_units == 0 and
  .gross_cost_ceiling_eur == "40.00" and .decision == "permit" and .provisioning_request_epoch == null and
  .elapsed_hours == null and .conservative_actual_gross_eur == null
' "$tmp/records/preflight.json" >/dev/null || die 'projection record fields are wrong'
# 0.2398739 * 72 + 600 * 0.0618332 * 72 / 730 + 0.0008648 * 72 = 17.2709208 + 3.6591542 + 0.0622656
awk -v total="$(jq -r '.projected_gross_total_eur' "$tmp/records/preflight.json")" \
    'BEGIN { exit !(total > 20.9923 && total < 20.9924) }' || die 'projected gross total is not 20.9923...'
pass 'a current price list projects the 72-hour gross cost and permits below the ceiling'

expect_status 3 --hours 72 --billable-outbound-bytes 400000000000 --ceiling-eur 20.99 --out "$tmp/records/refuse.json"
assert_contains "$stdout" 'decision=refuse'
assert_contains "$stderr" 'not strictly below its ceiling'
jq -e '.decision == "refuse"' "$tmp/records/refuse.json" >/dev/null || die 'refusal record missing'
expect_status 3 --hours 72 --billable-outbound-bytes 400000000000 --ceiling-eur 20.99235659178082 --out "$tmp/records/equal.json"
pass 'a projection at or above the ceiling refuses with exit 3 and a record'

expect_status 0 --hours 72 --billable-outbound-bytes 34985348833280 --ceiling-eur 40.00 --out "$tmp/records/overage.json"
jq -e '.outbound_overage_units == 2 and .gross_outbound_eur == "2.1620000000"' "$tmp/records/overage.json" >/dev/null ||
    die 'outbound overage was not rounded up per billing unit'
pass 'outbound bytes above the included allowance bill in rounded-up units'

origin=$(( $(date +%s) - 7200 ))
expect_status 0 --hours 72 --billable-outbound-bytes 400000000000 --ceiling-eur 40.00 \
    --provisioning-request-epoch "$origin" --out "$tmp/records/elapsed.json"
assert_contains "$stdout" 'elapsed_hours='
assert_contains "$stdout" 'conservative_actual_gross_eur='
jq -e --argjson origin "$origin" '.provisioning_request_epoch == $origin and .elapsed_hours > 1.99 and .elapsed_hours < 2.1' \
    "$tmp/records/elapsed.json" >/dev/null || die 'elapsed hours are wrong'
# three billed hours * (0.2398739 + 600 * 0.0618332 / 730 + 0.0008648) = 3 * 0.2915605 = 0.8746815
awk -v actual="$(jq -r '.conservative_actual_gross_eur' "$tmp/records/elapsed.json")" \
    'BEGIN { exit !(actual > 0.8746 && actual < 0.8748) }' || die 'conservative actual spend is wrong'
pass 'a provisioning epoch adds elapsed hours and a conservative actual spend'

expect_status 3 --hours 72 --billable-outbound-bytes 400000000000 --ceiling-eur 40.00 \
    --provisioning-request-epoch "$(( $(date +%s) - 259200 ))" --out "$tmp/records/timeout.json"
assert_contains "$stdout" 'decision=refuse'
expect_status 3 --hours 1 --billable-outbound-bytes 0 --ceiling-eur 1.00 \
    --provisioning-request-epoch "$origin" --out "$tmp/records/actual-over.json"
expect_status 1 --hours 72 --billable-outbound-bytes 0 --ceiling-eur 40.00 \
    --provisioning-request-epoch "$(( $(date +%s) + 3600 ))" --out "$tmp/records/future.json"
assert_contains "$stderr" 'provisioning request epoch is in the future'
pass 'elapsed time at the ceiling, actual spend at the ceiling, and a future epoch refuse'

jq '.pricing.server_types[1].prices[0].price_hourly.gross = null' "$HFX_TEST_PRICE_FIXTURE" >"$tmp/missing.json"
HFX_TEST_PRICE_FIXTURE=$tmp/missing.json expect_status 1 --hours 72 --billable-outbound-bytes 0 --ceiling-eur 40.00 --out "$tmp/records/missing.json"
assert_contains "$stderr" 'missing a required value'
[[ ! -e "$tmp/records/missing.json" ]] || die 'a record was written for a malformed price list'
jq '.pricing.currency = "USD"' "$HFX_TEST_PRICE_FIXTURE" >"$tmp/usd.json"
HFX_TEST_PRICE_FIXTURE=$tmp/usd.json expect_status 1 --hours 72 --billable-outbound-bytes 0 --ceiling-eur 40.00 --out "$tmp/records/usd.json"
assert_contains "$stderr" 'currency is not EUR'
expect_status 1 --hours 72 --billable-outbound-bytes 0 --ceiling-eur 40.00 --location xyz --out "$tmp/records/xyz.json"
assert_contains "$stderr" 'missing a required value'
HFX_TEST_CURL_FAIL=1 expect_status 1 --hours 72 --billable-outbound-bytes 0 --ceiling-eur 40.00 --out "$tmp/records/curl.json"
assert_contains "$stderr" 'could not retrieve the current Hetzner Cloud price list'
assert_not_contains "$stderr" "$token"
pass 'a missing price, a foreign currency, an unpriced location, or a fetch failure refuses without estimating'

expect_status 1 --hours 0 --billable-outbound-bytes 0 --ceiling-eur 40.00 --out "$tmp/records/zero.json"
assert_contains "$stderr" 'greater than zero'
expect_status 1 --hours 72 --billable-outbound-bytes 1.5 --ceiling-eur 40.00 --out "$tmp/records/bytes.json"
assert_contains "$stderr" 'invalid --billable-outbound-bytes'
expect_status 1 --hours 72 --billable-outbound-bytes 0 --ceiling-eur NaN --out "$tmp/records/nan.json"
assert_contains "$stderr" 'invalid --ceiling-eur'
expect_status 1 --hours 72 --billable-outbound-bytes 0 --ceiling-eur 40.00 --out relative.json
assert_contains "$stderr" '--out must be an absolute path'
expect_status 1 --hours 72 --billable-outbound-bytes 0 --ceiling-eur 40.00 --out "$tmp/records/preflight.json"
assert_contains "$stderr" '--out already exists'
expect_status 1 --hours 72 --hours 72 --billable-outbound-bytes 0 --ceiling-eur 40.00 --out "$tmp/records/dup.json"
assert_contains "$stderr" 'may not be repeated'
pass 'malformed hours, bytes, ceiling, or record path refuse before any fetch'

printf '1..%d\n' "$passed"
printf 'test-price-preflight: all %d cases passed\n' "$passed"
