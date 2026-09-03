#!/usr/bin/env bash
# preflight : (CurrentHetznerPrices, CampaignShape, GrossCeilingEUR) -> Permit | Refuse
#
# Fetches the current Hetzner Cloud price list, projects the gross cost of one
# campaign shape (server type, location, volume size, wall-clock hours, billable
# outbound bytes), records inputs and arithmetic as JSON, and refuses when the
# projection or the conservative actual spend is not strictly below the ceiling.
# The project token never appears in argv, stdout, stderr, or the record.

set -Eeuo pipefail
IFS=$'\n\t'
set +x

SCRIPT_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/hetzner/common.sh
source "$SCRIPT_DIR/common.sh"

readonly HFX_PRICE_SOURCE=https://api.hetzner.cloud/v1/pricing
readonly HFX_HOURS_PER_MONTH=730
readonly HFX_OUTBOUND_UNIT_BYTES=1000000000000

usage() {
    cat <<'USAGE'
Usage: price-preflight.sh --hours <decimal> --billable-outbound-bytes <integer>
                          --ceiling-eur <decimal> --out <json-path>
                          [--server-type ccx33] [--location fsn1] [--volume-size-gb 600]
                          [--provisioning-request-epoch <integer>]

Options:
  --hours <decimal>                    wall-clock hours to project (the campaign ceiling)
  --billable-outbound-bytes <integer>  conservative outbound byte estimate
  --ceiling-eur <decimal>              gross cost ceiling; equality refuses
  --out <json-path>                    absolute path for the projection record
  --server-type <type>                 optional, default ccx33
  --location <name>                    optional, default fsn1
  --volume-size-gb <integer>           optional, default 600
  --provisioning-request-epoch <int>   optional; adds elapsed time and a conservative
                                       actual spend to the record and the decision
  -h, --help                           print usage and exit 0

Exit status: 0 permit; 3 refuse; 1 on any other failure.
USAGE
}

decimal_ok() {
    [[ "$1" =~ ^[0-9]+([.][0-9]+)?$ ]]
}

hours=
billable_outbound_bytes=
ceiling_eur=
out=
server_type=$HFX_DEFAULT_SERVER_TYPE
location=$HFX_DEFAULT_LOCATION
volume_size_gb=600
provisioning_request_epoch=
seen=' '
while (($#)); do
    case $1 in
        -h | --help)
            usage
            exit 0
            ;;
        --hours | --billable-outbound-bytes | --ceiling-eur | --out | --server-type | --location | --volume-size-gb | --provisioning-request-epoch)
            [[ "$seen" != *" $1 "* ]] || hfx_die "option $1 may not be repeated"
            seen="$seen$1 "
            (($# >= 2)) || hfx_die "option $1 requires a value"
            case $1 in
                --hours) hours=$2 ;;
                --billable-outbound-bytes) billable_outbound_bytes=$2 ;;
                --ceiling-eur) ceiling_eur=$2 ;;
                --out) out=$2 ;;
                --server-type) server_type=$2 ;;
                --location) location=$2 ;;
                --volume-size-gb) volume_size_gb=$2 ;;
                --provisioning-request-epoch) provisioning_request_epoch=$2 ;;
            esac
            shift 2
            ;;
        *) hfx_die "unknown argument: $1" ;;
    esac
done

decimal_ok "$hours" || hfx_die 'invalid --hours; expected a nonnegative decimal'
awk -v hours="$hours" 'BEGIN { exit !(hours > 0) }' || hfx_die 'invalid --hours; expected a value greater than zero'
[[ "$billable_outbound_bytes" =~ ^[0-9]+$ ]] || hfx_die 'invalid --billable-outbound-bytes; expected a base-10 integer'
decimal_ok "$ceiling_eur" || hfx_die 'invalid --ceiling-eur; expected a nonnegative decimal'
[[ -n "$out" && "$out" == /* ]] || hfx_die '--out must be an absolute path'
[[ ! -e "$out" && ! -L "$out" ]] || hfx_die "--out already exists; choose a new record path: $out"
[[ -d "${out%/*}" && ! -L "${out%/*}" ]] || hfx_die "--out parent is not a regular directory: ${out%/*}"
hfx_validate_name 'server type' "$server_type"
hfx_validate_name 'location' "$location"
hfx_validate_positive_integer 'volume size' "$volume_size_gb"
if [[ -n "$provisioning_request_epoch" ]]; then
    [[ "$provisioning_request_epoch" =~ ^[0-9]+$ ]] ||
        hfx_die 'invalid --provisioning-request-epoch; expected a base-10 integer'
fi

hfx_require_command curl
hfx_require_command awk
hfx_require_command date
hfx_authenticate

retrieved_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
# The bearer header travels through curl's stdin config, never through argv.
if ! response=$(printf 'header = "Authorization: Bearer %s"\nurl = "%s"\n' "$HCLOUD_TOKEN" "$HFX_PRICE_SOURCE" |
    curl --silent --show-error --fail --max-time 60 --config -); then
    hfx_clear_auth
    hfx_die 'could not retrieve the current Hetzner Cloud price list'
fi
hfx_clear_auth

extracted=$(
    jq -c --arg server_type "$server_type" --arg location "$location" '
        def price_string: if type == "string" and test("^[0-9]+([.][0-9]+)?$") then . else error("malformed price") end;
        def integer_bytes: if type == "number" and . == floor and . >= 0 then . else error("malformed byte count") end;
        (.pricing // error("missing pricing")) as $p |
        ($p.server_types // [] | map(select(.name == $server_type)) | first // error("server type absent from price list")) as $server |
        ($server.prices // [] | map(select(.location == $location)) | first // error("server type is not priced at the location")) as $server_price |
        ($p.primary_ips // [] | map(select(.type == "ipv4")) | first // error("primary IPv4 pricing absent")) as $ip |
        ($ip.prices // [] | map(select(.location == $location)) | first // error("primary IPv4 is not priced at the location")) as $ip_price |
        {
            currency: ($p.currency // error("missing currency")),
            vat_rate_percent: ($p.vat_rate | price_string),
            gross_server_eur_per_hour: ($server_price.price_hourly.gross | price_string),
            included_outbound_bytes: ($server_price.included_traffic | integer_bytes),
            gross_outbound_eur_per_unit: ($server_price.price_per_tb_traffic.gross | price_string),
            gross_volume_eur_per_gb_month: ($p.volume.price_per_gb_month.gross | price_string),
            gross_ipv4_eur_per_hour: ($ip_price.price_hourly.gross | price_string)
        }' <<<"$response" 2>/dev/null
) || hfx_die 'current price list is missing a required value; refusing to estimate'
[[ $(jq -r '.currency' <<<"$extracted") == EUR ]] || hfx_die 'price list currency is not EUR'

server_rate=$(jq -r '.gross_server_eur_per_hour' <<<"$extracted")
volume_rate=$(jq -r '.gross_volume_eur_per_gb_month' <<<"$extracted")
ipv4_rate=$(jq -r '.gross_ipv4_eur_per_hour' <<<"$extracted")
included_bytes=$(jq -r '.included_outbound_bytes' <<<"$extracted")
outbound_rate=$(jq -r '.gross_outbound_eur_per_unit' <<<"$extracted")

now_epoch=$(date +%s)
elapsed_hours=null
conservative_actual=null
if [[ -n "$provisioning_request_epoch" ]]; then
    ((now_epoch >= provisioning_request_epoch)) || hfx_die 'provisioning request epoch is in the future'
    IFS=' ' read -r elapsed_hours conservative_actual < <(
        awk -v now="$now_epoch" -v origin="$provisioning_request_epoch" -v gb="$volume_size_gb" \
            -v server="$server_rate" -v volume="$volume_rate" -v ipv4="$ipv4_rate" \
            -v hours_per_month="$HFX_HOURS_PER_MONTH" '
            BEGIN {
                elapsed = (now - origin) / 3600
                billed_hours = int(elapsed) + 1
                actual = billed_hours * (server + gb * volume / hours_per_month + ipv4)
                printf "%.10f %.10f\n", elapsed, actual
            }'
    )
fi

IFS=' ' read -r gross_server gross_volume gross_ipv4 overage_units gross_outbound gross_total < <(
    awk -v hours="$hours" -v gb="$volume_size_gb" -v server="$server_rate" -v volume="$volume_rate" \
        -v ipv4="$ipv4_rate" -v included="$included_bytes" -v overage="$outbound_rate" \
        -v unit="$HFX_OUTBOUND_UNIT_BYTES" -v outbound="$billable_outbound_bytes" \
        -v hours_per_month="$HFX_HOURS_PER_MONTH" '
        BEGIN {
            gross_server = server * hours
            gross_volume = gb * volume * hours / hours_per_month
            gross_ipv4 = ipv4 * hours
            excess = outbound > included ? outbound - included : 0
            units = excess == 0 ? 0 : int((excess + unit - 1) / unit)
            gross_outbound = units * overage
            total = gross_server + gross_volume + gross_ipv4 + gross_outbound
            printf "%.10f %.10f %.10f %d %.10f %.10f\n", gross_server, gross_volume, gross_ipv4, units, gross_outbound, total
        }'
)

decision=permit
if ! awk -v total="$gross_total" -v ceiling="$ceiling_eur" 'BEGIN { exit !(total < ceiling) }'; then
    decision=refuse
fi
if [[ "$conservative_actual" != null ]]; then
    awk -v actual="$conservative_actual" -v ceiling="$ceiling_eur" 'BEGIN { exit !(actual < ceiling) }' || decision=refuse
    awk -v elapsed="$elapsed_hours" -v hours="$hours" 'BEGIN { exit !(elapsed < hours) }' || decision=refuse
fi

temporary=$out.tmp.$$
jq -n --arg retrieved_at "$retrieved_at" --arg source "$HFX_PRICE_SOURCE" \
    --arg server_type "$server_type" --arg location "$location" \
    --argjson volume_size_gb "$volume_size_gb" --arg hours "$hours" \
    --argjson billable_outbound_bytes "$billable_outbound_bytes" \
    --argjson outbound_unit_bytes "$HFX_OUTBOUND_UNIT_BYTES" \
    --arg ceiling_eur "$ceiling_eur" --argjson prices "$extracted" \
    --arg gross_server "$gross_server" --arg gross_volume "$gross_volume" \
    --arg gross_ipv4 "$gross_ipv4" --argjson overage_units "$overage_units" \
    --arg gross_outbound "$gross_outbound" --arg gross_total "$gross_total" \
    --argjson provisioning_request_epoch "${provisioning_request_epoch:-null}" \
    --argjson elapsed_hours "$elapsed_hours" --arg conservative_actual "$conservative_actual" \
    --arg decision "$decision" '{
        schema_version: 2,
        retrieved_at: $retrieved_at,
        source: $source,
        currency: $prices.currency,
        vat_included: 1,
        vat_rate_percent: $prices.vat_rate_percent,
        server_type: $server_type,
        location: $location,
        volume_size_gb: $volume_size_gb,
        wall_clock_ceiling_hours: ($hours | tonumber),
        gross_server_eur_per_hour: $prices.gross_server_eur_per_hour,
        gross_volume_eur_per_gb_month: $prices.gross_volume_eur_per_gb_month,
        gross_ipv4_eur_per_hour: $prices.gross_ipv4_eur_per_hour,
        included_outbound_bytes: $prices.included_outbound_bytes,
        gross_outbound_eur_per_unit: $prices.gross_outbound_eur_per_unit,
        outbound_billing_unit_bytes: $outbound_unit_bytes,
        billable_outbound_estimate_bytes: $billable_outbound_bytes,
        gross_server_eur: $gross_server,
        gross_volume_eur: $gross_volume,
        gross_ipv4_eur: $gross_ipv4,
        outbound_overage_units: $overage_units,
        gross_outbound_eur: $gross_outbound,
        projected_gross_total_eur: $gross_total,
        gross_cost_ceiling_eur: $ceiling_eur,
        provisioning_request_epoch: $provisioning_request_epoch,
        elapsed_hours: (if $elapsed_hours == null then null else ($elapsed_hours | tonumber) end),
        conservative_actual_gross_eur: (if $conservative_actual == "null" then null else $conservative_actual end),
        decision: $decision
    }' >"$temporary"
mv -- "$temporary" "$out"

printf 'price_record=%s\n' "$out"
printf 'projected_gross_total_eur=%s\n' "$gross_total"
printf 'gross_cost_ceiling_eur=%s\n' "$ceiling_eur"
if [[ "$conservative_actual" != null ]]; then
    printf 'elapsed_hours=%s\n' "$elapsed_hours"
    printf 'conservative_actual_gross_eur=%s\n' "$conservative_actual"
fi
printf 'decision=%s\n' "$decision"
if [[ "$decision" != permit ]]; then
    printf 'hfx: error: %s\n' 'projected or actual gross cost, or elapsed time, is not strictly below its ceiling' >&2
    exit 3
fi
