#!/usr/bin/env bash
# verify : (TrackedRunbook, Check, EvidenceRoot?) -> Pass | Refusal
#
# Proves that the tracked seven-basin compile runbook still carries the
# machine-readable contract the campaign operator relies on: exact scope,
# fixed ceilings, the approval precondition, the pinned control hotpatch, the
# pinned control digests, the pinned control adjudication record, the pinned
# baseline, and a current authority ref.
# Reading the runbook performs no cloud action. The approval record is only
# tested for existence, regular-file-ness, and non-emptiness.

set -Eeuo pipefail
IFS=$'\n\t'
set +x

SCRIPT_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/hetzner/common.sh
source "$SCRIPT_DIR/common.sh"

readonly HFX_RUNBOOK_REL=scripts/hetzner/RUNBOOK-tdx-hydro-seven-basin-compile.md
readonly HFX_PLANETARY_RECORD_REL=scripts/hetzner/CAMPAIGN-tdx-hydro-planetary.md
readonly HFX_CONTROL_ADJUDICATION_REL=scripts/hetzner/seven-basin-control-adjudication.json
readonly HFX_EXPECTED_CONTROL_UNIT_COUNT=331263
readonly HFX_EXPECTED_CONTROL_ADJUDICATION_DATE=2026-09-04
readonly HFX_CONTRACT_BEGIN='<!-- BEGIN COMPILE CAMPAIGN CONTRACT'
readonly HFX_CONTRACT_END='END COMPILE CAMPAIGN CONTRACT -->'
readonly HFX_EXPECTED_CAMPAIGN=seven-basin-extension
readonly HFX_EXPECTED_ABSENT_BASINS='["1020018110","2020003440","2020065840","2020071190","4020050470","5020049720","6020000010"]'
readonly HFX_EXPECTED_CONTROL_BASIN=7020000010
readonly HFX_EXPECTED_PLANETARY_REF=43a98aff8c15a1a196f47b10217ad2f5553b6611
readonly HFX_EXPECTED_HOTPATCH='{"commit":"bde61149d3fefc5e3f30435bf7ed3d0bb32a519c","path":"scripts/hetzner/tdx-hydro-campaign.sh","pre_patch_blob":"41d6df3f10030a481b2227a878837c7f23f3e658","post_patch_blob":"d227920a7ac0ab98ffcc80aac2c72a5dfc9c2429"}'
readonly HFX_EXPECTED_CONTROL_DIGESTS='{"aux/snap_stems.parquet":"caf2eec3d1930f25d932b559ca943c7710a9a6cf18b4a9f84d3d94cbf379d9b2","catchments.parquet":"d2019ba08fd39c873eb0bac22946d25c91e785ef3345538447d30fc15ccf3be4","graph.parquet":"fb64ce0fa941f244841ffc5eeed4f2057ea65262a0183a1ac1e81c67380e6cc5","manifest.json":"8ca5b2135d19c18a4b8fba6c93c63ffb7a784a2749867483ea6c0c49c46560c4"}'
readonly HFX_EXPECTED_BASELINE='{"prefix":"s3://pourpoint-hfx/scratch/tdx-hydro-tdx-m5-planetary/planetary-hfx-v0-3-0/dataset/","region":"tdx-hydro-partial-4dbff0d6ec31","basin_count":55,"unit_count":12748154,"exported_bytes":114063230627,"roster_digest_prefix":"4dbff0d6ec31"}'

usage() {
    cat <<'USAGE'
Usage: verify-compile-runbook.sh [--runbook <absolute-path>] [--evidence-root <absolute-path>]
                                 [--adjudication-record <absolute-path>] --check <name>

Checks:
  scope-permits-compilation   campaign identity, exact resources, basins, permitted acts
  ceilings-and-kill-switches  server, location, volume, hours, cost, retention, teardown
  approval-is-a-precondition  approval record pinned and present under --evidence-root
  control-hotpatch-is-pinned  ARG_MAX hotpatch commit, path, and blobs match Git objects
  control-digests-are-pinned  preserved planetary control digests are pinned
  control-adjudication-is-pinned
                              adjudicated corrected-control difference record is tracked and complete
  baseline-is-pinned          baseline prefix, region, counts match the merged campaign record
  authority-is-current        authority ref is an ancestor of HEAD and names the vision section

Options:
  --runbook <path>        runbook to verify; default is the tracked runbook in this checkout
  --evidence-root <path>  required by approval-is-a-precondition
  --adjudication-record <path>
                          control adjudication record to verify; default is the tracked record
  -h, --help              print usage and exit 0
USAGE
}

fail() {
    printf 'FAIL: %s\n' "$1" >&2
    exit 1
}

hfx_require_command jq
hfx_require_command git

runbook=
evidence_root=
adjudication_record=
check=
seen=' '
while (($#)); do
    case $1 in
        -h | --help)
            usage
            exit 0
            ;;
        --runbook | --evidence-root | --adjudication-record | --check)
            [[ "$seen" != *" $1 "* ]] || hfx_die "option $1 may not be repeated"
            seen="$seen$1 "
            (($# >= 2)) || hfx_die "option $1 requires a value"
            [[ "$2" != --* ]] || hfx_die "option $1 requires a value"
            case $1 in
                --runbook) runbook=$2 ;;
                --evidence-root) evidence_root=$2 ;;
                --adjudication-record) adjudication_record=$2 ;;
                --check) check=$2 ;;
            esac
            shift 2
            ;;
        *) hfx_die "unknown argument: $1" ;;
    esac
done
[[ -n "$check" ]] || hfx_die '--check is required'

repo_root=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel) || hfx_die 'not inside a Git checkout'
if [[ -z "$runbook" ]]; then
    runbook=$repo_root/$HFX_RUNBOOK_REL
    git -C "$repo_root" ls-files --error-unmatch -- "$HFX_RUNBOOK_REL" >/dev/null 2>&1 ||
        fail 'compile campaign runbook is not tracked'
else
    [[ "$runbook" == /* ]] || hfx_die '--runbook must be an absolute path'
fi
[[ -f "$runbook" && ! -L "$runbook" ]] || fail 'compile campaign runbook is not a regular file'

contract=$(
    awk -v begin="$HFX_CONTRACT_BEGIN" -v end="$HFX_CONTRACT_END" '
        $0 == begin { inside = 1; count++; next }
        $0 == end { inside = 0; next }
        inside { print }
        END { if (count != 1) exit 1 }
    ' "$runbook"
) || fail 'missing or repeated compile campaign contract'
[[ -n "$contract" ]] || fail 'missing compile campaign contract'
jq -e 'type == "object"' <<<"$contract" >/dev/null 2>&1 || fail 'malformed compile campaign contract'
duplicate_count=$(jq --stream -c 'select(length == 2) | .[0]' <<<"$contract" | sort | uniq -d | wc -l | tr -d ' ')
[[ "$duplicate_count" == 0 ]] || fail 'duplicate contract key'

contract_field() {
    jq -c "$1" <<<"$contract"
}

require_field() {
    local expression=$1
    local expected=$2
    local message=$3
    [[ $(contract_field "$expression") == "$expected" ]] || fail "$message"
}

require_phrase() {
    local phrase=$1
    grep -F -q -- "$phrase" "$runbook" || fail "runbook omits required operational text: $phrase"
}

case $check in
    scope-permits-compilation)
        require_field '.schema' 3 'unknown contract schema'
        require_field '.campaign' "\"$HFX_EXPECTED_CAMPAIGN\"" 'wrong campaign identity'
        require_field '.lifecycles_authorized' 1 'contract must authorize exactly one lifecycle'
        require_field '.server_name' "\"$(hfx_server_name "$HFX_EXPECTED_CAMPAIGN")\"" 'wrong mutable server'
        require_field '.volume_name' "\"$(hfx_volume_name "$HFX_EXPECTED_CAMPAIGN")\"" 'wrong mutable volume'
        require_field '.out_of_scope_resources' '["pourpoint-web-1"]' 'pourpoint-web-1 is not declared out of scope'
        require_field '.absent_basins' "$HFX_EXPECTED_ABSENT_BASINS" 'seven-basin scope is not exact'
        require_field '.control_basin' "\"$HFX_EXPECTED_CONTROL_BASIN\"" 'control basin is not pinned'
        require_field '.control_builds' "[\"corrected-adapter\",\"planetary-revision-$HFX_EXPECTED_PLANETARY_REF-with-recorded-ARG_MAX-hotpatch\"]" 'both control builds are not pinned'
        require_field '.source_corpus' '{"file_count":16,"total_bytes":84101885952}' 'preserved source corpus is not pinned'
        require_field '.permitted_acts' '["transfer-preserved-source-corpus","reacquire-selected-source-on-integrity-failure","compile-both-control-builds","compare-control-outputs","compile-selected-basins","pull-baseline-read-only","assemble-extension","attempt-strict-validation","preserve-all-produced-output-off-vm","read-only-audit","exact-resource-teardown"]' 'permitted acts are incomplete or overbroad'
        require_field '.sole_destructive_act' '"exact-resource-teardown"' 'destructive scope is not exact teardown only'
        require_phrase 'Only the named campaign server and volume may be mutated.'
        require_phrase '`pourpoint-web-1` is outside scope and must remain untouched.'
        require_phrase 'A repository change cannot manufacture that authority.'
        require_phrase 'Any further lifecycle needs new maintainer authority before provisioning.'
        require_phrase 'No produced output may remain unique to the VM or volume.'
        require_phrase 'The baseline prefix is read-only; nothing under it is modified or deleted.'
        ;;
    ceilings-and-kill-switches)
        require_field '.server_type' '"ccx33"' 'server type is absent or changed'
        require_field '.location' '"fsn1"' 'location is absent or changed'
        require_field '.volume_size_gb' 600 'volume size is absent or changed'
        require_field '.wall_clock_ceiling_hours' 72 'wall-clock ceiling is absent or changed'
        require_field '.gross_cost_ceiling_eur' 40.0 'gross cost ceiling is absent or changed'
        require_field '.price_source' '"https://api.hetzner.cloud/v1/pricing"' 'price source is absent or changed'
        require_field '.decision_points_hours' '[24,48,66]' 'decision points are absent or changed'
        require_field '.retention_policy' '"retain-all-produced-output-off-vm-before-exact-resource-teardown"' 'retention policy is absent or changed'
        require_field '.sole_destructive_act' '"exact-resource-teardown"' 'destructive scope is not exact teardown only'
        require_phrase 'These limits exist before any provisioning step and cannot be raised while the campaign is running:'
        require_phrase 'required: projected_gross_total < 40.00'
        require_phrase 'Equality with a ceiling refuses the act.'
        require_phrase 'mandatory on success, failure, refusal, interruption, and timeout'
        require_phrase '`--keep-volume` is forbidden.'
        require_phrase 'An interrupted or OOM-killed validation is recorded as `incomplete`, never as passing.'
        require_phrase 'Only a completed pass is called strict-validated.'
        ;;
    approval-is-a-precondition)
        require_field '.approval_record' '"provisioner-transfer-approval.txt"' 'approval record is not pinned'
        require_phrase 'This campaign may not create the approval record.'
        require_phrase 'No automated process may create, edit, replace, regenerate, copy, upload, print, log, hash, or display the record or its contents.'
        require_phrase 'If it is absent, empty, not a regular file, or a symlink, refuse provisioning with zero cloud mutation.'
        [[ -n "$evidence_root" ]] || fail 'evidence root is required'
        [[ "$evidence_root" == /* ]] || fail 'evidence root must be absolute'
        [[ -d "$evidence_root" && ! -L "$evidence_root" ]] || fail 'evidence root must be an existing non-symlink directory'
        approval=$evidence_root/provisioner-transfer-approval.txt
        [[ ! -L "$approval" ]] || fail 'approval record must be a regular non-symlink file'
        [[ -f "$approval" ]] || fail 'approval record must be a regular non-symlink file'
        [[ -s "$approval" ]] || fail 'approval record must be non-empty'
        ;;
    control-hotpatch-is-pinned)
        require_field '.control_hotpatch' "$HFX_EXPECTED_HOTPATCH" 'control ARG_MAX hotpatch is not pinned'
        hotpatch_commit=$(contract_field '.control_hotpatch.commit' | tr -d '"')
        hotpatch_path=$(contract_field '.control_hotpatch.path' | tr -d '"')
        pre_patch_blob=$(contract_field '.control_hotpatch.pre_patch_blob' | tr -d '"')
        post_patch_blob=$(contract_field '.control_hotpatch.post_patch_blob' | tr -d '"')
        git_object() {
            git -C "$repo_root" "$@" 2>/dev/null || fail 'pinned control hotpatch object is unavailable'
        }
        [[ $(git_object cat-file -t "$hotpatch_commit") == commit ]] || fail 'control hotpatch ref is not a commit'
        [[ $(git_object rev-parse "$hotpatch_commit^:$hotpatch_path") == "$pre_patch_blob" ]] ||
            fail 'control hotpatch pre-patch blob does not match'
        [[ $(git_object rev-parse "$hotpatch_commit:$hotpatch_path") == "$post_patch_blob" ]] ||
            fail 'control hotpatch post-patch blob does not match'
        [[ $(git_object rev-parse "$HFX_EXPECTED_PLANETARY_REF:$hotpatch_path") == "$pre_patch_blob" ]] ||
            fail 'planetary revision is not the pinned pre-patch input'
        require_phrase "Require a clean detached worktree at planetary revision \`$HFX_EXPECTED_PLANETARY_REF\`."
        require_phrase "Apply only the path-scoped diff from pinned hotpatch commit \`$hotpatch_commit\`."
        require_phrase "Refuse unless the pre-patch blob is \`$pre_patch_blob\`."
        require_phrase "Refuse unless the post-patch blob is \`$post_patch_blob\`."
        ;;
    control-digests-are-pinned)
        require_field '.control_digests' "$HFX_EXPECTED_CONTROL_DIGESTS" 'preserved planetary control digests are not pinned'
        require_phrase 'Any other difference stops the work for adjudication.'
        require_phrase 'Do not explain a difference away'
        ;;
    control-adjudication-is-pinned)
        require_field '.control_adjudication_record' "\"$HFX_CONTROL_ADJUDICATION_REL\"" 'control adjudication record is not pinned'
        if [[ -z "$adjudication_record" ]]; then
            adjudication=$repo_root/$HFX_CONTROL_ADJUDICATION_REL
            git -C "$repo_root" ls-files --error-unmatch -- "$HFX_CONTROL_ADJUDICATION_REL" >/dev/null 2>&1 ||
                fail 'control adjudication record is not tracked'
        else
            [[ "$adjudication_record" == /* ]] || hfx_die '--adjudication-record must be an absolute path'
            adjudication=$adjudication_record
        fi
        [[ -f "$adjudication" && ! -L "$adjudication" && -s "$adjudication" ]] ||
            fail 'control adjudication record is not a nonempty regular file'
        jq -e 'type == "object"' "$adjudication" >/dev/null 2>&1 || fail 'malformed control adjudication record'
        ! grep -F -q -- 'PENDING' "$adjudication" || fail 'control adjudication record still carries a PENDING placeholder'
        jq -e --arg basin "$HFX_EXPECTED_CONTROL_BASIN" --arg revision "$HFX_EXPECTED_PLANETARY_REF" \
            --argjson units "$HFX_EXPECTED_CONTROL_UNIT_COUNT" --arg date "$HFX_EXPECTED_CONTROL_ADJUDICATION_DATE" '
            .schema_version == 1
            and .processing_basin_id == $basin
            and .planetary_revision == $revision
            and .unit_count == $units
            and .adjudication.date == $date
            and (.adjudication.maintainer | type == "string" and length > 0)
            and (.adjudication.decision | type == "string" and length > 0)
            and (.planetary_orientation_digest | type == "string" and test("^[0-9a-f]{64}$"))
            and (.corrected_orientation_digest | type == "string" and test("^[0-9a-f]{64}$"))
            and .planetary_orientation_digest != .corrected_orientation_digest
            and .downstream_differences == 0
            and (.max_shift_deg | type == "number" and . > 0)
            and (.outlet_difference_native_linknos | type == "array" and length > 0
                 and all(.[]; type == "number" and . == floor and . > 0)
                 and . == (unique | sort))
            and .outlet_differences == (.outlet_difference_native_linknos | length)
            and (.differences_by_class | type == "object" and length > 0 and all(.[]; type == "number"))
            and (.differences_by_class | [.[]] | add) == (.outlet_difference_native_linknos | length)
        ' "$adjudication" >/dev/null 2>&1 || fail 'control adjudication record is incomplete or inconsistent'
        require_phrase 'The corrected control build is accepted only when the adjudicated comparison reports `accepted`.'
        require_phrase 'Every same-level graph edge, every polygon, and every non-outlet attribute must be identical between the two builds.'
        require_phrase 'The set of units whose outlet differs must equal exactly the adjudicated set pinned in the tracked record.'
        require_phrase 'Any other difference stops the work for adjudication.'
        ;;
    baseline-is-pinned)
        require_field '.baseline' "$HFX_EXPECTED_BASELINE" 'baseline artifact is not pinned'
        require_field '.extension_scratch_prefix' '"scratch/tdx-hydro-seven-basin-extension/"' 'extension scratch prefix is not pinned'
        planetary_record=$repo_root/$HFX_PLANETARY_RECORD_REL
        [[ -f "$planetary_record" && ! -L "$planetary_record" ]] || fail 'planetary campaign record is missing'
        for needle in 'tdx-hydro-partial-4dbff0d6ec31' '12,748,154' '114,063,230,627' '55 of 62' \
            's3://pourpoint-hfx/scratch/tdx-hydro-tdx-m5-planetary/planetary-hfx-v0-3-0/' '4dbff0d6ec31'; do
            grep -F -q -- "$needle" "$planetary_record" || fail "planetary campaign record does not carry: $needle"
        done
        ;;
    authority-is-current)
        authority_ref=$(contract_field '.authority_ref' | tr -d '"')
        authority_document=$(contract_field '.authority_document' | tr -d '"')
        authority_section=$(contract_field '.authority_section' | tr -d '"')
        [[ "$authority_ref" =~ ^[0-9a-f]{40}$ ]] || fail 'authority ref is not a full commit hash'
        [[ "$authority_document" == planning/visions/*.md ]] || fail 'authority document is not a vision'
        [[ -n "$authority_section" ]] || fail 'authority section is empty'
        [[ $(git -C "$repo_root" cat-file -t "$authority_ref" 2>/dev/null) == commit ]] || fail 'authority ref is unavailable'
        git -C "$repo_root" merge-base --is-ancestor "$authority_ref" HEAD 2>/dev/null || fail 'authority ref is not an ancestor of HEAD'
        git -C "$repo_root" show "$authority_ref:$authority_document" 2>/dev/null | grep -F -q -- "## $authority_section" ||
            fail 'authority document at the authority ref lacks the named section'
        git -C "$repo_root" show "$authority_ref:$authority_document" 2>/dev/null | grep -F -q -- 'single additional bounded lifecycle' ||
            fail 'authority document does not carry forward the single bounded lifecycle'
        require_phrase "$authority_ref"
        ;;
    *) fail 'unknown check' ;;
esac

printf 'PASS %s\n' "$check"
