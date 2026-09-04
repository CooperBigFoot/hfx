#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
set +x

# The launch.sh runner must leave its finish record in both logs before it exits,
# whatever the workload's exit status; on 2026-09-04 rehearsal run 2 the record was
# lost when tmux destroyed the pane before the tee had written it.
SCRIPT_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
launcher=$SCRIPT_DIR/launch.sh
tmp=$(mktemp -d "${TMPDIR:-/tmp}/hfx-launch-test.XXXXXX")
cleanup() { rm -rf -- "$tmp"; }
trap cleanup EXIT
passed=0
die() { printf 'test-launch: error: %s\n' "$1" >&2; exit 1; }
pass() { passed=$((passed + 1)); printf 'ok %d - %s\n' "$passed" "$1"; }

runner=$(sed -n "/^runner='/,/^exit \"\$command_status\"'$/p" "$launcher" | sed "1s/^runner='//; \$s/'\$//")
[[ -n "$runner" ]] || die 'runner string was not found in launch.sh'
grep -q 'wait "$tee_pid"' <<<"$runner" || die 'runner does not wait for its tee'

for expected in 0 3; do
    canonical=$tmp/canonical-$expected.log
    run_log=$tmp/run-$expected.log
    : >"$canonical"
    : >"$run_log"
    status=0
    bash -c "$runner" bash "$canonical" "$run_log" sh -c "printf 'work line\n'; printf 'status_line=1\n'; exit $expected" >/dev/null 2>&1 || status=$?
    [[ "$status" -eq "$expected" ]] || die "runner exit $status, expected $expected"
    for log in "$canonical" "$run_log"; do
        [[ $(tail -n 1 "$log") =~ ^launch:\ finished\ at\ [0-9T:Z-]+\ with\ exit\ $expected$ ]] ||
            die "finish record missing or wrong at the end of $log: $(tail -n 1 "$log")"
        grep -q '^launch: started at ' "$log" || die "start record missing in $log"
        grep -q '^launch: command: sh -c ' "$log" || die "command record missing in $log"
        grep -q '^status_line=1$' "$log" || die "workload output missing in $log"
    done
done
pass 'the runner leaves the finish record as the last line of both logs, for exit 0 and exit 3, before it returns'

# A slow tee stand-in: the runner must still have the finish line on disk when it returns,
# because it waits for the tee instead of racing it.
mkdir -p "$tmp/slow/bin"
cat >"$tmp/slow/bin/tee" <<'SLOW'
#!/usr/bin/env bash
sleep 1
exec /usr/bin/tee "$@"
SLOW
chmod +x "$tmp/slow/bin/tee"
canonical=$tmp/slow-canonical.log
run_log=$tmp/slow-run.log
: >"$canonical"
: >"$run_log"
started=$(date +%s)
status=0
PATH="$tmp/slow/bin:$PATH" bash -c "$runner" bash "$canonical" "$run_log" sh -c 'printf "work\n"; exit 0' >/dev/null 2>&1 || status=$?
[[ "$status" -eq 0 ]] || die "runner exit $status with the slow tee"
(( $(date +%s) - started >= 1 )) || die 'runner returned before the slow tee could have written'
[[ $(tail -n 1 "$canonical") =~ ^launch:\ finished\ at\ [0-9T:Z-]+\ with\ exit\ 0$ ]] || die "finish record missing behind a slow tee: $(tail -n 1 "$canonical")"
[[ $(tail -n 1 "$run_log") =~ ^launch:\ finished\ at ]] || die 'finish record missing in the timestamped log behind a slow tee'
pass 'with a tee that delays every write, the finish record is on disk in both logs when the runner returns'

printf '1..%d\n' "$passed"
