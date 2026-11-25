#!/bin/bash
# Runs the benchmark workloads against target/release/pulsar and prints a
# markdown table with the median elapsed time of RUNS runs (default 3) and,
# when perf is installed, the median CPU time.
#
#   bench/run.sh              build, then run every workload
#   WORKERS=4 bench/run.sh    run with -j 4
#   RUNS=5 bench/run.sh       more runs per workload
set -e
cd "$(dirname "$0")/.."

# Decimal points, not commas, whatever the user's locale is.
export LC_ALL=C

BIN=./target/release/pulsar
RUNS=${RUNS:-3}
EXTRA=()
if [ -n "$WORKERS" ]; then EXTRA=(-j "$WORKERS"); fi

bench/gen.sh
cargo build --release 2>&1 | tail -1 >&2

median() {
    sort -n | awk '{ v[NR] = $1 } END { print v[int((NR + 1) / 2)] }'
}

# measure NAME ARGS...: runs pulsar RUNS times and prints one table row.
measure() {
    local name=$1
    shift
    local elapsed=() cpu=()
    for ((i = 0; i < RUNS; i++)); do
        local start=$EPOCHREALTIME
        if command -v perf >/dev/null; then
            local line
            line=$(perf stat -x, -e task-clock "$BIN" "$@" "${EXTRA[@]}" 2>&1 >/dev/null | grep task-clock || true)
            cpu+=("$(echo "$line" | cut -d, -f1 | awk '{ printf "%.1f", $1 / 1000 }')")
        else
            "$BIN" "$@" "${EXTRA[@]}" >/dev/null
        fi
        local end=$EPOCHREALTIME
        elapsed+=("$(awk -v a="$start" -v b="$end" 'BEGIN { printf "%.2f", b - a }')")
    done
    local cpu_median="n/a"
    if [ ${#cpu[@]} -gt 0 ]; then cpu_median=$(printf '%s\n' "${cpu[@]}" | median); fi
    printf '| %s | %s s | %s s |\n' "$name" "$(printf '%s\n' "${elapsed[@]}" | median)" "$cpu_median"
}

echo "| workload | elapsed | CPU |"
echo "|---|---|---|"
measure "default word count (98 MB)" -f bench/data/text.txt
measure "async split map (98 MB)" -f bench/data/text.txt -s bench/scripts/cheap-async.js
measure "sync split map (98 MB)" -f bench/data/text.txt -s bench/scripts/cheap-sync.js
measure "3M distinct keys" -f bench/data/distinct.txt
measure "latency script (22k lines)" -f bench/data/moby.txt -s bench/scripts/latency.js
