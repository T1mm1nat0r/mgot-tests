#!/bin/zsh
# Run the process_time 2x2 (plus cell B) over a window, one Redis db per cell.
#
# Each cell is a separate process so that `REDIS_DB` is set before mgot_utils is
# imported — the services bind their connection at module scope, so a shared
# process could not isolate them. Databases 1-13 are scratch; 0 is live and 15
# is the integration-test db, and measure_process_time.py refuses both.
#
# Usage:
#   ./harness/run_process_time_matrix.sh 15m 2026-07-01 2026-08-01 jul [outdir]
set -e
HERE=${0:a:h}
TF=${1:-15m}; START=${2:-2026-07-01}; END=${3:-2026-08-01}; TAG=${4:-run}
OUT=${5:-/tmp/ptm}
TREES=${MGOT_CELLS:-/tmp/mgotvar}

[[ -d $TREES/base ]] || $HERE/build_process_time_cells.sh $TREES
mkdir -p $OUT
cd ${0:a:h:h}

run() {  # cell db
  PYTHONPATH=$TREES/$1 uv run python harness/measure_process_time.py \
    --cell $1 --timeframe $TF --db $2 --start $START --end $END --warmup-days 7 \
    --out $OUT/${TAG}_$1_$TF.json > $OUT/${TAG}_$1_$TF.log 2>&1
  echo "  $1 rc=$?"
}
echo "process_time matrix: $TF $START..$END -> $OUT/${TAG}_*"
run base 1 & run P 2 & run L 3 & run PL 4 &
wait
run B 5
echo "done"
