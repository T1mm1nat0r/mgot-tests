#!/bin/zsh
# Build the five variant source trees the process_time 2x2 measures.
#
# The cells differ from the working tree by at most three lines. Keeping them as
# separate trees rather than monkeypatches is deliberate: every service does
# `from mgot_utils import *` and holds its own reference from import time, so a
# patch at the definition site changes nothing the service calls — the error
# CLAUDE.md lists first under "Wrong answers this project keeps producing". A
# tree cannot half-apply.
#
#   cell   process_time                     block_one_time (mth)  backfill cap
#   base   bar.time + delta*order   (OLD)   zone.time     (OLD)   2   <- production
#   P      bar.time                 (NEW)   zone.time     (OLD)   2   <- Aug-25's variant
#   L      bar.time + delta*order   (OLD)   move_end_time (NEW)   2
#   PL     bar.time                 (NEW)   move_end_time (NEW)   2   <- working tree
#   B      bar.time + delta*order   (OLD)   move_end_time (NEW)   order+1
#
# Usage:  ./harness/build_process_time_cells.sh [outdir]     (default /tmp/mgotvar)
set -e
REPO=${0:a:h:h:h}
SRC=$REPO/utils/src/mgot_utils
OUT=${1:-/tmp/mgotvar}

[[ -d $SRC ]] || { echo "no mgot_utils at $SRC" >&2; exit 1 }

rm -rf $OUT && mkdir -p $OUT
for cell in base P L PL B; do
  mkdir -p $OUT/$cell
  cp -R $SRC $OUT/$cell/mgot_utils
  find $OUT/$cell -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
done

python3 - "$OUT" <<'PY'
import pathlib, sys
out = pathlib.Path(sys.argv[1])

NEW_PT = '    process_time = bar.time'
OLD_PT = '    process_time = bar.time + (config.delta_epoch[timeframe] * config.order)'
NEW_BOT = "    block_one_time = zone.time if zone.type == 'squeeze' else zone.move_end_time"
OLD_BOT = "    block_one_time = zone.time if zone.type in ('mth', 'squeeze') else zone.move_end_time"
NARROW = '    if 0 < delta_t <= 2:'
WIDE = ('    # CELL B: the cap was 2, narrower than the gap the `+ order` delay itself\n'
        '    # creates — an MTH arrives at delta_t = 3 and fell through to no-backfill.\n'
        '    if 0 < delta_t <= config.order + 1:')

# (process_time OLD?, block_one_time OLD?, widen cap?)
CELLS = {'base': (1, 1, 0), 'P': (0, 1, 0), 'L': (1, 0, 0),
         'PL': (0, 0, 0), 'B': (1, 0, 1)}

for cell, (pt_old, bot_old, wide) in CELLS.items():
    zp = out / cell / 'mgot_utils/processing/zone_preprocessor.py'
    lp = out / cell / 'mgot_utils/processing/lvl_preprocessor.py'
    s = zp.read_text()
    if s.count(NEW_PT + '\n') != 1:
        raise SystemExit(f'{cell}: process_time anchor not found once — the working '
                         f'tree must carry the NEW form for this script to build cells')
    if pt_old:
        s = s.replace(NEW_PT + '\n', OLD_PT + '\n')
    zp.write_text(s)

    t = lp.read_text()
    if t.count(NEW_BOT + '\n') != 1 or t.count(NARROW + '\n') != 1:
        raise SystemExit(f'{cell}: lvl_preprocessor anchors not found once')
    if bot_old:
        t = t.replace(NEW_BOT + '\n', OLD_BOT + '\n')
    if wide:
        t = t.replace(NARROW + '\n', WIDE + '\n')
    lp.write_text(t)
    print(f'{cell:<5} process_time={"OLD" if pt_old else "NEW"} '
          f'block_one_time={"OLD" if bot_old else "NEW"} '
          f'cap={"order+1" if wide else "2"}')
PY
echo
echo "trees under $OUT — verify:"
for cell in base P L PL B; do
  printf '  %-5s ' $cell
  grep -h "^    process_time = bar.time" $OUT/$cell/mgot_utils/processing/zone_preprocessor.py | tr -d ' ' | head -1
done
