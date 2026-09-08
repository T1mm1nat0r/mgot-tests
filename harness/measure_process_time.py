"""Measure the MTH `process_time` and level-backfill knobs against each other.

Why this exists
---------------
`business_rules_sweeps_tests.md` records a 2026-08-25 replay concluding that
removing the `+ config.order` term from MTH `process_time` makes detection
*worse* — 18 more `invalid` MTHs on 15m, dance triggers halved. That measurement
was taken while a second bug was live: `create_lvls` measured an MTH's blind
window from `zone.time` (the move **start**), so `delta_t` came out at
`length_bar + 2` bars against a cap of 2 and the backfill never ran for any MTH.

The two interact. `process_time` sets when a zone's levels come alive;
`block_one_time` sets how much of the gap before that the levels are given a
chance to see. Changing one while the other is wrong measures a cell, not a
factor. So: run the full 2x2, plus the cell the working tree does not implement.

    cell   process_time                      block_one_time (mth)   backfill cap
    base   bar.time + delta*order  (OLD)     zone.time      (OLD)   2   <- production
    P      bar.time                (NEW)     zone.time      (OLD)   2   <- Aug-25's variant
    L      bar.time + delta*order  (OLD)     move_end_time  (NEW)   2
    PL     bar.time                (NEW)     move_end_time  (NEW)   2   <- working tree
    B      bar.time + delta*order  (OLD)     move_end_time  (NEW)   order+1

Cell B is the one nobody has run: keep the delay, but widen the cap so the
backfill actually replays the window the delay blinds. It separates "processed
earlier" from "saw those bars at all", which the other four cells confound.

Variants are separate **source trees** on `PYTHONPATH`, not monkeypatches. A
patch that lands where a symbol is defined rather than where it is bound is the
error this repo produces most often, and every service here does
`from mgot_utils import *`. A different tree cannot half-apply.

Two traps this file fell into on 2026-09-07, both caught by adversarial audit,
both of which made the change under test look far worse than it is:

* **The blind window is three bars, not two.** `bar` in `create_mth_zone` is the
  direction-change bar, so OLD `process_time` is `move_end + 3`. The bar *at*
  `process_time` is blind too: `create_lvls` runs at the end of 10's handling of
  it, the only thing applied to `row` is `set_state_from_bar`, which records no
  gain or loss, and `update_mth` decides on gains and losses alone. An exclusive
  window undercounts by a third.
* **`taken_out` implies the zone was complete first.** Keying on final
  `completion` misses every zone that completed and was then taken out — 843 of
  production's 2098 15m MTHs were ever complete against 38 that still say so.
  `was_completed` is the field that answers "did it reach complete".

With both corrected the statistic reproduces the working-tree comment's figure
exactly (121 on 15m, production). With either wrong it reads as 3.

Usage. `harness/build_process_time_cells.sh` materialises the five trees and
`harness/run_process_time_matrix.sh <tf> <start> <end> <tag>` runs them all. One
cell by hand:

    PYTHONPATH=/tmp/mgotvar/PL uv run python harness/measure_process_time.py \
        --cell PL --timeframe 15m --db 12 --start 2026-07-01 --end 2026-08-01

Writes a metrics JSON to --out. Read-only against production db 0: bars are
pulled from it, nothing is written back.
"""

import argparse
import inspect
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

CELLS = {                    # cell -> (process_time, block_one_time, backfill cap)
    'base': ('OLD', 'OLD', '2'),
    'P':    ('NEW', 'OLD', '2'),
    'L':    ('OLD', 'NEW', '2'),
    'PL':   ('NEW', 'NEW', '2'),
    'B':    ('OLD', 'NEW', 'order+1'),
    # S extends PL to the squeeze: `find_secondary_swing` carries the same
    # live-bar `+ config.order` term. A squeeze is created in 03 and collected by
    # 10 on the same bar, so dropping the term puts its levels live on the bar it
    # is drawn on, with no gap to replay.
    'S':    ('NEW', 'NEW', '2'),
}


def _epoch(day: str) -> int:
    return int(datetime.strptime(day, '%Y-%m-%d')
               .replace(tzinfo=timezone.utc).timestamp() * 1000)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--cell', required=True, choices=sorted(CELLS))
    ap.add_argument('--timeframe', required=True)
    ap.add_argument('--db', type=int, required=True)
    ap.add_argument('--start', required=True, help='UTC date, inclusive')
    ap.add_argument('--end', required=True,
                    help='UTC date. The bar opening exactly here is included — '
                         'replay.load_from uses zrangebyscore, which is inclusive.')
    ap.add_argument('--warmup-days', type=int, default=0,
                    help='replay this many extra days before --start; zones '
                         'created in the warm-up are excluded from the counts')
    ap.add_argument('--symbol', default='BTCUSDT')
    ap.add_argument('--out', required=True)
    args = ap.parse_args()

    if args.db in (0, 15):
        raise SystemExit(f'refusing to use db {args.db}')

    # Isolation must be established before mgot_utils is imported anywhere:
    # every service binds its connection at module scope. Replay.__enter__
    # re-asserts this and refuses to run if it was missed.
    os.environ['REDIS_DB'] = str(args.db)

    start, end = _epoch(args.start), _epoch(args.end)
    load_start = start - args.warmup_days * 86_400_000

    # The variant tree must be chosen explicitly. Without this the installed
    # wheel — which differs from cell `base` only in the two knob lines — would
    # satisfy every check below while measuring something nobody selected.
    pythonpath = os.environ.get('PYTHONPATH', '').split(':')[0]
    if not pythonpath:
        raise SystemExit('PYTHONPATH must name the variant tree for this cell')

    from harness.replay import Replay
    import mgot_utils
    import redis

    tree = Path(mgot_utils.__file__).resolve().parents[1]
    if not str(tree).startswith(str(Path(pythonpath).resolve())):
        raise SystemExit(
            f'PYTHONPATH did not take effect: mgot_utils resolved to {tree}, '
            f'not under {pythonpath}.')

    # -- proof that this cell is the code it claims to be -------------------
    # Read the switched lines out of the *imported modules*, not off disk. A
    # disk read would pass while stale bytecode executed, and a cell that
    # reports numbers without this proof is indistinguishable from a no-op run
    # — which is how a null result gets mistaken for a finding.
    from mgot_utils.processing import zone_preprocessor, lvl_preprocessor, squeeze
    zp = inspect.getsource(zone_preprocessor.create_mth_zone)
    lp = inspect.getsource(lvl_preprocessor.create_lvls)
    knobs = {
        'source_tree': str(tree),
        'process_time': 'OLD' if 'process_time = bar.time + (config.delta_epoch' in zp else 'NEW',
        'block_one_time': 'OLD' if "zone.type in ('mth', 'squeeze')" in lp else 'NEW',
        'backfill_cap': 'order+1' if 'config.order + 1' in lp else '2',
        'squeeze_process_time': ('NEW' if 'process_time = bar.time\n' in
                                 inspect.getsource(squeeze.find_secondary_swing) else 'OLD'),
        'squeeze_anchor': ('NEW' if 'zone.process_time' in lp else 'OLD'),
    }
    want = CELLS[args.cell]
    got = (knobs['process_time'], knobs['block_one_time'], knobs['backfill_cap'])
    if got != want:
        raise SystemExit(f'cell {args.cell} wanted knobs {want}, modules have {got}')
    # Cell S moves only the squeeze's `process_time`. Its anchor stays `zone.time`
    # because at delta_t = 0 there is no gap to replay and the anchor never
    # comes into it — the squeeze is collected on the bar that created it.
    sq_want = 'NEW' if args.cell == 'S' else 'OLD'
    if knobs['squeeze_process_time'] != sq_want:
        raise SystemExit(f'cell {args.cell} wanted squeeze process_time {sq_want}, '
                         f"module has {knobs['squeeze_process_time']}")
    print(f'[{args.cell}/{args.timeframe}] {knobs}', flush=True)

    prod = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    with Replay(args.symbol, [args.timeframe], db=args.db, capture=False) as rp:
        rp.reset()

        # -- instrument the backfill at its BINDING site --------------------
        # `10_zone_processor/main.py` does `from mgot_utils import *`, so it
        # holds its own reference from import time. Patching the definition in
        # lvl_preprocessor would change nothing the service calls.
        module = rp._modules['10_zone_processor']
        inner = module.create_lvls
        calls = []

        def counting_create_lvls(zone, bar):
            levels = inner(zone, bar)
            calls.append({'zone_id': zone.id, 'type': zone.type,
                          'row_time': int(bar.time),
                          'process_time': int(zone.process_time or 0),
                          'zone_time': int(zone.time or 0),
                          'move_end_time': int(zone.move_end_time or 0),
                          'n_levels': len(levels)})
            return levels

        module.create_lvls = counting_create_lvls
        assert module.create_lvls is not inner, 'patch did not take'

        n = rp.load_from(prod, load_start, end)
        print(f'[{args.cell}/{args.timeframe}] loaded {n} bars', flush=True)
        rp.run(progress_every=1000)

        r = rp.r
        from mgot_utils.core.configs import Config
        cfg = Config()
        d = cfg.delta_epoch[args.timeframe]
        cap = cfg.order + 1 if knobs['backfill_cap'] == 'order+1' else 2

        # -- proof the derived layers actually ran --------------------------
        # `advance_s2` swallows every exception into `{symbol}:s2_errors`, so a
        # dance count of zero would otherwise be reported as a finding when it
        # is a crash. This is the "print proof the patch took effect" rule
        # applied to a layer rather than a patch.
        s2_errors = r.hgetall(f'{args.symbol}:s2_errors')

        # A squeeze's `time` is the PREVIOUS mth's move start — production
        # median 15 bars, max 531, before the squeeze exists — so filtering the
        # window on `zone.time` silently drops squeezes created inside it.
        # `squeezes_by_creation` is scored by the creating bar.
        created = dict(r.zrange(f'{args.symbol}:{args.timeframe}:squeezes_by_creation',
                                0, -1, withscores=True))

        def in_window(z):
            if z.get('type') == 'squeeze':
                return int(created.get(z['id'], z.get('time') or 0)) >= start
            return int(z.get('time') or 0) >= start

        zones = [z for z in rp.zones(args.timeframe) if in_window(z)]
        ids = {z['id'] for z in zones}
        ever_complete = lambda z: z.get('was_completed') == '1'

        by_state = Counter((z.get('type'), z.get('completion')) for z in zones)
        # `taken_out` implies the zone passed through `complete`, so the two
        # questions "what is it now" and "did it ever complete" are different.
        ever = Counter((z.get('type'), ever_complete(z)) for z in zones)

        lag = Counter()
        for z in zones:
            if z.get('type') == 'mth':
                pt, me = int(z.get('process_time') or 0), int(z.get('move_end_time') or 0)
                lag[(pt - me) // d] += 1

        backfill = Counter()
        no_levels = []
        for c in calls:
            if c['zone_id'] not in ids:
                continue
            anchor = (c['zone_time'] if c['type'] == 'squeeze'
                      or (knobs['block_one_time'] == 'OLD' and c['type'] == 'mth')
                      else c['move_end_time'])
            dt = (c['row_time'] - anchor) // d
            backfill[(c['type'], 'ran' if 0 < dt <= cap else 'skipped')] += 1
            if c['n_levels'] == 0:
                no_levels.append(c['zone_id'])

        # -- the claim under test -------------------------------------------
        # A close through block_one inside the window the delay blinds. The
        # window runs move_end+1 .. process_time INCLUSIVE: levels created on
        # the process_time bar record no gain or loss for that bar, so it is
        # blind too. `seen` says whether this cell's backfill replayed it.
        bar_ids = r.zrangebyscore(f'{args.symbol}:{args.timeframe}:bars_index',
                                  load_start - 10 * d, end)
        pipe = r.pipeline()
        for b in bar_ids:
            pipe.hgetall(b)
        bars = {int(b['time']): b for b in pipe.execute() if b and b.get('time')}

        blind = Counter()
        blind_ids = []
        for z in zones:
            if z.get('type') != 'mth':
                continue
            pt, me = int(z.get('process_time') or 0), int(z.get('move_end_time') or 0)
            if pt <= me + d:
                continue                       # no window to be blind in
            b1, direction = float(z.get('block_one') or 0), int(z.get('direction') or 0)
            # block_one is the move extreme: an mth is invalidated by a close
            # above it when bullish, below it when bearish (post_process.update_mth,
            # achievements.gaining_lvl / losing_lvl).
            if any(t in bars and ((float(bars[t]['close']) > b1) if direction == 1
                                  else (float(bars[t]['close']) < b1))
                   for t in range(me + d, pt + d, d)):
                blind['break'] += 1
                if ever_complete(z):
                    blind['break_then_completed'] += 1
                    blind_ids.append(z['id'])
        # Whether this cell's backfill covered that window at all, so a reader
        # cannot mistake "there was a break" for "the break was missed".
        blind['window_replayed_by_backfill'] = int(
            knobs['block_one_time'] == 'NEW' and 0 < (cfg.order + 1) <= cap)

        sq_blind = Counter()
        for z in zones:
            if z.get('type') != 'squeeze':
                continue
            cb = int(created.get(z['id'], 0))
            pt = int(z.get('process_time') or 0)
            if not cb or pt <= cb:
                continue
            b1, direction = float(z.get('block_one') or 0), int(z.get('direction') or 0)
            # The squeeze is fetched on the bar after creation at the earliest,
            # so the window runs creation+1 .. process_time inclusive.
            if any(t in bars and ((float(bars[t]['close']) > b1) if direction == 1
                                  else (float(bars[t]['close']) < b1))
                   for t in range(cb + d, pt + d, d)):
                sq_blind['break'] += 1
                if ever_complete(z):
                    sq_blind['break_then_completed'] += 1

        dance = Counter()
        for entry in r.zrange(f'{args.symbol}:{args.timeframe}:dance_index', 0, -1):
            if int(entry.rsplit(':', 1)[1]) >= start:
                dance[r.hget(entry, 'state')] += 1

        out = {
            'cell': args.cell, 'timeframe': args.timeframe, 'symbol': args.symbol,
            'window': [args.start, args.end], 'warmup_days': args.warmup_days,
            'bars_replayed': n, 'knobs': knobs,
            's2_errors': s2_errors or {},
            'zones_total': len(zones),
            'by_type': dict(Counter(z.get('type') for z in zones)),
            'by_type_state': {f'{k[0]}|{k[1]}': v for k, v in sorted(by_state.items())},
            'ever_completed': {f'{k[0]}|{"yes" if k[1] else "no"}': v
                               for k, v in sorted(ever.items(), key=lambda kv: (kv[0][0], kv[0][1]))},
            'mth_process_lag_bars': {str(k): v for k, v in sorted(lag.items())},
            'backfill': {f'{k[0]}|{k[1]}': v for k, v in sorted(backfill.items())},
            'zones_with_no_levels': len(no_levels),
            'blind_window': dict(blind),
            'squeeze_blind_window': dict(sq_blind),
            'blind_window_ids': blind_ids,
            'dance': dict(dance),
        }

    Path(args.out).write_text(json.dumps(out, indent=2))
    print(f'[{args.cell}/{args.timeframe}] wrote {args.out}', flush=True)
    print(json.dumps({k: out[k] for k in
                      ('s2_errors', 'by_type', 'by_type_state', 'ever_completed',
                       'mth_process_lag_bars', 'backfill', 'blind_window',
                       'squeeze_blind_window', 'dance')},
                     indent=2), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
