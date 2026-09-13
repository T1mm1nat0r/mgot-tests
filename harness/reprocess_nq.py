"""Replay stored NQU6 15m bars through the live pipeline, the way 02 emits them.

NQ has no ingest path: `01_retriever` is Binance-only and never reads
`mgot:research_symbols`, and the Massive loader writes to a scratch db by its own
docstring. So the bars in db 0 are the only copy, and a reprocess means feeding
them back into `stream:clean_candles` exactly as `02_timeframe_creator` would.

Two things that are easy to get wrong and silent if you do:

* **Emit a clean bar, not the stored hash.** A stored bar carries fields assigned
  two stages downstream — `achievements`, `mth`, `origin`, `is_peak`, `structure`.
  Feeding those back is the replay-contamination trap in CLAUDE.md: the service
  sees state it would not have live.
* **`03` skips any bar whose symbol reads ingestion mode `stopped`**, and
  `clear_all_data` deletes `ingestion:{symbol}:status`. Without re-setting the
  mode every bar is dropped and the run looks like it did nothing.

Move assignment is `02._attach_move`, replicated here — it reads the *previous*
bar from Redis, so bars must be written one at a time, in order.
"""
import argparse, json, sys, time
sys.path.insert(0, '/Users/timothy/Projects/MGOT/utils/src')
from mgot_utils import (Bar, connect_to_redis, produce, assign_move,
                        retrieve_window, clear_all_data)

SYMBOL, TF = 'NQU6', '15m'
STREAM_OUT = 'stream:clean_candles'
OHLCV = ('open', 'high', 'low', 'close', 'volume')


def clean_bar(h: dict) -> Bar:
    """Only what 02 would have: identity, OHLCV, time. Nothing downstream."""
    return Bar(id=h['id'], symbol=h['symbol'], timeframe=h['timeframe'],
               time=int(h['time']), **{k: float(h[k]) for k in OHLCV})


def attach_move(bar: Bar, r) -> None:
    window = retrieve_window(bar, 2)
    previous = window[1] if len(window) > 1 and window[1] != 'New Run: Empty Bar' else None
    assign_move(bar, r, previous)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--backup', required=True)
    ap.add_argument('--limit', type=int, default=0)
    ap.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()

    data = json.load(open(a.backup))[TF]
    rows = sorted(data['hashes'].values(), key=lambda h: int(h['time']))
    if a.limit:
        rows = rows[:a.limit]
    print(f'{len(rows)} bars  {rows[0]["time"]} -> {rows[-1]["time"]}')

    if a.dry_run:
        for h in rows[:3]:
            b = clean_bar(h)
            print('  would emit:', json.dumps(b.model_dump(mode='json'))[:300])
        print('\nstored hash for comparison (note the downstream fields):')
        print(' ', {k: v for k, v in rows[0].items()
                    if k in ('achievements','mth','origin','is_peak','structure','move_id')})
        return 0

    r = connect_to_redis()
    print('clearing NQU6 derived state (bars included — they are re-created below)')
    print(' ', clear_all_data(SYMBOL, r))
    r.hset(f'ingestion:{SYMBOL}:status', mapping={
        'mode': 'historic', 'bars_processed': 0, 'pipeline_processed': 0,
        'total_estimated': len(rows), 'sync_bounded': 1})

    t0 = time.time()
    for i, h in enumerate(rows, 1):
        bar = clean_bar(h)
        bar.add_direction()
        attach_move(bar, r)
        bar.sync_with_db(r)
        produce(r, STREAM_OUT, bar.model_dump(mode='json'))
        if i % 500 == 0:
            r.hset(f'ingestion:{SYMBOL}:status', 'bars_processed', i)
            print(f'  {i}/{len(rows)}  {i/(time.time()-t0):.0f} bars/s emitted')
    r.hset(f'ingestion:{SYMBOL}:status', mapping={'bars_processed': len(rows),
                                                  'mode': 'paused'})
    print(f'emitted {len(rows)} bars in {time.time()-t0:.1f}s; mode -> paused')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
