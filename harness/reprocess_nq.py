"""Replay stored CME futures bars through the live pipeline, the way 02 emits them.

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

TF = '15m'
STREAM_OUT = 'stream:clean_candles'
OHLCV = ('open', 'high', 'low', 'close', 'volume')


def rows_from_backup(path: str) -> list[dict]:
    """Bars from a `{tf: {"hashes": {...}}}` dump, or from JSON lines.

    Two shapes because two things write them: the original NQU6 dump nests by
    timeframe, and a straight `hgetall` export writes one bar per line. Sniffing
    the first character is enough to tell them apart and costs nothing.
    """
    with open(path) as f:
        head = f.read(1)
        f.seek(0)
        if head == '{':
            try:
                return list(json.load(f)[TF]['hashes'].values())
            except (KeyError, TypeError, ValueError):
                f.seek(0)
        return [h for h in (json.loads(x) for x in f if x.strip())
                if h.get('timeframe', TF) == TF]


def rows_from_db(symbol: str, db: int) -> list[dict]:
    """Bars straight out of a scratch db — where `fetch_massive_bars` puts them.

    The fetch loader writes to a scratch keyspace and stops there, so without
    this the only way into the pipeline was a hand-made backup file in the one
    shape this script used to accept.
    """
    # A plain client, not `connect_to_redis`: that one takes its db from
    # `REDIS_DB` and pools per process, so asking it for a scratch db would
    # either be ignored or repoint the connection the replay writes through.
    import os, redis as _redis
    src = _redis.Redis(host=os.getenv('REDIS_HOST', 'localhost'),
                       port=int(os.getenv('REDIS_PORT', 6379)),
                       db=db, decode_responses=True)
    ids = src.zrange(f'{symbol}:{TF}:bars_index', 0, -1)
    pipe = src.pipeline(transaction=False)
    for zid in ids:
        pipe.hgetall(zid)
    return [h for h in pipe.execute() if h]


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
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument('--backup', help='bars from a dump file')
    src.add_argument('--from-db', type=int,
                     help='bars from a scratch redis db, e.g. 13 — where '
                          'fetch_massive_bars stores them')
    ap.add_argument('--symbol', default='NQU6',
                    help='contract to replay, e.g. NQZ6. Each expiry is its own '
                         'symbol: the December contract trades above the '
                         'September one by roughly the carry, so concatenating '
                         'them would mint an MTH out of the roll gap.')
    ap.add_argument('--limit', type=int, default=0)
    ap.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()
    SYMBOL = a.symbol

    rows = (rows_from_backup(a.backup) if a.backup
            else rows_from_db(SYMBOL, a.from_db))
    rows = sorted(rows, key=lambda h: int(h['time']))
    if not rows:
        print(f'no {SYMBOL} {TF} bars found'); return 1
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
    print(f'clearing {SYMBOL} derived state (bars included — re-created below)')
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
