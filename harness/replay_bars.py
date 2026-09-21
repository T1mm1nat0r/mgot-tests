"""Feed a symbol's stored 1m bars into the live pipeline, the way 01_retriever does.

Fetch with `fetch_massive_bars.py` into a scratch db, then replay from there into
`stream:raw_candles` so `02` resamples every timeframe from one source and
`htf_links` resolves upward with the right interleaving. The alternative —
fetching a higher timeframe directly — is what left NQ with no 1h or 4h zones at
all, and there is no reason to repeat it for a new market.

Clears the symbol's derived state first, so a replay is a replay and not a merge.
Never touches any other symbol.

    python harness/replay_bars.py --symbol EURUSD --src-db 15

Then wait for every consumer group to read lag 0 before measuring anything: the
stages race, and a sample taken early samples work that has not happened yet.
"""
import argparse
import sys
import time

import redis

sys.path.insert(0, '/Users/timothy/Projects/MGOT/utils/src')
from mgot_utils import produce, clear_all_data                     # noqa: E402

OHLCV = ('open', 'high', 'low', 'close', 'volume')
STREAM_OUT = 'stream:raw_candles'


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--symbol', required=True, help='e.g. EURUSD, NQZ6')
    ap.add_argument('--src-db', type=int, default=15, help='scratch db holding {symbol}:1m (default 15)')
    ap.add_argument('--keep', action='store_true', help='do not clear the symbol first')
    args = ap.parse_args()
    if args.src_db == 0:
        raise SystemExit('--src-db 0 is live state, not a scratch db')

    src = redis.Redis(host='localhost', port=6379, db=args.src_db, decode_responses=True)
    live = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    ids = src.zrange(f'{args.symbol}:1m:bars_index', 0, -1)
    if not ids:
        raise SystemExit(f'no {args.symbol}:1m bars in db {args.src_db} — fetch them first')
    pipe = src.pipeline(transaction=False)
    for b in ids:
        pipe.hgetall(b)
    rows = sorted((h for h in pipe.execute() if h), key=lambda h: int(h['time']))
    print(f'{len(rows)} {args.symbol} 1m bars, {rows[0]["time"]} -> {rows[-1]["time"]}')

    if not args.keep:
        print(' clearing derived state:', clear_all_data(args.symbol, live))

    # 03 drops any bar whose symbol reads ingestion mode `stopped`, so the
    # symbol has to be known and running before the first bar lands.
    live.hset(f'ingestion:{args.symbol}:status', mapping={
        'mode': 'historic', 'bars_processed': 0, 'pipeline_processed': 0,
        'total_estimated': len(rows), 'sync_bounded': 1})
    live.sadd('mgot:tracked_tickers', args.symbol)

    t0 = time.time()
    for i, h in enumerate(rows, 1):
        produce(live, STREAM_OUT, {
            'id': f'{args.symbol}:1m:bar:{h["time"]}', 'symbol': args.symbol,
            'timeframe': '1m', 'time': int(h['time']),
            **{k: float(h[k]) for k in OHLCV},
        })
        if i % 10_000 == 0:
            live.hset(f'ingestion:{args.symbol}:status', 'bars_processed', i)
            print(f'  {i}/{len(rows)}  {i / (time.time() - t0):.0f} bars/s', flush=True)
    live.hset(f'ingestion:{args.symbol}:status',
              mapping={'bars_processed': len(rows), 'mode': 'paused'})
    print(f'emitted {len(rows)} bars in {time.time() - t0:.0f}s')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
