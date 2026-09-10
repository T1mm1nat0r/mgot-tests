"""Pull Binance USDT-perpetual bars into a scratch keyspace, for comparison replays.

Why
---
MGOT ingests Binance **spot** (`01_retriever` uses `get_historical_klines_generator`
and `kline_socket`). The courses' annotated BTC examples — which CLAUDE.md names as
ground truth — are drawn on **perpetuals**: "our BTCUSDT.P price-action"
(`advanced/3-1:15`), "another BTCUSDT.P example" (`basic/101-8:27`). So the
detector is being validated against a different instrument from the one the method
was taught on.

Perps cost none of the four things CLAUDE.md defers markets to S10 for: same
Binance client, same 24/7 continuous calendar, no sessions, and no rollover
because perps never expire. NQ is a different matter — it is not on Binance at
all, so it needs a new venue and stays S10.

This writes bars only. It never touches db 0, and it never writes anything the
pipeline computes — the replay harness derives all of that. Bars are stored under
their own symbol so nothing can collide with live spot state.

Usage:
    uv run python harness/fetch_perp_bars.py \
        --symbol BTCUSDT --timeframe 15m \
        --start 2026-06-24 --end 2026-08-01 --db 13

Then replay from it:
    src = redis.Redis(port=6379, db=13, decode_responses=True)
    with Replay('BTCUSDTPERP', ['15m'], db=14) as rp:
        rp.load_from(src, start_ms, end_ms)
"""

import argparse
import asyncio
from datetime import datetime, timezone

import redis
from binance import AsyncClient

# What `02_timeframe_creator` emits, and all the replay harness reads. Everything
# else on a Bar is assigned by a later stage and must NOT be seeded here — a bar
# carrying fields it could not have live is the exact defect that made the S3
# failure classifier look correct. See replay.py's UPSTREAM_FIELDS.
STORED = ('id', 'symbol', 'timeframe', 'time', 'open', 'high', 'low', 'close', 'volume')


def _epoch(day: str) -> int:
    return int(datetime.strptime(day, '%Y-%m-%d')
               .replace(tzinfo=timezone.utc).timestamp() * 1000)


async def fetch(symbol: str, timeframe: str, start: int, end: int) -> list[dict]:
    """Perpetual klines, paged, oldest first.

    `futures_historical_klines_generator` pages for us and needs no API key —
    klines are a public endpoint.
    """
    client = await AsyncClient.create()
    rows = []
    try:
        async for k in await client.futures_historical_klines_generator(
                symbol, timeframe, str(start), str(end)):
            rows.append(k)
    finally:
        await client.close_connection()
    return rows


def store(rows: list, symbol: str, timeframe: str, r) -> int:
    """Write Bar-shaped hashes plus the index the harness reads."""
    pipe = r.pipeline()
    index = f'{symbol}:{timeframe}:bars_index'
    n = 0
    for k in rows:
        t = int(k[0])
        bar_id = f'{symbol}:{timeframe}:bar:{t}'
        pipe.hset(bar_id, mapping={
            'id': bar_id, 'symbol': symbol, 'timeframe': timeframe, 'time': t,
            'open': k[1], 'high': k[2], 'low': k[3], 'close': k[4], 'volume': k[5],
        })
        pipe.zadd(index, {bar_id: t})
        n += 1
    pipe.execute()
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--symbol', default='BTCUSDT', help='the Binance perp symbol')
    ap.add_argument('--store-as', default=None,
                    help='symbol to store under (default: <symbol>PERP)')
    ap.add_argument('--timeframe', default='15m')
    ap.add_argument('--start', required=True, help='UTC date')
    ap.add_argument('--end', required=True, help='UTC date')
    ap.add_argument('--db', type=int, required=True)
    args = ap.parse_args()

    if args.db == 0:
        raise SystemExit('refusing to write to db 0 — that is live state')

    stored_as = args.store_as or f'{args.symbol}PERP'
    start, end = _epoch(args.start), _epoch(args.end)

    rows = asyncio.run(fetch(args.symbol, args.timeframe, start, end))
    if not rows:
        raise SystemExit('no klines returned')

    r = redis.Redis(host='localhost', port=6379, db=args.db, decode_responses=True)
    n = store(rows, stored_as, args.timeframe, r)

    fmt = lambda t: datetime.fromtimestamp(int(t) / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M')
    print(f'{n} {args.timeframe} perp bars -> db {args.db} as {stored_as}')
    print(f'   {fmt(rows[0][0])} .. {fmt(rows[-1][0])} UTC')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
