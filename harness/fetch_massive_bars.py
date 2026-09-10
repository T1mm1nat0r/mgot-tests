"""Pull CME futures bars from Massive into a scratch keyspace.

Why Massive
-----------
NQ is not on Binance, so the pipeline's only data source cannot reach it.
Massive's free futures tier gives **2 years of CME minute bars** at 5 requests a
minute — slow for a bulk pull, fine for an overnight backfill, and far deeper
than Yahoo's rolling 60-day 15m window. Live data needs their $199/mo Advanced
tier, which is poor value for one symbol; a broker feed at the CME non-pro fee
(~$10-20/mo) is the cheaper live path. The two do not have to come from the same
vendor — `01_retriever` already separates historical sync from live streaming.

What this does NOT do
---------------------
Writes bars into a scratch database, nothing else. It does not touch db 0, does
not feed the live pipeline, and does not make NQ processable — that still needs
`profile_for()` routed to `SessionMarketProfile` and the three ring-fenced
`REMAINING_RAW_ARITHMETIC` sites routed through the profile. This is the input
side of the research path, so a replay can be run against real NQ bars.

**Contracts, not a continuous series.** Massive lists continuous contracts as
"coming soon", so `--ticker` is one expiry (e.g. NQZ6). That is the better input
anyway: a stitched continuous series injects a price jump at every quarterly
roll, and MGOT would read that as a large real move and mint MTHs from it.
Handling the roll is deferred, not solved — fetch one contract at a time and do
not concatenate them into one symbol without deciding what happens at the seam.

Setup:
    Create a key at https://massive.com/dashboard/keys, then add it to the
    repo-root `.env` — which is gitignored, untracked, and already holds
    GITHUB_TOKEN:

        MASSIVE_API_KEY=your_key_here

    This file reads that automatically. A real `export` still takes precedence,
    and the key is never accepted as a command-line argument, where it would end
    up in shell history and in `ps`.

Usage:
    uv run python harness/fetch_massive_bars.py --list-contracts NQ
    uv run python harness/fetch_massive_bars.py --ticker NQZ6 \
        --resolution 15min --start 2026-06-01 --db 13
"""

import argparse
import json
import os
import pathlib
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import redis

def _load_dotenv() -> None:
    """Read the repo-root .env, without overriding anything already exported.

    The root `.env` is gitignored and already holds `GITHUB_TOKEN`; the deploy
    skill sources it the same way. Reading it here means the key lives in one
    place instead of in a shell history, and a real export still wins.
    """
    env = pathlib.Path(__file__).resolve().parents[2] / '.env'
    if not env.exists():
        return
    for line in env.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        name, _, value = line.partition('=')
        os.environ.setdefault(name.strip(), value.strip().strip('"').strip("'"))


_load_dotenv()

BASE = os.environ.get('MASSIVE_BASE_URL', 'https://api.massive.com')

# Massive's resolution strings -> our timeframe names. "minute candles go up to
# 59min; after that, use 1hour."
RESOLUTIONS = {'1min': '1m', '3min': '3m', '15min': '15m',
               '1hour': '1h', '4hour': '4h', '1day': '1d'}

# The free tier is 5 requests/minute. Pace deliberately rather than discovering
# the limit as a 429 halfway through a backfill.
FREE_TIER_SLEEP = 13.0


def _request(path: str, params: dict, key: str) -> dict:
    """GET with the API key, trying both auth conventions.

    The published docs say to read `MASSIVE_API_KEY` from the environment but do
    not state whether it travels as a bearer token or a query parameter, so this
    tries the header first and falls back. Whichever works is reported once.
    """
    qs = urllib.parse.urlencode(params)
    url = f'{BASE}{path}?{qs}'
    attempts = (
        ('header', {'Authorization': f'Bearer {key}'}, url),
        ('query', {}, f'{url}&apiKey={urllib.parse.quote(key)}'),
    )
    last = None
    for how, headers, target in attempts:
        try:
            req = urllib.request.Request(
                target, headers={'User-Agent': 'mgot/1.0', **headers})
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = json.load(resp)
            if _request.auth is None:
                _request.auth = how
                print(f'   (auth: {how})')
            return body
        except urllib.error.HTTPError as e:
            last = f'HTTP {e.code} {e.reason}'
            if e.code not in (401, 403):
                raise SystemExit(f'{target.split("?")[0]}: {last}')
        except Exception as e:                       # noqa: BLE001
            raise SystemExit(f'{target.split("?")[0]}: {e}')
    raise SystemExit(f'authentication failed both ways ({last}). Check MASSIVE_API_KEY.')


_request.auth = None


def list_contracts(product: str, key: str, spreads: bool = False) -> None:
    """Outright contracts for a product, as they stand today.

    `/contracts` is a **point-in-time** endpoint: each row is a contract as of a
    `date`, so without one you get the same contract repeated across every date
    it existed. Asking for today gives one row per live contract.

    Calendar spreads (`NQU6-NQZ6`) come back from the same query and are hidden
    by default — a spread's price is the difference between two contracts, which
    is not a market MGOT can read structure from.
    """
    today = datetime.now(timezone.utc).date().isoformat()
    body = _request('/futures/v1/contracts',
                    {'product_code': product, 'date': today,
                     'limit': 200, 'sort': 'ticker.asc'}, key)
    rows = body.get('results') or []
    if not rows:
        raise SystemExit(f'no contracts for product_code={product} on {today}')

    seen = {}
    for c in rows:
        if '-' in (c.get('ticker') or '') and not spreads:
            continue
        seen.setdefault(c['ticker'], c)
    if not seen:
        raise SystemExit(f'only spreads returned for {product}')

    print(f'\n{len(seen)} outright contracts for {product}, as of {today}:\n')
    print('   %-9s %-12s %-12s %-7s %s'
          % ('ticker', 'first trade', 'last trade', 'active', ''))
    for t, c in sorted(seen.items(), key=lambda kv: kv[1].get('last_trade_date') or ''):
        expiry = c.get('last_trade_date') or '—'
        note = ''
        if expiry >= today:
            days = (datetime.fromisoformat(expiry).date()
                    - datetime.now(timezone.utc).date()).days
            note = f'expires in {days}d' if days < 120 else ''
        print('   %-9s %-12s %-12s %-7s %s'
              % (t, c.get('first_trade_date') or '—', expiry, c.get('active'), note))
    print('\n   The front month is the nearest unexpired one. A deferred contract'
          '\n   exists long before it trades — its early bars are thin, and thin'
          '\n   volume means flat candles, which carry no direction for MGOT.')


def fetch(ticker: str, resolution: str, start: str, key: str) -> list[dict]:
    """Page forward from `start`, oldest first."""
    rows, cursor, pages = [], start, 0
    while True:
        body = _request(f'/futures/v1/aggs/{urllib.parse.quote(ticker)}', {
            'resolution': resolution,
            'window_start.gte': cursor,
            'limit': 50_000,
            'sort': 'window_start.asc',
        }, key)
        page = body.get('results') or []
        if not page:
            break
        # window_start is NANOSECONDS. Treating it as ms would place every bar
        # in 1970 and the pipeline would accept it without complaint.
        rows.extend(page)
        pages += 1
        newest = max(int(r['window_start']) for r in page)
        if len(page) < 2:
            break
        cursor = str(newest + 1)
        print(f'   page {pages}: {len(page)} bars, through '
              f'{datetime.fromtimestamp(newest / 1e9, timezone.utc):%Y-%m-%d %H:%M}',
              flush=True)
        time.sleep(FREE_TIER_SLEEP)
    # de-duplicate on the bar's own timestamp; paging boundaries overlap
    return list({int(r['window_start']): r for r in rows}.values())


def store(rows: list[dict], symbol: str, timeframe: str, r) -> int:
    pipe = r.pipeline()
    index = f'{symbol}:{timeframe}:bars_index'
    for row in rows:
        t = int(row['window_start']) // 1_000_000        # ns -> ms
        bar_id = f'{symbol}:{timeframe}:bar:{t}'
        pipe.hset(bar_id, mapping={
            'id': bar_id, 'symbol': symbol, 'timeframe': timeframe, 'time': t,
            'open': row['open'], 'high': row['high'], 'low': row['low'],
            'close': row['close'], 'volume': row.get('volume', 0),
        })
        pipe.zadd(index, {bar_id: t})
    pipe.execute()
    return len(rows)


def report_gaps(rows: list[dict], timeframe: str, symbol: str) -> None:
    """Check every gap against the session calendar rather than assuming.

    A dropped bar and a session boundary look identical in a gap listing, and
    treating a vendor's omission as a closure silently rewrites the calendar.
    """
    from mgot_utils.core.configs import Config, SessionMarketProfile
    profile = SessionMarketProfile(Config().delta_epoch)
    times = sorted(int(r['window_start']) // 1_000_000 for r in rows)
    delta = Config().delta_epoch[timeframe]
    explained = unexplained = 0
    first = None
    for a, b in zip(times, times[1:]):
        if b - a <= delta:
            continue
        try:
            profile.assert_gap_explained(a, b, timeframe)
            explained += 1
        except ValueError as e:
            unexplained += 1
            first = first or str(e)
    print(f'\n   gaps closed by the session calendar : {explained}')
    print(f'   gaps NOT explained                  : {unexplained}')
    if first:
        print(f'\n   first unexplained:\n     {first[:200]}')
        print('\n   Either the feed dropped bars or a CME holiday is missing from'
              '\n   SessionMarketProfile.holidays. Do not assume a closure — check'
              '\n   the published CME calendar.')


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--list-contracts', metavar='PRODUCT',
                    help='list outright contracts for a product code (e.g. NQ) and exit')
    ap.add_argument('--spreads', action='store_true',
                    help='include calendar spreads in --list-contracts')
    ap.add_argument('--ticker', help='one contract, e.g. NQZ6')
    ap.add_argument('--store-as', default=None, help='symbol to store under (default: --ticker)')
    ap.add_argument('--resolution', default='15min', choices=sorted(RESOLUTIONS))
    ap.add_argument('--start', help='YYYY-MM-DD')
    ap.add_argument('--db', type=int, help='scratch redis db')
    args = ap.parse_args()

    key = os.environ.get('MASSIVE_API_KEY')
    if not key:
        raise SystemExit('set MASSIVE_API_KEY (https://massive.com/dashboard/keys)')

    if args.list_contracts:
        list_contracts(args.list_contracts, key, spreads=args.spreads)
        return 0

    if not (args.ticker and args.start and args.db is not None):
        raise SystemExit('--ticker, --start and --db are required')
    if args.db == 0:
        raise SystemExit('refusing to write to db 0 — that is live state')

    timeframe = RESOLUTIONS[args.resolution]
    symbol = args.store_as or args.ticker
    print(f'{args.ticker} {args.resolution} from {args.start} '
          f'-> db {args.db} as {symbol}:{timeframe}')

    rows = fetch(args.ticker, args.resolution, args.start, key)
    if not rows:
        raise SystemExit('no bars returned')

    r = redis.Redis(host='localhost', port=6379, db=args.db, decode_responses=True)
    n = store(rows, symbol, timeframe, r)
    lo = min(int(x['window_start']) for x in rows) / 1e9
    hi = max(int(x['window_start']) for x in rows) / 1e9
    print(f'\n   {n} bars stored   '
          f'{datetime.fromtimestamp(lo, timezone.utc):%Y-%m-%d %H:%M} .. '
          f'{datetime.fromtimestamp(hi, timezone.utc):%Y-%m-%d %H:%M} UTC')
    report_gaps(rows, timeframe, symbol)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
