"""
Deterministic replay of the real pipeline over a fixed bar range.

The harness exists because three questions in a row took the shape *"why did the
pipeline decide X at time T"* — a bar that should have been an origin and was
not, leg endings that looked wrong, a gate whose branch could not be recovered —
and none of them were answerable after the fact. The state that decides them
(`last_origin`, `last_mth`, the pending potential-origin set) lives in keys that
are **overwritten every bar**, so by the time the question is asked, the answer
is gone.

Two jobs, then:

1. **Reproduce.** Run a bar range through the pipeline against an isolated
   keyspace, repeatably, without touching live state.
2. **Explain.** Capture the decision-relevant state *per bar*, so "what did the
   detector know at T" is a lookup rather than an archaeology exercise.

It runs the **actual service code** — `03_levels_and_zones/main.py` and friends,
imported from their real paths — not a reimplementation. A harness that
reimplements the pipeline tests the reimplementation. Only the transport between
services is replaced: the live pipeline chains stages through Redis Streams and
a `processed_bar` pub/sub round-trip, and the harness chains them by calling one
after another, which is the same order with the waiting removed.

Isolation is by `REDIS_DB`, read by `connect_to_redis`. The environment must be
set *before* the service modules are imported, because each takes its connection
at module scope — `Replay.__enter__` handles that, which is why the class is a
context manager rather than a plain constructor.

Usage:

    with Replay('BTCUSDT', ['15m'], db=9) as rp:
        rp.load_from(prod_redis, start_ms, end_ms)
        rp.run()
        print(rp.trace_at('15m', ts)['last_origin'])
"""

import importlib.util
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

# Pipeline order. These are the four services that transform a bar; 01 and 02
# produce bars (the harness supplies them instead) and 11 only snapshots.
SERVICES = [
    ('03_levels_and_zones', 'stream:clean_candles'),
    ('04_peaks_and_structure', 'stream:leveled_and_updated'),
    ('07_origins', 'stream:bars_with_structure'),
    ('10_zone_processor', 'stream:bars_with_origins'),
]

# Keys whose value at a given bar decides a detection outcome and which the next
# bar overwrites. Capturing these is the whole point — `last_origin` in
# particular is a single hash, so the value it held when a given MTH was
# processed is unrecoverable from a finished run.
TRACKED_HASHES = ['last_origin', 'last_mth']
TRACKED_STRINGS = ['last_mth:0', 'last_mth:1']
TRACKED_ZSETS = ['sorted:origin', 'sorted:moves']


class Replay:
    """Drives real service code over a bar range in an isolated keyspace."""

    def __init__(self, symbol: str, timeframes: list[str], db: int = 9,
                 host: str | None = None, port: int | None = None,
                 capture: bool = True):
        self.symbol = symbol
        self.timeframes = list(timeframes)
        self.db, self.host, self.port = db, host, port
        self.capture = capture
        self.bars: list[dict] = []
        self.traces: dict[tuple[str, int], dict] = {}
        self._saved_env: dict[str, str | None] = {}
        self._modules: dict[str, object] = {}
        self.r = None

    # -- lifecycle ---------------------------------------------------------

    def __enter__(self):
        for key, value in (('REDIS_DB', str(self.db)),
                           ('REDIS_HOST', self.host),
                           ('REDIS_PORT', str(self.port) if self.port else None)):
            self._saved_env[key] = os.environ.get(key)
            if value is not None:
                os.environ[key] = value

        # Import only now: each service binds its Redis connection at module
        # scope, so anything imported before the env is set would hold a
        # connection to the live keyspace.
        from mgot_utils.core.functions import connect_to_redis
        self.r = connect_to_redis()
        if self.r.connection_pool.connection_kwargs.get('db') != self.db:
            raise RuntimeError(
                f'refusing to run: connected to db '
                f'{self.r.connection_pool.connection_kwargs.get("db")}, not {self.db}. '
                'mgot_utils was imported before Replay set REDIS_DB, so the '
                'connection pool is already bound to the live keyspace.'
            )
        self._load_services()
        return self

    def __exit__(self, *exc):
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        return False

    def _load_services(self):
        """Import each service's main.py from its real path.

        Directory names begin with digits, so they are not importable as
        packages — hence loading by file location. Importing is side-effect
        free: `stream_consumer` creates its consumer group when the wrapped
        function is *called*, not at decoration.
        """
        for name, _ in SERVICES:
            path = REPO / name / 'main.py'
            spec = importlib.util.spec_from_file_location(f'harness_{name}', path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
            self._neutralise(name, module)
            self._modules[name] = module

    def _neutralise(self, name: str, module) -> None:
        """Replace inter-service transport, and nothing else.

        The harness chains stages directly, so `produce` would only write to
        streams nobody reads. 03 additionally blocks on a `processed_bar`
        publish from 10 and consults the ingestion mode — both are orchestration
        for a live multi-process pipeline, and neither affects detection.
        """
        module.produce = lambda *a, **k: None
        if name == '03_levels_and_zones':
            module.sync_with_achiever = lambda bar: True
            module.get_ingestion_mode = lambda symbol: 'running'
            module.clear_stale_messages = lambda: None

    # -- input -------------------------------------------------------------

    def load_from(self, source, start_ms: int, end_ms: int) -> int:
        """Pull raw bars for the configured timeframes from another Redis.

        Bars are pipeline *input*, so reading them from production couples the
        replay to nothing computed. Ordered by time across timeframes, which is
        how the live pipeline sees them — a 15m bar closing at the same instant
        as a 1h bar reaches the detector in one specific order, and htf_links
        depends on it.
        """
        rows = []
        for timeframe in self.timeframes:
            ids = source.zrangebyscore(
                f'{self.symbol}:{timeframe}:bars_index', start_ms, end_ms)
            pipe = source.pipeline()
            for bar_id in ids:
                pipe.hgetall(bar_id)
            for data in pipe.execute():
                if data and data.get('time'):
                    rows.append(data)
        # Sort by close, not open: a 1h bar opening at 10:00 becomes knowable at
        # 11:00, after every 15m bar inside it.
        from mgot_utils.core.configs import Config
        profile = Config().profile_for(self.symbol)
        rows.sort(key=lambda d: (profile.close_time(int(d['time']), d['timeframe']),
                                 Config().timeframes.index(d['timeframe'])))
        self.bars = rows
        return len(rows)

    def load_bars(self, bars: list[dict]) -> int:
        """Use a caller-supplied bar list, already in pipeline order."""
        self.bars = list(bars)
        return len(self.bars)

    # -- execution ---------------------------------------------------------

    def run(self, progress_every: int = 0) -> int:
        """Feed every loaded bar through the four stages, in order."""
        if not self.bars:
            raise RuntimeError('no bars loaded — call load_from or load_bars')
        for n, bar in enumerate(self.bars, start=1):
            for name, _ in SERVICES:
                self._modules[name].process_bar(dict(bar))
            if self.capture:
                self._capture(bar)
            if progress_every and n % progress_every == 0:
                print(f'  replayed {n}/{len(self.bars)}')
        return len(self.bars)

    def _capture(self, bar: dict) -> None:
        """Snapshot the state that decided this bar, before the next overwrites it."""
        timeframe, time = bar['timeframe'], int(bar['time'])
        prefix = f'{self.symbol}:{timeframe}'
        pipe = self.r.pipeline()
        for key in TRACKED_HASHES:
            pipe.hgetall(f'{prefix}:{key}')
        for key in TRACKED_STRINGS:
            pipe.get(f'{prefix}:{key}')
        for key in TRACKED_ZSETS:
            pipe.zrange(f'{prefix}:{key}', -8, -1)
        pipe.hgetall(f'{prefix}:bar:{time}')
        result = pipe.execute()

        n_h, n_s = len(TRACKED_HASHES), len(TRACKED_STRINGS)
        trace = dict(zip(TRACKED_HASHES, result[:n_h]))
        trace.update(zip(TRACKED_STRINGS, result[n_h:n_h + n_s]))
        trace.update(zip(TRACKED_ZSETS, result[n_h + n_s:-1]))
        trace['bar'] = result[-1]
        self.traces[(timeframe, time)] = trace

    # -- inspection --------------------------------------------------------

    def trace_at(self, timeframe: str, time: int) -> dict | None:
        """The decision-relevant state as it stood after `time` was processed."""
        return self.traces.get((timeframe, int(time)))

    def zones(self, timeframe: str, zone_type: str | None = None) -> list[dict]:
        """Every zone the replay produced, oldest first."""
        ids = self.r.zrange(f'{self.symbol}:{timeframe}:zones_index', 0, -1)
        pipe = self.r.pipeline()
        for zone_id in ids:
            pipe.hgetall(zone_id)
        out = [d for d in pipe.execute() if d]
        if zone_type:
            out = [d for d in out if d.get('type') == zone_type]
        return out

    def reset(self) -> None:
        """Clear the isolated keyspace.

        Guarded on the db number: flushing is only ever safe here because the
        connection is pinned away from live state, so the guard is the safety
        property, not a formality.
        """
        db = self.r.connection_pool.connection_kwargs.get('db')
        if db in (None, 0):
            raise RuntimeError(f'refusing to flush db {db} — that is live state')
        self.r.flushdb()
        self.traces.clear()
