#!/usr/bin/env python3
"""
MGOT Pipeline Performance Benchmark

Measures Redis operation counts and wall-clock latency for every key processing
function across the critical path (03 → 04 → 07 → 10).

Runs against a real Redis instance using db=15 for isolation.

Usage:
    cd /Users/timothy/Projects/MGOT/tests
    uv run python performance/benchmark.py
    uv run python performance/benchmark.py --port 6379 --db 15 --iterations 100
"""

import argparse
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import redis as redis_lib

# ─────────────────────────────────────────────────────────────────────────────
# Path setup
# ─────────────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent.parent  # /MGOT
sys.path.insert(0, str(ROOT / '04_peaks_and_structure'))
sys.path.insert(0, str(ROOT / '03_levels_and_zones'))


# ─────────────────────────────────────────────────────────────────────────────
# Redis Profiler
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Stats:
    direct_cmds: int = 0
    pipeline_cmds: int = 0
    pipeline_executes: int = 0
    direct_latency_ms: float = 0.0
    pipeline_latency_ms: float = 0.0
    cmd_counts: dict = field(default_factory=lambda: defaultdict(int))

    def reset(self):
        self.direct_cmds = 0
        self.pipeline_cmds = 0
        self.pipeline_executes = 0
        self.direct_latency_ms = 0.0
        self.pipeline_latency_ms = 0.0
        self.cmd_counts.clear()

    @property
    def total_ops(self):
        return self.direct_cmds + self.pipeline_cmds

    @property
    def total_latency_ms(self):
        return self.direct_latency_ms + self.pipeline_latency_ms


class ProfilingPipeline:
    """Wraps a Redis pipeline, counting every queued command and timing execute()."""

    def __init__(self, real_pipeline, stats: Stats):
        object.__setattr__(self, '_pipe', real_pipeline)
        object.__setattr__(self, '_stats', stats)

    def execute(self):
        t0 = time.perf_counter()
        result = self._pipe.execute()
        elapsed = (time.perf_counter() - t0) * 1000
        self._stats.pipeline_executes += 1
        self._stats.pipeline_latency_ms += elapsed
        return result

    def reset(self):
        return self._pipe.reset()

    def __getattr__(self, name):
        attr = getattr(self._pipe, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                self._stats.pipeline_cmds += 1
                self._stats.cmd_counts[name] += 1
                return attr(*args, **kwargs)
            return wrapper
        return attr


class ProfilingRedis:
    """Wraps a Redis client, counting every direct call and proxying pipeline creation."""

    def __init__(self, real_redis: redis_lib.Redis):
        object.__setattr__(self, '_r', real_redis)
        object.__setattr__(self, 'stats', Stats())

    def reset_stats(self):
        self.stats.reset()

    def pipeline(self, transaction=True):
        return ProfilingPipeline(self._r.pipeline(transaction), self.stats)

    def pubsub(self):
        return self._r.pubsub()

    def __getattr__(self, name):
        attr = getattr(self._r, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                t0 = time.perf_counter()
                result = attr(*args, **kwargs)
                elapsed = (time.perf_counter() - t0) * 1000
                self.stats.direct_cmds += 1
                self.stats.direct_latency_ms += elapsed
                self.stats.cmd_counts[name] += 1
                return result
            return wrapper
        return attr


# ─────────────────────────────────────────────────────────────────────────────
# Benchmark runner
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class BenchmarkResult:
    name: str
    service: str
    iterations: int
    wall_times_ms: list
    redis_stats_per_iter: list  # list of Stats snapshots

    @property
    def median_ms(self):
        return statistics.median(self.wall_times_ms)

    @property
    def p95_ms(self):
        s = sorted(self.wall_times_ms)
        return s[int(len(s) * 0.95)]

    @property
    def p99_ms(self):
        s = sorted(self.wall_times_ms)
        return s[int(len(s) * 0.99)]

    @property
    def avg_direct(self):
        return statistics.mean(s.direct_cmds for s in self.redis_stats_per_iter)

    @property
    def avg_piped(self):
        return statistics.mean(s.pipeline_cmds for s in self.redis_stats_per_iter)

    @property
    def avg_executes(self):
        return statistics.mean(s.pipeline_executes for s in self.redis_stats_per_iter)

    @property
    def avg_total(self):
        return statistics.mean(s.total_ops for s in self.redis_stats_per_iter)

    @property
    def avg_redis_ms(self):
        return statistics.mean(s.total_latency_ms for s in self.redis_stats_per_iter)

    @property
    def top_commands(self):
        merged = defaultdict(int)
        for s in self.redis_stats_per_iter:
            for k, v in s.cmd_counts.items():
                merged[k] += v
        total_iters = len(self.redis_stats_per_iter)
        return {k: v / total_iters for k, v in sorted(merged.items(), key=lambda x: -x[1])}


def run_benchmark(name: str, service: str, profiling_r: ProfilingRedis,
                  setup_fn, bench_fn, iterations: int) -> BenchmarkResult:
    wall_times = []
    stats_list = []

    for i in range(iterations):
        # Fresh state for each iteration
        setup_fn()
        profiling_r.reset_stats()

        t0 = time.perf_counter()
        bench_fn()
        elapsed = (time.perf_counter() - t0) * 1000

        wall_times.append(elapsed)
        # Snapshot the stats (copy values)
        s = Stats()
        s.direct_cmds = profiling_r.stats.direct_cmds
        s.pipeline_cmds = profiling_r.stats.pipeline_cmds
        s.pipeline_executes = profiling_r.stats.pipeline_executes
        s.direct_latency_ms = profiling_r.stats.direct_latency_ms
        s.pipeline_latency_ms = profiling_r.stats.pipeline_latency_ms
        s.cmd_counts = dict(profiling_r.stats.cmd_counts)
        stats_list.append(s)

    return BenchmarkResult(name, service, iterations, wall_times, stats_list)


# ─────────────────────────────────────────────────────────────────────────────
# Report printer
# ─────────────────────────────────────────────────────────────────────────────

def print_result(r: BenchmarkResult):
    cmds_str = '  '.join(f'{k}×{v:.0f}' for k, v in list(r.top_commands.items())[:6])
    print(f"\n  [{r.service}] {r.name}")
    print(f"    Wall     median={r.median_ms:.2f}ms  p95={r.p95_ms:.2f}ms  p99={r.p99_ms:.2f}ms")
    print(f"    Redis    direct={r.avg_direct:.0f}  piped={r.avg_piped:.0f}  executes={r.avg_executes:.0f}  total={r.avg_total:.0f}  redis_time={r.avg_redis_ms:.2f}ms")
    if cmds_str:
        print(f"    Cmds     {cmds_str}")


def print_summary(results: list[BenchmarkResult]):
    W = 78
    print()
    print('═' * W)
    print('SUMMARY — Redis Ops per Critical-Path Bar')
    print('═' * W)
    print(f"{'Benchmark':<42} {'Direct':>6} {'Piped':>6} {'Exec':>5} {'Total':>6} {'Median':>8}")
    print('─' * W)

    total_direct = 0
    total_piped = 0
    total_executes = 0
    total_median = 0.0

    for r in results:
        total_direct += r.avg_direct
        total_piped += r.avg_piped
        total_executes += r.avg_executes
        total_median += r.median_ms
        print(f"  {r.name:<40} {r.avg_direct:>6.0f} {r.avg_piped:>6.0f} {r.avg_executes:>5.0f} {r.avg_total:>6.0f} {r.median_ms:>7.2f}ms")

    print('─' * W)
    print(f"  {'TOTAL (sequential)':<40} {total_direct:>6.0f} {total_piped:>6.0f} {total_executes:>5.0f} {total_direct+total_piped:>6.0f} {total_median:>7.2f}ms")
    print('═' * W)


def print_bottlenecks(results: list[BenchmarkResult]):
    print()
    print('BOTTLENECKS')
    print('─' * 60)

    # Find direct calls that could be pipelined
    for r in results:
        if r.avg_direct > 2:
            print(f"  ● {r.name}: {r.avg_direct:.0f} direct (non-pipelined) calls")
            cmds = [f'{k}' for k in r.top_commands if r.top_commands[k] >= 1]
            print(f"    Standalone: {', '.join(cmds[:4])}")

    # Find high execute counts
    for r in results:
        if r.avg_executes > 2:
            print(f"  ● {r.name}: {r.avg_executes:.0f} pipeline.execute() calls — consider consolidating")

    # Slowest operations
    slowest = sorted(results, key=lambda x: x.median_ms, reverse=True)[:3]
    print()
    print('  Slowest functions:')
    for r in slowest:
        pct_redis = (r.avg_redis_ms / r.median_ms * 100) if r.median_ms > 0 else 0
        print(f"    {r.name:<42} {r.median_ms:.2f}ms  ({pct_redis:.0f}% Redis wait)")


# ─────────────────────────────────────────────────────────────────────────────
# Test data
# ─────────────────────────────────────────────────────────────────────────────

SYMBOL = 'BTCUSDT'
TF = '1h'
T = 1700000000000  # base timestamp ms
TF_DELTA = 3_600_000

BAR_DATA = {
    'id': f'{SYMBOL}:{TF}:bar:{T}',
    'symbol': SYMBOL,
    'timeframe': TF,
    'time': T,
    'open': '49800.0',
    'high': '50600.0',
    'low': '49400.0',
    'close': '50200.0',
    'volume': '1000.0',
    'direction': '1',
    'move_open': '49000.0',
    'move_start_time': str(T - 5 * TF_DELTA),
    'move_id': f'{SYMBOL}:{TF}:move:{T - 5 * TF_DELTA}',
    'top': '0',
    'mth': '0',
    'origin': '0',
    'sequence': '0',
    'structure': 'HH',
    'potential_origin': '0',
    'achievements': '',
}

MOVE_DATA = {
    'id': f'{SYMBOL}:{TF}:move:{T - 5 * TF_DELTA}',
    'symbol': SYMBOL,
    'timeframe': TF,
    'direction': '1',
    'time': str(T - 5 * TF_DELTA),
    'type': 'move',
    'open': '49000.0',
    'high': '50600.0',
    'low': '48800.0',
    'close': '50200.0',
    'volume': '5000.0',
    'length_bar': '5',
    'length_perc': '2.45',
    'og_mth_value': '0.0',
}

MTH_ZONE_DATA = {
    'id': f'{SYMBOL}:{TF}:mth:{T - 10 * TF_DELTA}',
    'symbol': SYMBOL,
    'timeframe': TF,
    'type': 'mth',
    'direction': '1',
    'completion': 'incomplete',
    'time': str(T - 10 * TF_DELTA),
    'process_time': str(T - 8 * TF_DELTA),
    'move_end_time': '0',
    'block_zero': '49000.0',
    'block_zero_id': f'{SYMBOL}:{TF}:mth:{T - 10 * TF_DELTA}:block_zero',
    'block_one': '51000.0',
    'block_one_id': f'{SYMBOL}:{TF}:mth:{T - 10 * TF_DELTA}:block_one',
    'block_half': '0.0',
    'block_half_id': '',
    'mth_value': '49000.0',
    'mth_move_id': f'{SYMBOL}:{TF}:move:{T - 15 * TF_DELTA}',
    'og_mth_value': '49000.0',
    'age_bars': '8',
    'touches': '0',
    'distance_from_price': '200.0',
    'base_open': '47500.0',
    'base_close': '49000.0',
    'previous_mth_id': '',
}

ORIGIN_ZONE_DATA = {
    'id': f'{SYMBOL}:{TF}:origin:{T - 20 * TF_DELTA}',
    'symbol': SYMBOL,
    'timeframe': TF,
    'type': 'origin',
    'direction': '1',
    'completion': 'incomplete',
    'time': str(T - 20 * TF_DELTA),
    'process_time': str(T - 18 * TF_DELTA),
    'move_end_time': str(T - 16 * TF_DELTA),
    'block_zero': '51000.0',
    'block_zero_id': f'{SYMBOL}:{TF}:origin:{T - 20 * TF_DELTA}:block_zero',
    'block_one': '47000.0',
    'block_one_id': f'{SYMBOL}:{TF}:origin:{T - 20 * TF_DELTA}:block_one',
    'block_half': '49000.0',
    'block_half_id': f'{SYMBOL}:{TF}:origin:{T - 20 * TF_DELTA}:block_half',
    'mth_value': '52000.0',
    'mth_move_id': f'{SYMBOL}:{TF}:move:{T - 22 * TF_DELTA}',
    'og_mth_value': '52000.0',
    'age_bars': '18',
    'touches': '1',
    'distance_from_price': '1200.0',
    'base_open': '0.0',
    'base_close': '0.0',
    'previous_mth_id': '',
}

SQUEEZE_ZONE_DATA = {
    'id': f'{SYMBOL}:{TF}:squeeze:{T - 10 * TF_DELTA}',
    'symbol': SYMBOL,
    'timeframe': TF,
    'type': 'squeeze',
    'direction': '1',
    'completion': 'incomplete',
    'time': str(T - 15 * TF_DELTA),
    'process_time': str(T - 8 * TF_DELTA),
    'move_end_time': str(T - 10 * TF_DELTA),
    'block_zero': '47500.0',
    'block_zero_id': f'{SYMBOL}:{TF}:squeeze:{T - 10 * TF_DELTA}:block_zero',
    'block_one': '49000.0',
    'block_one_id': f'{SYMBOL}:{TF}:squeeze:{T - 10 * TF_DELTA}:block_one',
    'block_half': '0.0',
    'block_half_id': '',
    'mth_value': '49800.0',
    'mth_move_id': '',
    'og_mth_value': '0.0',
    'age_bars': '8',
    'touches': '0',
    'distance_from_price': '1200.0',
    'base_open': '0.0',
    'base_close': '0.0',
    'previous_mth_id': '',
}


def seed_bars(r_raw):
    """Seed 5 historical bars for retrieve_window."""
    pipe = r_raw.pipeline()
    for i in range(5):
        t = T - i * TF_DELTA
        bar = {
            'id': f'{SYMBOL}:{TF}:bar:{t}',
            'symbol': SYMBOL,
            'timeframe': TF,
            'time': str(t),
            'open': str(49800.0 - i * 20),
            'high': str(50600.0 - i * 20),
            'low': str(49400.0 - i * 20),
            'close': str(50200.0 - i * 20),
            'volume': '1000.0',
            'direction': '1',
            'move_open': '49000.0',
            'move_start_time': str(T - 5 * TF_DELTA),
            'move_id': f'{SYMBOL}:{TF}:move:{T - 5 * TF_DELTA}',
            'top': '0',
            'mth': '0',
            'origin': '0',
            'sequence': '0',
            'structure': 'HH',
            'potential_origin': '0',
            'achievements': '',
        }
        pipe.hset(bar['id'], mapping=bar)
        pipe.zadd(f'{SYMBOL}:{TF}:bars_index', {bar['id']: t})
    pipe.execute()


def seed_move(r_raw):
    pipe = r_raw.pipeline()
    pipe.hset(MOVE_DATA['id'], mapping=MOVE_DATA)
    pipe.set(f'move:id_{SYMBOL}_{TF}', MOVE_DATA['id'])
    pipe.set(f'move:direction_{SYMBOL}_{TF}', '1')
    pipe.set(f'move:start_time_{SYMBOL}_{TF}', MOVE_DATA['time'])
    pipe.zadd(f'{SYMBOL}:{TF}:sorted:moves', {MOVE_DATA['id']: int(MOVE_DATA['time'])})
    pipe.execute()


def seed_levels(r_raw, zone_id: str, block_zero_val: float, block_one_val: float,
                zero_gains: int = 0, zero_losses: int = 0,
                one_gains: int = 0, one_losses: int = 0,
                direction: int = 1):
    """Seed block_zero and block_one levels for a zone."""
    pipe = r_raw.pipeline()
    levels = [
        {
            'id': f'{zone_id}:block_zero',
            'zone_id': zone_id,
            'name': 'block_zero',
            'direction': str(direction),
            'value': str(block_zero_val),
            'gains': str(zero_gains),
            'losses': str(zero_losses),
            'conseq_gain': '0',
            'last_gain': '0',
            'conseq_loss': '0',
            'last_loss': '0',
            'bl_tests': '0',
            'last_bl_test': '0',
            'br_tests': '0',
            'last_br_test': '0',
            'tested': '0',
            'state': 'awaiting_gain',
        },
        {
            'id': f'{zone_id}:block_one',
            'zone_id': zone_id,
            'name': 'block_one',
            'direction': str(direction),
            'value': str(block_one_val),
            'gains': str(one_gains),
            'losses': str(one_losses),
            'conseq_gain': '0',
            'last_gain': '0',
            'conseq_loss': '0',
            'last_loss': '0',
            'bl_tests': '0',
            'last_bl_test': '0',
            'br_tests': '0',
            'last_br_test': '0',
            'tested': '0',
            'state': 'awaiting_loss',
        },
    ]
    for lvl in levels:
        pipe.hset(lvl['id'], mapping=lvl)
    pipe.execute()
    return [lvl['id'] for lvl in levels]


def seed_level_queues(r_raw, n_levels: int = 15):
    """Seed to_lose and to_gain sorted sets with realistic levels."""
    pipe = r_raw.pipeline()
    to_lose = f'{SYMBOL}:{TF}:lvls:to_lose'
    to_gain = f'{SYMBOL}:{TF}:lvls:to_gain'

    # Close is 50200. to_lose has levels close < score (above close).
    # to_gain has levels score < close (below close).
    for i in range(n_levels):
        zone_id = f'{SYMBOL}:{TF}:mth:{T - (20 + i) * TF_DELTA}'
        lvl_id = f'{zone_id}:block_zero'
        price_above = 50300.0 + i * 200
        price_below = 49900.0 - i * 200
        pipe.zadd(to_lose, {lvl_id: price_above})
        pipe.zadd(to_gain, {f'{zone_id}:block_one': price_below})
    pipe.execute()


def seed_level_queues_with_crossings(r_raw):
    """Seed queues where the current bar (close=50200) actually crosses some levels."""
    pipe = r_raw.pipeline()
    to_lose = f'{SYMBOL}:{TF}:lvls:to_lose'
    to_gain = f'{SYMBOL}:{TF}:lvls:to_gain'

    # Levels below close → in to_lose with score < close → will be "lost"
    for i, price in enumerate([49800.0, 50000.0, 50100.0]):
        lvl_id = f'{SYMBOL}:{TF}:mth:{T - (20 + i) * TF_DELTA}:block_zero'
        lvl_data = {
            'id': lvl_id,
            'zone_id': f'{SYMBOL}:{TF}:mth:{T - (20 + i) * TF_DELTA}',
            'name': 'block_zero',
            'direction': '1',
            'value': str(price),
            'gains': '0', 'losses': '0', 'conseq_gain': '0', 'last_gain': '0',
            'conseq_loss': '0', 'last_loss': '0', 'bl_tests': '0', 'last_bl_test': '0',
            'br_tests': '0', 'last_br_test': '0', 'tested': '0', 'state': 'awaiting_gain',
        }
        pipe.hset(lvl_id, mapping=lvl_data)
        pipe.zadd(to_lose, {lvl_id: price})

    # Levels above close → in to_gain with score > close → will be "gained"
    for i, price in enumerate([50400.0, 50600.0]):
        lvl_id = f'{SYMBOL}:{TF}:origin:{T - (25 + i) * TF_DELTA}:block_one'
        lvl_data = {
            'id': lvl_id,
            'zone_id': f'{SYMBOL}:{TF}:origin:{T - (25 + i) * TF_DELTA}',
            'name': 'block_one',
            'direction': '1',
            'value': str(price),
            'gains': '0', 'losses': '0', 'conseq_gain': '0', 'last_gain': '0',
            'conseq_loss': '0', 'last_loss': '0', 'bl_tests': '0', 'last_bl_test': '0',
            'br_tests': '0', 'last_br_test': '0', 'tested': '0', 'state': 'awaiting_loss',
        }
        pipe.hset(lvl_id, mapping=lvl_data)
        pipe.zadd(to_gain, {lvl_id: price})

    # Background non-crossing levels
    for i in range(10):
        pipe.zadd(to_lose, {f'bg_lose_{i}': 55000.0 + i * 500})
        pipe.zadd(to_gain, {f'bg_gain_{i}': 44000.0 - i * 500})

    pipe.execute()


def seed_mth_index(r_raw):
    """Seed mth_index with a previous MTH that has base data."""
    prev_mth_id = f'{SYMBOL}:{TF}:mth:{T - 25 * TF_DELTA}'
    prev_mth_time = T - 25 * TF_DELTA
    prev_mth_data = {
        'id': prev_mth_id,
        'symbol': SYMBOL,
        'timeframe': TF,
        'type': 'mth',
        'direction': '1',
        'completion': 'complete',
        'time': str(prev_mth_time),
        'process_time': str(prev_mth_time + TF_DELTA),
        'move_end_time': '0',
        'block_zero': '46000.0',
        'block_zero_id': f'{prev_mth_id}:block_zero',
        'block_one': '48000.0',
        'block_one_id': f'{prev_mth_id}:block_one',
        'block_half': '0.0',
        'block_half_id': '',
        'mth_value': '46000.0',
        'mth_move_id': f'{SYMBOL}:{TF}:move:{prev_mth_time - 5 * TF_DELTA}',
        'og_mth_value': '46000.0',
        'age_bars': '23',
        'touches': '1',
        'distance_from_price': '4200.0',
        'base_open': '44500.0',
        'base_close': '46000.0',
        'previous_mth_id': '',
    }
    pipe = r_raw.pipeline()
    pipe.hset(prev_mth_id, mapping=prev_mth_data)
    pipe.zadd(f'{SYMBOL}:{TF}:mth_index', {prev_mth_id: prev_mth_time})
    pipe.execute()
    return prev_mth_id


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='MGOT Pipeline Performance Benchmark')
    parser.add_argument('--port', type=int, default=6379)
    parser.add_argument('--db', type=int, default=15)
    parser.add_argument('--iterations', type=int, default=50)
    args = parser.parse_args()

    # ── Connect ───────────────────────────────────────────────────────────────
    raw_r = redis_lib.Redis(host='localhost', port=args.port, db=args.db, decode_responses=True)
    try:
        raw_r.ping()
    except Exception as e:
        print(f'Cannot connect to Redis on port {args.port}: {e}')
        sys.exit(1)

    profiling_r = ProfilingRedis(raw_r)

    W = 78
    print('═' * W)
    print('  MGOT PIPELINE PERFORMANCE BENCHMARK')
    print(f'  Redis localhost:{args.port}/db{args.db}  |  Iterations: {args.iterations}  |  Symbol: {SYMBOL}/{TF}')
    print('═' * W)

    # Flush test DB
    raw_r.flushdb()

    # ── Patch module-level r references ───────────────────────────────────────
    # Must happen BEFORE importing service modules that call connect_to_redis
    import mgot_utils.core.functions as fu
    import mgot_utils.processing.lvl_preprocessor as lp
    import mgot_utils.processing.squeeze as sq
    import mgot_utils.processing.zone_preprocessor as zp
    from mgot_utils.models import Bar, Zone, Level, Move

    fu.r = profiling_r
    lp.r = profiling_r
    sq.r = profiling_r
    zp.r = profiling_r

    # Import service modules after patching (use importlib to avoid name collisions)
    import importlib.util

    def load_service(path: Path, module_name: str):
        spec = importlib.util.spec_from_file_location(module_name, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = mod
        spec.loader.exec_module(mod)
        return mod

    svc04 = load_service(ROOT / '04_peaks_and_structure' / 'main.py', 'svc04_main')
    svc04.r = profiling_r

    svc03 = load_service(ROOT / '03_levels_and_zones' / 'main.py', 'svc03_main')
    svc03.r = profiling_r

    results = []
    N = args.iterations

    # ══════════════════════════════════════════════════════════════════════════
    # GROUP 1: 03 — Level Achievement Processing
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{'─'*W}")
    print('  GROUP 1: 03_levels_and_zones — Level Achievement Processing')
    print(f"{'─'*W}")

    # 1a. identify_achievements — cold bar (no crossings)
    def setup_1a():
        raw_r.delete(f'{SYMBOL}:{TF}:lvls:to_lose', f'{SYMBOL}:{TF}:lvls:to_gain')
        seed_level_queues(raw_r, n_levels=15)

    bar = Bar.initiate_bar(BAR_DATA)

    def bench_1a():
        bar.identify_achievements(profiling_r)

    r1a = run_benchmark('identify_achievements (cold, 0 crossings)', '03', profiling_r, setup_1a, bench_1a, N)
    results.append(r1a)
    print_result(r1a)

    # 1b. identify_achievements — active bar (3 losses + 2 gains)
    def setup_1b():
        raw_r.delete(f'{SYMBOL}:{TF}:lvls:to_lose', f'{SYMBOL}:{TF}:lvls:to_gain')
        seed_level_queues_with_crossings(raw_r)

    def bench_1b():
        bar.identify_achievements(profiling_r)

    r1b = run_benchmark('identify_achievements (3 losses + 2 gains)', '03', profiling_r, setup_1b, bench_1b, N)
    results.append(r1b)
    print_result(r1b)

    # 1c. Level.fetch_many — fetch 3 levels from Redis
    seed_level_queues_with_crossings(raw_r)
    lost_ids, gained_ids = bar.identify_achievements(raw_r)
    profiling_r.reset_stats()

    def setup_1c():
        pass  # levels already seeded

    def bench_1c():
        pipe = profiling_r.pipeline()
        Level.fetch_many(lost_ids, pipe)
        Level.fetch_many(gained_ids, pipe)

    r1c = run_benchmark(f'Level.fetch_many ({len(lost_ids)} lost + {len(gained_ids)} gained)', '03', profiling_r, setup_1c, bench_1c, N)
    results.append(r1c)
    print_result(r1c)

    # 1d. Full process_level_achievements (fetch + record + track)
    def setup_1d():
        raw_r.delete(f'{SYMBOL}:{TF}:lvls:to_lose', f'{SYMBOL}:{TF}:lvls:to_gain')
        seed_level_queues_with_crossings(raw_r)

    def bench_1d():
        svc03.process_level_achievements(bar, profiling_r)

    r1d = run_benchmark('process_level_achievements (3+2 crossings)', '03', profiling_r, setup_1d, bench_1d, N)
    results.append(r1d)
    print_result(r1d)

    # ══════════════════════════════════════════════════════════════════════════
    # GROUP 2: 04 — Peaks & Structure
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{'─'*W}")
    print('  GROUP 2: 04_peaks_and_structure — Move & Peak Processing')
    print(f"{'─'*W}")

    # Seed base data
    seed_bars(raw_r)
    seed_move(raw_r)

    # 2a. retrieve_window (5-bar fetch)
    def setup_2a():
        pass  # bars already seeded

    def bench_2a():
        fu.retrieve_window(bar, 5)

    r2a = run_benchmark('retrieve_window (5-bar history fetch)', '04', profiling_r, setup_2a, bench_2a, N)
    results.append(r2a)
    print_result(r2a)

    # 2b. extend_existing_move (common path — same direction)
    def setup_2b():
        seed_move(raw_r)

    def bench_2b():
        svc04.extend_existing_move(bar)

    r2b = run_benchmark('extend_existing_move (same direction)', '04', profiling_r, setup_2b, bench_2b, N)
    results.append(r2b)
    print_result(r2b)

    # 2c. create_new_move (direction change)
    def setup_2c():
        pass  # no setup needed

    def bench_2c():
        svc04.create_new_move(bar)

    r2c = run_benchmark('create_new_move (direction change)', '04', profiling_r, setup_2c, bench_2c, N)
    results.append(r2c)
    print_result(r2c)

    # 2d. identify_peaks (no-peak path — most common)
    all_bars = fu.retrieve_window(bar, 5)
    profiling_r.reset_stats()

    def setup_2d():
        pass

    def bench_2d():
        svc04.identify_peaks(all_bars)

    r2d = run_benchmark('identify_peaks (no-peak, 0 Redis writes)', '04', profiling_r, setup_2d, bench_2d, N)
    results.append(r2d)
    print_result(r2d)

    # 2e. track_previous_moves (non-MTH bar — setter only)
    peak_bar_data = {**BAR_DATA, 'top': '1', 'mth': '0', 'structure': 'HH'}
    peak_bar = Bar.initiate_bar(peak_bar_data)

    def setup_2e():
        pipe = raw_r.pipeline()
        pipe.set(f'{SYMBOL}:{TF}:previous_move_open', '48000.0')
        pipe.set(f'{SYMBOL}:{TF}:previous_move_close', '49000.0')
        pipe.set(f'{SYMBOL}:{TF}:previous_move_time', str(T - 8 * TF_DELTA))
        pipe.execute()

    def bench_2e():
        svc04.track_previous_moves(peak_bar)

    r2e = run_benchmark('track_previous_moves (peak, valid_mth check)', '04', profiling_r, setup_2e, bench_2e, N)
    results.append(r2e)
    print_result(r2e)

    # 2f. process_structure → process_high → create_mth_zone (full MTH path)
    mth_peak_data = {**BAR_DATA, 'top': '1', 'mth': '1', 'structure': 'HH',
                     'close': '51500.0', 'high': '51600.0', 'move_open': '49000.0'}
    mth_bar = Bar.initiate_bar(mth_peak_data)

    def setup_2f():
        pipe = raw_r.pipeline()
        pipe.set(f'{SYMBOL}:{TF}:previous_move_open', '48000.0')
        pipe.set(f'{SYMBOL}:{TF}:previous_move_close', '50500.0')
        pipe.set(f'{SYMBOL}:{TF}:previous_move_time', str(T - 8 * TF_DELTA))
        pipe.set(f'{SYMBOL}:{TF}:previous_top:1', '50500.0')
        pipe.execute()
        # Reset mth index
        raw_r.delete(f'{SYMBOL}:{TF}:mth_index')
        seed_mth_index(raw_r)

    def bench_2f():
        svc04.process_structure(mth_bar)

    r2f = run_benchmark('process_structure → MTH zone creation', '04', profiling_r, setup_2f, bench_2f, N)
    results.append(r2f)
    print_result(r2f)

    # ══════════════════════════════════════════════════════════════════════════
    # GROUP 3: 10 / utils — Zone Processing
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{'─'*W}")
    print('  GROUP 3: 10_zone_processor / post_process — Zone State Machine')
    print(f"{'─'*W}")

    from mgot_utils.processing.lvl_preprocessor import create_lvls
    from mgot_utils.processing.post_process import post_process_zone, update_mth, update_origin, update_squeeze

    # 3a. create_lvls — new MTH zone, delta_t = 0 (no bar retrieval)
    fresh_zone_data = {**MTH_ZONE_DATA, 'time': str(T), 'id': f'{SYMBOL}:{TF}:mth:{T}'}
    fresh_zone = Zone.initiate_zone(fresh_zone_data)

    def setup_3a():
        pass

    def bench_3a():
        create_lvls(fresh_zone, bar)

    r3a = run_benchmark('create_lvls (new MTH zone, 2 levels)', '10', profiling_r, setup_3a, bench_3a, N)
    results.append(r3a)
    print_result(r3a)

    # 3b. post_process_zone → update_mth (incomplete, no achievement)
    raw_r.hset(MTH_ZONE_DATA['id'], mapping=MTH_ZONE_DATA)
    seed_levels(raw_r, MTH_ZONE_DATA['id'], 49000.0, 51000.0,
                zero_gains=0, zero_losses=0, one_gains=0, one_losses=0)

    mth_zone = Zone.initiate_zone(MTH_ZONE_DATA)

    def setup_3b():
        raw_r.hset(MTH_ZONE_DATA['id'], mapping=MTH_ZONE_DATA)
        seed_levels(raw_r, MTH_ZONE_DATA['id'], 49000.0, 51000.0)

    def bench_3b():
        post_process_zone(bar, mth_zone, profiling_r)

    r3b = run_benchmark('post_process MTH (incomplete, no change)', '10', profiling_r, setup_3b, bench_3b, N)
    results.append(r3b)
    print_result(r3b)

    # 3c. post_process_zone → update_mth (completion: block_zero lost)
    completing_bar_data = {**BAR_DATA, 'close': '48800.0', 'low': '48700.0'}
    completing_bar = Bar.initiate_bar(completing_bar_data)

    def setup_3c():
        raw_r.hset(MTH_ZONE_DATA['id'], mapping=MTH_ZONE_DATA)
        seed_levels(raw_r, MTH_ZONE_DATA['id'], 49000.0, 51000.0,
                    zero_losses=1)  # block_zero just got lost → MTH completes
        raw_r.delete(f'{SYMBOL}:{TF}:mth_index')
        seed_mth_index(raw_r)

    def bench_3c():
        zone = Zone.initiate_zone(raw_r.hgetall(MTH_ZONE_DATA['id']))
        post_process_zone(completing_bar, zone, profiling_r)

    r3c = run_benchmark('post_process MTH (completing → find_secondary_swing)', '10', profiling_r, setup_3c, bench_3c, N)
    results.append(r3c)
    print_result(r3c)

    # 3d. post_process_zone → update_origin (incomplete, no achievement)
    raw_r.hset(ORIGIN_ZONE_DATA['id'], mapping=ORIGIN_ZONE_DATA)
    seed_levels(raw_r, ORIGIN_ZONE_DATA['id'], 51000.0, 47000.0, direction=1)

    def setup_3d():
        raw_r.hset(ORIGIN_ZONE_DATA['id'], mapping=ORIGIN_ZONE_DATA)
        seed_levels(raw_r, ORIGIN_ZONE_DATA['id'], 51000.0, 47000.0, direction=1)

    def bench_3d():
        zone = Zone.initiate_zone(raw_r.hgetall(ORIGIN_ZONE_DATA['id']))
        post_process_zone(bar, zone, profiling_r)

    r3d = run_benchmark('post_process Origin (incomplete, no change)', '10', profiling_r, setup_3d, bench_3d, N)
    results.append(r3d)
    print_result(r3d)

    # 3e. post_process_zone → update_squeeze (incomplete, external check)
    raw_r.hset(SQUEEZE_ZONE_DATA['id'], mapping=SQUEEZE_ZONE_DATA)
    raw_r.hset(MTH_ZONE_DATA['id'], mapping=MTH_ZONE_DATA)
    seed_levels(raw_r, SQUEEZE_ZONE_DATA['id'], 47500.0, 49000.0, direction=1)

    def setup_3e():
        raw_r.hset(SQUEEZE_ZONE_DATA['id'], mapping=SQUEEZE_ZONE_DATA)
        raw_r.hset(MTH_ZONE_DATA['id'], mapping=MTH_ZONE_DATA)
        seed_levels(raw_r, SQUEEZE_ZONE_DATA['id'], 47500.0, 49000.0, direction=1)

    def bench_3e():
        zone = Zone.initiate_zone(raw_r.hgetall(SQUEEZE_ZONE_DATA['id']))
        post_process_zone(bar, zone, profiling_r)

    r3e = run_benchmark('post_process Squeeze (incomplete, external check)', '10', profiling_r, setup_3e, bench_3e, N)
    results.append(r3e)
    print_result(r3e)

    # ══════════════════════════════════════════════════════════════════════════
    # GROUP 4: find_secondary_swing (squeeze creation)
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{'─'*W}")
    print('  GROUP 4: squeeze.py — find_secondary_swing')
    print(f"{'─'*W}")

    from mgot_utils.processing.squeeze import find_secondary_swing

    mth_zone = Zone.initiate_zone(MTH_ZONE_DATA)

    def setup_4a():
        raw_r.delete(f'{SYMBOL}:{TF}:mth_index')
        seed_mth_index(raw_r)

    def bench_4a():
        find_secondary_swing(mth_zone, bar, profiling_r)

    r4a = run_benchmark('find_secondary_swing (prev MTH found, squeeze created)', 'squeeze', profiling_r, setup_4a, bench_4a, N)
    results.append(r4a)
    print_result(r4a)

    # ── Summary ───────────────────────────────────────────────────────────────
    print_summary(results)
    print_bottlenecks(results)

    print()
    print('═' * W)

    raw_r.flushdb()
    raw_r.close()


if __name__ == '__main__':
    main()
