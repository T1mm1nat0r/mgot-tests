"""
The secondary-swing level: taken from the two candles at the turn, tracked as a Level.

Rebuilt 2026-08-28 (TA). It was briefly its own `SSLevel` model with its own
index, open set and per-bar hook in `10_zone_processor` — a parallel copy of
machinery `Level` already had. `sweep_level` was the standing precedent: a named
extra level on the MTH zone. So the level now rides `get_lvl_ids` ->
`create_lvls` -> the existing `to_gain`/`to_lose` queues, and "tested" is
`Level.is_untested()` rather than a second definition.

Before 2026-08-27 the squeeze base came from `prev_move.open`, a move boundary,
while `_find_base_candle` swept the *whole* range from the previous move's start
to the MTH move's start looking for a body extreme — and then threw the price
away, storing only `base_candle_time`, which nothing read.

Both halves were wrong. The mechanic is: take the last candle of the move up and
the first candle of the move down — two candles, adjacent — and use the highest
body low of the pair (mirrored for a move to the high).

`test_ignores_a_deeper_body_outside_the_turn` is the regression guard: it places
a more extreme body earlier in the previous move, where the old sweep would have
found it, and pins that the turn pair wins.

Measured on live Redis the same day: across 608 MTH zones on 15m/1h/4h the turn
window held **exactly two candles every time**, and the new level fell inside the
old base range in 99.8% of cases — so this tightens the base rather than moving
it somewhere else.
"""

import pytest

from mgot_utils.models import Bar, Level, Move, Zone
from mgot_utils.processing.lvl_preprocessor import _get_level_test_params
from mgot_utils.processing.squeeze import _turn_base_level


SYMBOL, TF = 'BTCUSDT', '15m'
DELTA = 900_000
T0 = 1_784_541_600_000


class FakeRedis:
    """Hashes and sorted sets, in memory, with a pipeline that just defers.

    Hand-rolled for the same reason `test_htf_links_storage.py` hand-rolls its
    own: the `redis_client` fixture needs a server on 6479 and *skips* without
    one, and a guard test that silently skips guards nothing.
    """

    def __init__(self):
        self.h: dict[str, dict[str, str]] = {}
        self.z: dict[str, dict[str, float]] = {}

    # -- hashes
    def hset(self, key, field=None, value=None, mapping=None):
        d = self.h.setdefault(key, {})
        if mapping:
            d.update({k: str(v) for k, v in mapping.items()})
        if field is not None:
            d[field] = str(value)

    def hgetall(self, key):
        return dict(self.h.get(key, {}))

    def hvals(self, key):
        return list(self.h.get(key, {}).values())

    def hdel(self, key, *fields):
        d = self.h.get(key, {})
        for f in fields:
            d.pop(f, None)

    def hmget(self, key, *fields):
        d = self.h.get(key, {})
        return [d.get(f) for f in fields]

    # -- sorted sets
    def zadd(self, key, mapping):
        self.z.setdefault(key, {}).update(mapping)

    def zrangebyscore(self, key, lo, hi):
        lo = float('-inf') if lo == '-inf' else float(lo)
        hi = float('inf') if hi == '+inf' else float(hi)
        items = [(m, s) for m, s in self.z.get(key, {}).items() if lo <= s <= hi]
        return [m for m, _ in sorted(items, key=lambda kv: kv[1])]

    # -- pipeline: this code only ever batches, never transacts
    def pipeline(self, transaction=True):
        return FakePipeline(self)


class FakePipeline:
    def __init__(self, r):
        self._r, self._calls = r, []

    def __getattr__(self, name):
        def record(*a, **kw):
            self._calls.append((name, a, kw))
            return self
        return record

    def execute(self):
        return [getattr(self._r, n)(*a, **kw) for n, a, kw in self._calls]


def bar_at(r, i, o, h, l, c, move_id=''):
    """Write one bar into the fake at index `i` past T0."""
    t = T0 + i * DELTA
    bid = f'{SYMBOL}:{TF}:bar:{t}'
    r.hset(bid, mapping={'open': o, 'high': h, 'low': l, 'close': c,
                         'time': t, 'move_id': move_id})
    r.zadd(f'{SYMBOL}:{TF}:bars_index', {bid: t})
    return t


def a_zone(direction):
    return Zone(
        id=f'{SYMBOL}:{TF}:mth:{T0}', symbol=SYMBOL, timeframe=TF,
        type='mth', direction=direction, time=T0, process_time=T0 + 2 * DELTA,
    )


def a_move(start_index, length, direction):
    t = T0 + start_index * DELTA
    return Move(id=f'{SYMBOL}:{TF}:move:{t}', symbol=SYMBOL, timeframe=TF,
                direction=direction, time=t, type='move', length_bar=length,
                open=0, high=0, low=0, close=0)


def a_bar(index, high, low, move_id='', close=None):
    t = T0 + index * DELTA
    return Bar(id=f'{SYMBOL}:{TF}:bar:{t}', symbol=SYMBOL, timeframe=TF, time=t,
               open=low, high=high, low=low,
               close=high if close is None else close, volume=1,
               move_id=move_id)


# ============================================================
# THE LEVEL ITSELF
# ============================================================

def test_move_to_low_takes_the_highest_body_low_of_the_pair():
    r = FakeRedis()
    # bars 0-2 are the move up; bar 3 starts the move down.
    bar_at(r, 0, o=100, h=105, l=99, c=104)
    bar_at(r, 1, o=104, h=110, l=103, c=109)
    bar_at(r, 2, o=109, h=118, l=108, c=117)   # last of the up move: body low 109
    bar_at(r, 3, o=116, h=119, l=110, c=111)   # first of the down move: body low 111

    level, time = _turn_base_level(a_zone(0), a_move(0, 3, 1), a_move(3, 4, 0), r)

    assert level == 111          # the higher of the two body lows
    assert time == T0 + 3 * DELTA


def test_move_to_high_takes_the_lowest_body_high_of_the_pair():
    r = FakeRedis()
    bar_at(r, 0, o=120, h=121, l=115, c=116)
    bar_at(r, 1, o=116, h=117, l=110, c=111)   # last of the down move: body high 116
    bar_at(r, 2, o=112, h=118, l=111, c=117)   # first of the up move: body high 117

    level, time = _turn_base_level(a_zone(1), a_move(0, 2, 0), a_move(2, 3, 1), r)

    assert level == 116          # the lower of the two body highs
    assert time == T0 + 1 * DELTA


def test_ignores_a_deeper_body_outside_the_turn():
    """The old rule swept the whole move and would have picked bar 0."""
    r = FakeRedis()
    bar_at(r, 0, o=150, h=151, l=100, c=149)   # body low 149 — highest in the range
    bar_at(r, 1, o=120, h=125, l=118, c=124)
    bar_at(r, 2, o=124, h=130, l=123, c=129)   # last of the up move: body low 124
    bar_at(r, 3, o=128, h=131, l=120, c=121)   # first of the down move: body low 121

    level, _ = _turn_base_level(a_zone(0), a_move(0, 3, 1), a_move(3, 4, 0), r)

    assert level == 124, 'took a body from outside the two turn candles'


def test_returns_none_when_the_bars_are_missing():
    assert _turn_base_level(a_zone(0), a_move(0, 3, 1), a_move(3, 4, 0), FakeRedis()) is None


# ============================================================
# TESTED / UNTESTED — Level.is_untested
# ============================================================

def a_level(direction, tests=0, gains=0, losses=0):
    return Level(id=f'{SYMBOL}:{TF}:mth:{T0}:ss_level', zone_id=f'{SYMBOL}:{TF}:mth:{T0}',
                 name='ss_level', direction=direction, value=111.0,
                 tests=tests, gains=gains, losses=losses)


def test_a_fresh_level_is_untested():
    assert a_level(0).is_untested() is True
    assert a_level(1).is_untested() is True


def test_a_wick_touch_makes_it_tested():
    assert a_level(0, tests=1).is_untested() is False
    assert a_level(1, tests=1).is_untested() is False


def test_a_close_on_the_returning_side_makes_it_tested():
    # direction 0: the move went down, so the level sits above — a close ABOVE
    # it is price coming back, which Level counts as a gain.
    assert a_level(0, gains=1).is_untested() is False
    # direction 1: mirrored, the level sits below and a close below is the return
    assert a_level(1, losses=1).is_untested() is False


def test_closes_on_the_departed_side_are_ignored():
    """The regression this rule exists for.

    A level the market left downward sits above price, so *every* later close is
    a "loss". Counting both sides would mark it tested on the very next bar and
    make untested levels vanish entirely.
    """
    assert a_level(0, losses=9).is_untested() is True, 'losses below a level above price are not a test'
    assert a_level(1, gains=9).is_untested() is True, 'gains above a level below price are not a test'


# ============================================================
# IT IS A REAL LEVEL ON THE ZONE
# ============================================================

def an_mth(direction=0, ss=111.0):
    return Zone(id=f'{SYMBOL}:{TF}:mth:{T0}', symbol=SYMBOL, timeframe=TF, type='mth',
                direction=direction, time=T0, process_time=T0 + 2 * DELTA,
                block_zero=120.0, block_one=100.0, ss_level=ss,
                move_end_time=T0 + DELTA)


def test_the_zone_declares_it_as_a_level():
    assert f'{SYMBOL}:{TF}:mth:{T0}:ss_level' in an_mth().get_lvl_ids()


def test_no_level_when_the_turn_could_not_be_read():
    assert not any(i.endswith(':ss_level') for i in an_mth(ss=0).get_lvl_ids())


def test_block_zero_and_block_one_keep_positions_0_and_1():
    """`update_mth` and `update_squeeze` read `zone_lvls[0]` and `[1]` positionally.

    Inserting the new level rather than appending it would silently hand zone
    completion the wrong level.
    """
    ids = an_mth().get_lvl_ids()
    assert ids[0].endswith(':block_zero')
    assert ids[1].endswith(':block_one')


def test_an_origin_gets_no_ss_level():
    origin = Zone(id=f'{SYMBOL}:{TF}:origin:{T0}', symbol=SYMBOL, timeframe=TF,
                  type='origin', direction=0, time=T0, process_time=T0,
                  block_zero=120.0, block_one=100.0, ss_level=111.0)
    assert not any(i.endswith(':ss_level') for i in origin.get_lvl_ids())


def test_tests_only_count_after_the_move_away_ends():
    """Its own bars sit against the level, so `test_time` is the move end.

    Without this every SS level reads tested on the bar after it was created.
    """
    zone = an_mth()
    _td, test_time, created_by = _get_level_test_params('ss_level', zone)
    assert test_time == zone.move_end_time
    assert created_by == 'body', 'must not be "close", which would add a confirmation move'
