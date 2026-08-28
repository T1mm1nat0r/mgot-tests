"""
The secondary-swing level is taken from the two candles at the turn — and only those.

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

import json
from types import SimpleNamespace

import pytest

from mgot_utils.models import Bar, Move, Zone, SSLevel
from mgot_utils.processing import ss_levels
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
# TESTED / UNTESTED
# ============================================================

def a_level(direction=0, value=111.0, move_id='mth-move'):
    return SSLevel(id=SSLevel.build_id(SYMBOL, TF, T0), symbol=SYMBOL, timeframe=TF,
                   direction=direction, value=value, time=T0, mth_move_id=move_id)


def test_a_wick_touch_counts_as_a_test():
    # level above (direction 0); the bar's high just reaches it
    assert a_level().is_test(a_bar(4, high=111.0, low=105.0, close=108.0)) is True


def test_a_wick_touch_is_not_a_close_beyond():
    """A wick reaches the level; it does not close through it."""
    level, bar = a_level(), a_bar(4, high=111.5, low=105.0, close=109.0)
    assert level.is_test(bar) is True
    assert level.is_close_beyond(bar) is False
    level.register_bar(bar)
    assert (level.tested, level.fakeouts, level.achieved) == (1, 0, 0)


def test_one_close_beyond_is_unresolved_not_a_grade():
    """Until the next bar it is neither a fakeout nor an achievement."""
    level = a_level()
    level.register_bar(a_bar(4, high=120.0, low=105.0, close=115.0))
    assert level.pending_close_at == T0 + 4 * DELTA
    assert (level.fakeouts, level.achieved) == (0, 0)
    assert level.is_tradeable() is True
    assert level.is_settled() is False, 'a single close must not retire the level'


def test_close_beyond_then_directly_back_inside_is_a_fakeout():
    """TA, 2026-08-28: 'a single close above and a close below directly'."""
    level = a_level()
    level.register_bar(a_bar(4, high=120.0, low=105.0, close=115.0))
    level.register_bar(a_bar(5, high=118.0, low=104.0, close=108.0))

    assert level.fakeouts == 1
    assert level.last_fakeout_at == T0 + 5 * DELTA
    assert level.pending_close_at == 0
    assert level.achieved == 0
    assert level.is_tradeable() is True, 'a fakeout must not retire the level'


def test_two_consecutive_closes_achieve_and_take_it_out():
    level = a_level()
    level.register_bar(a_bar(4, high=120.0, low=105.0, close=115.0))
    level.register_bar(a_bar(5, high=121.0, low=112.0, close=116.0))

    assert level.achieved == 1
    assert level.achieved_at == T0 + 5 * DELTA
    assert level.fakeouts == 0
    assert level.is_taken_out() is True
    assert level.is_tradeable() is False, 'an achieved level is taken out'
    assert level.is_settled() is True


def test_a_gap_between_closes_is_not_an_achievement():
    """Two closes beyond that are not consecutive leave the level alive."""
    level = a_level()
    level.register_bar(a_bar(4, high=120.0, low=105.0, close=115.0))
    level.register_bar(a_bar(5, high=112.0, low=104.0, close=106.0))   # back inside
    level.register_bar(a_bar(6, high=120.0, low=105.0, close=115.0))

    assert level.closes_beyond == 2
    assert level.conseq_close_beyond == 1
    assert level.achieved == 0
    assert level.fakeouts == 1
    assert level.is_tradeable() is True


def test_a_wick_after_a_close_beyond_is_not_a_fakeout():
    """The fakeout needs a *close* back inside, not merely a bar that failed to close beyond.

    Guard against the pending marker surviving to match a later, unrelated bar.
    """
    level = a_level()
    level.register_bar(a_bar(4, high=120.0, low=105.0, close=115.0))
    level.register_bar(a_bar(9, high=118.0, low=104.0, close=108.0))   # not adjacent
    assert level.fakeouts == 0
    assert level.pending_close_at == 0, 'stale pending marker left to match later'


def test_grades_are_addressable_by_name():
    level = a_level()
    level.register_bar(a_bar(4, high=120.0, low=105.0, close=115.0))
    assert level.has_reached('tested') is True
    assert level.has_reached('achieved') is False
    assert level.reached_at('tested') == T0 + 4 * DELTA
    with pytest.raises(ValueError):
        # a single close beyond is unresolved, so it is not a grade
        level.has_reached('closed_beyond')


def test_falling_short_is_not_a_test():
    assert a_level().is_test(a_bar(4, high=110.9, low=105.0)) is False


def test_the_move_away_does_not_test_its_own_level():
    """Its bars start at the level, so counting them would test everything at once."""
    bar = a_bar(4, high=115.0, low=105.0, move_id='mth-move')
    assert a_level(move_id='mth-move').is_test(bar) is False


def test_a_bar_at_or_before_the_turn_does_not_test():
    assert a_level().is_test(a_bar(0, high=200.0, low=100.0)) is False


def test_an_already_tested_level_is_not_retested():
    level = a_level()
    level.tested = 1
    assert level.is_test(a_bar(4, high=200.0, low=100.0)) is False


def test_a_move_to_high_level_is_tested_from_above():
    level = a_level(direction=1, value=111.0)
    assert level.is_test(a_bar(4, high=120.0, low=111.0)) is True
    assert level.is_test(a_bar(4, high=120.0, low=111.1)) is False


# ============================================================
# THE REGISTRY
# ============================================================

def test_a_tested_level_stays_open_until_it_is_achieved():
    """Dropping it at the first touch would make achievement unobservable."""
    r = FakeRedis()
    ss_levels.register(a_zone(0), value=111.0, candle_time=T0, mth_move_id='m1', r=r)
    key = ss_levels.open_key(SYMBOL, TF)

    changed = ss_levels.record_bar(a_bar(4, high=111.5, low=105.0, close=109.0, move_id='m2'), r)
    assert [lvl.value for lvl in changed] == [111.0]
    assert changed[0].tested_at == T0 + 4 * DELTA
    assert len(r.hgetall(key)) == 1, 'a merely tested level must stay open'
    assert r.hgetall(SSLevel.build_id(SYMBOL, TF, T0))['tested'] == '1'

    # two consecutive closes beyond retire it
    ss_levels.record_bar(a_bar(5, high=120.0, low=105.0, close=115.0, move_id='m2'), r)
    assert len(r.hgetall(key)) == 1, 'one close is not an achievement'
    ss_levels.record_bar(a_bar(6, high=121.0, low=112.0, close=116.0, move_id='m2'), r)
    assert r.hgetall(key) == {}, 'achieved level left in the open set'
    assert r.hgetall(SSLevel.build_id(SYMBOL, TF, T0))['achieved'] == '1'


def test_untested_ignores_a_level_already_touched():
    r = FakeRedis()
    ss_levels.register(a_zone(0), value=111.0, candle_time=T0, mth_move_id='m1', r=r)
    assert len(ss_levels.untested(SYMBOL, TF, r)) == 1

    ss_levels.record_bar(a_bar(4, high=111.5, low=105.0, close=109.0, move_id='m2'), r)

    assert ss_levels.untested(SYMBOL, TF, r) == []
    # ...but by the stricter grade it is still outstanding
    assert len(ss_levels.untested(SYMBOL, TF, r, grade='achieved')) == 1


def test_untested_orders_by_time_and_filters_by_direction():
    r = FakeRedis()
    for i, direction in [(3, 0), (1, 1), (2, 0)]:
        ss_levels.register(SimpleNamespace(symbol=SYMBOL, timeframe=TF, direction=direction,
                                           id=f'{SYMBOL}:{TF}:mth:{T0 + i * DELTA}'),
                           value=100.0 + i, candle_time=T0 + i * DELTA, mth_move_id='m', r=r)

    assert [lvl.time for lvl in ss_levels.untested(SYMBOL, TF, r)] == [
        T0 + DELTA, T0 + 2 * DELTA, T0 + 3 * DELTA]
    assert [lvl.value for lvl in ss_levels.untested(SYMBOL, TF, r, direction=0)] == [102.0, 103.0]


def test_as_of_recovers_what_was_untested_at_a_past_bar():
    """A level tested later was still open then — `tested_at` makes that recoverable.

    The open set only knows the present, so S4 cannot ask it this question.
    """
    r = FakeRedis()
    zone = a_zone(0)
    ss_levels.register(zone, value=111.0, candle_time=T0, mth_move_id='m1', r=r)
    ss_levels.record_bar(a_bar(8, high=115.0, low=105.0, close=109.0, move_id='m2'), r)

    assert ss_levels.untested(SYMBOL, TF, r) == []          # tested by now
    later = [lvl.time for lvl in ss_levels.untested(SYMBOL, TF, r, as_of=T0 + 4 * DELTA)]
    assert later == [T0], 'lost the fact that it was open four bars earlier'
    # and it is not open before it existed
    assert ss_levels.untested(SYMBOL, TF, r, as_of=T0) == []


def test_register_is_idempotent_across_a_reprocess():
    """The id comes from the turn candle's time, so a second pass overwrites."""
    r = FakeRedis()
    zone = a_zone(0)
    for _ in range(2):
        ss_levels.register(zone, value=111.0, candle_time=T0, mth_move_id='m1', r=r)
    assert len(r.hgetall(ss_levels.open_key(SYMBOL, TF))) == 1
