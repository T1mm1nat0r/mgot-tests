"""The inverted secondary-swing level — `iss_level`, the far end of the MTH's move.

`ss_level` reads the turn an MTH's move started *from*. This reads the turn it
ended *at*: the last candle of the move and the bar that changed direction. Same
two-candle mechanic, opposite end, and the pick mirrors it — at a top the level
is the **highest body low** of the pair, at a bottom the lowest body high. Both
sit just inside the extreme rather than on it.

Named for its role (TA, 2026-09-08): the SS level is the base a with-trend swing
trades from; this is its inverted counterpart at the other end.

It is the third price at that end, and the tightest of the three:

    sweep_level   the wick extreme          prev_move.high / .low
    block_one     the move close            prev_move.close
    iss_level     the body edge of the pair  <- this

Available at `move_end + 1` by construction: both candles exist the moment the
direction change is seen, which is the bar `create_mth_zone` runs on. Measured on
production 15m before wiring — computable for 2 094 of 2 099 MTHs (the five
misses are bar gaps), and inside `block_one` in 2 093 of those, so it tightens
the edge rather than relocating it, exactly as the SS level did at the base.

**Recorded only.** Nothing reads it, deliberately, as `ss_level` was when it
landed. S4 measures it before any rule keys off it.

No candle time is stored. `base_candle_time` was computed and written for months
and never read once — a stored field is evidence of intent, never of a wired
mechanism.
"""

import pytest

from mgot_utils.models import Bar, Zone
from mgot_utils.processing.lvl_preprocessor import _get_level_test_params
from mgot_utils.processing.squeeze import turn_top_level

from tests.unit.test_ss_level import FakeRedis, bar_at, SYMBOL, TF, DELTA, T0


def an_mth(direction, move_end_index, iss=0.0):
    return Zone(
        id=f'{SYMBOL}:{TF}:mth:{T0}', symbol=SYMBOL, timeframe=TF, type='mth',
        direction=direction, time=T0, process_time=T0 + move_end_index * DELTA,
        move_end_time=T0 + move_end_index * DELTA,
        block_zero=100.0, block_one=120.0, ss_level=101.0, iss_level=iss,
    )


def the_bar_after(index):
    """The direction-change bar `create_mth_zone` is called on."""
    t = T0 + index * DELTA
    return Bar(id=f'{SYMBOL}:{TF}:bar:{t}', symbol=SYMBOL, timeframe=TF, time=t,
               open=0, high=0, low=0, close=0, volume=1)


# ============================================================
# THE LEVEL ITSELF
# ============================================================

def test_a_top_takes_the_highest_body_low_of_the_pair():
    r = FakeRedis()
    # bars 0-2 run up; bar 2 is the last of the move, bar 3 turns down.
    bar_at(r, 0, o=100, h=105, l=99, c=104)
    bar_at(r, 1, o=104, h=112, l=103, c=111)
    bar_at(r, 2, o=111, h=120, l=110, c=119)   # last of the up move: body low 111
    bar_at(r, 3, o=118, h=119, l=108, c=113)   # first of the down move: body low 113

    level = turn_top_level(an_mth(1, 2), the_bar_after(3), r)

    assert level == 113          # the higher of the two body lows


def test_a_bottom_takes_the_lowest_body_high_of_the_pair():
    r = FakeRedis()
    bar_at(r, 0, o=120, h=121, l=112, c=113)
    bar_at(r, 1, o=113, h=114, l=104, c=105)   # last of the down move: body high 113
    bar_at(r, 2, o=107, h=118, l=106, c=117)   # first of the up move: body high 117

    level = turn_top_level(an_mth(0, 1), the_bar_after(2), r)

    assert level == 113          # the lower of the two body highs


def test_it_ignores_bodies_earlier_in_the_move():
    """Only the two candles at the turn count — the same rule as `ss_level`.

    Bar 0 has a far higher body low than the turn pair; reading the whole move
    would pick it and put the level nowhere near the top.
    """
    r = FakeRedis()
    bar_at(r, 0, o=150, h=155, l=149, c=154)   # body low 150, well above the turn
    bar_at(r, 1, o=104, h=112, l=103, c=111)
    bar_at(r, 2, o=111, h=120, l=110, c=119)   # last of the move: body low 111
    bar_at(r, 3, o=118, h=119, l=108, c=113)   # turn bar: body low 113

    level = turn_top_level(an_mth(1, 2), the_bar_after(3), r)

    assert level == 113


def test_it_sits_inside_block_one():
    """block_one is the move close; the body edge of the pair is tighter."""
    r = FakeRedis()
    bar_at(r, 0, o=104, h=112, l=103, c=111)
    bar_at(r, 1, o=111, h=120, l=110, c=119)   # move close 119 -> block_one
    bar_at(r, 2, o=118, h=119, l=108, c=113)

    zone = an_mth(1, 1)
    zone.block_one = 119.0
    level = turn_top_level(zone, the_bar_after(2), r)

    assert level < zone.block_one


def test_returns_none_when_the_bars_are_missing():
    assert turn_top_level(an_mth(1, 2), the_bar_after(3), FakeRedis()) is None


# ============================================================
# HOW IT RIDES THE LEVEL MACHINERY
# ============================================================

def test_the_zone_declares_it_as_a_level():
    assert f'{SYMBOL}:{TF}:mth:{T0}:iss_level' in an_mth(1, 2, iss=113.0).get_lvl_ids()


def test_no_level_when_the_turn_could_not_be_read():
    assert not any(i.endswith(':iss_level') for i in an_mth(1, 2, iss=0).get_lvl_ids())


def test_it_is_appended_after_the_ss_level():
    """Zone completion reads `zone_lvls[0]` and `[1]` positionally.

    Both turn levels must stay behind block_zero and block_one, and `iss_level`
    behind `ss_level`, or `update_mth` silently grades the wrong price.
    """
    ids = an_mth(1, 2, iss=113.0).get_lvl_ids()
    assert ids[0].endswith(':block_zero')
    assert ids[1].endswith(':block_one')
    assert ids.index(':'.join([ids[0].rsplit(':', 1)[0], 'iss_level'])) > \
           ids.index(':'.join([ids[0].rsplit(':', 1)[0], 'ss_level']))


def test_an_origin_gets_no_iss_level():
    origin = Zone(id=f'{SYMBOL}:{TF}:origin:{T0}', symbol=SYMBOL, timeframe=TF,
                  type='origin', direction=1, time=T0, process_time=T0,
                  block_zero=100.0, block_one=120.0, iss_level=113.0)
    assert not any(i.endswith(':iss_level') for i in origin.get_lvl_ids())


def test_tests_only_count_after_the_move_ends():
    zone = an_mth(1, 2, iss=113.0)
    _td, test_time, created_by = _get_level_test_params('iss_level', zone)
    assert test_time == zone.move_end_time
    assert created_by == 'body'


def test_it_mirrors_block_one_s_test_direction():
    """It sits beside block_one at the far end, not beside block_zero."""
    zone = an_mth(1, 2, iss=113.0)
    iss_td, _, _ = _get_level_test_params('iss_level', zone)
    one_td, _, _ = _get_level_test_params('block_one', zone)
    assert iss_td == one_td
