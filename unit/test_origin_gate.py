"""
Unit tests for the origin gate (structures.origin_gate_open).

Business rule (TA):
- An origin in the direction OPPOSITE to the last completed origin is always
  permitted. Nothing gates it.
- Two consecutive origins in the SAME direction are only permitted once the
  first has been resolved, by any one of:
    a. it was taken out
    b. it was tested or swept
    c. an mth in the opposite direction was lost in between — "lost" meaning
       block_zero breached, recorded here as the mth's 'complete' state.
"""

import pytest
from unittest.mock import MagicMock
from mgot_utils.models import Bar
from mgot_utils.processing.structures import origin_gate_open


SYM, TF = 'BTCUSDT', '1h'
PREV_ORIGIN_ID = f'{SYM}:{TF}:origin:1700000000000'
COMPLETED_AT = 1700000000000
BAR_TIME = 1700100000000


def make_bar(**kw):
    defaults = dict(
        id=f'{SYM}:{TF}:bar:{BAR_TIME}',
        symbol=SYM, timeframe=TF,
        time=BAR_TIME,
        open=50000.0, high=50500.0, low=49500.0, close=50200.0,
        volume=1000.0, direction=0,
    )
    defaults.update(kw)
    return Bar(**defaults)


def make_redis(last_origin=None, prev_zone=None, last_mth=None):
    """Redis double serving the three keys the gate reads."""
    last_mth = last_mth or {}

    def hgetall(key):
        if key == f'{SYM}:{TF}:last_origin':
            return last_origin or {}
        if key == PREV_ORIGIN_ID:
            return prev_zone or {}
        return {}

    r = MagicMock()
    r.hgetall.side_effect = hgetall
    r.get.side_effect = lambda key: last_mth.get(key)
    return r


def completed_origin(direction):
    return {'time_completed': str(COMPLETED_AT), 'direction': str(direction), 'id': PREV_ORIGIN_ID}


def unresolved_zone():
    """Previous origin: not taken out, never tested, never swept."""
    return {'time_taken_out': '0', 'zone_tests': '0', 'last_zone_test_time': '0',
            'sweeps': '0', 'last_sweep_time': '0'}


# ── No prior origin ─────────────────────────────────────────────

def test_open_when_no_last_origin():
    assert origin_gate_open(make_bar(), make_redis()) is True


def test_open_when_last_origin_malformed():
    r = make_redis(last_origin={'direction': '0'})  # no time_completed
    assert origin_gate_open(make_bar(), r) is True


# ── Opposite direction is unconditional ─────────────────────────

@pytest.mark.parametrize('prev_dir,bar_dir', [(0, 1), (1, 0)])
def test_opposite_direction_always_open(prev_dir, bar_dir):
    """Even with the previous origin wholly unresolved and no mth lost."""
    r = make_redis(last_origin=completed_origin(prev_dir), prev_zone=unresolved_zone())
    assert origin_gate_open(make_bar(direction=bar_dir), r) is True


# ── Same direction requires resolution ──────────────────────────

def test_same_direction_blocked_when_unresolved():
    r = make_redis(last_origin=completed_origin(0), prev_zone=unresolved_zone())
    assert origin_gate_open(make_bar(direction=0), r) is False


def test_same_direction_open_when_previous_taken_out():
    zone = unresolved_zone() | {'time_taken_out': str(BAR_TIME - 1)}
    r = make_redis(last_origin=completed_origin(0), prev_zone=zone)
    assert origin_gate_open(make_bar(direction=0), r) is True


def test_same_direction_open_when_previous_tested():
    zone = unresolved_zone() | {'zone_tests': '1', 'last_zone_test_time': str(BAR_TIME - 1)}
    r = make_redis(last_origin=completed_origin(0), prev_zone=zone)
    assert origin_gate_open(make_bar(direction=0), r) is True


def test_same_direction_open_when_previous_swept():
    zone = unresolved_zone() | {'sweeps': '1', 'last_sweep_time': str(BAR_TIME - 1)}
    r = make_redis(last_origin=completed_origin(0), prev_zone=zone)
    assert origin_gate_open(make_bar(direction=0), r) is True


def test_same_direction_open_when_opposite_mth_lost():
    """Previous origin bearish -> needs a BULLISH mth lost in between."""
    r = make_redis(last_origin=completed_origin(0), prev_zone=unresolved_zone(),
                   last_mth={f'{SYM}:{TF}:last_mth:1': str(COMPLETED_AT + 1)})
    assert origin_gate_open(make_bar(direction=0), r) is True


def test_same_direction_blocked_when_same_direction_mth_lost():
    """A BEARISH mth lost does not license another bearish origin.

    This is the 02 Aug 2026 15m case: the gate admitted a second bearish origin
    off a bearish mth completing, because last_mth was keyed by the completing
    bar's own direction rather than by the direction of the zone that was lost.
    """
    r = make_redis(last_origin=completed_origin(0), prev_zone=unresolved_zone(),
                   last_mth={f'{SYM}:{TF}:last_mth:0': str(COMPLETED_AT + 1)})
    assert origin_gate_open(make_bar(direction=0), r) is False


def test_same_direction_blocked_when_mth_lost_before_previous_origin():
    """The opposite mth must be lost AFTER the previous origin completed."""
    r = make_redis(last_origin=completed_origin(0), prev_zone=unresolved_zone(),
                   last_mth={f'{SYM}:{TF}:last_mth:1': str(COMPLETED_AT - 1)})
    assert origin_gate_open(make_bar(direction=0), r) is False


# ── Ordering safety ─────────────────────────────────────────────

def test_blocked_when_previous_origin_completes_after_this_bar():
    """A potential origin can never predate the origin it succeeds."""
    last = completed_origin(1) | {'time_completed': str(BAR_TIME + 1)}
    r = make_redis(last_origin=last, prev_zone=unresolved_zone())
    assert origin_gate_open(make_bar(direction=0), r) is False
