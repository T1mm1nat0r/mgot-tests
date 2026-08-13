"""
Unit tests for level wick tests (achievements.wick_level).

Tests the core business rule: a level is "tested" when price wicks through
the level value AND a time/move-based validation gate has been passed.
"""

import pytest
from mgot_utils.models import Bar, Level
from mgot_utils.processing.achievements import wick_level, gaining_lvl, losing_lvl


# ── Helpers ─────────────────────────────────────────────────────

def make_bar(**kw):
    defaults = dict(
        id='BTCUSDT:1h:bar:1700000000000',
        symbol='BTCUSDT', timeframe='1h',
        time=1700000000000,
        open=50000.0, high=50500.0, low=49500.0, close=50200.0,
        volume=1000.0,
    )
    defaults.update(kw)
    return Bar(**defaults)


def make_level(**kw):
    defaults = dict(
        id='BTCUSDT:1h:mth:1700000000000:block_zero',
        zone_id='BTCUSDT:1h:mth:1700000000000',
        name='block_zero',
        direction=1,
        value=49800.0,
    )
    defaults.update(kw)
    return Level(**defaults)


# ── Tests ───────────────────────────────────────────────────────

class TestWickLevel:
    """Tests for wick_level() — level wick test detection."""

    def test_wick_test_detected(self):
        """Wick through level value with valid move gate triggers a test."""
        lvl = make_level(
            value=49800.0,
            confirmation_move_id='BTCUSDT:1h:move:1699990000000',
            confirmation_d=0,
        )
        # Low wick reaches 49700, which is below level value 49800.
        # body_low = min(50000, 50200) = 50000, so 49700 <= 49800 < 50000 → in_low_wick.
        bar = make_bar(
            open=50000.0, close=50200.0,
            low=49700.0, high=50500.0,
            move_id='BTCUSDT:1h:move:1700000000000',  # later move
        )
        result = wick_level(lvl, bar)
        assert result.tests == 1
        assert result.last_test == bar.time

    def test_time_gate_blocks_early_test(self):
        """Wick through level, but bar.time <= test_time → no test (time-based gate)."""
        lvl = make_level(
            value=49800.0,
            test_time=1700000000000,  # same as bar time
            test_direction=0,
            # No confirmation_move_id → falls back to time-based gate
        )
        bar = make_bar(
            open=50000.0, close=50200.0,
            low=49700.0, high=50500.0,
            time=1700000000000,
        )
        result = wick_level(lvl, bar)
        assert result.tests == 0

    def test_time_gate_allows_after_move(self):
        """Wick through level with bar.time > test_time → test allowed (time-based gate)."""
        lvl = make_level(
            value=49800.0,
            test_time=1699990000000,  # earlier than bar time
            test_direction=0,
        )
        bar = make_bar(
            open=50000.0, close=50200.0,
            low=49700.0, high=50500.0,
            time=1700000000000,
        )
        result = wick_level(lvl, bar)
        assert result.tests == 1

    def test_state_becomes_tested(self):
        """After valid wick test, level state changes to 'tested'."""
        lvl = make_level(
            value=49800.0,
            state='awaiting_gain',
            confirmation_move_id='BTCUSDT:1h:move:1699990000000',
            confirmation_d=0,
        )
        bar = make_bar(
            open=50000.0, close=50200.0,
            low=49700.0, high=50500.0,
            move_id='BTCUSDT:1h:move:1700000000000',
        )
        result = wick_level(lvl, bar)
        assert result.state == 'tested'

    def test_confirmation_d1_blocks_first_move(self):
        """D=1: first qualifying move promotes confirmation but blocks the test."""
        lvl = make_level(
            value=49800.0,
            confirmation_move_id='BTCUSDT:1h:move:1699990000000',
            confirmation_d=1,
        )
        bar = make_bar(
            open=50000.0, close=50200.0,
            low=49700.0, high=50500.0,
            move_id='BTCUSDT:1h:move:1700000000000',  # first move after conf
        )
        result = wick_level(lvl, bar)
        assert result.tests == 0  # blocked — first move after D=1
        assert result.confirmation_move_id == bar.move_id  # promoted
        assert result.confirmation_d == 0  # D reset to 0

        # Now a bar from an even later move CAN test
        bar2 = make_bar(
            open=50100.0, close=50300.0,
            low=49750.0, high=50600.0,
            move_id='BTCUSDT:1h:move:1700010000000',
            time=1700010000000,
        )
        result = wick_level(result, bar2)
        assert result.tests == 1  # now allowed

    def test_test_count_accumulates(self):
        """Multiple valid wick tests increment the tests count."""
        lvl = make_level(
            value=49800.0,
            test_time=1699980000000,
            test_direction=0,
        )
        bar1 = make_bar(
            open=50000.0, close=50200.0,
            low=49700.0, high=50500.0,
            time=1699990000000,
        )
        bar2 = make_bar(
            open=50100.0, close=50300.0,
            low=49750.0, high=50600.0,
            time=1700000000000,
        )
        lvl = wick_level(lvl, bar1)
        assert lvl.tests == 1
        # get_test_state() still returns initial test_time (no gains/losses occurred),
        # so bar2 with a later timestamp also passes the time gate.
        lvl = wick_level(lvl, bar2)
        assert lvl.tests == 2


class TestGainingLosingInteraction:
    """Tests for interaction between wick tests and gain/loss events."""

    def test_gain_updates_confirmation_blocking_same_move(self):
        """After gaining a level, confirmation_move_id updates so same-move wicks are blocked."""
        lvl = make_level(
            value=49800.0,
            confirmation_move_id='BTCUSDT:1h:move:1699990000000',
            confirmation_d=0,
        )
        # Bar that gains the level (close > value)
        gain_bar = make_bar(
            open=49700.0, close=50000.0,
            low=49600.0, high=50100.0,
            move_id='BTCUSDT:1h:move:1700000000000',
            time=1700000000000,
        )
        lvl = gaining_lvl(lvl, gain_bar)
        assert lvl.gains == 1
        assert lvl.confirmation_move_id == gain_bar.move_id  # updated

        # Wick test from the same move should be blocked
        wick_bar = make_bar(
            open=50000.0, close=50200.0,
            low=49700.0, high=50500.0,
            move_id='BTCUSDT:1h:move:1700000000000',  # same move
            time=1700003600000,
        )
        lvl = wick_level(lvl, wick_bar)
        assert lvl.tests == 0  # blocked — same move as gain

    def test_high_wick_test(self):
        """Level value in high wick area triggers test."""
        lvl = make_level(
            value=50400.0,  # between body_high (50200) and bar.high (50500)
            test_time=1699980000000,
            test_direction=1,
        )
        bar = make_bar(
            open=50000.0, close=50200.0,
            low=49500.0, high=50500.0,
            time=1700000000000,
        )
        # body_high = max(50000, 50200) = 50200
        # 50200 < 50400 <= 50500 → in_high_wick ✓
        result = wick_level(lvl, bar)
        assert result.tests == 1

    def test_no_wick_no_test(self):
        """Level value inside the candle body does not trigger a test."""
        lvl = make_level(
            value=50100.0,  # between open (50000) and close (50200)
            test_time=1699980000000,
        )
        bar = make_bar(
            open=50000.0, close=50200.0,
            low=49500.0, high=50500.0,
            time=1700000000000,
        )
        # body_low=50000, body_high=50200
        # 50000 <= 50100 — NOT < body_low → not in low wick
        # 50100 <= 50200 — NOT > body_high → not in high wick
        result = wick_level(lvl, bar)
        assert result.tests == 0
