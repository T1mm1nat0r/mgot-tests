"""
Unit tests for zone test eligibility and detection.

Tests the business rules for when and how a zone is "tested":
- Eligibility gate (confirming move must end first)
- Complete zone test conditions (block_zero break, block_half wick, wick-close-outside)
- Taken-out zone test conditions (block_one break, block_one wick)
- Internal vs external classification
- Lifecycle phase data preservation
"""

import pytest
from unittest.mock import MagicMock
from mgot_utils.models import Bar, Level, Zone
from mgot_utils.processing.post_process import _check_and_record_zone_test, _classify_zone_test


# ── Helpers ─────────────────────────────────────────────────────

def make_bar(**kw):
    defaults = dict(
        id='BTCUSDT:1h:bar:1700100000000',
        symbol='BTCUSDT', timeframe='1h',
        time=1700100000000,
        open=50000.0, high=50500.0, low=49500.0, close=50200.0,
        volume=1000.0, direction=1,
    )
    defaults.update(kw)
    return Bar(**defaults)


def make_zone(**kw):
    """Create a bullish origin zone with block_zero=50500 (high), block_one=49500 (low).
    zone_low=49500, zone_high=50500."""
    defaults = dict(
        id='BTCUSDT:1h:origin:1700000000000',
        symbol='BTCUSDT', timeframe='1h',
        type='origin',
        direction=1,
        completion='complete',
        time=1700000000000,
        process_time=1700007200000,
        block_zero=50500.0,
        block_zero_id='BTCUSDT:1h:origin:1700000000000:block_zero',
        block_one=49500.0,
        block_one_id='BTCUSDT:1h:origin:1700000000000:block_one',
        block_half=50000.0,
        block_half_id='BTCUSDT:1h:origin:1700000000000:block_half',
        time_completed=1700050000000,
        test_eligible_time=1700060000000,
        zone_tests=0,
    )
    defaults.update(kw)
    return Zone(**defaults)


def make_level(zone_id, name, value, **kw):
    defaults = dict(
        id=f'{zone_id}:{name}',
        zone_id=zone_id,
        name=name,
        direction=1,
        value=value,
    )
    defaults.update(kw)
    return Level(**defaults)


def _zone_lvls(zone):
    """Create level objects matching a zone's block_zero, block_one, block_half."""
    lvls = [
        make_level(zone.id, 'block_zero', float(zone.block_zero)),
        make_level(zone.id, 'block_one', float(zone.block_one)),
    ]
    if zone.type == 'origin' and zone.block_half:
        lvls.append(make_level(zone.id, 'block_half', float(zone.block_half)))
    return lvls


# ── Eligibility Gate Tests ──────────────────────────────────────

class TestZoneTestEligibility:
    """Tests for zone test eligibility gate."""

    def test_complete_zone_eligibility_gate(self):
        """Zone with test_eligible_time=0 cannot be tested."""
        zone = make_zone(test_eligible_time=0)
        lvls = _zone_lvls(zone)
        pipe = MagicMock()

        # Bar that would otherwise trigger a test (breaks block_zero, closes inside)
        bar = make_bar(
            open=50100.0, close=50200.0,
            low=49800.0, high=50600.0,
        )
        result = _check_and_record_zone_test(bar, zone, lvls, pipe)
        assert result is False
        assert zone.zone_tests == 0

    def test_taken_out_zone_eligibility_gate(self):
        """Taken-out zone with test_eligible_time=0 cannot be tested."""
        zone = make_zone(
            completion='taken_out',
            time_taken_out=1700070000000,
            test_eligible_time=0,
        )
        lvls = _zone_lvls(zone)
        pipe = MagicMock()

        # Bar that would break block_one
        bar = make_bar(
            open=49800.0, close=49600.0,
            low=49400.0, high=49900.0,
        )
        result = _check_and_record_zone_test(bar, zone, lvls, pipe)
        assert result is False

    def test_already_tested_zone_blocked(self):
        """Zone that was already tested (zone_tests > 0) cannot be tested again."""
        zone = make_zone(zone_tests=1)
        lvls = _zone_lvls(zone)
        pipe = MagicMock()

        bar = make_bar(
            open=50100.0, close=50200.0,
            low=49800.0, high=50600.0,
        )
        result = _check_and_record_zone_test(bar, zone, lvls, pipe)
        assert result is False


# ── Complete Zone Test Conditions ───────────────────────────────

class TestCompleteZoneTest:
    """Tests for complete zone test detection conditions."""

    def test_block_zero_break_closes_inside(self):
        """Price breaks block_zero AND closes inside zone → test detected.

        Zone: block_zero=50500 (high), block_one=49500 (low).
        block_zero at zone_high → broke_zero requires bar.high > 50500.
        close_inside: 49500 <= close <= 50500.
        """
        zone = make_zone()
        lvls = _zone_lvls(zone)
        pipe = MagicMock()

        bar = make_bar(
            open=50100.0, close=50200.0,
            low=49800.0, high=50600.0,  # high > 50500 → breaks block_zero
            # close=50200 is inside [49500, 50500]
        )
        result = _check_and_record_zone_test(bar, zone, lvls, pipe)
        assert result is True
        assert zone.zone_tests == 1
        assert zone.last_zone_test_time == bar.time

    def test_block_half_wick_origin(self):
        """block_half tested after completion → zone test (origin only).

        Condition 2: block_half level has tests > 0 with last_test > time_completed.
        """
        zone = make_zone()
        lvls = _zone_lvls(zone)
        # Set block_half level as having been tested after completion
        lvls[2].tests = 1
        lvls[2].last_test = zone.time_completed + 10000000
        pipe = MagicMock()

        # Bar that doesn't break block_zero or close inside — only block_half matters
        bar = make_bar(
            open=50100.0, close=50200.0,
            low=50050.0, high=50300.0,  # no break of block_zero (50500)
        )
        result = _check_and_record_zone_test(bar, zone, lvls, pipe)
        assert result is True
        assert zone.last_zone_test_type == 'external'

    def test_wick_close_outside(self):
        """Wick through block_zero/one with close outside zone → test.

        Condition 3: bar.close outside [zone_low, zone_high], but wick hits a level.
        """
        zone = make_zone()
        lvls = _zone_lvls(zone)
        pipe = MagicMock()

        # Bar with close above zone (50600 > zone_high=50500)
        # but low wick reaching block_one level at 49500
        bar = make_bar(
            open=50550.0, close=50600.0,
            low=49450.0, high=50700.0,
            # close=50600 is outside zone (> 50500)
            # body_low = min(50550, 50600) = 50550
            # bar.low=49450 <= block_one(49500) < 50550 → in_low_wick for block_one
        )
        result = _check_and_record_zone_test(bar, zone, lvls, pipe)
        assert result is True


# ── Taken-Out Zone Test Conditions ──────────────────────────────

class TestTakenOutZoneTest:
    """Tests for taken-out zone test detection conditions."""

    def test_block_one_break_closes_inside(self):
        """Taken-out zone: price breaks block_one AND closes inside → test.

        Zone: block_zero=50500 (high), block_one=49500 (low).
        block_one at zone_low → broke_one requires bar.low < 49500.
        close_inside: 49500 <= close <= 50500.
        """
        zone = make_zone(
            completion='taken_out',
            time_taken_out=1700070000000,
            test_eligible_time=1700080000000,
        )
        lvls = _zone_lvls(zone)
        pipe = MagicMock()

        bar = make_bar(
            open=49800.0, close=49600.0,
            low=49400.0, high=49900.0,
            # low=49400 < 49500 → breaks block_one
            # close=49600 is inside [49500, 50500]
        )
        result = _check_and_record_zone_test(bar, zone, lvls, pipe)
        assert result is True
        assert zone.zone_tests == 1

    def test_block_one_wicked(self):
        """Taken-out zone: price wicks block_one from either side → test.

        Wick hits block_one without needing close inside zone.
        """
        zone = make_zone(
            completion='taken_out',
            time_taken_out=1700070000000,
            test_eligible_time=1700080000000,
        )
        lvls = _zone_lvls(zone)
        pipe = MagicMock()

        # Bar closes below zone but wicks up to block_one (49500)
        bar = make_bar(
            open=49200.0, close=49300.0,
            low=49100.0, high=49550.0,
            # close=49300 is below zone_low (49500) → not close_inside
            # body_high = max(49200, 49300) = 49300
            # body_high(49300) < block_one(49500) <= bar.high(49550) → in_high_wick
        )
        result = _check_and_record_zone_test(bar, zone, lvls, pipe)
        assert result is True


# ── Classification Tests ────────────────────────────────────────

class TestZoneTestClassification:
    """Tests for internal vs external zone test classification."""

    def test_internal_low_wick_exits_zone(self):
        """Low wick exits zone boundary → internal test."""
        bar = make_bar(low=49400.0)  # below zone_low=49500
        result = _classify_zone_test(bar, zone_low=49500.0, zone_high=50500.0, in_low_wick=True)
        assert result == 'internal'

    def test_external_low_wick_stays_inside(self):
        """Low wick stays inside zone → external test."""
        bar = make_bar(low=49600.0)  # above zone_low=49500
        result = _classify_zone_test(bar, zone_low=49500.0, zone_high=50500.0, in_low_wick=True)
        assert result == 'external'

    def test_internal_high_wick_exits_zone(self):
        """High wick exits zone boundary → internal test."""
        bar = make_bar(high=50600.0)  # above zone_high=50500
        result = _classify_zone_test(bar, zone_low=49500.0, zone_high=50500.0, in_low_wick=False)
        assert result == 'internal'

    def test_external_high_wick_stays_inside(self):
        """High wick stays inside zone → external test."""
        bar = make_bar(high=50400.0)  # below zone_high=50500
        result = _classify_zone_test(bar, zone_low=49500.0, zone_high=50500.0, in_low_wick=False)
        assert result == 'external'


# ── Lifecycle Phase Tests ───────────────────────────────────────

class TestLifecyclePhases:
    """Tests for test data preservation across zone state transitions."""

    def test_complete_data_preserved_on_taken_out(self):
        """Complete-phase test data preserved in complete_zone_tests fields on transition."""
        zone = make_zone(
            zone_tests=1,
            last_zone_test_time=1700090000000,
            last_zone_test_type='external',
        )

        zone.mark_taken_out(bar_time=1700100000000)

        # Complete-phase data preserved
        assert zone.complete_zone_tests == 1
        assert zone.complete_zone_test_time == 1700090000000
        assert zone.complete_zone_test_type == 'external'

        # Current-phase fields reset for taken_out phase
        assert zone.zone_tests == 0
        assert zone.last_zone_test_time == 0
        assert zone.last_zone_test_type == ''
        assert zone.test_eligible_time == 0
