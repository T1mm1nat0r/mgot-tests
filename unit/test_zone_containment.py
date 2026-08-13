"""
Unit tests for zone containment tracking.

Tests entry/exit detection logic, count accumulation, state transition
persistence, and invalid zone cleanup.

The installed version of check_zone_containment uses Python-side set
operations (zrangebyscore + smembers), not Lua scripts.
"""

import json
import pytest
from unittest.mock import MagicMock, call
from mgot_utils.models import Bar, Zone
from mgot_utils.processing.containment import check_zone_containment


# ── Helpers ─────────────────────────────────────────────────────

ZONE_ID = 'BTCUSDT:1h:origin:1700000000000'


def make_bar(**kw):
    defaults = dict(
        id='BTCUSDT:1h:bar:1700100000000',
        symbol='BTCUSDT', timeframe='1h',
        time=1700100000000,
        open=50000.0, high=50500.0, low=49500.0, close=50200.0,
        volume=1000.0,
    )
    defaults.update(kw)
    return Bar(**defaults)


def make_zone(**kw):
    defaults = dict(
        id=ZONE_ID,
        symbol='BTCUSDT', timeframe='1h',
        type='origin', direction=1,
        completion='complete',
        time=1700000000000,
        process_time=1700007200000,
        block_zero=50500.0,
        block_one=49500.0,
        entry_count=0, exit_count=0,
        price_inside=0,
    )
    defaults.update(kw)
    return Zone(**defaults)


def _zone_dict(**overrides):
    """Return a dict suitable for Zone.initiate_zone (simulates hgetall result)."""
    d = dict(
        id=ZONE_ID,
        symbol='BTCUSDT', timeframe='1h',
        type='origin', direction=1,
        completion='complete',
        time=1700000000000,
        process_time=1700007200000,
        block_zero=50500.0, block_one=49500.0,
        block_zero_id=f'{ZONE_ID}:block_zero',
        block_one_id=f'{ZONE_ID}:block_one',
        entry_count=0, exit_count=0,
        price_inside=0,
    )
    d.update(overrides)
    return d


def _mock_redis_for_entry(zone_dict, previously_inside=None):
    """Mock Redis for an entry scenario: close is inside zone, not previously tracked."""
    r = MagicMock()

    # zrangebyscore returns the zone_id for both lows and highs queries
    # (meaning zone_low <= close AND zone_high >= close)
    r.zrangebyscore.return_value = [zone_dict['id']]
    # Not previously inside
    r.smembers.return_value = set(previously_inside or [])

    # Pipeline for hgetall
    hgetall_pipe = MagicMock()
    hgetall_pipe.execute.return_value = [zone_dict]

    # Pipeline for sync_with_db + sadd
    write_pipe = MagicMock()
    write_pipe.execute.return_value = []

    r.pipeline.side_effect = [hgetall_pipe, write_pipe]

    return r, write_pipe


def _mock_redis_for_exit(zone_dict):
    """Mock Redis for an exit scenario: close is outside zone, was previously tracked.

    The installed containment code creates an initial pipeline before the if/else
    branches, plus two more for the exit path (hgetall + sync), totaling 3 calls.
    """
    r = MagicMock()

    # For exits: zrangebyscore returns different sets that DON'T intersect
    # (zone is not containing price anymore)
    zone_id = zone_dict['id']

    def zrangebyscore_side_effect(key, *args):
        if 'zone_lows' in key:
            return [zone_id]  # zone_low <= close
        if 'zone_highs' in key:
            return []  # zone_high < close → NOT containing
        return []

    r.zrangebyscore.side_effect = zrangebyscore_side_effect
    r.smembers.return_value = {zone_id}  # was previously inside

    # 3 pipelines: initial (unused), hgetall, sync+srem
    initial_pipe = MagicMock()
    hgetall_pipe = MagicMock()
    hgetall_pipe.execute.return_value = [zone_dict]
    write_pipe = MagicMock()
    write_pipe.execute.return_value = []

    r.pipeline.side_effect = [initial_pipe, hgetall_pipe, write_pipe]

    return r, write_pipe


# ── Entry/Exit Detection Tests ──────────────────────────────────

class TestEntryDetection:
    """Tests for zone entry detection via check_zone_containment."""

    def test_entry_detected(self):
        """Close inside zone when previously outside → entry event published."""
        zd = _zone_dict(entry_count=0)
        r, write_pipe = _mock_redis_for_entry(zd)
        bar = make_bar(close=50200.0)  # inside zone [49500, 50500]

        check_zone_containment(bar, r)

        # Entry event should be published directly on r
        publish_calls = [c for c in r.publish.call_args_list]
        assert len(publish_calls) >= 1
        event_data = json.loads(publish_calls[0][0][1])
        assert event_data['event'] == 'entry'
        assert event_data['zone_id'] == ZONE_ID
        assert event_data['entry_count'] == 1


class TestExitDetection:
    """Tests for zone exit detection via check_zone_containment."""

    def test_exit_detected(self):
        """Close outside zone when previously inside → exit event published."""
        zd = _zone_dict(exit_count=2)
        r, write_pipe = _mock_redis_for_exit(zd)
        bar = make_bar(close=50800.0)  # outside zone [49500, 50500]

        check_zone_containment(bar, r)

        publish_calls = [c for c in r.publish.call_args_list]
        assert len(publish_calls) >= 1
        event_data = json.loads(publish_calls[0][0][1])
        assert event_data['event'] == 'exit'
        assert event_data['exit_count'] == 3  # was 2, now 3


class TestEntryCountAccumulation:
    """Tests for entry/exit count accumulation."""

    def test_entry_count_accumulates(self):
        """Multiple entries increment entry_count from existing value."""
        zd = _zone_dict(entry_count=3)
        r, write_pipe = _mock_redis_for_entry(zd)
        bar = make_bar(close=50200.0)

        check_zone_containment(bar, r)

        publish_calls = [c for c in r.publish.call_args_list]
        event_data = json.loads(publish_calls[0][0][1])
        assert event_data['entry_count'] == 4  # 3 + 1


# ── State Transition Tests ──────────────────────────────────────

class TestCountsPersistAcrossTransitions:
    """Tests for count persistence across zone state transitions."""

    def test_counts_persist_complete_to_taken_out(self):
        """entry/exit counts survive complete → taken_out transition (model test)."""
        zone = make_zone(
            entry_count=5, exit_count=3,
            price_inside=1,
            last_entry_time=1700090000000,
            last_exit_time=1700080000000,
        )

        zone.mark_taken_out(bar_time=1700100000000)

        # Entry/exit counts should NOT be reset by mark_taken_out
        assert zone.entry_count == 5
        assert zone.exit_count == 3
        assert zone.price_inside == 1  # not reset
        assert zone.last_entry_time == 1700090000000


class TestInvalidZoneCleanup:
    """Tests for invalid zone removal from containment tracking."""

    def test_invalid_zone_removed_from_tracking(self):
        """Invalid zones are removed from containment sorted sets on sync."""
        zone = make_zone(completion='invalid')
        target = MagicMock()

        zone.sync_with_db(target)

        # Should call zrem for zone_lows, zone_highs, and srem for price_in_zones
        zrem_calls = [c for c in target.zrem.call_args_list]
        assert len(zrem_calls) >= 2  # zone_lows and zone_highs
        srem_calls = [c for c in target.srem.call_args_list]
        assert len(srem_calls) >= 1  # price_in_zones

    def test_active_zone_added_to_tracking(self):
        """Active (complete) zones are added to containment sorted sets on sync."""
        zone = make_zone(completion='complete')
        target = MagicMock()

        zone.sync_with_db(target)

        # Should call zadd for zone_lows and zone_highs
        zadd_calls = [c for c in target.zadd.call_args_list]
        # zadd is called for zones_index + zone_lows + zone_highs = 3 calls minimum
        assert len(zadd_calls) >= 3
