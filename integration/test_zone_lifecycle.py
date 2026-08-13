"""
Integration tests for zone lifecycle state machine through Redis.

Tests the zone state transitions: incomplete → complete → taken_out / invalid,
verifying correct timestamps, data preservation, and index maintenance.

Requires: Redis on port 6479, db 15
"""

import pytest
from mgot_utils.models import Bar, Level, Zone
from mgot_utils.models.enums import Completion


# ── Helpers ─────────────────────────────────────────────────────

def make_zone(**kw):
    defaults = dict(
        id='BTCUSDT:1h:origin:1700000000000',
        symbol='BTCUSDT', timeframe='1h',
        type='origin', direction=0,
        completion='incomplete',
        time=1700000000000,
        process_time=1700007200000,
        block_zero=49500.0,
        block_zero_id='BTCUSDT:1h:origin:1700000000000:block_zero',
        block_one=50500.0,
        block_one_id='BTCUSDT:1h:origin:1700000000000:block_one',
        block_half=50000.0,
        block_half_id='BTCUSDT:1h:origin:1700000000000:block_half',
    )
    defaults.update(kw)
    return Zone(**defaults)


def make_level(zone_id, name, value, **kw):
    defaults = dict(
        id=f'{zone_id}:{name}',
        zone_id=zone_id,
        name=name,
        direction=0,
        value=value,
    )
    defaults.update(kw)
    return Level(**defaults)


# ── Tests ───────────────────────────────────────────────────────

@pytest.mark.integration
class TestZoneIncompleteToComplete:
    """Test zone transitions from incomplete to complete."""

    def test_zone_incomplete_to_complete(self, redis_client):
        """Zone transitions to complete with correct timestamps and indices."""
        r = redis_client

        zone = make_zone(completion='incomplete')
        zone.sync_with_db(r)

        # Verify initial state
        stored = r.hgetall(zone.id)
        assert stored['completion'] == 'incomplete'

        # Transition to complete
        pipe = r.pipeline()
        zone.mark_complete(pipe, bar_time=1700050000000)
        zone.sync_with_db(pipe)
        pipe.execute()

        # Verify final state in Redis
        stored = r.hgetall(zone.id)
        assert stored['completion'] == 'complete'
        assert stored['time_completed'] == '1700050000000'
        assert stored['was_completed'] == '1'

        # Verify zone is in the zones_index
        index_key = f'{zone.symbol}:{zone.timeframe}:zones_index'
        zone_ids = r.zrange(index_key, 0, -1)
        assert zone.id in zone_ids

        # Verify zone is in containment index (active zones should be tracked)
        lows_key = f'{zone.symbol}:{zone.timeframe}:zone_lows'
        highs_key = f'{zone.symbol}:{zone.timeframe}:zone_highs'
        assert r.zscore(lows_key, zone.id) is not None
        assert r.zscore(highs_key, zone.id) is not None


@pytest.mark.integration
class TestZoneCompleteToTakenOut:
    """Test zone transitions from complete to taken_out."""

    def test_zone_complete_to_taken_out(self, redis_client):
        """Preserves complete-phase test data on taken_out transition."""
        r = redis_client

        # Create a complete zone that was already tested
        zone = make_zone(
            completion='complete',
            time_completed=1700050000000,
            was_completed=1,
            zone_tests=1,
            last_zone_test_time=1700060000000,
            last_zone_test_type='internal',
            test_eligible_time=1700055000000,
        )
        zone.sync_with_db(r)

        # Transition to taken_out
        zone.mark_taken_out(bar_time=1700070000000)
        zone.sync_with_db(r)

        # Verify state in Redis
        stored = r.hgetall(zone.id)
        assert stored['completion'] == 'taken_out'
        assert stored['time_taken_out'] == '1700070000000'
        assert stored['was_completed'] == '1'  # preserved

        # Complete-phase data preserved
        assert stored['complete_zone_tests'] == '1'
        assert stored['complete_zone_test_time'] == '1700060000000'
        assert stored['complete_zone_test_type'] == 'internal'

        # Current-phase fields reset
        assert stored['zone_tests'] == '0'
        assert stored['test_eligible_time'] == '0'

        # Zone still in containment index (taken_out is active)
        lows_key = f'{zone.symbol}:{zone.timeframe}:zone_lows'
        assert r.zscore(lows_key, zone.id) is not None


@pytest.mark.integration
class TestZoneCompleteToInvalid:
    """Test zone transitions from incomplete to invalid."""

    def test_zone_to_invalid_cleans_containment(self, redis_client):
        """Invalid zones cleaned from containment sorted sets."""
        r = redis_client

        zone = make_zone(completion='incomplete')
        zone.sync_with_db(r)

        # Verify initially in containment index
        lows_key = f'{zone.symbol}:{zone.timeframe}:zone_lows'
        highs_key = f'{zone.symbol}:{zone.timeframe}:zone_highs'
        assert r.zscore(lows_key, zone.id) is not None

        # Transition to invalid
        zone.mark_invalid(bar_time=1700050000000)
        zone.sync_with_db(r)

        stored = r.hgetall(zone.id)
        assert stored['completion'] == 'invalid'
        assert stored['time_invalid'] == '1700050000000'

        # Containment index should be cleaned
        assert r.zscore(lows_key, zone.id) is None
        assert r.zscore(highs_key, zone.id) is None

        # Should be removed from price_in_zones too
        in_zones_key = f'{zone.symbol}:{zone.timeframe}:price_in_zones'
        assert not r.sismember(in_zones_key, zone.id)
