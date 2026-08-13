"""
Integration tests for level achievement tracking with Redis.

These tests verify that gaining_lvl/losing_lvl correctly update Redis state
when levels are synced, and that tracking lists are maintained properly.

Requires: Redis on port 6479, db 15
"""

import pytest
from mgot_utils.models import Bar, Level, Zone
from mgot_utils.processing.achievements import gaining_lvl, losing_lvl, wick_level


# ── Helpers ─────────────────────────────────────────────────────

def make_bar(**kw):
    defaults = dict(
        id='BTCUSDT:1h:bar:1700000000000',
        symbol='BTCUSDT', timeframe='1h',
        time=1700000000000,
        open=50000.0, high=50500.0, low=49500.0, close=50200.0,
        volume=1000.0, direction=1,
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
        state='awaiting_gain',
    )
    defaults.update(kw)
    return Level(**defaults)


def make_zone(**kw):
    defaults = dict(
        id='BTCUSDT:1h:origin:1700000000000',
        symbol='BTCUSDT', timeframe='1h',
        type='origin', direction=0,
        completion='complete',
        time=1700000000000,
        process_time=1700007200000,
        block_zero=49500.0,
        block_zero_id='BTCUSDT:1h:origin:1700000000000:block_zero',
        block_one=50500.0,
        block_one_id='BTCUSDT:1h:origin:1700000000000:block_one',
        sweep_level=49200.0,
        sweeps=0,
    )
    defaults.update(kw)
    return Zone(**defaults)


# ── Tests ───────────────────────────────────────────────────────

@pytest.mark.integration
class TestLevelGainUpdatesRedis:
    """Verify level gains/losses are written correctly to Redis."""

    def test_gain_level_updates_redis(self, redis_client):
        """Level gain updates stored in Redis hash and tracking sorted set."""
        r = redis_client

        # Create level and store in Redis
        lvl = make_level(value=49800.0, state='awaiting_gain')
        lvl.sync_with_db(r)

        # Add to to_gain tracking list
        r.zadd('BTCUSDT:1h:lvls:to_gain', {lvl.id: lvl.value})

        # Bar closes above level → gain
        bar = make_bar(close=50200.0, time=1700003600000)
        lvl = gaining_lvl(lvl, bar)
        lvl.sync_with_db(r)

        # Verify in Redis
        stored = r.hgetall(lvl.id)
        assert stored['gains'] == '1'
        assert stored['last_gain'] == str(bar.time)

    def test_loss_level_updates_redis(self, redis_client):
        """Level loss updates stored in Redis hash."""
        r = redis_client

        lvl = make_level(
            value=50300.0,
            state='awaiting_loss',
        )
        lvl.sync_with_db(r)

        bar = make_bar(close=50100.0, time=1700003600000)
        lvl = losing_lvl(lvl, bar)
        lvl.sync_with_db(r)

        stored = r.hgetall(lvl.id)
        assert stored['losses'] == '1'
        assert stored['last_loss'] == str(bar.time)


@pytest.mark.integration
class TestLevelThreshold:
    """Verify level tracking stops after threshold is reached."""

    def test_level_threshold_reached(self, redis_client):
        """Level removed from tracking after exceeding 4 gains."""
        r = redis_client

        lvl = make_level(value=49800.0, gains=4, state='awaiting_gain')
        lvl.sync_with_db(r)
        r.zadd('BTCUSDT:1h:lvls:to_gain', {lvl.id: lvl.value})

        # Process one more gain → should exceed threshold
        bar = make_bar(close=50200.0, time=1700003600000)
        lvl = gaining_lvl(lvl, bar)
        assert lvl.gains == 5
        assert lvl.should_stop_tracking_gains(max_gains=4)

        # Simulate pipeline tracking removal
        pipe = r.pipeline()
        lvl.remove_from_tracking(pipe, 'BTCUSDT', '1h', 'to_gain')
        lvl.sync_with_db(pipe)
        pipe.execute()

        # Verify removed from to_gain
        remaining = r.zrangebyscore('BTCUSDT:1h:lvls:to_gain', '-inf', '+inf')
        assert lvl.id not in remaining

        # Verify level still exists with correct count
        stored = r.hgetall(lvl.id)
        assert stored['gains'] == '5'


@pytest.mark.integration
class TestAchievementUpdatesZone:
    """Verify zone fields are updated via level achievements."""

    def test_zone_sweep_tracked_in_redis(self, redis_client):
        """Sweep event recorded in zone and sweep index."""
        r = redis_client

        zone = make_zone(sweeps=0, sweep_level=49200.0)
        zone.sync_with_db(r)

        # Simulate sweep detection: set zone.sweeps directly
        zone.sweeps = 1
        zone.last_sweep_time = 1700050000000

        pipe = r.pipeline()
        sweep_key = f'{zone.symbol}:{zone.timeframe}:sweeps_index'
        sweep_entry = f'{zone.id}:sweep:{zone.last_sweep_time}'
        pipe.zadd(sweep_key, {sweep_entry: zone.last_sweep_time})
        zone.sync_with_db(pipe)
        pipe.execute()

        # Verify zone in Redis
        stored = r.hgetall(zone.id)
        assert stored['sweeps'] == '1'
        assert stored['last_sweep_time'] == str(zone.last_sweep_time)

        # Verify sweep index
        sweeps = r.zrange(sweep_key, 0, -1)
        assert sweep_entry in sweeps

    def test_zone_test_tracked_in_redis(self, redis_client):
        """Zone test recorded in zone hash and zone_tests_index."""
        r = redis_client

        zone = make_zone(
            zone_tests=0,
            completion='complete',
            test_eligible_time=1700060000000,
        )
        zone.sync_with_db(r)

        # Simulate zone test detection
        zone.zone_tests = 1
        zone.last_zone_test_time = 1700070000000
        zone.last_zone_test_type = 'external'

        pipe = r.pipeline()
        test_key = f'{zone.symbol}:{zone.timeframe}:zone_tests_index'
        test_entry = f'{zone.id}:zone_test:{zone.last_zone_test_time}'
        pipe.zadd(test_key, {test_entry: zone.last_zone_test_time})
        zone.sync_with_db(pipe)
        pipe.execute()

        stored = r.hgetall(zone.id)
        assert stored['zone_tests'] == '1'
        assert stored['last_zone_test_type'] == 'external'
