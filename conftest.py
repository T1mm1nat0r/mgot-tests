"""
Global pytest configuration and fixtures for MGOT testing.

This module provides shared test fixtures, pytest configuration, and
test utilities used across unit, integration, and E2E tests.
"""

import pytest
import redis
import subprocess
import time
from typing import Generator


# ============================================================
# PYTEST CONFIGURATION
# ============================================================

def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line(
        "markers",
        "unit: unit tests (no external dependencies, use mocks)"
    )
    config.addinivalue_line(
        "markers",
        "integration: integration tests (require Redis on port 6479, db 15)"
    )
    config.addinivalue_line(
        "markers",
        "e2e: end-to-end tests (require full pipeline via Docker Compose)"
    )
    config.addinivalue_line(
        "markers",
        "slow: slow tests taking >10 seconds"
    )


# ============================================================
# REDIS FIXTURES
# ============================================================

@pytest.fixture(scope="session")
def redis_test_client() -> Generator[redis.Redis, None, None]:
    """
    Session-scoped Redis client for integration tests.

    Uses port 6479 (test Redis instance) and db 15 (isolated from production).
    Flushes database at end of session.
    """
    r = redis.Redis(host='localhost', port=6479, db=15, decode_responses=True)

    # Verify connection
    try:
        r.ping()
    except redis.ConnectionError:
        pytest.skip("Redis test instance not available on port 6479")

    yield r

    # Cleanup
    r.flushdb()
    r.close()


@pytest.fixture(scope="function")
def redis_client(redis_test_client: redis.Redis) -> Generator[redis.Redis, None, None]:
    """
    Function-scoped Redis client with fresh database for each test.

    Flushes db before and after each test to ensure isolation.
    """
    redis_test_client.flushdb()
    yield redis_test_client
    redis_test_client.flushdb()


@pytest.fixture
def redis_pipeline(redis_client: redis.Redis) -> redis.client.Pipeline:
    """Redis pipeline for batch operations."""
    return redis_client.pipeline()


# ============================================================
# MODEL FIXTURES - Bar
# ============================================================

@pytest.fixture
def sample_bar() -> dict:
    """
    Basic bar fixture with typical OHLCV data.

    Returns a bullish 1h bar for BTCUSDT at $50k.
    """
    return {
        'id': 'BTCUSDT:1h:bar:1700000000000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1700000000000,
        'open': 50000.0,
        'high': 50500.0,
        'low': 49500.0,
        'close': 50200.0,
        'volume': 1000.0,
        'direction': 1,  # Bullish
        'move_id': '',
        'move_start_time': 0,
        'move_open': 0,
        'top': 0,
        'mth': 0,
        'origin': 0,
        'structure': 'Empty',
        'achievements': '',
        'sequence': 0,
        'potential_origin': 0
    }


@pytest.fixture
def bullish_bar(sample_bar: dict) -> dict:
    """Bullish bar (close > open)."""
    return {**sample_bar, 'direction': 1, 'open': 50000, 'close': 50200}


@pytest.fixture
def bearish_bar(sample_bar: dict) -> dict:
    """Bearish bar (close < open)."""
    return {**sample_bar, 'direction': 0, 'open': 50200, 'close': 50000}


# ============================================================
# MODEL FIXTURES - Move
# ============================================================

@pytest.fixture
def sample_move() -> dict:
    """
    Basic move fixture.

    Returns a bullish move with 3 bars.
    """
    return {
        'id': 'BTCUSDT:1h:move:1700000000000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'direction': 1,
        'type': 'move',
        'time': 1700000000000,
        'open': 50000.0,
        'high': 50500.0,
        'low': 49500.0,
        'close': 50200.0,
        'volume': 3000.0,
        'length_bar': 3,
        'length_perc': 0.71,
        'og_mth_value': 0.0
    }


# ============================================================
# MODEL FIXTURES - Zone
# ============================================================

@pytest.fixture
def sample_mth_zone() -> dict:
    """
    Basic MTH zone fixture.

    Returns an incomplete bullish MTH zone.
    """
    return {
        'id': 'BTCUSDT:1h:mth:1700000000000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'type': 'mth',
        'direction': 1,
        'completion': 'incomplete',
        'time': 1700000000000,
        'process_time': 1700007200000,
        'move_end_time': 0,
        'block_zero': 49500.0,  # Move open (support)
        'block_zero_id': 'BTCUSDT:1h:mth:1700000000000:block_zero',
        'block_one': 50500.0,  # Peak close (resistance)
        'block_one_id': 'BTCUSDT:1h:mth:1700000000000:block_one',
        'block_half': 0.0,
        'block_half_id': '',
        'mth_value': 50500.0,
        'mth_move_id': '',
        'og_mth_value': 0.0,
        'age_bars': 0,
        'touches': 0,
        'distance_from_price': 0.0,
        'volume_at_formation': 0.0,
        'state': '',
        'last_touch_bars_ago': 0,
        'mtf_alignments': 0,
        'move_id': ''
    }


@pytest.fixture
def sample_origin_zone() -> dict:
    """
    Basic origin zone fixture.

    Returns an incomplete bullish origin zone with block_half.
    """
    return {
        'id': 'BTCUSDT:1h:origin:1700000000000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'type': 'origin',
        'direction': 1,
        'completion': 'incomplete',
        'time': 1700000000000,
        'process_time': 1700007200000,
        'move_end_time': 1700003600000,
        'block_zero': 50500.0,  # High (resistance for bullish)
        'block_zero_id': 'BTCUSDT:1h:origin:1700000000000:block_zero',
        'block_one': 49500.0,  # Low (support for bullish)
        'block_one_id': 'BTCUSDT:1h:origin:1700000000000:block_one',
        'block_half': 50000.0,  # Midpoint
        'block_half_id': 'BTCUSDT:1h:origin:1700000000000:block_half',
        'mth_value': 51000.0,
        'mth_move_id': 'BTCUSDT:1h:move:1699996400000',
        'og_mth_value': 51000.0,
        'age_bars': 0,
        'touches': 0,
        'distance_from_price': 0.0,
        'volume_at_formation': 0.0,
        'state': '',
        'last_touch_bars_ago': 0,
        'mtf_alignments': 0,
        'move_id': 'BTCUSDT:1h:move:1700000000000'
    }


# ============================================================
# MODEL FIXTURES - Level
# ============================================================

@pytest.fixture
def sample_level() -> dict:
    """
    Basic level fixture.

    Returns a level awaiting gain at $49500.
    """
    return {
        'id': 'BTCUSDT:1h:mth:1700000000000:block_zero',
        'zone_id': 'BTCUSDT:1h:mth:1700000000000',
        'name': 'block_zero',
        'direction': 1,
        'value': 49500.0,
        'gains': 0,
        'conseq_gain': 0,
        'last_gain': 0,
        'losses': 0,
        'conseq_loss': 0,
        'last_loss': 0,
        'bl_tests': 0,
        'last_bl_test': 0,
        'br_tests': 0,
        'last_br_test': 0,
        'tested': 0,
        'state': 'awaiting_gain'
    }


# ============================================================
# E2E FIXTURES
# ============================================================

class PipelineEnvironment:
    """Helper class for E2E test pipeline interactions."""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    def inject_bar(self, bar_dict: dict) -> str:
        """
        Inject a bar into the pipeline via stream:raw_candles.

        Returns the message ID.
        """
        return self.redis.xadd('stream:raw_candles', bar_dict)

    def wait_for_processing(self, bar_id: str, timeout: int = 10) -> bool:
        """
        Wait for a bar to complete pipeline processing.

        Checks if bar exists in Redis with timeout.
        """
        start = time.time()
        while time.time() - start < timeout:
            if self.redis.exists(bar_id):
                return True
            time.sleep(0.1)
        return False

    def get_bar(self, bar_id: str) -> dict:
        """Fetch bar data from Redis."""
        return self.redis.hgetall(bar_id)

    def get_zones(self, symbol: str, timeframe: str) -> list:
        """Fetch all zones for a symbol/timeframe."""
        index_key = f'{symbol}:{timeframe}:zones_index'
        zone_ids = self.redis.zrange(index_key, 0, -1)
        return zone_ids

    def clear_streams(self):
        """Clear all Redis streams for clean test."""
        streams = [
            'stream:raw_candles',
            'stream:clean_candles',
            'stream:leveled_and_updated',
            'stream:bars_with_structure',
            'stream:bars_with_origins',
            'stream:bars_preprocessed'
        ]
        for stream in streams:
            try:
                self.redis.delete(stream)
            except:
                pass


@pytest.fixture(scope="session")
def pipeline_env_session() -> Generator[PipelineEnvironment, None, None]:
    """
    Session-scoped E2E pipeline environment.

    Starts Docker Compose test services and provides helper for pipeline interaction.
    """
    # Start test services
    print("\nStarting test pipeline services...")
    subprocess.run(
        ['docker', 'compose', '-f', 'docker-compose.test.yml', 'up', '-d'],
        check=True,
        capture_output=True
    )

    # Wait for services to be ready
    time.sleep(15)

    r = redis.Redis(host='localhost', port=6479, db=0, decode_responses=True)

    # Verify connection
    try:
        r.ping()
    except redis.ConnectionError:
        pytest.fail("Test Redis not reachable after starting services")

    yield PipelineEnvironment(r)

    # Teardown
    print("\nStopping test pipeline services...")
    subprocess.run(
        ['docker', 'compose', '-f', 'docker-compose.test.yml', 'down'],
        check=True,
        capture_output=True
    )
    r.close()


@pytest.fixture(scope="function")
def pipeline_env(pipeline_env_session: PipelineEnvironment) -> Generator[PipelineEnvironment, None, None]:
    """
    Function-scoped pipeline environment with clean streams.

    Clears all streams before each test.
    """
    pipeline_env_session.clear_streams()
    pipeline_env_session.redis.flushdb()
    yield pipeline_env_session


# ============================================================
# DATA GENERATOR FIXTURES
# ============================================================

@pytest.fixture
def bar_generator():
    """
    Bar sequence generator fixture.

    Provides access to test data generation functions.
    """
    from tests.fixtures.test_data_generator import (
        create_trending_bars,
        create_hh_pattern,
        create_ll_pattern,
        create_origin_pattern,
        create_gap_sequence
    )

    class BarGenerator:
        trending = staticmethod(create_trending_bars)
        hh_pattern = staticmethod(create_hh_pattern)
        ll_pattern = staticmethod(create_ll_pattern)
        origin_pattern = staticmethod(create_origin_pattern)
        gap_sequence = staticmethod(create_gap_sequence)

    return BarGenerator()


# ============================================================
# MOCK FIXTURES
# ============================================================

@pytest.fixture
def mock_redis():
    """Mock Redis client for unit tests (use fakeredis if needed)."""
    from unittest.mock import MagicMock
    return MagicMock()
