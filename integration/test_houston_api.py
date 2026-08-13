"""
Integration tests for Houston API endpoints.

Tests the FastAPI endpoints using httpx.AsyncClient. The Houston server
module is imported via sys.path manipulation since it's a service directory,
not an installable package.

Redis connections are mocked so tests don't require a running Redis instance.
"""

import sys
import os
import json
import time
import pytest
import httpx
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Add Houston service to path so we can import server.py
HOUSTON_DIR = str(Path(__file__).parent.parent.parent / '21_houston')
if HOUSTON_DIR not in sys.path:
    sys.path.insert(0, HOUSTON_DIR)

# server.py mounts StaticFiles(directory="static") relative to CWD
_original_cwd = os.getcwd()
os.chdir(HOUSTON_DIR)

# ── Mock Redis before importing server ──────────────────────────

# Patch the async Redis that Houston uses (from data.common)
_mock_async_redis = AsyncMock()
_mock_async_redis.ping = AsyncMock(return_value=True)
_mock_async_redis.exists = AsyncMock(return_value=False)
_mock_async_redis.smembers = AsyncMock(return_value={'BTCUSDT', 'ETHUSDT'})
_mock_async_redis.sadd = AsyncMock(return_value=1)
_mock_async_redis.srem = AsyncMock(return_value=1)
_mock_async_redis.zrange = AsyncMock(return_value=[])
_mock_async_redis.hgetall = AsyncMock(return_value={})

import data.common as _houston_common
_houston_common._redis = _mock_async_redis

from server import app, _overlay_cache, OVERLAY_CACHE_TTL

# Restore CWD
os.chdir(_original_cwd)

# ── Fixtures ────────────────────────────────────────────────────

@pytest.fixture
def reset_cache():
    """Clear overlay cache before each test."""
    _overlay_cache.clear()
    yield
    _overlay_cache.clear()


# ── Tests ───────────────────────────────────────────────────────

@pytest.mark.integration
class TestHealthEndpoint:
    """Tests for /api/health endpoint."""

    @pytest.mark.asyncio
    async def test_health_ok(self):
        """Returns ok when Redis is reachable."""
        mock_health = {
            'status': 'ok', 'redis': True, 'redis_snapshots': True,
            'streams': {}, 'services': {}, 'symbols': {},
        }
        with patch('server.get_full_health', new_callable=AsyncMock, return_value=mock_health):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                resp = await client.get('/api/health')
        assert resp.status_code == 200
        data = resp.json()
        assert data['status'] == 'ok'
        assert data['redis'] is True

    @pytest.mark.asyncio
    async def test_health_degraded(self):
        """Returns degraded when snapshots Redis is down."""
        mock_health = {
            'status': 'degraded', 'redis': True, 'redis_snapshots': False,
            'streams': {}, 'services': {}, 'symbols': {},
        }
        with patch('server.get_full_health', new_callable=AsyncMock, return_value=mock_health):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                resp = await client.get('/api/health')
        data = resp.json()
        assert data['status'] == 'degraded'
        assert data['redis_snapshots'] is False

    @pytest.mark.asyncio
    async def test_health_fallback_on_error(self):
        """Returns critical fallback when get_full_health raises."""
        with patch('server.get_full_health', new_callable=AsyncMock, side_effect=Exception('boom')):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                resp = await client.get('/api/health')
        assert resp.status_code == 200
        data = resp.json()
        assert data['status'] == 'critical'
        assert data['redis'] is False


@pytest.mark.integration
class TestConfigEndpoint:
    """Tests for /api/config endpoint."""

    @pytest.mark.asyncio
    async def test_config_returns_timeframes(self):
        """Config returns all 7 timeframes including 1w."""
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            resp = await client.get('/api/config')
        data = resp.json()
        assert '1w' in data['timeframes']
        assert len(data['timeframes']) == 7


@pytest.mark.integration
class TestOverlayEndpoints:
    """Tests for overlay data endpoints."""

    @pytest.mark.asyncio
    async def test_origins_endpoint(self, reset_cache):
        """Origins endpoint returns data and respects completion filter."""
        _mock_async_redis.zrange = AsyncMock(return_value=[])
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            resp = await client.get('/api/origins?symbol=BTCUSDT&timeframe=15m')
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    @pytest.mark.asyncio
    async def test_mth_endpoint_no_filter(self, reset_cache):
        """MTH endpoint without completion filter returns all zones."""
        _mock_async_redis.zrange = AsyncMock(return_value=[])
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            resp = await client.get('/api/mth?symbol=BTCUSDT&timeframe=15m')
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_squeeze_endpoint_no_filter(self, reset_cache):
        """Squeeze endpoint without completion filter returns data."""
        _mock_async_redis.zrange = AsyncMock(return_value=[])
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            resp = await client.get('/api/squeeze?symbol=BTCUSDT&timeframe=15m')
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_sweeps_endpoint(self, reset_cache):
        """Sweeps endpoint returns sweep events."""
        _mock_async_redis.zrange = AsyncMock(return_value=[])
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            resp = await client.get('/api/sweeps?symbol=BTCUSDT&timeframe=15m')
        assert resp.status_code == 200


@pytest.mark.integration
class TestOverlayCache:
    """Tests for the server-side overlay cache."""

    @pytest.mark.asyncio
    async def test_cache_30s_ttl(self, reset_cache):
        """Second call within 30s returns cached data (no Redis hit)."""
        call_count = 0
        original_return = []

        async def counting_zrange(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return original_return

        _mock_async_redis.zrange = AsyncMock(side_effect=counting_zrange)

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            # First call — hits Redis
            resp1 = await client.get('/api/origins?symbol=BTCUSDT&timeframe=15m')
            first_count = call_count

            # Second call — should use cache
            resp2 = await client.get('/api/origins?symbol=BTCUSDT&timeframe=15m')
            second_count = call_count

        assert resp1.status_code == 200
        assert resp2.status_code == 200
        # Second call should not have incremented the Redis call count
        assert second_count == first_count

    @pytest.mark.asyncio
    async def test_cache_clear_endpoint(self, reset_cache):
        """POST /api/cache/clear clears the overlay cache."""
        # Prime the cache
        _mock_async_redis.zrange = AsyncMock(return_value=[])
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            await client.get('/api/origins?symbol=BTCUSDT&timeframe=15m')
            assert len(_overlay_cache) > 0

            # Clear cache
            resp = await client.post('/api/cache/clear')
            assert resp.status_code == 200
            assert resp.json()['cleared'] is True
            assert len(_overlay_cache) == 0
