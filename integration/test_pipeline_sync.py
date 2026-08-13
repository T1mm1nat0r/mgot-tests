"""
Integration tests for pipeline Pub/Sub synchronization.

Tests the processed_bar Pub/Sub channel that coordinates between
service 03 (levels/zones) and service 10 (zone processor).

Requires: Redis on port 6479, db 15
"""

import asyncio
import threading
import time
import pytest


# ── Tests ───────────────────────────────────────────────────────

@pytest.mark.integration
class TestProcessedBarSignal:
    """Tests for the processed_bar Pub/Sub channel."""

    def test_processed_bar_signal_received(self, redis_client):
        """Pub/Sub signal published on processed_bar is received by subscriber."""
        r = redis_client
        received = []

        pubsub = r.pubsub()
        pubsub.subscribe('processed_bar')
        # Consume subscription confirmation
        msg = pubsub.get_message(timeout=1)

        # Publish a bar signal
        bar_id = 'BTCUSDT:1h:bar:1700000000000'
        r.publish('processed_bar', bar_id)

        # Receive the message
        msg = pubsub.get_message(timeout=2)
        assert msg is not None
        assert msg['type'] == 'message'
        assert msg['data'] == bar_id

        pubsub.unsubscribe()
        pubsub.close()

    def test_pipeline_reset_signal(self, redis_client):
        """PIPELINE_RESET signal is distinct from bar IDs and handled correctly."""
        r = redis_client

        pubsub = r.pubsub()
        pubsub.subscribe('processed_bar')
        pubsub.get_message(timeout=1)  # consume subscription confirmation

        # Publish reset signal (sent when ingestion mode changes)
        r.publish('processed_bar', 'PIPELINE_RESET')

        msg = pubsub.get_message(timeout=2)
        assert msg is not None
        assert msg['data'] == 'PIPELINE_RESET'

        # Publish a normal bar after reset
        bar_id = 'BTCUSDT:1h:bar:1700003600000'
        r.publish('processed_bar', bar_id)

        msg = pubsub.get_message(timeout=2)
        assert msg is not None
        assert msg['data'] == bar_id

        pubsub.unsubscribe()
        pubsub.close()
