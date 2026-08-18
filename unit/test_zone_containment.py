"""
Unit tests for zone containment tracking.

Tests entry/exit detection logic, count accumulation, state transition
persistence, and invalid zone cleanup.

`check_zone_containment` does its detection in a **Lua script** and publishes
on a **pipeline**, not on the client. These tests were written against an older
shape — client-side `zrangebyscore`/`smembers` and `r.publish` — and silently
stopped exercising the function at commit 3acdf3c, because a MagicMock script
returns a MagicMock that iterates empty, so the function early-returned and
every assertion failed on zero publishes rather than on behaviour.

Mock the script, not the sorted sets.
"""

import json
import pytest
from unittest.mock import MagicMock, call
from mgot_utils.models import Bar, Zone
from mgot_utils.processing import containment
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


# The detect script is cached in a module-level global, registered against
# whichever client it first saw. Left alone, the first test's mock script would
# serve every later test and the ones after it would assert against stale
# markers.
@pytest.fixture(autouse=True)
def _reset_script_cache():
    containment._detect_script = None
    yield
    containment._detect_script = None


def _meta(zone_type='origin', direction=1, block_zero=50500.0,
          block_one=49500.0, entry_count=0, exit_count=0):
    """One row as `hmget` returns it — positional, in the order the code reads."""
    return [zone_type, str(direction), str(block_zero), str(block_one),
            str(entry_count), str(exit_count)]


def _mock_redis(markers, metadata):
    """Wire a MagicMock to the contract `check_zone_containment` actually uses.

    Returns (r, write_pipe). Publishes land on `write_pipe`, which is the second
    pipeline the function opens — the first is the metadata `hmget` batch.
    """
    r = MagicMock()
    r.register_script.return_value = MagicMock(return_value=list(markers))

    meta_pipe = MagicMock()
    meta_pipe.execute.return_value = list(metadata)
    write_pipe = MagicMock()
    write_pipe.execute.return_value = []
    r.pipeline.side_effect = [meta_pipe, write_pipe]
    return r, write_pipe


def _published(pipe):
    """Decoded `zone_containment` payloads, in order."""
    return [json.loads(c[0][1]) for c in pipe.publish.call_args_list
            if c[0][0] == 'zone_containment']


# ── Entry/Exit Detection Tests ──────────────────────────────────

class TestEntryDetection:
    """Tests for zone entry detection via check_zone_containment."""

    def test_entry_detected(self):
        """A newly-inside zone publishes one entry carrying the incremented count."""
        r, write_pipe = _mock_redis([f'E:{ZONE_ID}'], [_meta(entry_count=0)])
        bar = make_bar(close=50200.0)  # inside [49500, 50500]

        check_zone_containment(bar, r)

        events = _published(write_pipe)
        assert len(events) == 1
        assert events[0]['event'] == 'entry'
        assert events[0]['zone_id'] == ZONE_ID
        assert events[0]['entry_count'] == 1
        assert events[0]['bar_time'] == bar.time

    def test_entry_marks_price_inside(self):
        """The published event and the persisted hash must not disagree."""
        r, write_pipe = _mock_redis([f'E:{ZONE_ID}'], [_meta(entry_count=0)])

        check_zone_containment(make_bar(close=50200.0), r)

        written = dict(write_pipe.hset.call_args_list[0][1]['mapping'])
        assert written['price_inside'] == 1
        assert written['entry_count'] == 1

    def test_zone_with_no_metadata_is_skipped(self):
        """A marker for a zone whose hash has vanished publishes nothing.

        The script reads the sorted sets, which can outlive the hash — a deleted
        zone that was never zrem'd would otherwise publish an event naming a
        zone that no longer exists.
        """
        r, write_pipe = _mock_redis([f'E:{ZONE_ID}'], [[None] * 6])

        check_zone_containment(make_bar(close=50200.0), r)

        assert _published(write_pipe) == []


class TestExitDetection:
    """Tests for zone exit detection via check_zone_containment."""

    def test_exit_detected(self):
        """A zone price has left publishes one exit with the incremented count."""
        r, write_pipe = _mock_redis([f'X:{ZONE_ID}'], [_meta(exit_count=3)])

        check_zone_containment(make_bar(close=60000.0), r)

        events = _published(write_pipe)
        assert len(events) == 1
        assert events[0]['event'] == 'exit'
        assert events[0]['zone_id'] == ZONE_ID
        assert events[0]['exit_count'] == 4

    def test_entries_and_exits_in_one_bar(self):
        """One bar can leave one zone and enter another; both must be reported."""
        other = 'BTCUSDT:1h:origin:1700000111000'
        r, write_pipe = _mock_redis(
            [f'E:{ZONE_ID}', f'X:{other}'],
            [_meta(entry_count=0), _meta(exit_count=0)],
        )

        check_zone_containment(make_bar(close=50200.0), r)

        events = _published(write_pipe)
        assert {e['event'] for e in events} == {'entry', 'exit'}
        assert {e['zone_id'] for e in events} == {ZONE_ID, other}


class TestEntryCountAccumulation:
    """Counts increment from whatever is stored, not from zero."""

    def test_entry_count_accumulates(self):
        """A zone entered before reports the next number, not 1."""
        r, write_pipe = _mock_redis([f'E:{ZONE_ID}'], [_meta(entry_count=7)])

        check_zone_containment(make_bar(close=50200.0), r)

        assert _published(write_pipe)[0]['entry_count'] == 8

    def test_no_markers_touches_nothing(self):
        """An empty script result must not open a write pipeline at all."""
        r = MagicMock()
        r.register_script.return_value = MagicMock(return_value=[])

        check_zone_containment(make_bar(close=50200.0), r)

        assert r.pipeline.call_count == 0


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
