"""Consumer registrations must not accumulate across deploys.

Consumer names are `{group}-{pid}`, so every container restart registers a new
one and the previous entry stays in the group indefinitely. Observed on `03`
after a day of deploys: three registrations, two idle for 72 minutes and 32
hours.

They are inert — `claim_pending_messages` scans the whole group with
`xpending_range(stream, group, '-', '+')`, so entries stranded on a dead consumer
are still reclaimed — but the list grows without bound.

The dangerous half is the fix, not the problem: `XGROUP DELCONSUMER` discards the
consumer's pending list along with the registration, so reaping one that still
owns unacknowledged messages silently drops work. These cases pin that it never
happens.
"""

import pytest

from mgot_utils.core.redis_streams import reap_stale_consumers

STREAM, GROUP, ME = 'stream:clean_candles', '03_levels_and_zones', '03_levels_and_zones-10'
HOUR = 3_600_000


class FakeRedis:
    def __init__(self, consumers):
        self.consumers = list(consumers)
        self.deleted = []

    def xinfo_consumers(self, stream, group):
        return self.consumers

    def xgroup_delconsumer(self, stream, group, name):
        def _name(c):
            n = c.get('name', c.get(b'name'))
            return n.decode() if isinstance(n, bytes) else n
        self.deleted.append(name)
        self.consumers = [c for c in self.consumers if _name(c) != name]
        return 1


def c(name, pending=0, idle=HOUR):
    return {'name': name, 'pending': pending, 'idle': idle}


def test_stale_idle_consumers_are_removed():
    r = FakeRedis([c(ME, idle=60), c('03_levels_and_zones-9', idle=32 * HOUR),
                   c('03_levels_and_zones-11', idle=72 * 60_000)])
    removed = reap_stale_consumers(r, STREAM, GROUP, ME)
    assert set(removed) == {'03_levels_and_zones-9', '03_levels_and_zones-11'}


def test_a_consumer_holding_pending_entries_is_never_reaped():
    """DELCONSUMER would discard its PEL — that is lost work, not tidy-up."""
    r = FakeRedis([c(ME, idle=60), c('dead-but-owns-work', pending=2, idle=99 * HOUR)])
    assert reap_stale_consumers(r, STREAM, GROUP, ME) == []
    assert r.deleted == []


def test_the_live_consumer_is_never_reaped():
    """Even if it looks idle — a blocked XREADGROUP is idle by definition."""
    r = FakeRedis([c(ME, idle=99 * HOUR)])
    assert reap_stale_consumers(r, STREAM, GROUP, ME) == []


def test_recently_idle_consumers_are_left_alone():
    """The threshold is well past any block_ms cycle, so a consumer merely
    between reads is not a candidate."""
    r = FakeRedis([c(ME, idle=60), c('other', idle=5_000)])
    assert reap_stale_consumers(r, STREAM, GROUP, ME) == []


def test_a_redis_failure_is_not_fatal():
    """Tidy-up must never stop a service from consuming."""
    class Broken(FakeRedis):
        def xinfo_consumers(self, stream, group):
            raise RuntimeError('NOGROUP')
    assert reap_stale_consumers(Broken([]), STREAM, GROUP, ME) == []


def test_byte_keyed_fields_are_handled():
    """A client without decode_responses returns bytes for names and fields."""
    r = FakeRedis([{b'name': b'old-one', b'pending': 0, b'idle': 9 * HOUR}])
    assert reap_stale_consumers(r, STREAM, GROUP, ME) == ['old-one']
