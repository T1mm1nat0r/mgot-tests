"""What is allowed to kill an SS.

Until 2026-09-11 a squeeze was invalidated by its own `block_one`, and that was
the dominant cause of squeeze mortality — 530 of 662 deaths on NQU6 15m. It was
not reading an event. `Bar.identify_achievements` selects levels by a bare
position test against the close::

    zrangebyscore(to_gain, 0, close)     # every level below the close: "gained"
    zrangebyscore(to_lose, close, +inf)  # every level above the close: "lost"

No crossing, no proximity. An MTH survives that because it is drawn at the
extreme just printed, so its levels sit on top of the close. An SS is drawn at a
remote historical base, so the same test is true from the moment the zone
exists: on 660 of 662 deaths price was already past the level, a median of 134
points away, against an 85-point average bar. The zone was being killed for the
one thing that is always true of a zone you have just drawn and are waiting
on — that price has not come back to it yet.

What replaced it (TA, 2026-09-11) is a **trade**, not a zone status:

    entry       price touches the base
    taken_out   the base is achieved — two closes through it
    complete    after entry, price reaches the triggering MTH's move open
    invalid     the MTH the SS tracks is invalidated

These tests pin both halves — the removal and the replacement — because the
suite previously had no coverage of squeeze completion or invalidation at all,
which is how the position test survived this long.

The two-closes count is kept on the zone rather than read off
`Level.conseq_gain/conseq_loss`: those cannot reach 2 for a squeeze (0 of 1368
measured), because `03` alternates a level between the to_gain/to_lose queues on
every event. `387dcf9` raised that and left it alone deliberately.
"""

import pytest

from mgot_utils.models import Zone
from mgot_utils.models.level import Level
from mgot_utils.processing import post_process

SYMBOL, TF = 'NQU6', '15m'
D = 900_000
T0 = 1782288000000


class FakeRedis:
    """Hashes plus a sorted set that can be walked backwards.

    `zrevrangebyscore` is the one the real code needs here and the reason this
    is not the `FakeRedis` in `test_process_time.py`, which lacks it.
    """

    def __init__(self):
        self.h: dict[str, dict[str, str]] = {}
        self.z: dict[str, dict[str, float]] = {}

    def hset(self, key, field=None, value=None, mapping=None):
        d = self.h.setdefault(key, {})
        if mapping:
            d.update({k: str(v) for k, v in mapping.items()})
        if field is not None:
            d[field] = str(value)

    def hgetall(self, key):
        return dict(self.h.get(key, {}))

    def hget(self, key, field):
        return self.h.get(key, {}).get(field)

    def zadd(self, key, mapping):
        self.z.setdefault(key, {}).update(mapping)

    def zrevrangebyscore(self, key, hi, lo, start=0, num=None, **kw):
        lo = float('-inf') if lo == '-inf' else float(lo)
        hi = float('inf') if hi == '+inf' else float(hi)
        items = [k for k, v in sorted(self.z.get(key, {}).items(),
                                      key=lambda kv: -kv[1]) if lo <= v <= hi]
        return items[start:start + num] if num else items[start:]

    def zrangebyscore(self, key, lo, hi, start=0, num=None, **kw):
        lo = float('-inf') if str(lo) == '-inf' else float(str(lo).lstrip('('))
        hi = float('inf') if str(hi) == '+inf' else float(str(hi).lstrip('('))
        items = [k for k, v in sorted(self.z.get(key, {}).items(),
                                      key=lambda kv: kv[1]) if lo <= v <= hi]
        return items[start:start + num] if num else items[start:]

    def pipeline(self, transaction=True):
        # Must buffer: `_latest_same_direction_mth` queues its hgetalls and then
        # reads them off `execute()`. A pipeline that returns [] makes the walk
        # silently find nothing, which is indistinguishable from "no MTH".
        return FakePipe(self)

    def hdel(self, *a, **kw):
        pass

    def zrem(self, *a, **kw):
        pass

    def srem(self, *a, **kw):
        pass

    def sadd(self, *a, **kw):
        pass

    def expire(self, *a, **kw):
        pass


class FakePipe:
    def __init__(self, conn):
        self.conn = conn
        self.queued = []

    def __getattr__(self, name):
        def queue(*args, **kwargs):
            self.queued.append((name, args, kwargs))
            return self
        return queue

    def execute(self):
        out = [getattr(self.conn, n)(*a, **k) for n, a, k in self.queued]
        self.queued.clear()
        return out


def a_bar(time: int, close: float, high: float = None, low: float = None):
    from mgot_utils.models.bar import Bar
    return Bar(id=f'{SYMBOL}:{TF}:bar:{time}', symbol=SYMBOL, timeframe=TF,
               time=time, open=close,
               high=close + 10 if high is None else high,
               low=close - 10 if low is None else low,
               close=close, volume=1)


def a_squeeze(direction: int = 1) -> Zone:
    """An SS band at 29668.25 - 29686.50 — the worked example from the finding."""
    return Zone.initiate_zone({
        'id': f'{SYMBOL}:{TF}:squeeze:{T0}', 'symbol': SYMBOL, 'timeframe': TF,
        'type': 'squeeze', 'direction': str(direction), 'time': str(T0),
        'process_time': str(T0 + D), 'completion': 'incomplete',
        'block_zero': '29686.5', 'block_one': '29668.25',
    })


def lvls_reading_achieved(zone: Zone) -> list[Level]:
    """block_zero untouched, block_one showing the spurious position 'achievement'.

    dir=1 invalidated on block_one GAINED, dir=0 on block_one LOST — so set
    whichever counter the old rule keyed on.
    """
    zero = Level(id=f'{zone.id}:block_zero', zone_id=zone.id, name='block_zero',
                 direction=zone.direction, value=29686.5)
    one = Level(id=f'{zone.id}:block_one', zone_id=zone.id, name='block_one',
                direction=zone.direction, value=29668.25)
    if zone.direction == 1:
        one.gains = 1
    else:
        one.losses = 1
    return [zero, one]


def put_mth(r, time: int, direction: int, completion: str, block_id: str = '',
            mth_value: float = 0.0):
    mid = f'{SYMBOL}:{TF}:mth:{time}'
    r.hset(mid, mapping={
        'id': mid, 'symbol': SYMBOL, 'timeframe': TF, 'type': 'mth',
        'direction': direction, 'time': time, 'process_time': time,
        'completion': completion, 'block_id': block_id,
        'mth_value': mth_value,
    })
    r.zadd(f'{SYMBOL}:{TF}:mth_index', {mid: time})
    return mid


@pytest.fixture
def r(monkeypatch):
    fake = FakeRedis()
    # The SS-vs-trend rule is a separate concern with its own measurement; keep
    # it out of these cases so a failure here names the rule under test.
    monkeypatch.setattr(
        'mgot_utils.processing.ss_invalidation.trend_confirmed_against',
        lambda squeeze, conn: None)
    return fake


class TestPriceSittingAwayFromTheZoneDoesNotKillIt:
    """The defect that was removed. Both directions, because it inverted."""

    @pytest.mark.parametrize('direction', [0, 1])
    def test_block_one_no_longer_invalidates(self, r, direction):
        zone = a_squeeze(direction)
        put_mth(r, T0 - D, direction, 'complete')
        post_process.update_squeeze(a_bar(T0 + D, 29836.25), zone,
                                    lvls(zone), r)
        assert zone.completion == 'incomplete', (
            'an SS must survive price simply being away from it — that is the '
            'state it is drawn in')


BASE = 29686.5          # a_squeeze's block_zero
UP_TARGET = 29800.0     # above the base, for direction=1
DOWN_TARGET = 29600.0   # below the base, for direction=0


def a_trade(r, direction: int, target: float = None):
    """A squeeze carrying its trade, as `find_secondary_swing` builds it."""
    from mgot_utils.models import SqueezeZone
    if target is None:
        target = UP_TARGET if direction == 1 else DOWN_TARGET
    zone = a_squeeze(direction)
    zone.target = target
    zone.trade_invalidation = SqueezeZone.mirror(zone.base, target)
    put_mth(r, T0 - D, direction, 'complete', mth_value=target - 500)
    put_mth(r, T0, direction, 'complete', mth_value=target)   # the source MTH
    return zone, target


def lvls(zone):
    return lvls_reading_achieved(zone)


def enter(r, zone, direction, t=T0 + D):
    """A bar that touches the base without closing through it."""
    bar = (a_bar(t, BASE + 5, low=BASE - 1) if direction == 1
           else a_bar(t, BASE - 5, high=BASE + 1))
    post_process.update_squeeze(bar, zone, lvls(zone), r)
    return bar


class TestEntry:
    """A touch of the base arms the trade. It is not a completion state."""

    @pytest.mark.parametrize('direction', [0, 1])
    def test_touching_the_base_records_entry_and_stays_incomplete(self, r, direction):
        zone, _ = a_trade(r, direction)
        enter(r, zone, direction)
        assert zone.base_touched_time == T0 + D
        assert zone.completion == 'incomplete', (
            'entry is not a completion state — an SS that has been touched but '
            'has gone nowhere is still incomplete')

    @pytest.mark.parametrize('direction', [0, 1])
    def test_price_away_from_the_base_is_not_entry(self, r, direction):
        zone, _ = a_trade(r, direction)
        far = BASE + 300 if direction == 1 else BASE - 300
        post_process.update_squeeze(a_bar(T0 + D, far), zone,
                                    lvls(zone), r)
        assert not zone.has_entered
        assert zone.completion == 'incomplete'


class TestStop:
    """taken_out = price touched the trade invalidation level.

    That level is the target mirrored through the entry, so the trade is 1:1.
    TA, 2026-09-13: "If the source mth move open is at 1 dollar, entry is at 5
    dollar, then the invalidation price is at 9."
    """

    def test_ta_worked_example(self):
        from mgot_utils.models import SqueezeZone
        assert SqueezeZone.mirror(entry=5, target=1) == 9

    @pytest.mark.parametrize('direction', [0, 1])
    def test_touching_the_invalidation_level_stops_it_out(self, r, direction):
        zone, _ = a_trade(r, direction)
        enter(r, zone, direction)
        stop = zone.trade_invalidation
        bar = (a_bar(T0 + 2 * D, stop + 30, low=stop - 1) if direction == 1
               else a_bar(T0 + 2 * D, stop - 30, high=stop + 1))
        post_process.update_squeeze(bar, zone, lvls(zone), r)
        assert zone.completion == 'taken_out'

    @pytest.mark.parametrize('direction', [0, 1])
    def test_one_tick_short_of_the_stop_survives(self, r, direction):
        zone, _ = a_trade(r, direction)
        enter(r, zone, direction)
        stop = zone.trade_invalidation
        bar = (a_bar(T0 + 2 * D, stop + 30, low=stop + 0.1) if direction == 1
               else a_bar(T0 + 2 * D, stop - 30, high=stop - 0.1))
        post_process.update_squeeze(bar, zone, lvls(zone), r)
        assert zone.completion == 'incomplete'

    @pytest.mark.parametrize('direction', [0, 1])
    def test_closes_through_the_base_no_longer_stop_it(self, r, direction):
        """The two-closes rule was superseded on 2026-09-13. Price can sit well
        past the base without the trade being stopped — only the invalidation
        level ends it."""
        zone, _ = a_trade(r, direction)
        enter(r, zone, direction)
        through = BASE - 20 if direction == 1 else BASE + 20
        for k in (2, 3, 4):
            post_process.update_squeeze(a_bar(T0 + k * D, through), zone, lvls(zone), r)
        assert zone.completion == 'incomplete'


class TestMinimumTradeSize:
    """A target too close to the base is not a trade (TA, 2026-09-13).

    The zone is still drawn and can still be invalidated — TA chose that over
    dropping it, so S4 keeps both populations.
    """

    def test_below_the_minimum_no_trade_is_entered(self, r):
        zone, _ = a_trade(r, 1, target=BASE + 5)     # NQ minimum is 20 points
        enter(r, zone, 1)
        assert zone.tradeable == 0
        assert not zone.has_entered

    def test_at_the_minimum_the_trade_is_entered(self, r):
        zone, _ = a_trade(r, 1, target=BASE + 20)
        enter(r, zone, 1)
        assert zone.tradeable == 1
        assert zone.has_entered

    def test_an_untradeable_squeeze_can_never_complete(self, r):
        zone, target = a_trade(r, 1, target=BASE + 5)
        enter(r, zone, 1)
        post_process.update_squeeze(a_bar(T0 + 2 * D, target + 50, high=target + 60),
                                    zone, lvls(zone), r)
        assert zone.completion == 'incomplete'

    def test_it_is_still_invalidated_by_its_mth(self, r):
        zone, _ = a_trade(r, 1, target=BASE + 5)
        put_mth(r, T0 + D, 1, 'invalid', mth_value=BASE + 900)
        post_process.update_squeeze(a_bar(T0 + 2 * D, BASE + 50), zone, lvls(zone), r)
        assert zone.completion == 'invalid'


class TestTarget:
    """complete = after entry, price reaches the triggering MTH's move open."""

    @pytest.mark.parametrize('direction', [0, 1])
    def test_reaching_the_target_after_entry_completes(self, r, direction):
        zone, target = a_trade(r, direction)
        enter(r, zone, direction)
        bar = (a_bar(T0 + 2 * D, target - 5, high=target + 1) if direction == 1
               else a_bar(T0 + 2 * D, target + 5, low=target - 1))
        post_process.update_squeeze(bar, zone, lvls(zone), r)
        assert zone.completion == 'complete'
        assert zone.was_completed

    @pytest.mark.parametrize('direction', [0, 1])
    def test_reaching_the_target_without_entry_does_nothing(self, r, direction):
        """"after touching the secondary swing base" — the order is the rule.
        Price at the target having never visited the base is not a trade."""
        zone, target = a_trade(r, direction)
        bar = (a_bar(T0 + D, target - 5, high=target + 1) if direction == 1
               else a_bar(T0 + D, target + 5, low=target - 1))
        post_process.update_squeeze(bar, zone, lvls(zone), r)
        assert zone.completion == 'incomplete'
        assert not zone.was_completed

    def test_the_target_is_the_source_mth_not_the_previous_one(self, r):
        """Resolved by TA on 2026-09-11 via /boards/ss-target."""
        zone, target = a_trade(r, 1)
        assert zone.trigger_mth_id == f'{SYMBOL}:{TF}:mth:{T0}'
        assert zone.target == target
        assert target != float(zone.block_one), (
            'block_one is the *previous* MTH move open — the rejected candidate')


class TestTrackedMth:
    """The SS hands its fate to whichever MTH carried the trend furthest."""

    def test_invalidated_when_the_tracked_mth_is_invalidated(self, r):
        zone, target = a_trade(r, 1)
        put_mth(r, T0 + D, 1, 'invalid', mth_value=target + 200)  # supersedes
        post_process.update_squeeze(a_bar(T0 + 2 * D, BASE + 50), zone,
                                    lvls(zone), r)
        assert zone.completion == 'invalid'

    def test_a_new_mth_short_of_the_trigger_does_not_take_over(self, r):
        """It has to *exceed* the trigger's value to become the one that counts."""
        zone, target = a_trade(r, 1)
        put_mth(r, T0 + D, 1, 'invalid', mth_value=target - 200)
        post_process.update_squeeze(a_bar(T0 + 2 * D, BASE + 50), zone,
                                    lvls(zone), r)
        assert zone.completion == 'incomplete'

    def test_direction_sense_inverts(self, r):
        """For a down-trading SS, "exceeds" means a *lower* value."""
        zone, target = a_trade(r, 0)
        put_mth(r, T0 + D, 0, 'invalid', mth_value=target - 200)   # further down
        post_process.update_squeeze(a_bar(T0 + 2 * D, BASE - 50), zone,
                                    lvls(zone), r)
        assert zone.completion == 'invalid'
