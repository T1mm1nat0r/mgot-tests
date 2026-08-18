"""
Which zone does "the job of the MTH" — Adv 3.9.

    "what happens when we lose the MTH and the SS of that MTH? In that scenario,
     the SS takes over the job of the MTH. If we reject the bottom side of the SS
     (without achieving it twice), we still have the higher probability of making
     new lows."

Three things here are easy to get subtly wrong, and each has a test that fails
loudly rather than producing a plausible wrong answer:

* **The handover condition is "and", not "or".** Losing the MTH alone does not
  hand the job to the SS; the course says "we lose the MTH *and* the SS".
* **"Bottomside" flips with direction.** It is the bottom of a bullish MTH and
  the top of a bearish MTL, and `block_zero` already flips too — so reading
  block_zero as "the low" is right half the time.
* **Rejection needs a test.** Untested and rejected are different claims. Merging
  them reports a failing dance on every zone price never came back to.
"""

import pytest

from mgot_utils.models import Zone
from mgot_utils.processing import active_reference as ar


def make_zone(ztype='mth', direction=1, completion='complete',
              block_zero=49000.0, block_one=50000.0, time=1700000000000,
              zone_tests=0, complete_zone_tests=0):
    return Zone(
        id=f'BTCUSDT:15m:{ztype}:{time}', symbol='BTCUSDT', timeframe='15m',
        type=ztype, direction=direction, completion=completion,
        time=time, process_time=time + 900000,
        block_zero=block_zero, block_one=block_one,
        zone_tests=zone_tests, complete_zone_tests=complete_zone_tests,
    )


class FakeLevel:
    def __init__(self, value, conseq_gain=0, conseq_loss=0):
        self.value, self.conseq_gain, self.conseq_loss = value, conseq_gain, conseq_loss


# ── bottomside ───────────────────────────────────────────────────

def test_bottomside_is_the_low_for_a_bullish_zone():
    assert ar.bottomside(make_zone(direction=1, block_zero=49000.0, block_one=50000.0)) == 49000.0


def test_bottomside_is_the_high_for_a_bearish_zone():
    assert ar.bottomside(make_zone(direction=0, block_zero=50000.0, block_one=49000.0)) == 50000.0


def test_bottomside_ignores_which_block_field_holds_it():
    """block_zero flips with direction; the answer must not."""
    a = ar.bottomside(make_zone(direction=1, block_zero=49000.0, block_one=50000.0))
    b = ar.bottomside(make_zone(direction=1, block_zero=50000.0, block_one=49000.0))
    assert a == b == 49000.0


# ── the handover ─────────────────────────────────────────────────

def test_holding_mth_keeps_the_job():
    ref = ar.resolve(make_zone(completion='complete'), None)
    assert ref['role'] == 'mth' and ref['handed_over'] == 0


def test_lost_mth_with_holding_ss_keeps_the_job():
    """"we lose the MTH AND the SS" — one is not enough."""
    mth = make_zone(completion='taken_out')
    ss = make_zone(ztype='squeeze', completion='complete')
    ref = ar.resolve(mth, ss)
    assert ref['role'] == 'mth', 'handed over on the MTH alone; the condition is "and"'
    assert ref['handed_over'] == 0


def test_both_lost_hands_the_job_to_the_ss():
    mth = make_zone(completion='taken_out')
    ss = make_zone(ztype='squeeze', completion='taken_out',
                   block_zero=47000.0, block_one=48000.0)
    ref = ar.resolve(mth, ss)
    assert ref['role'] == 'ss' and ref['handed_over'] == 1
    assert ref['zone_id'] == ss.id
    assert ref['level'] == 47000.0, 'the level must move to the SS bottomside'


def test_lost_mth_with_no_ss_keeps_the_job():
    ref = ar.resolve(make_zone(completion='taken_out'), None)
    assert ref['role'] == 'mth'


def test_no_mth_means_no_reference():
    assert ar.resolve(None, make_zone(ztype='squeeze')) is None


@pytest.mark.parametrize('completion', ['incomplete', 'complete', 'invalid', 'absorbed'])
def test_only_taken_out_counts_as_lost(completion):
    """`invalid` never became structure, so it cannot hand its job over."""
    assert ar.is_lost(make_zone(completion=completion)) is False


def test_taken_out_is_lost():
    assert ar.is_lost(make_zone(completion='taken_out')) is True


# ── the verdict ──────────────────────────────────────────────────

def test_two_closes_through_a_bullish_bottomside_is_achieved():
    zone = make_zone(direction=1, block_zero=49000.0, block_one=50000.0, zone_tests=1)
    levels = [FakeLevel(49000.0, conseq_loss=2), FakeLevel(50000.0)]
    assert ar.verdict(zone, levels) == 'achieved'


def test_one_close_is_not_achievement():
    zone = make_zone(direction=1, block_zero=49000.0, block_one=50000.0, zone_tests=1)
    levels = [FakeLevel(49000.0, conseq_loss=1), FakeLevel(50000.0)]
    assert ar.verdict(zone, levels) == 'rejected'


def test_bearish_achievement_reads_gains_not_losses():
    """Closing *above* a bearish MTL's top is the achievement. Reading losses
    here would report every bearish reference as never achieved."""
    zone = make_zone(direction=0, block_zero=50000.0, block_one=49000.0, zone_tests=1)
    assert ar.verdict(zone, [FakeLevel(50000.0, conseq_gain=2)]) == 'achieved'
    assert ar.verdict(zone, [FakeLevel(50000.0, conseq_loss=2)]) == 'rejected'


def test_tested_without_achievement_is_rejected():
    zone = make_zone(direction=1, zone_tests=2)
    assert ar.verdict(zone, [FakeLevel(49000.0)]) == 'rejected'


def test_never_tested_is_untested_not_rejected():
    zone = make_zone(direction=1, zone_tests=0, complete_zone_tests=0)
    assert ar.verdict(zone, [FakeLevel(49000.0)]) == 'untested'


def test_a_test_preserved_across_takeout_still_counts():
    """zone_tests resets on the phase roll; complete_zone_tests keeps it."""
    zone = make_zone(direction=1, zone_tests=0, complete_zone_tests=3)
    assert ar.verdict(zone, [FakeLevel(49000.0)]) == 'rejected'


def test_missing_bottomside_level_is_not_an_achievement():
    zone = make_zone(direction=1, zone_tests=1)
    assert ar.verdict(zone, [FakeLevel(50000.0, conseq_loss=5)]) == 'rejected'


# ── resolving the current MTH from live state ────────────────────

class FakeRedis:
    """Hashes, one sorted set, and a string space — enough for `current`."""

    def __init__(self):
        self.hashes, self.zsets, self.strings, self.levels = {}, {}, {}, {}

    def get(self, key):
        return self.strings.get(key)

    def set(self, key, value):
        self.strings[key] = str(value)

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, mapping=None, **kw):
        self.hashes.setdefault(key, {}).update(
            {k: str(v) for k, v in (mapping or {}).items()})

    def zadd(self, key, mapping):
        self.zsets.setdefault(key, {}).update(mapping)

    def zrevrangebyscore(self, key, hi, lo, start=0, num=None):
        items = sorted(self.zsets.get(key, {}).items(),
                       key=lambda kv: kv[1], reverse=True)
        out = [m for m, _ in items]
        return out[start:start + num] if num else out[start:]

    def pipeline(self):
        return self

    def execute(self):
        out, self._q = getattr(self, '_q', []), []
        return out


def _put(r, zone):
    r.hset(zone.id, mapping=zone.model_dump(exclude_none=True, mode='json'))
    if zone.type == 'mth':
        r.zadd(f'{zone.symbol}:{zone.timeframe}:mth_index', {zone.id: zone.time})


class QueueingFake(FakeRedis):
    """`current` fetches through a pipeline; queue hgetall and replay on execute."""

    def __init__(self):
        super().__init__()
        self._q = []

    def hgetall(self, key):
        if getattr(self, '_piping', False):
            self._q.append(dict(self.hashes.get(key, {})))
            return None
        return dict(self.hashes.get(key, {}))

    def pipeline(self):
        self._piping = True
        return self

    def execute(self):
        out, self._q = self._q, []
        self._piping = False
        return out


def test_latest_mth_ignores_the_last_mth_key():
    """`last_mth:{dir}` holds bar.time, not the zone's time.

    Reading it as a zone time names a hash that does not exist — verified
    against live state on 2026-08-18. Pinned here because the dance uses that
    key only to watch for *movement*, so the mismatch is invisible there and
    would be re-introduced by anyone reasoning from the dance's usage.
    """
    r = QueueingFake()
    newest = make_zone(direction=1, time=1700000900000, completion='complete')
    _put(r, newest)
    r.set('BTCUSDT:15m:last_mth:1', 1700009999999)  # a bar time, matching nothing

    got = ar.latest_mth('BTCUSDT', '15m', 1, r)
    assert got is not None, 'resolved through last_mth and found nothing'
    assert got.id == newest.id


def test_latest_mth_picks_the_newest_of_the_right_direction():
    """Directions interleave, so the newest overall is often the wrong one."""
    r = QueueingFake()
    _put(r, make_zone(direction=1, time=1700000000000, completion='complete'))
    _put(r, make_zone(direction=0, time=1700000900000, completion='complete'))
    wanted = make_zone(direction=1, time=1700000450000, completion='complete')
    _put(r, wanted)

    got = ar.latest_mth('BTCUSDT', '15m', 1, r)
    assert got.id == wanted.id


def test_latest_mth_skips_incomplete_zones():
    """An incomplete MTH never became structure and has no job to hand over."""
    r = QueueingFake()
    _put(r, make_zone(direction=1, time=1700000900000, completion='incomplete'))
    older = make_zone(direction=1, time=1700000000000, completion='complete')
    _put(r, older)

    assert ar.latest_mth('BTCUSDT', '15m', 1, r).id == older.id


def test_latest_mth_returns_none_on_an_empty_index():
    assert ar.latest_mth('BTCUSDT', '15m', 1, QueueingFake()) is None
