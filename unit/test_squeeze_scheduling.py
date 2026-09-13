"""An SS must be looked at every bar from the bar it is drawn.

`collect_affected_zones` schedules a zone only when one of its own levels fires.
That is enough for a zone whose whole life is level events; it is not enough for
an SS, for two reasons:

* a **touch** fires no level event, so an in-flight fictional trade would never
  be checked against its target or stop;
* the **invalidation rule is not about its levels at all** — "when the new mth is
  invalid, the squeeze is invalid" (TA, 2026-09-13) — so an *un-entered* SS would
  never be checked either.

The first was fixed on 2026-09-11 by putting entered trades in
`{symbol}:{tf}:squeezes_live`, which `03` reads every bar. The second was not:
a squeeze joined that set only at entry, so it could sit unexamined while the MTH
it tracks went invalid and then be entered as though still alive. **229 of 429
entered trades (53%) on NQU6 15m had their tracked MTH already invalid at entry.**

TA, 2026-09-14, on that: *"Fix this."* So a squeeze now joins the set when it is
drawn. These cases pin the scheduling, not the rules it enables — the rules
themselves are covered in `test_squeeze_invalidation.py`, and they were always
correct. They simply never ran.
"""

import pytest

from mgot_utils.models import Zone
from mgot_utils.processing import squeeze

SYMBOL, TF = 'NQU6', '15m'
D = 900_000
T0 = 1782288000000


class FakeRedis:
    def __init__(self):
        self.h: dict[str, dict[str, str]] = {}
        self.z: dict[str, dict[str, float]] = {}
        self.kv: dict[str, str] = {}
        self._q: list = []

    # -- direct ----------------------------------------------------------
    def hset(self, key, field=None, value=None, mapping=None):
        d = self.h.setdefault(key, {})
        if mapping:
            d.update({k: str(v) for k, v in mapping.items()})
        elif field is not None:
            d[field] = str(value)

    def hgetall(self, key):
        return dict(self.h.get(key, {}))

    def hget(self, key, field):
        return self.h.get(key, {}).get(field)

    def zadd(self, key, mapping):
        self.z.setdefault(key, {}).update(mapping)

    def zcard(self, key):
        return len(self.z.get(key, {}))

    def exists(self, key):
        return 1 if key in self.kv or key in self.h else 0

    def set(self, key, value):
        self.kv[key] = str(value)

    def _bounds(self, lo, hi):
        def b(v, d):
            if isinstance(v, (int, float)):
                return float(v)
            t = str(v).lstrip('(')
            return d if t in ('+inf', '-inf') else float(t)
        return b(lo, float('-inf')), b(hi, float('inf'))

    def zrangebyscore(self, key, lo, hi, start=0, num=None, **kw):
        lo_v, hi_v = self._bounds(lo, hi)
        out = [m for _, m in sorted((s, m) for m, s in self.z.get(key, {}).items()
                                    if lo_v <= s <= hi_v)][start:]
        return out[:num] if num else out

    def zrevrangebyscore(self, key, hi, lo, start=0, num=None, **kw):
        lo_v, hi_v = self._bounds(lo, hi)
        out = [m for _, m in sorted(((s, m) for m, s in self.z.get(key, {}).items()
                                     if lo_v <= s <= hi_v), reverse=True)][start:]
        return out[:num] if num else out

    def zrange(self, key, a, b, **kw):
        return [m for _, m in sorted((s, m) for m, s in self.z.get(key, {}).items())]

    def delete(self, *keys):
        for k in keys:
            self.h.pop(k, None); self.z.pop(k, None); self.kv.pop(k, None)

    def zrem(self, key, *members):
        for m in members:
            self.z.get(key, {}).pop(m, None)

    def expire(self, *a, **kw):
        return True

    # -- pipeline --------------------------------------------------------
    def pipeline(self, transaction=True):
        return FakePipe(self)


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


def a_bar(time: int):
    from mgot_utils.models.bar import Bar
    return Bar(id=f'{SYMBOL}:{TF}:bar:{time}', symbol=SYMBOL, timeframe=TF,
               time=time, open=100.0, high=110.0, low=90.0, close=105.0, volume=1)


def put_leg(r, start: int, direction: int):
    lid = f'{SYMBOL}:{TF}:leg:{start}'
    r.hset(lid, mapping={'id': lid, 'symbol': SYMBOL, 'timeframe': TF,
                         'direction': direction, 'start_time': start,
                         'end_time': start + 100 * D, 'complete': 1,
                         'origin_extreme': 29000.0, 'extreme': 30000.0})
    r.zadd(f'{SYMBOL}:{TF}:legs_index', {lid: start})


def put_prev_mth(r, time: int, direction: int, base_open: float, base_close: float):
    mid = f'{SYMBOL}:{TF}:mth:{time}'
    r.hset(mid, mapping={
        'id': mid, 'symbol': SYMBOL, 'timeframe': TF, 'type': 'mth',
        'direction': direction, 'time': time, 'process_time': time,
        'completion': 'incomplete', 'block_id': '',
        'base_open': base_open, 'base_close': base_close,
        'mth_value': base_close,
    })
    r.zadd(f'{SYMBOL}:{TF}:mth_index', {mid: time})
    # an untested ss_level, so the selector prefers this base
    lid = f'{mid}:ss_level'
    r.hset(lid, mapping={'id': lid, 'zone_id': mid, 'name': 'ss_level',
                         'direction': direction, 'value': base_open,
                         'gains': 0, 'losses': 0, 'tests': 0})
    return mid


def a_source_mth(time: int, direction: int, mth_value: float) -> Zone:
    return Zone.initiate_zone({
        'id': f'{SYMBOL}:{TF}:mth:{time}', 'symbol': SYMBOL, 'timeframe': TF,
        'type': 'mth', 'direction': str(direction), 'time': str(time),
        'process_time': str(time), 'completion': 'incomplete',
        'mth_value': str(mth_value),
    })


@pytest.fixture
def r():
    return FakeRedis()


LIVE = f'{SYMBOL}:{TF}:squeezes_live'


class TestLiveFromTheBarItIsDrawn:
    def test_creation_puts_the_squeeze_in_the_live_set(self, r):
        """Before 2026-09-14 this only happened on entry, so the invalidation
        rule never ran for an SS that had not been entered yet."""
        put_leg(r, 0, 1)
        put_prev_mth(r, T0, 1, base_open=29500.0, base_close=29520.0)
        source = a_source_mth(T0 + 10 * D, 1, mth_value=29800.0)

        made = squeeze.find_secondary_swing(source, a_bar(T0 + 10 * D), r)

        assert made is not None, 'the swing should have been drawn'
        assert made.id in r.z.get(LIVE, {}), (
            'a squeeze must be watched from the bar it is drawn, not from the '
            'bar it is entered'
        )

    def test_a_squeeze_that_is_never_drawn_is_not_watched(self, r):
        """The leg gate rejects this one, so nothing should be scheduled."""
        put_leg(r, 0, 1)
        put_prev_mth(r, T0, 0, base_open=29500.0, base_close=29480.0)
        source = a_source_mth(T0 + 10 * D, 0, mth_value=29200.0)

        assert squeeze.find_secondary_swing(source, a_bar(T0 + 10 * D), r) is None
        assert not r.z.get(LIVE), 'a swing that was never drawn must not be watched'

    def test_the_live_key_matches_the_one_post_process_reads(self, r):
        """Two modules, one set — a mismatch here is silent and total."""
        from mgot_utils.processing import post_process
        put_leg(r, 0, 1)
        put_prev_mth(r, T0, 1, base_open=29500.0, base_close=29520.0)
        source = a_source_mth(T0 + 10 * D, 1, mth_value=29800.0)
        made = squeeze.find_secondary_swing(source, a_bar(T0 + 10 * D), r)
        assert post_process._live_key(made) == LIVE
