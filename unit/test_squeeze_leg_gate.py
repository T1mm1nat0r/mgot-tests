"""An SS may only be initiated by an MTH travelling with its own Leg.

Until 2026-09-13 `find_secondary_swing` never looked at the leg. It paired a new
MTH with the most recent same-direction MTH whose base was untested, walking back
up to `config.ss_untested_lookback` candidates with no regard for whether the two
belonged to the same trend at all.

NQU6 case 08 is the worked example. A 28-point bearish MTH at 29 Jun 18:15 sat
inside the bullish leg 25 Jun 14:00 -> 29 Jun 18:45 (29316.00 -> 30053.50) and
was paired with a bearish base from 25 Jun 13:00 — one whole leg and four days
earlier. What that draws is a 9-point base band with a target line near it and no
swing between them:

    > "The bearish mth you identified clearly had no previous mth. If the leg was
    >  checked this mth was part of, it would have been visible that this mth was
    >  in a bullish leg. In a bullish leg, only look for bullish mth's as squeeze
    >  initiators." — TA, 2026-09-13

Measured read-only before wiring, on stored state: the gate removes 110 of 684
squeezes on NQU6 15m (16.1%) and 130 of 1003 on BTCUSDT 15m (13.0%). The gate is
a pure per-zone filter — removing one squeeze cannot change another, since
creation reads only the MTH chain — so those counts are exact, not estimates.

Deliberately **not** asserted here: that the survivors trade better. They do not
(69.8% against 81.2% for the removed, on NQU6). That metric is the mirrored 1:1
stop measuring itself — a matched random wick entry scores ~71% — so it cannot
adjudicate a definitional question. See business_rules_squeezes.md.

Full creation is exercised by the harness, not here; these cases pin the gate.
"""

import pytest

from mgot_utils.models import Zone
from mgot_utils.processing import squeeze

SYMBOL, TF = 'NQU6', '15m'
D = 900_000


class FakeRedis:
    """Enough for `leg_at_time`: a backwards-walkable sorted set plus hashes.

    `exists` / `set` / `zcard` are here only so `_bootstrap_legs` can decline
    cleanly on the no-chain case rather than raising.
    """

    def __init__(self):
        self.h: dict[str, dict[str, str]] = {}
        self.z: dict[str, dict[str, float]] = {}
        self.kv: dict[str, str] = {}

    def hset(self, key, field=None, value=None, mapping=None):
        d = self.h.setdefault(key, {})
        if mapping:
            d.update({k: str(v) for k, v in mapping.items()})
        elif field is not None:
            d[field] = str(value)

    def hgetall(self, key):
        return dict(self.h.get(key, {}))

    def zadd(self, key, mapping):
        self.z.setdefault(key, {}).update(mapping)

    def zrevrangebyscore(self, key, hi, lo, start=0, num=None, **kw):
        def bound(v, default):
            # `leg_at_time` passes the time as an int and the far end as the
            # string '-inf'. An earlier version of this helper only unpacked
            # strings, so the int upper bound silently became +inf and every
            # lookup returned the newest leg regardless of when it was asked
            # about. The multi-leg case below is what caught it.
            if isinstance(v, (int, float)):
                return float(v)
            text = str(v).lstrip('(')
            return default if text in ('+inf', '-inf') else float(text)
        hi_v = bound(hi, float('inf'))
        lo_v = bound(lo, float('-inf'))
        items = sorted(((s, m) for m, s in self.z.get(key, {}).items()
                        if lo_v <= s <= hi_v), reverse=True)
        out = [m for _, m in items][start:]
        return out[:num] if num else out

    def exists(self, key):
        return 1 if key in self.kv else 0

    def set(self, key, value):
        self.kv[key] = str(value)

    def zcard(self, key):
        return len(self.z.get(key, {}))


def put_leg(r, start: int, end: int, direction: int,
            origin_extreme: float = 29316.0, extreme: float = 30053.5):
    lid = f'{SYMBOL}:{TF}:leg:{start}'
    r.hset(lid, mapping={
        'id': lid, 'symbol': SYMBOL, 'timeframe': TF, 'direction': direction,
        'start_time': start, 'end_time': end, 'complete': 1,
        'origin_extreme': origin_extreme, 'extreme': extreme,
    })
    r.zadd(f'{SYMBOL}:{TF}:legs_index', {lid: start})
    return lid


def an_mth(time: int, direction: int, mth_value: float = 30020.5) -> Zone:
    return Zone.initiate_zone({
        'id': f'{SYMBOL}:{TF}:mth:{time}', 'symbol': SYMBOL, 'timeframe': TF,
        'type': 'mth', 'direction': str(direction), 'time': str(time),
        'process_time': str(time), 'completion': 'incomplete',
        'mth_value': str(mth_value),
    })


@pytest.fixture
def r():
    return FakeRedis()


class TestTheGate:
    @pytest.mark.parametrize('direction', [0, 1])
    def test_an_mth_with_its_leg_initiates(self, r, direction):
        """The trend-defining case: the MTH's own move is what the leg is."""
        put_leg(r, 0, 100 * D, direction)
        assert squeeze._with_leg(an_mth(50 * D, direction), r) is True

    @pytest.mark.parametrize('direction', [0, 1])
    def test_an_mth_against_its_leg_does_not(self, r, direction):
        """A pullback inside the leg. Both directions — this is where an
        inverted comparison would still look right on one side."""
        put_leg(r, 0, 100 * D, direction)
        assert squeeze._with_leg(an_mth(50 * D, 1 - direction), r) is False

    def test_no_leg_chain_fails_open(self, r):
        """Early history has no chain. A gate that closed here would delete
        every squeeze before the first leg and read as a clean zero."""
        assert squeeze._with_leg(an_mth(50 * D, 1), r) is True

    def test_the_leg_running_at_the_mth_is_the_one_asked(self, r):
        """Not the latest leg overall — the last one started by the MTH's time."""
        put_leg(r, 0, 40 * D, 0)
        put_leg(r, 40 * D, 100 * D, 1)
        assert squeeze._with_leg(an_mth(20 * D, 0), r) is True
        assert squeeze._with_leg(an_mth(20 * D, 1), r) is False
        assert squeeze._with_leg(an_mth(60 * D, 1), r) is True
        assert squeeze._with_leg(an_mth(60 * D, 0), r) is False


class TestCaseZeroEight:
    """The real NQU6 numbers that produced the rule."""

    BULL_LEG_START = 1782396000000   # 25 Jun 14:00
    BULL_LEG_END = 1782758700000     # 29 Jun 18:45
    BEAR_BLIP = 1782757500000        # 29 Jun 18:15, a 28-point bearish MTH
    USERS_SOURCE = 1782432900000     # 26 Jun 00:15, a 502-point bearish MTH

    def test_the_counter_trend_blip_is_rejected(self, r):
        put_leg(r, self.BULL_LEG_START, self.BULL_LEG_END, 1)
        assert squeeze._with_leg(an_mth(self.BEAR_BLIP, 0), r) is False, (
            'a 28-point bearish MTH inside a four-day bullish leg is a '
            'pullback, not a swing initiator'
        )

    def test_the_pairing_ta_drew_is_also_inside_that_bullish_leg(self, r):
        """Honest about the cost: TA's own reading of case 08 paired the base
        with 26 Jun 00:15, and that MTH sits in the same bullish leg, so this
        gate rejects it too. The gate settles which MTHs may initiate; it does
        not by itself produce the swing TA drew. Recorded so the next reader
        does not assume it did."""
        put_leg(r, self.BULL_LEG_START, self.BULL_LEG_END, 1)
        assert squeeze._with_leg(an_mth(self.USERS_SOURCE, 0), r) is False


class TestItStopsCreation:
    def test_find_secondary_swing_returns_none_and_writes_nothing(self, r):
        put_leg(r, 0, 100 * D, 1)
        zone = an_mth(50 * D, 0)
        assert squeeze.find_secondary_swing(zone, None, r) is None
        assert not [k for k in r.h if ':squeeze:' in k]
        assert not r.z.get(f'{SYMBOL}:{TF}:zones_index')
