"""When a zone's levels come alive, and the blind window that used to precede it.

`process_time` decides when a zone is collected out of `temp_zones` and given
levels. Before that bar it records nothing live; `create_lvls` then replays the
gap, but only when the gap is short enough to fall inside the cap.

The MTH's `process_time` carried `+ config.order` until 2026-09-07, putting it at
`move_end + 3`. That is a three-bar blind spot, not a delay — the two bars after
the move ends, plus the bar the zone is processed on, which creates the levels
but records no gain or loss for itself (only `set_state_from_bar` touches it, and
`update_mth` decides on gains and losses alone). On 15m, 121 MTHs across
production took a close through `block_one` inside that window unseen and still
reached `complete`, a state `update_mth` makes unreachable once `block_one` is
lost.

Measured per zone before the change (`tests/harness/measure_process_time.py`):
every MTH whose outcome moves is that window — 25 corrections and 7 of the mirror
error in July, 17 and 5 in August, nothing unexplained.

`test_a_zone_always_gets_its_levels` guards a separate landmine in the same
function: `create_lvls` used to `return` an empty list when `delta_t <= 0`, under
a message describing the opposite condition — and a zone with no levels can never
complete or invalidate. Nothing reaches it today. It changes no behaviour; it
stops the next `process_time` that lands on the collecting bar from silently
inerting every zone it touches.
"""

import pytest

from mgot_utils.core.configs import Config
from mgot_utils.models import Bar, Move, Zone
from mgot_utils.processing import lvl_preprocessor, zone_preprocessor

SYMBOL, TF = 'BTCUSDT', '15m'
DELTA = 900_000
T0 = 1_784_541_600_000


class FakeRedis:
    """Hashes and sorted sets in memory, with a pipeline that just defers.

    Hand-rolled for the reason `test_ss_level.py` gives: the `redis_client`
    fixture needs a server on 6479 and *skips* without one, and a guard test that
    silently skips guards nothing.
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

    def zadd(self, key, mapping):
        self.z.setdefault(key, {}).update(mapping)

    def zrange(self, key, start, end, withscores=False, **kw):
        items = sorted(self.z.get(key, {}).items(), key=lambda kv: kv[1])
        sl = items[start:] if end == -1 else items[start:end + 1]
        return sl if withscores else [k for k, _ in sl]

    def zrangebyscore(self, key, lo, hi, **kw):
        return [k for k, v in sorted(self.z.get(key, {}).items(), key=lambda kv: kv[1])
                if lo <= v <= hi]

    def pipeline(self):
        return self

    def execute(self):
        return []

    def delete(self, *keys):
        for k in keys:
            self.h.pop(k, None)
            self.z.pop(k, None)


@pytest.fixture
def fake_redis(monkeypatch):
    """Bind every module-scope connection the two preprocessors hold.

    Each module builds its own `r` at import, so patching one leaves the other
    pointing at a real server — the binding-site problem CLAUDE.md lists first.
    """
    fake = FakeRedis()
    monkeypatch.setattr(zone_preprocessor, 'r', fake)
    monkeypatch.setattr(lvl_preprocessor, 'r', fake)
    monkeypatch.setattr(zone_preprocessor, 'apply_htf_links', lambda zone, conn: zone)
    monkeypatch.setattr(zone_preprocessor, 'initiate_base', lambda zone, conn: zone)
    monkeypatch.setattr(zone_preprocessor, 'expand_block', lambda zone, conn: zone)
    return fake


def a_move(length_bar: int, direction: int = 1) -> Move:
    return Move(
        id=f'{SYMBOL}:{TF}:move:{T0}', symbol=SYMBOL, timeframe=TF,
        direction=direction, type='move', time=T0,
        open=60_000.0, high=60_800.0, low=59_900.0, close=60_700.0,
        volume=100.0, length_bar=length_bar, length_perc=1.0,
    )


def a_bar(time: int) -> Bar:
    return Bar(
        id=f'{SYMBOL}:{TF}:bar:{time}', symbol=SYMBOL, timeframe=TF, time=time,
        open=60_700.0, high=60_750.0, low=60_400.0, close=60_450.0,
        volume=10.0, direction=0,
    )


class TestMthProcessTime:
    """The MTH goes live on the bar that ends its move, like an origin."""

    @pytest.mark.parametrize('length_bar', [1, 2, 3, 7])
    def test_process_time_is_the_direction_change_bar(self, fake_redis, length_bar):
        move = a_move(length_bar)
        # `create_mth_zone` is called on the bar whose direction differs, which
        # is the bar after the move's last.
        bar = a_bar(T0 + DELTA * length_bar)

        zone = zone_preprocessor.create_mth_zone(move, bar)

        assert zone.process_time == bar.time

    @pytest.mark.parametrize('length_bar', [1, 2, 3, 7])
    def test_the_blind_window_is_gone(self, fake_redis, length_bar):
        """One bar past move end — no gap for a `block_one` break to hide in.

        This is the whole change. At `+ config.order` the gap was three bars and
        `create_lvls` could not replay it, because `delta_t` came out above the
        cap either way.
        """
        move = a_move(length_bar)
        bar = a_bar(T0 + DELTA * length_bar)

        zone = zone_preprocessor.create_mth_zone(move, bar)

        assert (zone.process_time - zone.move_end_time) // DELTA == 1

    def test_it_matches_the_origin_footing(self, fake_redis):
        """`origins.py` computes move_end + 1 by arithmetic; this uses the bar.

        Same footing, and across a market-profile gap `bar.time` is the more
        correct of the two, because it is an actual bar rather than a delta
        added to one.
        """
        config = Config()
        length_bar = 4
        move = a_move(length_bar)
        bar = a_bar(T0 + DELTA * length_bar)

        zone = zone_preprocessor.create_mth_zone(move, bar)
        origin_form = move.time + (config.delta_epoch[TF] * move.length_bar)

        assert zone.process_time == origin_form


class TestLevelsAlwaysExist:
    """A zone without levels is inert for the rest of its life, and silent."""

    @pytest.mark.parametrize('gap_bars', [-2, -1, 0])
    def test_a_zone_always_gets_its_levels(self, fake_redis, gap_bars):
        """No replay to do is not a reason to withhold the levels.

        `delta_t <= 0` means the zone is collected on or before the bar its
        prices became meaningful — nothing to backfill. It used to return an
        empty list here, which is how the squeeze variant silently inerted 113
        of 160 zones.
        """
        zone = Zone(
            id=f'{SYMBOL}:{TF}:mth:{T0}', type='mth', symbol=SYMBOL, timeframe=TF,
            direction=1, completion='incomplete', time=T0,
            process_time=T0 + DELTA, move_end_time=T0 + DELTA * 4,
            block_zero=60_000.0, block_zero_id=f'{SYMBOL}:{TF}:mth:{T0}:block_zero',
            block_one=60_700.0, block_one_id=f'{SYMBOL}:{TF}:mth:{T0}:block_one',
            mth_value=60_000.0, move_id=f'{SYMBOL}:{TF}:move:{T0}',
        )
        row = a_bar(zone.move_end_time + DELTA * gap_bars)

        levels = lvl_preprocessor.create_lvls(zone, row)

        names = {lvl.name for lvl in levels}
        assert 'block_zero' in names and 'block_one' in names

    def test_a_long_gap_still_gets_levels_but_no_replay(self, fake_redis, monkeypatch):
        """Above the cap the replay is skipped — the levels are not."""
        called = []
        monkeypatch.setattr(lvl_preprocessor, 'retrieve_window',
                            lambda row, n: called.append(n) or [])
        zone = Zone(
            id=f'{SYMBOL}:{TF}:squeeze:{T0}', type='squeeze', symbol=SYMBOL,
            timeframe=TF, direction=1, completion='incomplete', time=T0,
            process_time=T0 + DELTA * 40, move_end_time=T0 + DELTA * 40,
            block_zero=60_000.0, block_zero_id=f'{SYMBOL}:{TF}:squeeze:{T0}:block_zero',
            block_one=60_700.0, block_one_id=f'{SYMBOL}:{TF}:squeeze:{T0}:block_one',
            mth_value=60_000.0, move_id=f'{SYMBOL}:{TF}:move:{T0}',
        )
        row = a_bar(T0 + DELTA * 40)

        levels = lvl_preprocessor.create_lvls(zone, row)

        assert called == [], 'a gap above the cap must not be replayed'
        assert {lvl.name for lvl in levels} >= {'block_zero', 'block_one'}
