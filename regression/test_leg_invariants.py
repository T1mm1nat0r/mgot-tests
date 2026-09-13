"""
A leg must not end before it begins.

Shipped broken on 2026-08-17 by "anchor a Leg from a secondary origin to the
original MTH". Resolving a chained origin's anchor to the original MTH was right
for the *price* — the chain has been retrying that structure all along — but the
same resolution was applied to the *turning point*, and the original MTH is by
definition earlier in time. So a leg whose end origin was chained received an end
anchor before its own start.

Measured before the fix: 24 of 88 legs on 15m and 198 of 1129 on 1m had
`end_time < start_time`. Because `legs_index` is scored by `start_time`, those
legs also sorted into the wrong place — 53 of 87 adjacencies on 15m no longer
followed the chain — which is what made legs render as if split into several.

Nothing raised. A negative-width leg is a perfectly good hash.
"""

import pytest

from mgot_utils.models import Zone
from mgot_utils.processing import legs


def _origin(oid, direction, mth_time, mth_value, og_mth_value=0.0, og_move=''):
    return Zone(
        id=f'BTCUSDT:15m:origin:{oid}', symbol='BTCUSDT', timeframe='15m',
        type='origin', direction=direction, completion='complete',
        time=oid, process_time=oid + 900000,
        block_zero=mth_value - 50, block_one=mth_value,
        mth_value=mth_value, og_mth_value=og_mth_value,
        mth_move_id=f'BTCUSDT:15m:move:{mth_time}',
    )


class FakeRedis:
    """Serves move hashes for `turning_points`, through a pipeline."""

    def __init__(self, moves):
        self.moves = moves
        self._q = []

    def hmget(self, key, *fields):
        self._q.append([self.moves.get(key, {}).get(f) for f in fields])
        return None

    def pipeline(self):
        return self

    def execute(self):
        out, self._q = self._q, []
        return out


def test_turning_point_uses_the_origins_own_move_not_the_chained_original():
    """The chained original is earlier; following it backwards is the defect."""
    own, original = 1700009000000, 1700001000000
    origins = [_origin(1700009000000, 1, own, 100.0)]
    r = FakeRedis({
        f'BTCUSDT:15m:move:{own}': {'time': str(own), 'length_bar': '2',
                                    'og_mth_move_id': f'BTCUSDT:15m:move:{original}'},
        f'BTCUSDT:15m:move:{original}': {'time': str(original), 'length_bar': '2'},
    })

    turn, anchor = legs.turning_points(origins, '15m', r)[origins[0].id]

    assert turn > original, (
        'the turning point resolved back to the chained original MTH; that is '
        'what put leg ends before their starts'
    )
    assert turn >= own
    # The anchoring move id still reports the original — that half was correct.
    assert anchor == f'BTCUSDT:15m:move:{original}'


def test_chain_never_produces_a_leg_that_ends_before_it_starts():
    """The invariant, end to end, over a chain containing a secondary origin."""
    own_a, own_b = 1700002000000, 1700009000000
    original = 1700001000000
    origins = [
        _origin(1700002000000, 1, own_a, 110.0),
        _origin(1700009000000, 0, own_b, 90.0),
    ]
    moves = {
        f'BTCUSDT:15m:move:{own_a}': {'time': str(own_a), 'length_bar': '2'},
        # b is a secondary origin pointing back at a much earlier MTH
        f'BTCUSDT:15m:move:{own_b}': {'time': str(own_b), 'length_bar': '2',
                                      'og_mth_move_id': f'BTCUSDT:15m:move:{original}'},
        f'BTCUSDT:15m:move:{original}': {'time': str(original), 'length_bar': '2'},
    }
    r = FakeRedis(moves)
    turns = legs.turning_points(origins, '15m', r)

    chain = legs._chain(origins, turns, None)

    assert chain, 'no legs built'
    for leg in chain:
        if leg.end_time:
            assert leg.end_time >= leg.start_time, (
                f'{leg.id} ends at {leg.end_time} before it starts at '
                f'{leg.start_time}'
            )


def test_chain_start_times_are_monotonic():
    """legs_index is scored by start_time, so order must follow the chain."""
    times = [1700002000000, 1700009000000, 1700016000000, 1700023000000]
    origins = [_origin(t, i % 2, t, 100.0 + i * 10) for i, t in enumerate(times)]
    r = FakeRedis({f'BTCUSDT:15m:move:{t}': {'time': str(t), 'length_bar': '2'}
                   for t in times})
    turns = legs.turning_points(origins, '15m', r)

    starts = [l.start_time for l in legs._chain(origins, turns, None)]

    assert starts == sorted(starts), f'chain order does not follow time: {starts}'


def test_chained_origins_do_not_collapse_leg_endpoints_onto_one_price():
    """A chain is repeated retries of one structure, so `og_mth_value` is shared.

    Using it as the leg endpoint therefore froze many legs onto the same two
    prices. Over 8-9 Jul 2026 on 15m the window ran 61,544.56 to 63,761.99 while
    the legs bounced between 62,888.35 and 61,329.98 — the latter below the
    window's own low. Nine completed origins shared one `og_mth_value`.

    The endpoint is the origin's own MTH extreme, which is distinct per origin.
    """
    times = [1700002000000, 1700009000000, 1700016000000]
    shared = 55000.0                       # what a chain would all point at
    own = [61883.73, 62336.01, 61608.14]   # their real, distinct extremes
    origins = [
        _origin(t, i % 2, t, own[i], og_mth_value=shared,
                og_move='BTCUSDT:15m:move:1700000000000')
        for i, t in enumerate(times)
    ]
    r = FakeRedis({f'BTCUSDT:15m:move:{t}': {'time': str(t), 'length_bar': '2'}
                   for t in times})
    turns = legs.turning_points(origins, '15m', r)

    chain = legs._chain(origins, turns, None)

    endpoints = set()
    for leg in chain:
        endpoints.add(round(leg.origin_extreme, 2))
        if leg.complete:
            endpoints.add(round(leg.extreme, 2))
    assert shared not in endpoints, (
        'leg endpoints resolved to the shared chain anchor; every leg in a '
        'chain collapses onto one price'
    )
    assert endpoints & {round(v, 2) for v in own}, \
        'leg endpoints do not use the origins own MTH extremes'


def test_start_time_and_start_mth_time_describe_the_same_move():
    """`refresh_legs` bounds its delete by one and its gather by the other.

    If they describe different moves the rebuild writes legs outside the range
    it cleared, laying new legs alongside stale ones. Before the fix,
    `start_mth_time` followed the chain anchor backwards: measured on 3m the two
    bounds came apart by 888 minutes and 6 of 10 rebuilt legs landed before the
    deletion window.
    """
    own, original = 1700009000000, 1700001000000
    origins = [
        _origin(1700009000000, 1, own, 100.0),
        _origin(1700016000000, 0, 1700016000000, 90.0),
    ]
    r = FakeRedis({
        f'BTCUSDT:15m:move:{own}': {'time': str(own), 'length_bar': '2',
                                    'og_mth_move_id': f'BTCUSDT:15m:move:{original}'},
        f'BTCUSDT:15m:move:{original}': {'time': str(original), 'length_bar': '2'},
        'BTCUSDT:15m:move:1700016000000': {'time': '1700016000000', 'length_bar': '2'},
    })
    turns = legs.turning_points(origins, '15m', r)

    leg = legs._chain(origins, turns, None)[0]

    assert leg.start_mth_time == own, (
        f'start_mth_time is {leg.start_mth_time}, not the origin own MTH move '
        f'{own} — it followed the chain anchor back to {original}'
    )
    # and it must precede the turning point, which is one bar past that move
    assert leg.start_mth_time <= leg.start_time


# ── extending an ending through a continuing sequence (TA, 2026-08-20) ──

def _o(t, direction, mth_value):
    return _origin(t, direction, t, mth_value)


def _turns_for(times):
    return FakeRedis({f'BTCUSDT:15m:move:{t}': {'time': str(t), 'length_bar': '2'}
                      for t in times})


def test_higher_high_extends_a_bearish_origin_ending():
    """"higher for bearish" — trade-side naming: a bearish origin is a sell zone
    at a high (stored direction=1), so a higher one is a higher high and the leg
    reaches further up."""
    times = [1700002000000, 1700009000000, 1700016000000]
    origins = [_o(times[0], 0, 90.0), _o(times[1], 1, 110.0), _o(times[2], 1, 115.0)]
    assert legs._extend_ending(origins, 1) == 2


def test_lower_low_extends_a_bullish_origin_ending():
    """"lower for bullish" — a bullish origin is a buy zone at a low (stored
    direction=0); a lower one is a lower low."""
    times = [1700002000000, 1700009000000, 1700016000000]
    origins = [_o(times[0], 1, 110.0), _o(times[1], 0, 90.0), _o(times[2], 0, 85.0)]
    assert legs._extend_ending(origins, 1) == 2


def test_extension_always_carries_the_leg_further_in_its_own_direction():
    """The whole point. Reading direction as stored inverts both tests and every
    extension then pulls the endpoint back toward the start."""
    times = [1700002000000, 1700009000000, 1700016000000]
    # bullish-origin ending (stored 0, a low): a *higher* low must NOT extend
    origins = [_o(times[0], 1, 110.0), _o(times[1], 0, 90.0), _o(times[2], 0, 95.0)]
    assert legs._extend_ending(origins, 1) == 1
    # bearish-origin ending (stored 1, a high): a *lower* high must NOT extend
    origins = [_o(times[0], 0, 90.0), _o(times[1], 1, 110.0), _o(times[2], 1, 105.0)]
    assert legs._extend_ending(origins, 1) == 1


def test_an_opposite_direction_neighbour_stops_the_walk():
    times = [1700002000000, 1700009000000, 1700016000000]
    origins = [_o(times[0], 1, 110.0), _o(times[1], 0, 90.0), _o(times[2], 1, 85.0)]
    assert legs._extend_ending(origins, 1) == 1


def test_the_walk_continues_across_several():
    times = [1700002000000, 1700009000000, 1700016000000, 1700023000000]
    origins = [_o(times[0], 1, 110.0), _o(times[1], 0, 90.0),
               _o(times[2], 0, 85.0), _o(times[3], 0, 80.0)]
    assert legs._extend_ending(origins, 1) == 3


def test_extending_never_moves_the_ending_backwards():
    """The leg must still not end before it starts — the invariant from the
    17 Aug regression has to survive this rule."""
    times = [1700002000000, 1700009000000, 1700016000000]
    origins = [_o(times[0], 1, 110.0), _o(times[1], 0, 90.0), _o(times[2], 0, 85.0)]
    r = _turns_for(times)
    turns = legs.turning_points(origins, '15m', r)
    for leg in legs._chain(origins, turns, extend=True):
        if leg.end_time:
            assert leg.end_time >= leg.start_time


def test_extend_can_be_switched_off():
    times = [1700002000000, 1700009000000, 1700016000000]
    origins = [_o(times[0], 1, 110.0), _o(times[1], 0, 90.0), _o(times[2], 0, 85.0)]
    r = _turns_for(times)
    turns = legs.turning_points(origins, '15m', r)
    off = legs._chain(origins, turns, extend=False)[0]
    on = legs._chain(origins, turns, extend=True)[0]
    assert on.extreme != off.extreme


# ---------------------------------------------------------------------------
# The extension scans the run; a pullback inside it does not stop the walk
# (TA, 2026-09-13). Everything above stayed green through this change because
# none of it covered a non-continuing origin followed by a continuing one.
# ---------------------------------------------------------------------------

# NQU6 case 08, real mth_values from `complete_origins_index`, MTH-ordered.
CASE_08 = [
    (1782384300000, 1, 30164.25),   # SELL — the bearish leg's start
    (1782399600000, 0, 29316.00),   # BUY  — turn 25 Jun 14:00, ended the leg
    (1782408600000, 0, 29651.00),   # BUY  — HIGHER: stopped the old walk here
    (1782415800000, 0, 29672.25),   # BUY  — higher still
    (1782424800000, 0, 29818.50),   # BUY  — higher still
    (1782446400000, 0, 29249.75),   # BUY  — turn 26 Jun 03:30, a real lower low
    (1782760500000, 1, 30053.50),   # SELL — closes the next leg
]


def _case_08_origins():
    return [_o(t, d, v) for t, d, v in CASE_08]


def test_a_pullback_inside_the_run_does_not_stop_the_walk():
    """The old consecutive walk stopped at the first higher low and missed the
    extreme the leg actually reached, three origins further on, with no
    opposite-direction origin anywhere between them.

        > "if a lower bullish origin was printed below. I would have prefered the
        >  leg to have continoud until 3.30. There was a second mth, with no
        >  bearish origin in between. so the leg actually just continous"
        >  — TA, 2026-09-13
    """
    origins = _case_08_origins()
    assert legs._extend_ending(origins, 1) == 5, (
        'the ending must reach 29249.75 (turn 26 Jun 03:30), not stop at the '
        'first higher low'
    )


def test_the_opposite_direction_origin_still_bounds_the_scan():
    """What keeps the scan from running away: it is the same terminus `_chain`
    uses. The trailing SELL at 30053.50 is never a candidate."""
    origins = _case_08_origins()
    assert legs._extend_ending(origins, 1) != 6


def test_the_scan_takes_the_furthest_not_the_last():
    """A lower low followed by a higher one keeps the lower."""
    times = [1700002000000, 1700009000000, 1700016000000, 1700023000000]
    origins = [_o(times[0], 1, 110.0), _o(times[1], 0, 90.0),
               _o(times[2], 0, 80.0), _o(times[3], 0, 95.0)]
    assert legs._extend_ending(origins, 1) == 2
    # mirrored for a sell-zone ending
    origins = [_o(times[0], 0, 90.0), _o(times[1], 1, 110.0),
               _o(times[2], 1, 120.0), _o(times[3], 1, 105.0)]
    assert legs._extend_ending(origins, 1) == 2


def test_case_08_leg_ends_at_the_lower_low_turn():
    """End to end through `_chain`: the bearish leg must span the whole move."""
    origins = _case_08_origins()
    r = _turns_for([t for t, _, _ in CASE_08])
    turns = legs.turning_points(origins, '15m', r)
    chain = legs._chain(origins, turns, extend=True)
    bearish = chain[0]
    assert int(bearish.direction) == 0
    assert float(bearish.extreme) == 29249.75, (
        'the leg must reach the lower low, not stop at 29316.00'
    )


# ---------------------------------------------------------------------------
# An MTH that runs past the extreme its leg departed from moves the boundary
# (TA, 2026-09-14). This is the half that arrives in time: an ending extension
# needs a completed origin, and on NQU6 case 08 that origin completed 2h45m
# after the MTH was processed.
# ---------------------------------------------------------------------------


class MthFake(FakeRedis):
    """`FakeRedis` plus an mth_index and hash reads through a pipeline."""

    def __init__(self, moves, mths=()):
        super().__init__(moves)
        self.h = {}
        self.z = {}
        for pt, direction, block_one in mths:
            zid = f'BTCUSDT:15m:mth:{pt}'
            self.h[zid] = {'id': zid, 'process_time': str(pt),
                           'direction': str(direction), 'block_one': str(block_one)}
            self.z.setdefault('BTCUSDT:15m:mth_index', {})[zid] = float(pt)
        self._hq = []

    def zrangebyscore(self, key, lo, hi, **kw):
        def b(v, d):
            if isinstance(v, (int, float)):
                return float(v)
            t = str(v).lstrip('(')
            return d if t in ('+inf', '-inf') else float(t)
        lo_v, hi_v = b(lo, float('-inf')), b(hi, float('inf'))
        return [m for _, m in sorted((s, m) for m, s in self.z.get(key, {}).items()
                                     if lo_v <= s <= hi_v)]

    def hgetall(self, key):
        self._hq.append(dict(self.h.get(key, {})))
        return None

    def pipeline(self, transaction=True):
        return self

    def execute(self):
        if self._hq:
            out, self._hq = self._hq, []
            return out
        return super().execute()


def _two_legs(mths=()):
    """A bearish leg then a bullish one, the bullish departing from 90.0."""
    times = [1700002000000, 1700009000000, 1700016000000]
    origins = [_o(times[0], 1, 110.0), _o(times[1], 0, 90.0), _o(times[2], 1, 115.0)]
    r = MthFake({f'BTCUSDT:15m:move:{t}': {'time': str(t), 'length_bar': '2'}
                 for t in times}, mths)
    chain = legs._chain(origins, legs.turning_points(origins, '15m', r), None)
    return legs._apply_mth_breaks(chain, 'BTCUSDT', '15m', r), r


def test_a_counter_trend_mth_past_the_leg_start_moves_the_boundary():
    """The bullish leg departs from 90.0; a bearish MTH closing at 85.0 has
    taken out the low the uptrend started from, so the uptrend ended there."""
    inside = 1700011000000
    chain, _ = _two_legs(mths=[(inside, 0, 85.0)])
    assert int(chain[0].end_time) == inside
    assert float(chain[0].extreme) == 85.0
    assert int(chain[1].start_time) == inside
    assert float(chain[1].origin_extreme) == 85.0
    assert chain[1].id.endswith(f':leg:{inside}')


def test_an_mth_that_holds_the_leg_start_changes_nothing():
    """A pullback that does not reach the departure low is just a pullback."""
    base, _ = _two_legs()
    held, _ = _two_legs(mths=[(1700011000000, 0, 95.0)])
    assert int(held[1].start_time) == int(base[1].start_time)


def test_a_same_direction_mth_never_moves_the_boundary():
    """Only a counter-trend MTH can take out the leg's own premise."""
    base, _ = _two_legs()
    same, _ = _two_legs(mths=[(1700011000000, 1, 85.0)])
    assert int(same[1].start_time) == int(base[1].start_time)


def test_the_furthest_qualifying_mth_wins():
    """One pass is only sufficient because the furthest is taken."""
    chain, _ = _two_legs(mths=[(1700010000000, 0, 88.0), (1700012000000, 0, 80.0)])
    assert int(chain[1].start_time) == 1700012000000
    assert float(chain[1].origin_extreme) == 80.0


def test_start_mth_time_does_not_move_with_the_start():
    """`refresh_legs` bounds its delete by start_time and its gather by
    start_mth_time. Moving both pulls them apart — the 3m defect above."""
    chain, _ = _two_legs(mths=[(1700011000000, 0, 85.0)])
    assert chain[1].start_mth_time <= chain[1].start_time


def test_an_mth_after_the_leg_ends_is_not_considered():
    """The boundary may only move inside the leg it belongs to."""
    base, _ = _two_legs()
    after, _ = _two_legs(mths=[(1700020000000, 0, 50.0)])
    assert int(after[1].start_time) == int(base[1].start_time)


def test_until_time_hides_an_mth_not_yet_processed():
    """Causality: process_time is when the MTH is knowable, and a historical
    query must not use one from after the moment being asked about."""
    times = [1700002000000, 1700009000000, 1700016000000]
    origins = [_o(times[0], 1, 110.0), _o(times[1], 0, 90.0), _o(times[2], 1, 115.0)]
    r = MthFake({f'BTCUSDT:15m:move:{t}': {'time': str(t), 'length_bar': '2'}
                 for t in times}, [(1700011000000, 0, 85.0)])
    chain = legs._chain(origins, legs.turning_points(origins, '15m', r), None)
    early = legs._apply_mth_breaks(chain, 'BTCUSDT', '15m', r,
                                   until_time=1700010000000)
    assert int(early[1].start_time) != 1700011000000


# ---------------------------------------------------------------------------
# A leg must end at the furthest point it actually reached, not at the origin
# that terminated it (TA, 2026-09-14). `Leg.extreme` is documented as "furthest
# body value reached in the trend direction" and was not: the endpoint came from
# the terminating origin's own mth_value, and an origin is anchored on *its*
# move, which need not be the deepest in the leg.
#
# NQU6 2 July: the bearish leg recorded 29413.00 and ended 19:15, while an MTH
# inside it closed at 29338.50 and was processed at 18:00. Price made a low,
# bounced, made a higher low, and the origin formed off the second one.
# ---------------------------------------------------------------------------


def _deep(mths=()):
    """Bearish leg 110 -> 90, then a bullish one. MTHs land inside the first."""
    times = [1700002000000, 1700009000000, 1700016000000]
    origins = [_o(times[0], 1, 110.0), _o(times[1], 0, 90.0), _o(times[2], 1, 115.0)]
    r = MthFake({f'BTCUSDT:15m:move:{t}': {'time': str(t), 'length_bar': '2'}
                 for t in times}, mths)
    chain = legs._chain(origins, legs.turning_points(origins, '15m', r), None)
    return legs._extend_to_deepest_mth(chain, 'BTCUSDT', '15m', r), r


def test_a_deeper_same_direction_mth_moves_the_leg_end():
    inside = 1700005000000
    chain, _ = _deep(mths=[(inside, 0, 85.0)])
    assert int(chain[0].end_time) == inside
    assert float(chain[0].extreme) == 85.0
    assert int(chain[1].start_time) == inside
    assert float(chain[1].origin_extreme) == 85.0
    assert chain[1].id.endswith(f':leg:{inside}')


def test_an_mth_short_of_the_extreme_changes_nothing():
    base, _ = _deep()
    shallow, _ = _deep(mths=[(1700005000000, 0, 95.0)])
    assert int(shallow[0].end_time) == int(base[0].end_time)
    assert float(shallow[0].extreme) == float(base[0].extreme)


def test_a_counter_direction_mth_is_not_this_rule():
    """That is `_apply_mth_breaks`, and it moves the *start*, not the end."""
    base, _ = _deep()
    counter, _ = _deep(mths=[(1700005000000, 1, 85.0)])
    assert int(counter[0].end_time) == int(base[0].end_time)


def test_the_deepest_wins_not_the_last():
    chain, _ = _deep(mths=[(1700004000000, 0, 88.0), (1700005000000, 0, 80.0),
                           (1700006000000, 0, 86.0)])
    assert int(chain[0].end_time) == 1700005000000
    assert float(chain[0].extreme) == 80.0


def test_start_mth_time_comes_back_with_the_boundary():
    """The boundary moves *backward*, so the gather bound has to be lowered or
    it sits after the leg's own start — 9 of 12 real cases would have."""
    inside = 1700005000000
    chain, _ = _deep(mths=[(inside, 0, 85.0)])
    assert int(chain[1].start_mth_time or 0) <= int(chain[1].start_time)


def test_the_leg_never_ends_before_it_starts():
    chain, _ = _deep(mths=[(1700005000000, 0, 85.0)])
    for leg in chain:
        if leg.end_time:
            assert int(leg.end_time) >= int(leg.start_time)


def test_an_mth_outside_the_leg_is_ignored():
    base, _ = _deep()
    outside, _ = _deep(mths=[(1700020000000, 0, 50.0)])
    assert int(outside[0].end_time) == int(base[0].end_time)


def test_until_time_hides_an_mth_not_yet_processed():
    times = [1700002000000, 1700009000000, 1700016000000]
    origins = [_o(times[0], 1, 110.0), _o(times[1], 0, 90.0), _o(times[2], 1, 115.0)]
    r = MthFake({f'BTCUSDT:15m:move:{t}': {'time': str(t), 'length_bar': '2'}
                 for t in times}, [(1700005000000, 0, 85.0)])
    chain = legs._chain(origins, legs.turning_points(origins, '15m', r), None)
    early = legs._extend_to_deepest_mth(chain, 'BTCUSDT', '15m', r,
                                        until_time=1700004000000)
    assert int(early[0].end_time) != 1700005000000


# ---------------------------------------------------------------------------
# Wiring. The two MTH passes above are tested directly, which does not prove
# `build_legs` calls them — removing either call left all of those green. These
# go through the real entry point.
# ---------------------------------------------------------------------------


class ChainFake(MthFake):
    """`MthFake` plus the completed-origin index `build_legs` reads."""

    def __init__(self, moves, origins, mths=()):
        super().__init__(moves, mths)
        self.origins = {}
        for o in origins:
            self.h[o.id] = {
                'id': o.id, 'symbol': o.symbol, 'timeframe': o.timeframe,
                'type': 'origin', 'direction': str(o.direction),
                'completion': 'complete', 'time': str(o.time),
                'process_time': str(o.process_time),
                'block_zero': str(o.block_zero), 'block_one': str(o.block_one),
                'mth_value': str(o.mth_value), 'mth_move_id': o.mth_move_id,
                'time_completed': str(o.time),
            }
            self.z.setdefault('BTCUSDT:15m:complete_origins_index', {})[o.id] = float(o.time)

    def zrange(self, key, a, b, **kw):
        return [m for _, m in sorted((s, m) for m, s in self.z.get(key, {}).items())]


def _built(mths=()):
    times = [1700002000000, 1700009000000, 1700016000000]
    origins = [_o(times[0], 1, 110.0), _o(times[1], 0, 90.0), _o(times[2], 1, 115.0)]
    r = ChainFake({f'BTCUSDT:15m:move:{t}': {'time': str(t), 'length_bar': '2'}
                   for t in times}, origins, mths)
    return legs.build_legs('BTCUSDT', '15m', r)


def test_build_legs_applies_the_deepest_mth_pass():
    inside = 1700005000000
    chain = _built(mths=[(inside, 0, 85.0)])
    assert len(chain) >= 2
    assert int(chain[0].end_time) == inside, (
        'build_legs must run _extend_to_deepest_mth, not only expose it'
    )
    assert float(chain[0].extreme) == 85.0


def test_build_legs_applies_the_mth_break_pass():
    """The bullish leg departs 90.0; a bearish MTH inside it closing at 85.0
    takes out the low it started from."""
    turn = legs.turning_points(
        [_o(1700009000000, 0, 90.0)], '15m',
        MthFake({'BTCUSDT:15m:move:1700009000000':
                 {'time': '1700009000000', 'length_bar': '2'}}))
    after = int(list(turn.values())[0][0]) + 900_000
    chain = _built(mths=[(after, 0, 85.0)])
    moved = [l for l in chain if int(l.start_time) == after]
    assert moved, 'build_legs must run _apply_mth_breaks, not only expose it'
