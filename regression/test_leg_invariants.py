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
