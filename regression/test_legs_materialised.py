"""
The materialised leg chain must equal the reference chain — at every point.

`build_legs` is the definition of a Leg: chain every completed origin, oldest
MTH first. `refresh_legs` maintains the same chain incrementally in
`legs_index`, rebuilding only a bounded tail, because the definition is O(n) and
`apply_htf_links` asks for the running leg on up to three higher timeframes at
every zone creation.

These tests replay a real BTCUSDT origin stream — each origin's completion, then
its takeout, in chronological order, with the takeout hidden until its event
fires — and hold the two implementations to the same answer.

Checking only the *final* state is not enough. It passes even with the takeout
hook removed, because the next origin completion rebuilds the tail from Redis
and quietly repairs it. What the pipeline actually depends on is the chain being
right in the window *between* those events, since zone creations read it
continuously. So the comparison runs after every event.

Fixture: `tests/fixtures/legs_origin_stream.json` — 1080 completed origins
across 3m/15m/1h/4h, captured from production Redis.
"""

import json
from pathlib import Path

import pytest

from mgot_utils.models import Zone
from mgot_utils.models.leg import Leg
from mgot_utils.processing.legs import build_legs, maintain_legs

FIXTURE = Path(__file__).parent.parent / 'fixtures' / 'legs_origin_stream.json'

# Comparing after every event costs a full build_legs each time, which is
# quadratic in the origin count. Dense timeframes are sampled instead; the
# sparse ones are checked exhaustively.
STRIDE = {'3m': 25, '15m': 1, '1h': 1, '4h': 1}


@pytest.fixture(scope='module')
def stream():
    with open(FIXTURE) as f:
        return json.load(f)


def _events(origins):
    """(time, kind, origin_id) for every completion and takeout, in order.

    kind 0 = completed, kind 1 = taken out. Ordering by time is what makes this
    a replay rather than a re-derivation: the chain has to be correct using only
    what had happened by each event.
    """
    events = []
    for zid, data in origins.items():
        completed = int(data.get('time_completed') or 0)
        if not completed:
            continue
        events.append((completed, 0, zid))
        taken_out = int(data.get('time_taken_out') or 0)
        if taken_out:
            events.append((taken_out, 1, zid))
    events.sort()
    return events


def _state_at(data, kind):
    """The origin as it stood at this event — no future takeout visible."""
    state = dict(data)
    if kind == 0:
        state.pop('time_taken_out', None)
        state['completion'] = 'complete'
    else:
        state['completion'] = 'taken_out'
    return state


def _signature(leg):
    return (int(leg.start_time), int(leg.end_time or 0), int(leg.direction),
            round(float(leg.origin_extreme), 6), round(float(leg.extreme), 6),
            int(leg.complete or 0))


def _materialised(symbol, timeframe, r):
    ids = r.zrange(f'{symbol}:{timeframe}:legs_index', 0, -1)
    pipe = r.pipeline()
    for zid in ids:
        pipe.hgetall(zid)
    return [Leg.initiate_leg(d) for d in pipe.execute() if d]


def _seed_moves(moves, r):
    pipe = r.pipeline()
    for move_id, fields in moves.items():
        pipe.hset(move_id, mapping=fields)
    pipe.execute()


@pytest.mark.integration
@pytest.mark.parametrize('timeframe', ['3m', '15m', '1h', '4h'])
def test_materialised_matches_reference_throughout_replay(stream, redis_client, timeframe):
    """After every event, legs_index equals build_legs on the same state."""
    symbol = stream['symbol']
    data = stream['timeframes'][timeframe]
    origins, stride = data['origins'], STRIDE[timeframe]
    _seed_moves(data['moves'], redis_client)

    events = _events(origins)
    assert events, f'fixture has no events for {timeframe}'

    complete_index = f'{symbol}:{timeframe}:complete_origins_index'
    checked = 0
    for n, (_, kind, zid) in enumerate(events, start=1):
        state = _state_at(origins[zid], kind)
        redis_client.hset(zid, mapping=state)
        redis_client.zadd(complete_index, {zid: int(state['time'])})
        maintain_legs(Zone.initiate_zone(state), redis_client)

        if n % stride and n != len(events):
            continue
        checked += 1
        expected = [_signature(l) for l in build_legs(symbol, timeframe, redis_client)]
        actual = [_signature(l) for l in _materialised(symbol, timeframe, redis_client)]
        assert actual == expected, (
            f'{timeframe}: diverged after event {n}/{len(events)} '
            f'({"takeout" if kind else "completion"} of {zid}); '
            f'reference has {len(expected)} legs, index has {len(actual)}'
        )

    assert checked > 0


@pytest.mark.integration
def test_takeout_invalidation_merges_legs(stream, redis_client):
    """A takeout that unseats an ending must shrink the chain, not orphan a leg.

    The ending-validity rule says an origin price blew straight through never
    ended a leg. When that becomes true only later — the origin is taken out
    before one appears on the other side — the leg it closed has to reopen and
    absorb the next. Guards the case where legs_index grows monotonically
    because the stale tail was never deleted.
    """
    symbol, timeframe = stream['symbol'], '15m'
    data = stream['timeframes'][timeframe]
    _seed_moves(data['moves'], redis_client)
    origins = data['origins']

    complete_index = f'{symbol}:{timeframe}:complete_origins_index'
    shrank = False
    previous = 0
    for _, kind, zid in _events(origins):
        state = _state_at(origins[zid], kind)
        redis_client.hset(zid, mapping=state)
        redis_client.zadd(complete_index, {zid: int(state['time'])})
        maintain_legs(Zone.initiate_zone(state), redis_client)
        count = redis_client.zcard(f'{symbol}:{timeframe}:legs_index')
        if count < previous:
            shrank = True
        previous = count

    assert shrank, (
        'no takeout in the fixture ever invalidated a leg ending — the merge '
        'path is untested, so this fixture no longer covers what it claims'
    )
