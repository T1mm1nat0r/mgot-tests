"""
The harness must reproduce the pipeline, or it proves nothing.

A replay that disagrees with the live pipeline cannot be used to answer "why did
the detector decide X at T", because the answer would be about the harness. So
before the harness is trusted for anything, it has to produce the same zones,
in the same states, as production did over the same bars.

Three properties, in increasing strength:

  determinism   the same window twice gives the same answer
  stability     a zone's state does not depend on how much history ran after it
  fidelity      the zones match what production actually produced

Fidelity is compared **as of the window end**. Production has seen bars the
replay has not, so any state change stamped after the window is future
information and is normalised away — otherwise the test would fail on the
harness being correctly ignorant.

That normalisation took two goes, and the reason is worth recording. The first
version treated `zone_tests` and `sweeps` as lifetime counters, and the test
failed on zones whose counters production had already reset. They are scoped to
the zone's **current lifecycle phase**: crossing complete -> taken_out zeroes
them and `test_eligible_time` along with them. So a zone needs both corrections
— roll the counter back when its event is merely later than the window, and drop
it entirely when a phase change reset it, since the pre-reset value is not
recoverable from the hash.

With both applied, production and replay agree exactly: same zones, same states,
on 3m/3d, 15m/10d and 1h/20d of BTCUSDT.

**After a deliberate change to detection, this test fails until production is
reprocessed** — production still holds state computed under the old rules, so
`test_replay_reproduces_production` is comparing two different detectors. That
is the intended signal, not a harness regression. `test_replay_is_deterministic`
and `test_replay_is_warmup_stable` compare the harness only against itself, so
they keep passing and localise the difference to the detector rather than the
harness.

Requires the production Redis on 6379; skips without it.
"""

import contextlib
import io

import pytest
import redis

from harness.replay import Replay

SYMBOL = 'BTCUSDT'
REPLAY_DB = 9

# Zone identity and geometry — never allowed to differ.
CORE = ['type', 'direction', 'time', 'block_zero', 'block_one', 'mth_value',
        'move_id', 'mth_move_id']

# State fields paired with the timestamp recording when they happened, so a
# change stamped after the window can be rolled back before comparing.
TIMED = [('time_completed', 'time_completed'),
         ('time_taken_out', 'time_taken_out'),
         ('was_completed', 'time_completed')]

# `zone_tests` and `sweeps` count within the zone's **current lifecycle phase**,
# not over its lifetime — crossing complete -> taken_out resets them, along with
# `test_eligible_time`. So they are only comparable when both sides sit in the
# same phase. If production was taken out after the window, its counters have
# already been reset by an event the replay has not seen, and the pre-takeout
# values are not recoverable from the hash. Those zones are compared on
# lifecycle only.
#
# Worth knowing beyond this test: read straight out of Redis these look like
# lifetime counters, and anything treating them as such — a feature extractor,
# for instance — silently undercounts every zone that changed phase.
PHASE_SCOPED = [('zone_tests', 'last_zone_test_time'),
                ('sweeps', 'last_sweep_time')]

# Zones formed this close to the window end may still be in flight — MTH zones
# are confirmed by later moves, squeezes by a parent MTH completing.
SETTLE_MS = 12 * 3600 * 1000


@pytest.fixture(scope='module')
def production():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    try:
        r.ping()
    except redis.ConnectionError:
        pytest.skip('production Redis not available on 6379')
    if not r.zcard(f'{SYMBOL}:15m:bars_index'):
        pytest.skip(f'no {SYMBOL} bars to replay')
    return r


def _window(production, timeframe, days):
    start = int(production.zrange(
        f'{SYMBOL}:{timeframe}:bars_index', 0, 0, withscores=True)[0][1])
    return start, start + days * 24 * 3600 * 1000


def _replay(production, timeframe, start, end):
    with Replay(SYMBOL, [timeframe], db=REPLAY_DB) as rp:
        rp.reset()
        rp.load_from(production, start, end)
        # The services narrate every detection to stdout; useful when running
        # the harness by hand, noise inside a test.
        with contextlib.redirect_stdout(io.StringIO()):
            rp.run()
        return {d['id']: d for d in rp.zones(timeframe)}


def _phase_changed_after(zone, end):
    """Did this zone cross a lifecycle boundary the replay has not seen?"""
    return int(zone.get('time_taken_out') or 0) > end \
        or int(zone.get('time_completed') or 0) > end


def _as_of(zone, end, phase_scoped=True):
    """The zone as it stood at `end`, with later state changes rolled back."""
    completed = int(zone.get('time_completed') or 0)
    taken_out = int(zone.get('time_taken_out') or 0)

    state = {f: str(zone.get(f, '') or '') for f in CORE}
    completion = str(zone.get('completion') or '')
    if taken_out and taken_out > end:
        completion = 'complete' if completed and completed <= end else 'incomplete'
    if completed and completed > end:
        completion = 'incomplete'
    state['completion'] = completion

    for field, stamp in TIMED:
        happened = int(zone.get(stamp) or 0)
        state[field] = '0' if (happened and happened > end) \
            else str(int(zone.get(field) or 0))

    # Two separate corrections, both needed. A counter whose event is simply
    # later than the window gets rolled back; a counter the zone reset by
    # changing phase cannot be rolled back at all, and is dropped.
    if phase_scoped:
        for field, stamp in PHASE_SCOPED:
            happened = int(zone.get(stamp) or 0)
            state[field] = '0' if (happened and happened > end) \
                else str(int(zone.get(field) or 0))
    return state


@pytest.mark.integration
def test_replay_is_deterministic(production):
    start, end = _window(production, '15m', 4)
    first = _replay(production, '15m', start, end)
    second = _replay(production, '15m', start, end)
    assert first == second, 'the same window replayed twice gave different zones'


@pytest.mark.integration
def test_replay_is_warmup_stable(production):
    """A zone's state must not depend on how much history ran after it."""
    start, short_end = _window(production, '15m', 5)
    _, long_end = _window(production, '15m', 8)
    short = _replay(production, '15m', start, short_end)
    long = _replay(production, '15m', start, long_end)

    settled = short_end - 2 * 24 * 3600 * 1000
    compared = [z for z in short if int(z.split(':')[-1]) <= settled]
    assert compared, 'nothing settled enough to compare'
    for zone_id in compared:
        assert zone_id in long, f'{zone_id} vanished when more history ran'
        # A zone that changed phase in the extra history has had its
        # phase-scoped counters reset by an event the short run never saw.
        scoped = not _phase_changed_after(long[zone_id], settled)
        assert (_as_of(short[zone_id], settled, scoped)
                == _as_of(long[zone_id], settled, scoped)), \
            f'{zone_id} changed depending on how much history followed it'


@pytest.mark.integration
@pytest.mark.parametrize('timeframe,days', [('3m', 3), ('15m', 10), ('1h', 20)])
def test_replay_reproduces_production(production, timeframe, days):
    start, end = _window(production, timeframe, days)
    replayed = _replay(production, timeframe, start, end)

    ids = production.zrangebyscore(
        f'{SYMBOL}:{timeframe}:zones_index', start, end - SETTLE_MS)
    pipe = production.pipeline()
    for zone_id in ids:
        pipe.hgetall(zone_id)
    produced = {d['id']: d for d in pipe.execute() if d}
    assert produced, f'no settled {timeframe} zones in production to compare'

    missing = set(produced) - set(replayed)
    assert not missing, (
        f'{timeframe}: replay did not produce {len(missing)} zone(s) production '
        f'did, e.g. {sorted(missing)[:3]}'
    )

    for zone_id, zone in produced.items():
        scoped = not _phase_changed_after(zone, end)
        expected = _as_of(zone, end, scoped)
        actual = _as_of(replayed[zone_id], end, scoped)
        if expected == actual:
            continue
        differing = sorted(f for f in expected if expected[f] != actual[f])
        pytest.fail(
            f'{timeframe}: {zone_id} differs in {differing} — '
            f'production={ {f: expected[f] for f in differing} } '
            f'replay={ {f: actual[f] for f in differing} }'
        )
