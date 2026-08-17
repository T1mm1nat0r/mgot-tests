"""
Whole detector features have produced nothing at all, for the entire dataset,
without anything complaining.

Three of them, all found by counting rather than by any test failing:

  * **MTH sweeps: zero, on every timeframe.** The spend check asked whether the
    swept level had been *either* gained or lost, when only one of the two
    consumes it — a high is spent by a close above, a low by a close below.
    Testing both rejects on the harmless direction, which price crosses
    constantly: 51 lows rejected for having gains, 40 highs for having losses,
    and not one MTH sweep recorded anywhere. A sweep of the MTH or its
    succeeding origin is what opens the dance's state 2, so half that trigger
    could never fire.
  * **Chained origins: zero.** `process_pso` wrote the original MTH value into
    `og_move_value`; the `Move` model carries `og_mth_value`. 94 of 311 origins
    on 15m carry a chained value now.
  * **Leg direction: "down" on 99% of bars.** An open Leg defaulted to direction
    0, and `leg_at_time` returns the last leg, which is normally the open one.
    The dance sat in a down state for 2019 bars against 16 up on 15m while the
    legs themselves ran 30 up to 32 down.

Every one is a *silent zero*: the code path runs, decides no, and produces an
empty result that reads exactly like a quiet market. Unit tests cannot catch it
— each of these functions passes its own tests in isolation — so the check has
to be "does the real pipeline, over real bars, produce any of these at all".

Deliberately weak assertions. `> 0` and "not all identical" are the strongest
claims that survive a change to detection without needing a golden number
rewritten; the fidelity tests next door pin exact behaviour. What these catch is
a feature that has stopped existing.

Window: 14 days of BTCUSDT 15m, ~1345 bars, about 30 seconds. On the current
detector that yields 9 MTH sweeps, 40 of 144 origins chained, 17/16 legs and
93/107 dance transitions — enough margin that a real regression, not noise,
is needed to cross zero.

Requires the production Redis on 6379; skips without it. Run on its own:

    pytest harness/
"""

import contextlib
import io

import pytest
import redis

from harness.replay import Replay

SYMBOL = 'BTCUSDT'
TIMEFRAME = '15m'
REPLAY_DB = 9
DAYS = 14


@pytest.fixture(scope='module')
def production():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    try:
        r.ping()
    except redis.ConnectionError:
        pytest.skip('production Redis not available on 6379')
    if not r.zcard(f'{SYMBOL}:{TIMEFRAME}:bars_index'):
        pytest.skip(f'no {SYMBOL} bars to replay')
    return r


@pytest.fixture(scope='module')
def replayed(production):
    """One replay, shared by every invariant — it is the expensive part."""
    start = int(production.zrange(
        f'{SYMBOL}:{TIMEFRAME}:bars_index', 0, 0, withscores=True)[0][1])
    end = start + DAYS * 24 * 3600 * 1000

    with Replay(SYMBOL, [TIMEFRAME], db=REPLAY_DB, capture=False) as rp:
        rp.reset()
        bars = rp.load_from(production, start, end)
        assert bars > 500, f'only {bars} bars in the window — nothing to conclude'
        # The services narrate every detection to stdout.
        with contextlib.redirect_stdout(io.StringIO()):
            rp.run()
        yield rp


def _hashes(r, ids):
    pipe = r.pipeline()
    for key in ids:
        pipe.hgetall(key)
    return [d for d in pipe.execute() if d]


@pytest.mark.integration
def test_the_replay_produced_something_at_all(replayed):
    """Guards every other assertion here from passing on an empty keyspace."""
    zones = replayed.zones(TIMEFRAME)
    assert len(zones) > 50, f'only {len(zones)} zones — the replay did not run'
    kinds = {z.get('type') for z in zones}
    assert {'mth', 'origin'} <= kinds, f'missing zone types, got {sorted(kinds)}'


@pytest.mark.integration
def test_mth_zones_are_swept(replayed):
    """MTH sweeps were zero across every timeframe for the entire dataset.

    Counted by type rather than in total, because origin sweeps kept working
    throughout — the index was never empty, which is exactly why nobody noticed.
    """
    entries = replayed.r.zrange(f'{SYMBOL}:{TIMEFRAME}:sweeps_index', 0, -1)
    assert entries, 'no sweeps of any kind — sweep detection is dead'

    by_type = {}
    for entry in entries:
        zone_id = entry.rsplit(':sweep:', 1)[0]
        kind = replayed.r.hget(zone_id, 'type') or 'unknown'
        by_type[kind] = by_type.get(kind, 0) + 1

    assert by_type.get('mth', 0) > 0, (
        f'no MTH zone was swept in {DAYS} days of {TIMEFRAME}: {by_type}. '
        f'The dance reaches state 2 on a sweep of the MTH or its succeeding '
        f'origin, so half that trigger cannot fire.'
    )


@pytest.mark.integration
def test_origins_carry_a_chained_mth_value(replayed):
    """A secondary origin must inherit the *original* MTH value, not its own.

    `og_mth_value` equal to `mth_value` means the origin came straight off its
    own MTH; a distinct value means it is a secondary that kept the anchor from
    where the move began. Zero distinct values across production was the symptom
    of the `og_move_value` typo — the write went to a field nothing reads, so
    every chain restarted at the first hop.
    """
    origins = [z for z in replayed.zones(TIMEFRAME, 'origin')]
    assert origins, 'no origins at all'

    chained = [z for z in origins
               if float(z.get('og_mth_value') or 0)
               and float(z.get('og_mth_value') or 0) != float(z.get('mth_value') or 0)]
    assert chained, (
        f'none of {len(origins)} origins carries an og_mth_value distinct from '
        f'its own mth_value — the chained-origin value is being dropped '
        f'somewhere between process_pso, the Move hash and _create_origin_zone'
    )


@pytest.mark.integration
def test_legs_run_in_both_directions(replayed):
    """The materialised chain must turn — a trend that never reverses is a bug.

    This guards the chain, not the open-leg default: `legs_index` is almost all
    *closed* legs, whose direction comes from price and was always right. The
    open-leg default is caught by the dance test below, which is where it
    actually did damage.
    """
    ids = replayed.r.zrange(f'{SYMBOL}:{TIMEFRAME}:legs_index', 0, -1)
    legs = _hashes(replayed.r, ids)
    assert len(legs) > 5, f'only {len(legs)} legs — the chain is not being built'

    directions = {leg.get('direction') for leg in legs}
    assert len(directions) > 1, (
        f'all {len(legs)} legs run in direction {directions.pop()!r}. A trend '
        f'that never turns is a default leaking through, not a market.'
    )


# The minority direction's share of dance transitions. Balanced is ~46% on this
# window; the open-leg default produced 0.5%. Anything in between is a genuine
# regression worth looking at, and the gap is wide enough that normal variation
# in the detector cannot cross it.
MIN_DANCE_DIRECTION_SHARE = 0.10


@pytest.mark.integration
def test_the_dance_is_not_stuck_in_one_direction(replayed):
    """The dance was "down" on 99% of bars — 2019 down against 16 up on 15m.

    It takes its direction from the running Leg via `leg_at_time`, which returns
    the *last* leg — normally the open one. So an open leg defaulting to 0 was
    not a rare edge; it was what the dance saw on almost every bar.

    A "not all identical" check is not enough and was tried: replaying this
    window with the default restored still produced one up transition against
    193 down, and passed. The skew is the signal, so the skew is what is
    asserted. `dance_index` rather than `dance_state`, because the latter holds
    only what is true now and cannot show a machine that never moved.
    """
    ids = replayed.r.zrange(f'{SYMBOL}:{TIMEFRAME}:dance_index', 0, -1)
    transitions = _hashes(replayed.r, ids)
    assert len(transitions) > 10, (
        f'only {len(transitions)} dance transitions recorded — the state '
        f'machine is not advancing'
    )

    counts = {}
    for entry in transitions:
        key = entry.get('direction')
        counts[key] = counts.get(key, 0) + 1
    assert len(counts) > 1, (
        f'every one of {len(transitions)} dance transitions ran in direction '
        f'{next(iter(counts))!r}; the dance is tracking a constant, not the trend'
    )

    share = min(counts.values()) / len(transitions)
    assert share >= MIN_DANCE_DIRECTION_SHARE, (
        f'the dance ran one-directional: {counts} over {len(transitions)} '
        f'transitions, minority share {share:.1%} < '
        f'{MIN_DANCE_DIRECTION_SHARE:.0%}. The direction comes from the running '
        f'Leg, so this is what a defaulted leg direction looks like downstream.'
    )

    states = {entry.get('state') for entry in transitions}
    assert len(states) > 1, (
        f'the dance only ever recorded state {states.pop()!r} — it resets but '
        f'never advances'
    )
