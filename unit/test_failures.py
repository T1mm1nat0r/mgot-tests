"""
Failure detection — the retrace that launched an expansion (S3).

The definition is positional, not structural: a failure is the same block shape
as an origin, sitting mid-move rather than at the start, and knowable only after
the expansion it launched has achieved something (business_rules_failures.md §1).

The test that matters most here is `test_block_is_keyed_to_expansion_direction`.
An origin's move runs *with* its leg, so ordering its block by its own direction
is correct. A failure's move runs *against* the expansion — a bearish retrace
precedes an upward expansion — so reusing the origin convention unchanged puts
the 0 at the top of the block. Nothing would raise; the 10SK would simply read
every failure upside down, and "hold the high range" would resolve to the wrong
price. That is a silent-wrong-answer bug of the same family as og_move_value.
"""

import pytest

from mgot_utils.models import Move, Zone
from mgot_utils.processing import failures


def make_move(direction, time, high, low, tf='15m', length=3):
    return Move(
        id=f'BTCUSDT:{tf}:move:{time}', symbol='BTCUSDT', timeframe=tf,
        direction=direction, time=time, type='move',
        open=low if direction == 1 else high,
        close=high if direction == 1 else low,
        high=high, low=low, volume=1.0, length_bar=length,
    )


def make_origin(tf='15m'):
    return Zone(
        id=f'BTCUSDT:{tf}:origin:1700000000000', symbol='BTCUSDT', timeframe=tf,
        type='origin', direction=1, completion='complete',
        time=1700000000000, process_time=1700000900000,
        block_zero=49000.0, block_one=50000.0,
    )


# ── the direction trap ───────────────────────────────────────────

def test_block_is_keyed_to_expansion_direction():
    """Bearish retrace + bullish expansion → 0 at the bottom, 1 at the top."""
    retrace = make_move(direction=0, time=1700000000000, high=51000.0, low=50000.0)
    expansion = make_move(direction=1, time=1700002700000, high=53000.0, low=50000.0)

    block = failures.failure_from(retrace, expansion)

    assert block['direction'] == 1
    assert block['block_zero'] == 50000.0, 'the 0 must sit where the expansion departed'
    assert block['block_one'] == 51000.0
    assert block['block_half'] == 50500.0
    assert block['sweep_level'] == 50000.0


def test_block_inverts_for_a_downward_expansion():
    retrace = make_move(direction=1, time=1700000000000, high=51000.0, low=50000.0)
    expansion = make_move(direction=0, time=1700002700000, high=51000.0, low=48000.0)

    block = failures.failure_from(retrace, expansion)

    assert block['direction'] == 0
    assert block['block_zero'] == 51000.0
    assert block['block_one'] == 50000.0
    assert block['sweep_level'] == 51000.0


def test_using_the_retrace_direction_would_invert_it():
    """Pins the bug the convention avoids, so a 'simplification' fails loudly."""
    retrace = make_move(direction=0, time=1700000000000, high=51000.0, low=50000.0)
    expansion = make_move(direction=1, time=1700002700000, high=53000.0, low=50000.0)

    block = failures.failure_from(retrace, expansion)
    origin_convention = retrace.high if retrace.direction == 0 else retrace.low

    assert block['block_zero'] != origin_convention, (
        'block_zero matches what the origin convention would give for the '
        'retrace\'s own direction — the failure block is inverted.'
    )


# ── rejections ───────────────────────────────────────────────────

def test_same_direction_is_not_a_retrace():
    a = make_move(direction=1, time=1700000000000, high=51000.0, low=50000.0)
    b = make_move(direction=1, time=1700002700000, high=53000.0, low=51000.0)
    assert failures.failure_from(a, b) is None


def test_retrace_must_precede_the_expansion():
    retrace = make_move(direction=0, time=1700002700000, high=51000.0, low=50000.0)
    expansion = make_move(direction=1, time=1700000000000, high=53000.0, low=50000.0)
    assert failures.failure_from(retrace, expansion) is None


def test_cross_timeframe_pairs_are_rejected():
    retrace = make_move(direction=0, time=1700000000000, high=51000.0, low=50000.0, tf='15m')
    expansion = make_move(direction=1, time=1700002700000, high=53000.0, low=50000.0, tf='1h')
    assert failures.failure_from(retrace, expansion) is None


def test_flat_move_is_rejected():
    retrace = make_move(direction=0, time=1700000000000, high=50000.0, low=50000.0)
    expansion = make_move(direction=1, time=1700002700000, high=53000.0, low=50000.0)
    assert failures.failure_from(retrace, expansion) is None


def test_none_inputs_are_rejected():
    m = make_move(direction=1, time=1700000000000, high=51000.0, low=50000.0)
    assert failures.failure_from(None, m) is None
    assert failures.failure_from(m, None) is None


# ── achievement ──────────────────────────────────────────────────

class FakeLevel:
    def __init__(self, conseq_gain=0, conseq_loss=0):
        self.conseq_gain, self.conseq_loss = conseq_gain, conseq_loss


@pytest.mark.parametrize('gain,loss,direction,expected', [
    (2, 0, 1, True),    # two closes above
    (1, 0, 1, False),   # one close is a poke, not an achievement
    (0, 2, 0, True),
    (0, 1, 0, False),
    (3, 0, 1, True),    # more than two still counts
    (2, 0, 0, False),   # gains do not achieve a downward level
])
def test_achievement_needs_two_consecutive_closes(gain, loss, direction, expected):
    assert failures.is_achievement(FakeLevel(gain, loss), direction) is expected


# ── zone construction ────────────────────────────────────────────

def test_build_zone_shape():
    retrace = make_move(direction=0, time=1700000000000, high=51000.0, low=50000.0, length=3)
    expansion = make_move(direction=1, time=1700002700000, high=53000.0, low=50000.0)

    zone = failures.build_zone(retrace, expansion, make_origin())

    assert zone.type == 'failure'
    assert zone.id == 'BTCUSDT:15m:failure:1700000000000'
    assert zone.move_id == retrace.id
    assert zone.direction == 1
    assert zone.block_zero == 50000.0 and zone.block_one == 51000.0
    # 3 bars of 15m: end is two deltas in, process one past the last.
    assert zone.move_end_time == 1700000000000 + 900000 * 2
    assert zone.process_time == 1700000000000 + 900000 * 3
    # What proved it — S4's first question is which failures came from achieving what.
    assert zone.mth_move_id == expansion.id


def test_build_zone_id_does_not_mangle_a_symbol_containing_move():
    """`replace('move', 'failure')` unanchored would corrupt ids. Pinned."""
    retrace = make_move(direction=0, time=1700000000000, high=51000.0, low=50000.0)
    zone = failures.build_zone(
        retrace, make_move(direction=1, time=1700002700000, high=53000.0, low=50000.0),
        make_origin())
    assert zone.id.count('failure') == 1
    assert zone.id.startswith('BTCUSDT:15m:failure:')


# ── the Redis layer ──────────────────────────────────────────────

class FakeRedis:
    """Enough Redis for `classify`: hashes, one sorted set, exists, pipeline.

    The pipeline is the identity object — `sync_with_db` and `zadd` both accept
    a client or a pipeline, and buffering adds nothing to test here.
    """

    def __init__(self):
        self.hashes, self.zsets = {}, {}

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, mapping=None, **kw):
        self.hashes.setdefault(key, {}).update(
            {k: str(v) for k, v in (mapping or {}).items()})

    def exists(self, key):
        return 1 if key in self.hashes else 0

    def zadd(self, key, mapping):
        self.zsets.setdefault(key, {}).update(mapping)

    def zrevrangebyscore(self, key, hi, lo, start=0, num=None):
        items = sorted(self.zsets.get(key, {}).items(),
                       key=lambda kv: kv[1], reverse=True)
        def below(score):
            return score < float(str(hi)[1:]) if str(hi).startswith('(') \
                else score <= float(hi)
        out = [m for m, sc in items if below(sc)]
        return out[start:start + num] if num else out[start:]

    def zrevrange(self, key, lo, hi):
        items = sorted(self.zsets.get(key, {}).items(),
                       key=lambda kv: kv[1], reverse=True)
        return [m for m, _ in items][lo:hi + 1]

    def pipeline(self):
        return self

    def execute(self):
        return []


class FakeBar:
    """A bar as `03_levels_and_zones` actually sees one.

    It carries `move_id` because **`02_timeframe_creator` assigns it**, upstream
    of the 03 seam this classifier hangs on (2026-08-25).

    That was not always true. Move assignment used to live in 04, one service
    *after* 03, so 03's bar had no `move_id` at all — and this fake supplied one
    anyway, which is why the classifier's dependency on a field it never had in
    production went unnoticed until a 55-day reprocess produced zero failures
    against 56 in replay. The fake is only honest now because the pipeline
    changed; if move assignment ever moves back downstream of 03, this fake
    becomes a lie again.
    """

    def __init__(self, move_id, time=0, direction=1,
                 symbol='BTCUSDT', timeframe='15m'):
        self.move_id = move_id
        self.time = time
        self.direction = direction
        self.symbol = symbol
        self.timeframe = timeframe


def _bar_in(move, offset_bars=0, delta=900000):
    """A bar inside `move`, carrying the move id 02 would have assigned."""
    return FakeBar(move_id=move.id,
                   time=int(move.time) + offset_bars * delta,
                   direction=int(move.direction),
                   symbol=move.symbol, timeframe=move.timeframe)


def _seed(r, *moves):
    for m in moves:
        r.hset(m.id, mapping=m.model_dump(exclude_none=True, mode='json'))
        r.zadd(f'{m.symbol}:{m.timeframe}:sorted:moves', {m.id: m.time})


def test_classify_creates_the_failure():
    r = FakeRedis()
    retrace = make_move(direction=0, time=1700000000000, high=51000.0, low=50000.0)
    expansion = make_move(direction=1, time=1700002700000, high=53000.0, low=50000.0)
    _seed(r, retrace, expansion)

    zone = failures.classify(_bar_in(expansion), make_origin(), r)

    assert zone is not None
    assert zone.id == 'BTCUSDT:15m:failure:1700000000000'
    assert zone.id in r.hashes
    assert failures.recent('BTCUSDT', '15m', r) == [zone.id]


def test_classify_is_idempotent():
    """One expansion achieves several levels of the same block; one failure.

    Without the exists() guard every achieved level in the block would re-emit
    the same failure, and S4 would count one structure as many.
    """
    r = FakeRedis()
    retrace = make_move(direction=0, time=1700000000000, high=51000.0, low=50000.0)
    expansion = make_move(direction=1, time=1700002700000, high=53000.0, low=50000.0)
    _seed(r, retrace, expansion)
    bar, origin = _bar_in(expansion), make_origin()

    first = failures.classify(bar, origin, r)
    second = failures.classify(bar, origin, r)
    third = failures.classify(bar, origin, r)

    assert first is not None
    assert second is None and third is None
    assert len(r.zsets['BTCUSDT:15m:failures_by_creation']) == 1


@pytest.mark.parametrize('zone_type', ['mth', 'squeeze', 'structure'])
def test_only_origins_and_failures_confirm_a_failure(zone_type):
    """Adv 3.1 names origins and failures. MTHs are achieved constantly."""
    r = FakeRedis()
    retrace = make_move(direction=0, time=1700000000000, high=51000.0, low=50000.0)
    expansion = make_move(direction=1, time=1700002700000, high=53000.0, low=50000.0)
    _seed(r, retrace, expansion)
    other = make_origin()
    other.type = zone_type

    assert failures.classify(_bar_in(expansion), other, r) is None


def test_classify_needs_a_preceding_move():
    """The first move of a dataset has nothing behind it."""
    r = FakeRedis()
    expansion = make_move(direction=1, time=1700002700000, high=53000.0, low=50000.0)
    _seed(r, expansion)

    assert failures.classify(_bar_in(expansion), make_origin(), r) is None



def test_classify_without_a_move_id_does_nothing():
    """A bar with no move cannot name an expansion.

    Live this means 02 has not assigned one — a cold start or a gap. Refusing is
    right; the alternative is guessing which move a bar belongs to.
    """
    assert failures.classify(FakeBar(''), make_origin(), FakeRedis()) is None


def test_previous_move_excludes_the_expansion_itself():
    """An inclusive bound would pair the expansion with itself, and the
    same-direction check would then return None — right answer, wrong reason,
    hiding a genuinely missing move."""
    r = FakeRedis()
    retrace = make_move(direction=0, time=1700000000000, high=51000.0, low=50000.0)
    expansion = make_move(direction=1, time=1700002700000, high=53000.0, low=50000.0)
    _seed(r, retrace, expansion)

    got = failures._previous_move(expansion, r)
    assert got is not None and got.id == retrace.id
