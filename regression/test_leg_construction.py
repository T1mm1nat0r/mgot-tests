"""How a Leg is built, and the invariants it must not break.

Replaces the origin-pair chain (`_chain` + `_extend_ending` +
`_extend_to_deepest_mth` + `_apply_mth_breaks`), all removed on 2026-09-15. TA's construction, in his words:

  1. a completed origin sets the leg's direction; the leg starts at its original MTH
  2. until an opposite origin completes, each same-direction MTH beyond the
     running extreme sets the leg's end
  3. when an opposite origin completes, the last recorded end is the new leg's start
  4. a same-direction MTH beyond the *previous* leg's end extends that leg

plus the rule he diagnosed from the `legs-v3` board: an origin that ended a leg
and was then **taken out**, with no new origin on **its own side** in between,
never ended it — the earlier leg is handed back. An ending only fails if the leg
it started never became anything.

Validated against TA's verdicts on seven windows — two he raised, five picked
mechanically from boundaries the old chain had and this one does not. He judged
the old chain wrong in **all seven** and this one right in all seven. NQU6 15m:
168 legs -> 123, zero degenerate, zero adjacent same-direction.

The lessons carried over from the deleted tests, because each cost real time:
a leg must never end before it starts (the 2026-08-17 regression, 24 of 88 legs
on 15m); `start_mth_time` must never sit after `start_time` (`refresh_legs`
bounds its delete by one and its gather by the other, and on 3m they came apart
by 888 minutes); and the direction sense inverts, so every rule is asserted both
ways round.
"""

import pytest

from mgot_utils.models import Zone
from mgot_utils.processing import legs

SYM, TF = 'BTCUSDT', '15m'
T0 = 1700000000000
D = 900_000


def _origin(t, direction, mth_value, completed=None, taken_out=0, og=0.0):
    """A completed origin. `direction` is formation-side: 0 is a buy zone."""
    return Zone(
        id=f'{SYM}:{TF}:origin:{t}', symbol=SYM, timeframe=TF, type='origin',
        direction=direction, completion='complete', time=t, process_time=t + D,
        block_zero=mth_value - 50, block_one=mth_value, mth_value=mth_value,
        og_mth_value=og, mth_move_id=f'{SYM}:{TF}:move:{t}',
        time_completed=completed if completed is not None else t + D,
        time_taken_out=taken_out,
    )


def _turns(origins):
    """`turning_points`' shape: {origin id: (turn, move id)}."""
    return {o.id: (int(o.time), o.mth_move_id) for o in origins}


def _mth(process_time, direction, block_one):
    return (process_time, direction, block_one)


def build(origins, mths):
    recs = legs._build_records(origins, _turns(origins), sorted(mths))
    return legs._records_to_legs(recs, SYM, TF)


# ── rule 1: the origin sets direction, and legs alternate ────────────────────

@pytest.mark.parametrize('side,expect', [(0, 1), (1, 0)])
def test_direction_comes_from_the_origin_not_from_travel(side, expect):
    """A buy zone (stored 0) is departed travelling **up**.

    The old chain took direction from the price travel between endpoints, so an
    origin-to-origin span that happened to fall was a bearish leg even when it
    departed a buy zone — which is what produced runs of three same-direction
    legs. Both ways round, because this is the inversion the terminology note
    warns about.
    """
    o = _origin(T0, side, 100.0)
    # A leg that never moved is not a leg and is dropped, so give it one MTH of
    # its own to travel on.
    travel = 120.0 if expect == 1 else 80.0
    out = build([o], [_mth(T0 + 5 * D, expect, travel)])
    assert out and int(out[0].direction) == expect


def test_legs_strictly_alternate():
    origins = [_origin(T0, 1, 110.0), _origin(T0 + 10 * D, 0, 90.0),
               _origin(T0 + 20 * D, 1, 115.0)]
    mths = [_mth(T0 + 5 * D, 0, 95.0), _mth(T0 + 15 * D, 1, 112.0)]
    out = build(origins, mths)
    dirs = [int(l.direction) for l in out]
    assert all(a != b for a, b in zip(dirs, dirs[1:])), dirs


# ── rule 2: a same-direction MTH beyond the extreme sets the end ─────────────

@pytest.mark.parametrize('side,ext,beyond', [(1, 90.0, 80.0), (0, 110.0, 120.0)])
def test_a_same_direction_mth_beyond_the_extreme_sets_the_end(side, ext, beyond):
    o = _origin(T0, side, ext)
    leg_dir = 1 - side
    out = build([o], [_mth(T0 + 5 * D, leg_dir, beyond)])
    assert int(out[0].end_time) == T0 + 5 * D
    assert float(out[0].extreme) == beyond


def test_an_mth_short_of_the_extreme_does_not_move_the_end():
    o = _origin(T0, 1, 110.0)                  # sell zone -> bearish leg
    out = build([o], [_mth(T0 + 5 * D, 0, 100.0),      # sets the extreme
                      _mth(T0 + 8 * D, 0, 105.0)])     # higher — must not count
    assert float(out[0].extreme) == 100.0
    assert int(out[0].end_time) == T0 + 5 * D


# ── rule 3 + the takeout rule ───────────────────────────────────────────────

def test_an_opposite_origin_starts_the_next_leg_at_the_last_end():
    origins = [_origin(T0, 1, 110.0), _origin(T0 + 10 * D, 0, 90.0)]
    out = build(origins, [_mth(T0 + 5 * D, 0, 85.0),
                          _mth(T0 + 15 * D, 1, 108.0)])   # the second leg travels
    assert len(out) == 2
    assert int(out[0].end_time) == int(out[1].start_time)
    assert float(out[0].extreme) == float(out[1].origin_extreme) == 85.0


def test_an_ending_taken_out_with_nothing_since_hands_the_leg_back():
    """The leg it started never moved, so the turn failed."""
    origins = [
        _origin(T0, 1, 110.0),
        _origin(T0 + 10 * D, 0, 90.0, completed=T0 + 11 * D, taken_out=T0 + 12 * D),
        _origin(T0 + 30 * D, 0, 70.0),
    ]
    out = build(origins, [_mth(T0 + 5 * D, 0, 85.0), _mth(T0 + 20 * D, 0, 75.0)])
    assert len(out) == 1, [(int(l.start_time), int(l.direction)) for l in out]
    assert int(out[0].direction) == 0 and float(out[0].extreme) == 75.0


def test_an_ending_whose_leg_became_something_is_not_handed_back():
    """`legs-v3` window 4: the leg made an MTH of its own, so the turn was real
    whatever later happened to the origin. Deleting it erased a 155-point move."""
    origins = [
        _origin(T0, 1, 110.0),
        _origin(T0 + 10 * D, 0, 90.0, completed=T0 + 11 * D, taken_out=T0 + 20 * D),
    ]
    out = build(origins, [_mth(T0 + 5 * D, 0, 85.0), _mth(T0 + 15 * D, 1, 105.0)])
    assert len(out) == 2
    assert int(out[1].direction) == 1 and float(out[1].extreme) == 105.0


# ── invariants carried over from the deleted tests ──────────────────────────

def _messy():
    origins = [
        _origin(T0, 1, 110.0),
        _origin(T0 + 8 * D, 0, 90.0, completed=T0 + 9 * D, taken_out=T0 + 14 * D),
        _origin(T0 + 20 * D, 1, 118.0),
        _origin(T0 + 34 * D, 0, 72.0, completed=T0 + 35 * D, taken_out=T0 + 60 * D),
        _origin(T0 + 48 * D, 1, 120.0, og=125.0),
    ]
    mths = [_mth(T0 + 4 * D, 0, 88.0), _mth(T0 + 12 * D, 1, 101.0),
            _mth(T0 + 25 * D, 0, 70.0), _mth(T0 + 40 * D, 1, 119.0),
            _mth(T0 + 55 * D, 0, 60.0)]
    return build(origins, mths)


def test_a_leg_never_ends_before_it_starts():
    """Shipped broken 2026-08-17: 24 of 88 legs on 15m had end < start, and
    because legs_index is scored by start_time they also sorted wrong."""
    for leg in _messy():
        if leg.end_time:
            assert int(leg.end_time) >= int(leg.start_time)


def test_start_mth_time_never_sits_after_start_time():
    """`refresh_legs` bounds its delete by start_time and its gather by
    start_mth_time. On 3m they came apart by 888 minutes and 6 of 10 rebuilt
    legs landed before the deletion window."""
    for leg in _messy():
        assert int(leg.start_mth_time or 0) <= int(leg.start_time)


def test_start_times_are_monotonic():
    out = _messy()
    starts = [int(l.start_time) for l in out]
    assert starts == sorted(starts)


def test_no_leg_is_degenerate_and_none_are_adjacent_same_direction():
    out = _messy()
    assert all(int(l.end_time) > int(l.start_time) for l in out if l.end_time)
    dirs = [int(l.direction) for l in out]
    assert all(a != b for a, b in zip(dirs, dirs[1:])), dirs


def test_a_chained_origins_extremes_are_candidates_not_the_endpoint():
    """Resolving endpoints to `og_mth_value` shipped 2026-08-17 and was reverted:
    a chain *is* repeated retries of one structure, so it collapsed nine origins
    onto a single price. Both values are now *candidates* and whichever reaches
    furthest in the leg's own travel direction wins, which can only extend.

    TA, 2026-09-14, on why the chained-origin question need not be settled:
    *"if the legs recorded end is already more extreme than any of the origins
    mth's or og_mth_values' then it will not get replaced anyway, since the most
    extreme leg end always stays."*

    **Note the seed sets `origin_extreme` and `extreme` to the same value**, and
    picks it by travel direction. For `extreme` — the running high-water mark —
    that is right. For `origin_extreme` — where the leg *departed* — it is
    arguably backwards on a chained origin: a bearish leg departs a *high*, so
    the higher candidate is the more natural departure point, but travel
    direction selects the lower. It only bites when a chain's two values differ
    and only on `origin_extreme`. Asserted as-built rather than as-argued,
    because this is the behaviour validated across TA's seven windows.
    """
    # bearish leg (from a sell zone): travel is down, so the lower value wins
    o = _origin(T0, 1, 110.0, og=130.0)
    out = build([o], [_mth(T0 + 5 * D, 0, 80.0)])
    assert float(out[0].origin_extreme) == 110.0

    # and it *does* extend when the chain original reaches further down
    o2 = _origin(T0, 1, 110.0, og=95.0)
    out2 = build([o2], [_mth(T0 + 5 * D, 0, 80.0)])
    assert float(out2[0].origin_extreme) == 95.0

    # mirrored for a bullish leg: travel is up, so the higher value wins
    o3 = _origin(T0, 0, 90.0, og=105.0)
    out3 = build([o3], [_mth(T0 + 5 * D, 1, 140.0)])
    assert float(out3[0].origin_extreme) == 105.0


# ---------------------------------------------------------------------------
# Two chains, side by side (TA, 2026-09-15)
#
#   "Now that I look at the new legs I think this is perfect for squeeze
#    directions. However I do think I would like to have the old leg derived from
#    origin setup back too. Preferably there are 2 types of legs identified, the
#    current new one, and the previous one based on the origins."
#
# They answer different questions and disagree often — 123 against 168 on NQU6
# 15m — so the risk is not that one is wrong but that they contaminate each
# other. These pin the separation.
# ---------------------------------------------------------------------------


class _Pipe:
    """Queues calls and replays them on execute, as redis-py does."""

    def __init__(self, conn):
        self.conn, self.queued = conn, []

    def __getattr__(self, name):
        def queue(*a, **kw):
            self.queued.append((name, a, kw))
            return self
        return queue

    def execute(self):
        out = [getattr(self.conn, n)(*a, **kw) for n, a, kw in self.queued]
        self.queued.clear()
        return out


class _ChainRedis:
    """Serves `complete_origins_index`, origin hashes and move hashes."""

    def __init__(self, origins, moves):
        self.h, self.z, self._q = {}, {}, []
        for o in origins:
            self.h[o.id] = {k: str(v) for k, v in
                            o.model_dump(exclude_none=True, mode='json').items()}
            self.z.setdefault(f'{SYM}:{TF}:complete_origins_index', {})[o.id] = float(o.time)
        self.h.update(moves)

    def zrange(self, key, a, b, **kw):
        return [m for _, m in sorted((s, m) for m, s in self.z.get(key, {}).items())]

    # A direct read returns its value; a pipelined one queues. The first version
    # of this fake queued on every call and returned None, so `Leg.fetch_by_id`
    # — which reads directly — silently got nothing and the bootstrap looked
    # broken when it was working. A fake that cannot tell the two apart will
    # always mislead here.
    def hgetall(self, key):
        return dict(self.h.get(key, {}))

    def hmget(self, key, *fields):
        row = self.h.get(key, {})
        return [row.get(f) for f in fields]

    def pipeline(self, transaction=True):
        return _Pipe(self)


def _both(origins, mths):
    moves = {f'{SYM}:{TF}:move:{o.time}': {'time': str(o.time), 'length_bar': '2'}
             for o in origins}
    r = _ChainRedis(origins, moves)
    mth_legs = build(origins, mths)                       # the event-driven chain
    origin_legs = legs.build_origin_legs(SYM, TF, r)      # the origin-pair chain
    return mth_legs, origin_legs


def _scenario():
    origins = [_origin(T0, 1, 110.0), _origin(T0 + 10 * D, 0, 90.0),
               _origin(T0 + 30 * D, 1, 118.0)]
    mths = [_mth(T0 + 5 * D, 0, 85.0), _mth(T0 + 20 * D, 1, 115.0)]
    return _both(origins, mths)


def test_the_two_chains_carry_their_own_kind():
    mth_legs, origin_legs = _scenario()
    assert mth_legs and origin_legs
    assert {l.kind for l in mth_legs} == {'mth'}
    assert {l.kind for l in origin_legs} == {'origin'}


def test_the_two_chains_never_share_an_id():
    """Separate namespaces, or one chain's `hset` silently overwrites the other's
    leg — the hashes live in the same keyspace."""
    mth_legs, origin_legs = _scenario()
    assert all(':leg:' in l.id for l in mth_legs)
    assert all(':originleg:' in l.id for l in origin_legs)
    assert not ({l.id for l in mth_legs} & {l.id for l in origin_legs})


def test_kind_defaults_to_mth_for_legs_written_before_the_field():
    """Stored legs predate `kind`, and must not read as the origin chain."""
    from mgot_utils.models.leg import Leg
    old = Leg(id=f'{SYM}:{TF}:leg:{T0}', symbol=SYM, timeframe=TF, direction=1,
              start_time=T0, end_time=T0 + D, extreme=100.0, origin_extreme=90.0)
    assert old.kind == 'mth'


def test_the_origin_chain_reads_only_completed_origins():
    """That is the property worth keeping it for: it cannot disagree with the
    origins it derives from, whatever the MTH stream does."""
    origins = [_origin(T0, 1, 110.0), _origin(T0 + 10 * D, 0, 90.0)]
    moves = {f'{SYM}:{TF}:move:{o.time}': {'time': str(o.time), 'length_bar': '2'}
             for o in origins}
    bare = legs.build_origin_legs(SYM, TF, _ChainRedis(origins, moves))
    # no MTHs supplied at all, and it still produces the chain
    assert bare and bare[0].kind == 'origin'


class _BootstrapRedis(_ChainRedis):
    """Adds the marker/zcard surface the bootstrap needs, and records writes."""

    def __init__(self, origins, moves):
        super().__init__(origins, moves)
        self.kv, self.written = {}, []

    def exists(self, key):
        return 1 if key in self.kv else 0

    def set(self, key, value):
        self.kv[key] = str(value)

    def zcard(self, key):
        return len(self.z.get(key, {}))

    def zrevrangebyscore(self, key, hi, lo, start=0, num=None, **kw):
        def b(v, d):
            if isinstance(v, (int, float)):
                return float(v)
            t = str(v).lstrip('(')
            return d if t in ('+inf', '-inf') else float(t)
        lo_v, hi_v = b(lo, float('-inf')), b(hi, float('inf'))
        out = [m for _, m in sorted(((s, m) for m, s in self.z.get(key, {}).items()),
                                    reverse=True) if lo_v <= self.z[key][m] <= hi_v]
        return out[start:start + num] if num else out[start:]

    def hset(self, key, mapping=None, **kw):
        self.h[key] = {k: str(v) for k, v in (mapping or {}).items()}
        self.written.append(key)

    def zadd(self, key, mapping):
        self.z.setdefault(key, {}).update(mapping)

    def zrem(self, key, *members):
        for m in members:
            self.z.get(key, {}).pop(m, None)

    def delete(self, *keys):
        for k in keys:
            self.h.pop(k, None); self.z.pop(k, None); self.kv.pop(k, None)


def _bootstrap_fixture():
    origins = [_origin(T0, 1, 110.0), _origin(T0 + 10 * D, 0, 90.0),
               _origin(T0 + 30 * D, 1, 118.0)]
    moves = {f'{SYM}:{TF}:move:{o.time}': {'time': str(o.time), 'length_bar': '2'}
             for o in origins}
    return _BootstrapRedis(origins, moves)


def test_the_origin_chain_bootstraps_on_first_query():
    """`refresh_origin_legs` only runs when an origin completes. A symbol that has
    finished replaying and parked never fires one again, so without this its chain
    stays empty forever — NQU6 came out of the 2026-09-15 deploy with 126 MTH legs
    and zero origin legs."""
    r = _bootstrap_fixture()
    assert r.zcard(f'{SYM}:{TF}:origin_legs_index') == 0
    leg = legs.origin_leg_at_time(SYM, TF, T0 + 40 * D, r)
    assert leg is not None and leg.kind == 'origin'
    assert r.zcard(f'{SYM}:{TF}:origin_legs_index') > 0


def test_the_bootstrap_runs_once():
    """An early-history query that legitimately finds no leg must not rebuild the
    chain on every call."""
    r = _bootstrap_fixture()
    legs.origin_leg_at_time(SYM, TF, T0 + 40 * D, r)
    n = len(r.written)
    legs.origin_leg_at_time(SYM, TF, T0 - D, r)      # before any leg starts
    assert len(r.written) == n


def test_the_two_chains_bootstrap_independently():
    """Sharing a marker would let whichever ran first suppress the other."""
    r = _bootstrap_fixture()
    legs.origin_leg_at_time(SYM, TF, T0 + 40 * D, r)
    assert f'{SYM}:{TF}:origin_legs_built' in r.kv
    assert f'{SYM}:{TF}:legs_built' not in r.kv
