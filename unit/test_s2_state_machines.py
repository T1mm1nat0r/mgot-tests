"""
Unit tests for the S2 state machines: the dance, the 10SK, internal SS
invalidation and the triple-touch streak.

These cover the transitions the course states explicitly and, more importantly,
the ones a naive implementation gets wrong: the dance demoting rather than
advancing on a new low, the touch counter restarting on a close beyond the
level, and an SS with no inner structure not counting as "every MTH tested".
"""

import pytest
from unittest.mock import MagicMock

from mgot_utils.core.configs import Config, MarketProfile, StockProfile
from mgot_utils.models import Level, Zone
from mgot_utils.processing import dance, ss_invalidation, tensk

SYM, TF = 'BTCUSDT', '15m'


def make_zone(zone_type='mth', direction=0, time=1000, **kw):
    base = dict(
        id=f'{SYM}:{TF}:{zone_type}:{time}', symbol=SYM, timeframe=TF,
        type=zone_type, direction=direction, time=time, process_time=time + 900000,
        block_zero=100.0, block_one=90.0, block_half=95.0, sweep_level=88.0,
    )
    base.update(kw)
    return Zone(**base)


def make_level(name='block_zero', value=100.0, direction=1, **kw):
    base = dict(id=f'{SYM}:{TF}:origin:1:{name}', zone_id=f'{SYM}:{TF}:origin:1',
                name=name, direction=direction, value=value)
    base.update(kw)
    return Level(**base)


class FakeRedis:
    """Hash-only stand-in — the dance stores a single hash per timeframe."""

    def __init__(self):
        self.hashes = {}

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, mapping=None, **kw):
        self.hashes.setdefault(key, {}).update(mapping or {})


# ── The dance ────────────────────────────────────────────────

class TestDance:
    def test_starts_in_state_one(self):
        r = FakeRedis()
        assert dance.read_dance(SYM, TF, r)['state'] == dance.STATE_1

    def test_sweep_advances_one_to_two_untriggered(self):
        r = FakeRedis()
        dance.on_new_mth(SYM, TF, make_zone(direction=0), 1000, r)
        state = dance.on_sweep(SYM, TF, make_zone('origin', 0, 2000), 2000, r)
        assert state['state'] == dance.STATE_2_UNTRIGGERED

    def test_ss_triggers_only_after_a_sweep(self):
        """An SS arriving in state 1 triggers nothing — the sweep comes first."""
        r = FakeRedis()
        dance.on_new_mth(SYM, TF, make_zone(direction=0), 1000, r)
        state = dance.on_ss_formed(SYM, TF, make_zone('squeeze', 1, 2000), 2000, r)
        assert state['state'] == dance.STATE_1

    def test_full_sequence_to_state_three(self):
        r = FakeRedis()
        squeeze = make_zone('squeeze', 1, 3000)
        dance.on_new_mth(SYM, TF, make_zone(direction=0), 1000, r)
        dance.on_sweep(SYM, TF, make_zone('origin', 0, 2000), 2000, r)
        dance.on_ss_formed(SYM, TF, squeeze, 3000, r)
        assert dance.read_dance(SYM, TF, r)['state'] == dance.STATE_2_TRIGGERED
        state = dance.on_ss_broken(SYM, TF, squeeze, 4000, r)
        assert state['state'] == dance.STATE_3

    def test_a_different_ss_breaking_does_not_advance(self):
        r = FakeRedis()
        dance.on_new_mth(SYM, TF, make_zone(direction=0), 1000, r)
        dance.on_sweep(SYM, TF, make_zone('origin', 0, 2000), 2000, r)
        dance.on_ss_formed(SYM, TF, make_zone('squeeze', 1, 3000), 3000, r)
        state = dance.on_ss_broken(SYM, TF, make_zone('squeeze', 1, 9999), 4000, r)
        assert state['state'] == dance.STATE_2_TRIGGERED

    def test_new_low_demotes_the_two(self):
        """IMAGE 17: "(2) → (1) Due to new Low" — the new low becomes the new 2."""
        r = FakeRedis()
        dance.on_new_mth(SYM, TF, make_zone(direction=0), 1000, r)
        dance.on_sweep(SYM, TF, make_zone('origin', 0, 2000, sweep_level=88.0), 2000, r)
        dance.on_ss_formed(SYM, TF, make_zone('squeeze', 1, 3000), 3000, r)
        assert dance.read_dance(SYM, TF, r)['state'] == dance.STATE_2_TRIGGERED

        state = dance.on_new_extreme(SYM, TF, 80.0, 5000, r)
        assert state['state'] == dance.STATE_2_UNTRIGGERED
        assert float(state['extreme']) == 80.0
        assert int(state['demotions']) == 1
        # The SS belonged to a level price has gone through.
        assert not state['trigger_zone_id']

    def test_a_higher_low_does_not_demote(self):
        r = FakeRedis()
        dance.on_new_mth(SYM, TF, make_zone(direction=0), 1000, r)
        dance.on_sweep(SYM, TF, make_zone('origin', 0, 2000, sweep_level=88.0), 2000, r)
        state = dance.on_new_extreme(SYM, TF, 95.0, 5000, r)
        assert int(state['demotions']) == 0

    def test_new_mth_resets_everything(self):
        r = FakeRedis()
        dance.on_new_mth(SYM, TF, make_zone(direction=0), 1000, r)
        dance.on_sweep(SYM, TF, make_zone('origin', 0, 2000), 2000, r)
        dance.on_ss_formed(SYM, TF, make_zone('squeeze', 1, 3000), 3000, r)
        dance.on_new_extreme(SYM, TF, 80.0, 4000, r)
        state = dance.on_new_mth(SYM, TF, make_zone(direction=0, time=5000), 5000, r)
        assert state['state'] == dance.STATE_1
        assert state['demotions'] == 0

    def test_a_trend_aligned_ss_does_not_trigger(self):
        """The trigger SS points at the reversal, not along the trend (TA).

        IMAGE 2 marks "SS Created / 2 State = Triggered" on an *upward* swing in
        a down 15m trend; IMAGE 17 the same. Measured against the leg chain,
        74-79% of detected squeezes run with the leg, so accepting any of them
        triggers on the wrong three quarters.
        """
        r = FakeRedis()
        dance.on_new_mth(SYM, TF, make_zone(direction=0), 1000, r, trend_direction=0)
        dance.on_sweep(SYM, TF, make_zone('origin', 0, 2000), 2000, r)
        aligned = dance.on_ss_formed(SYM, TF, make_zone('squeeze', 0, 3000), 3000, r)
        assert aligned['state'] == dance.STATE_2_UNTRIGGERED, 'trend-aligned SS must not trigger'
        reversal = dance.on_ss_formed(SYM, TF, make_zone('squeeze', 1, 3100), 3100, r)
        assert reversal['state'] == dance.STATE_2_TRIGGERED

    def test_trend_direction_overrides_the_mth_direction(self):
        """The dance tracks the trend, not whichever MTH happened to reset it."""
        r = FakeRedis()
        state = dance.on_new_mth(SYM, TF, make_zone(direction=1), 1000, r,
                                 trend_direction=0)
        assert int(state['direction']) == 0

    def test_permits_table_matches_the_spec(self):
        assert dance.permits(dance.STATE_2_UNTRIGGERED) == 'nothing'
        assert dance.permits(dance.STATE_2_TRIGGERED) == 'entries_against_the_1'
        assert dance.permits(dance.STATE_3) == 'momentum_with_breakout'


# ── The 10SK ─────────────────────────────────────────────────

class TestTenSK:
    def _levels(self, half_hits=0, one_hits=0, zero_hits=0, direction=1):
        """Bullish origin levels are achieved by being *lost*."""
        field = 'conseq_loss' if direction == 1 else 'conseq_gain'
        return (
            make_level('block_zero', 90.0, direction, **{field: zero_hits}),
            make_level('block_half', 95.0, direction, **{field: half_hits}),
            make_level('block_one', 100.0, direction, **{field: one_hits}),
        )

    def test_starts_awaiting_test(self):
        zone = make_zone('origin', 1)
        zero, half, one = self._levels()
        assert tensk.next_state('', zone, zero, half, one) == tensk.AWAITING_TEST

    def test_one_close_is_not_an_achievement(self):
        """Achievement needs two consecutive closes, not one."""
        zone = make_zone('origin', 1)
        zero, half, one = self._levels(half_hits=1)
        assert tensk.next_state(tensk.AWAITING_TEST, zone, zero, half, one) == tensk.AWAITING_TEST

    def test_half_achieved_moves_to_holding_half(self):
        zone = make_zone('origin', 1)
        zero, half, one = self._levels(half_hits=2)
        assert tensk.next_state(tensk.AWAITING_TEST, zone, zero, half, one) == tensk.HOLDING_HALF

    def test_zero_supersedes_half_and_one(self):
        zone = make_zone('origin', 1)
        zero, half, one = self._levels(half_hits=2, one_hits=2, zero_hits=2)
        assert tensk.next_state(tensk.HOLDING_HALF, zone, zero, half, one) == tensk.AT_ZERO

    def test_at_zero_only_leaves_by_reachieving_the_half(self):
        """Rule 3: after the 0, price must re-achieve the 0.5."""
        zone = make_zone('origin', 1)
        zero, half, one = self._levels(zero_hits=2)
        assert tensk.next_state(tensk.AT_ZERO, zone, zero, half, one) == tensk.AT_ZERO
        zero, half, one = self._levels(zero_hits=2, half_hits=2)
        assert tensk.next_state(tensk.AT_ZERO, zone, zero, half, one) == tensk.HOLDING_HALF

    def test_bearish_origin_uses_gains(self):
        zone = make_zone('origin', 0)
        zero, half, one = self._levels(half_hits=2, direction=0)
        assert tensk.next_state(tensk.AWAITING_TEST, zone, zero, half, one) == tensk.HOLDING_HALF

    def test_expectation_targets_the_zero_after_the_half(self):
        zone = make_zone('origin', 1, block_zero=90.0, block_half=95.0)
        rule, target = tensk.expectation_for(tensk.HOLDING_HALF, zone)
        assert rule == '10sk_half_achieved_expect_zero'
        assert target == 90.0

    def test_apply_ignores_non_origins(self):
        zone = make_zone('mth', 1)
        zero, half, one = self._levels(half_hits=2)
        assert tensk.apply(zone, zero, half, one) is False


# ── Internal SS invalidation ─────────────────────────────────

class TestInternalSSInvalidation:
    def _redis_with(self, mths):
        r = MagicMock()
        r.zrangebyscore.return_value = [m.id for m in mths]
        pipe = MagicMock()
        pipe.execute.return_value = [m.model_dump(mode='json') for m in mths]
        r.pipeline.return_value = pipe
        return r

    def test_all_inner_mths_tested_expects_a_sweep(self):
        squeeze = make_zone('squeeze', 1, 1000, move_end_time=5000)
        inner = [make_zone('mth', 1, 2000, zone_tests=1),
                 make_zone('mth', 1, 3000, zone_tests=1)]
        result = ss_invalidation.evaluate(squeeze, self._redis_with(inner))
        assert result['ss_expect_sweep'] == 1
        assert result['ss_internal_tested'] == 2

    def test_one_untested_mth_is_enough_to_hold(self):
        squeeze = make_zone('squeeze', 1, 1000, move_end_time=5000)
        inner = [make_zone('mth', 1, 2000, zone_tests=1),
                 make_zone('mth', 1, 3000, zone_tests=0)]
        result = ss_invalidation.evaluate(squeeze, self._redis_with(inner))
        assert result['ss_expect_sweep'] == 0

    def test_pre_takeout_tests_still_count(self):
        """A taken-out MTH keeps its complete-phase count in complete_zone_tests."""
        squeeze = make_zone('squeeze', 1, 1000, move_end_time=5000)
        inner = [make_zone('mth', 1, 2000, zone_tests=0, complete_zone_tests=1)]
        result = ss_invalidation.evaluate(squeeze, self._redis_with(inner))
        assert result['ss_expect_sweep'] == 1

    def test_no_inner_mths_is_not_invalidation(self):
        """Vacuous truth would flag every SS with nothing beneath it."""
        squeeze = make_zone('squeeze', 1, 1000, move_end_time=5000)
        result = ss_invalidation.evaluate(squeeze, self._redis_with([]))
        assert result['ss_expect_sweep'] == 0
        assert result['ss_internal_total'] == 0

    def test_opposite_direction_mths_are_not_counted(self):
        squeeze = make_zone('squeeze', 1, 1000, move_end_time=5000)
        inner = [make_zone('mth', 0, 2000, zone_tests=0)]
        result = ss_invalidation.evaluate(squeeze, self._redis_with(inner))
        assert result['ss_internal_total'] == 0


# ── Triple touch ─────────────────────────────────────────────

class TestTouchStreak:
    def test_two_touches_arm_the_third(self):
        lvl = make_level()
        lvl.record_touch()
        assert lvl.third_touch_pending() is False
        lvl.record_touch()
        assert lvl.third_touch_pending() is True

    def test_a_close_beyond_the_level_restarts_the_count(self):
        """IMAGE 11: "we closed 1 time above the bottomside so we don't start
        counting here" — the count restarts, it does not continue."""
        lvl = make_level()
        lvl.record_touch()
        lvl.record_touch()
        lvl.reset_touch_streak()
        assert lvl.touches_without_reachieving() == 0
        assert lvl.third_touch_pending() is False

    def test_both_achievement_paths_reset_the_streak(self):
        """There are two of them, and only patching one is a silent no-op.

        `Level.record_gain/record_loss` is what 03_levels_and_zones calls;
        `processing.achievements.gaining_lvl/losing_lvl` is what
        lvl_preprocessor calls. Patching only the second left touch_streak
        exactly equal to `tests` on all 1357 levels of a 20-day replay — a
        field that looks alive and carries no information.
        """
        from mgot_utils.models.bar import Bar
        from mgot_utils.processing.achievements import gaining_lvl, losing_lvl

        lvl = make_level(value=100.0)
        lvl.record_touch(); lvl.record_touch()
        lvl.record_gain(time_past_bar=1000, time_current_bar=2000)
        assert lvl.touch_streak == 0, 'Level.record_gain must reset the streak'

        lvl.record_touch(); lvl.record_touch()
        lvl.record_loss(time_past_bar=2000, time_current_bar=3000)
        assert lvl.touch_streak == 0, 'Level.record_loss must reset the streak'

        bar = Bar(id=f'{SYM}:{TF}:bar:4000', symbol=SYM, timeframe=TF, time=4000,
                  open=105.0, high=110.0, low=104.0, close=106.0, volume=1.0,
                  direction=1)
        lvl.record_touch(); lvl.record_touch()
        gaining_lvl(lvl, bar)
        assert lvl.touch_streak == 0, 'gaining_lvl must reset the streak'

        bar.close = 95.0
        lvl.record_touch(); lvl.record_touch()
        losing_lvl(lvl, bar)
        assert lvl.touch_streak == 0, 'losing_lvl must reset the streak'

    def test_streak_is_separate_from_lifetime_tests(self):
        lvl = make_level(tests=7)
        lvl.record_touch()
        assert lvl.tests == 7
        assert lvl.touch_streak == 1


# ── Market profile sweep policy ──────────────────────────────

class TestSweepPolicy:
    def test_crypto_requires_piercing_the_wick_extreme(self):
        profile = Config().profile_for('BTCUSDT')
        assert profile.sweep_allows_wick_entry is False
        assert profile.sweep_threshold(100.0, 95.0) == 100.0

    def test_stocks_accept_reaching_the_body_edge(self):
        """Adv 4.1: "if the price moves WITHIN the candle's low and its wick,
        I consider it a valid sweep" — scoped to stock trading."""
        profile = StockProfile(Config().delta_epoch)
        assert profile.sweep_threshold(100.0, 95.0) == 95.0

    def test_relaxed_profile_falls_back_when_the_body_is_unknown(self):
        profile = StockProfile(Config().delta_epoch)
        assert profile.sweep_threshold(100.0, None) == 100.0
