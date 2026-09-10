"""The session calendar behind NQ bar arithmetic.

Everything in MGOT that counts bars goes through `MarketProfile`. The continuous
profile is `time +/- delta * n`, which asserts the market never closes — true for
crypto, false for CME. On NQ that assertion breaks twice a day and twice a week:

  * the **daily pause**, 17:00-18:00 ET Monday to Thursday — 4 bars on 15m
  * the **weekly close**, Friday 17:00 ET to Sunday 18:00 ET — ~196 bars on 15m

The daily pause is the one that bites. It fires every weekday, and 4 bars is large
against every range MGOT counts in: `process_time` is +1, the level backfill cap
is 2, `ss_untested_lookback` is 3.

Validated against 4551 real `NQ=F` 15m bars over 71 days: the calendar called none
of them closed, and `advance(+1)` landed on the next real bar 4562 times out of
4564. Both misses were holidays — 3 July (Independence Day observed) and the
Labor Day weekend — which is what `holidays` is for.

**The holiday table ships empty on purpose.** A wrong holiday silently swallows
real bars, and a vendor dropping bars is indistinguishable from a closure in a gap
listing. Yahoo's NQ feed drops enough to matter, so `assert_gap_explained` refuses
to let a gap be assumed rather than checked.

Boundaries are ET wall clock and move with DST, so the tests below cross a DST
change deliberately.
"""

import datetime as dt

import pytest

from mgot_utils.core.configs import Config, MarketProfile, SessionMarketProfile

DELTA = Config().delta_epoch
FIFTEEN = 900_000
ET = 'America/New_York'


@pytest.fixture
def profile():
    return SessionMarketProfile(DELTA)


def at(y, m, d, hh, mm=0):
    """A UTC ms timestamp from an ET wall clock, DST handled."""
    from zoneinfo import ZoneInfo
    return int(dt.datetime(y, m, d, hh, mm, tzinfo=ZoneInfo(ET)).timestamp() * 1000)


class TestTheCalendar:
    def test_a_weekday_afternoon_is_open(self, profile):
        assert profile.is_open(at(2026, 7, 1, 14, 30))

    @pytest.mark.parametrize('hh,mm', [(17, 0), (17, 15), (17, 45)])
    def test_the_daily_pause_is_closed(self, profile, hh, mm):
        """17:00-18:00 ET. Observed 40 times out of 40 in the NQ=F sample."""
        assert not profile.is_open(at(2026, 7, 1, hh, mm))

    def test_the_bar_either_side_of_the_pause_is_open(self, profile):
        assert profile.is_open(at(2026, 7, 1, 16, 45))
        assert profile.is_open(at(2026, 7, 1, 18, 0))

    def test_friday_evening_is_closed(self, profile):
        assert profile.is_open(at(2026, 7, 10, 16, 45))
        assert not profile.is_open(at(2026, 7, 10, 17, 0))
        assert not profile.is_open(at(2026, 7, 10, 20, 0))

    def test_saturday_is_closed_all_day(self, profile):
        for hh in (0, 6, 12, 18, 23):
            assert not profile.is_open(at(2026, 7, 11, hh))

    def test_sunday_opens_at_1800(self, profile):
        assert not profile.is_open(at(2026, 7, 12, 17, 45))
        assert profile.is_open(at(2026, 7, 12, 18, 0))

    def test_a_holiday_closes_the_whole_day(self):
        p = SessionMarketProfile(DELTA, holidays=frozenset({dt.date(2026, 7, 3)}))
        assert not p.is_open(at(2026, 7, 3, 10, 0))
        assert p.is_open(at(2026, 7, 2, 10, 0))

    def test_the_holiday_table_is_empty_by_default(self, profile):
        """A wrong holiday swallows real bars; a missing one is caught by the
        validator. Empty is the safe default."""
        assert profile.holidays == frozenset()


class TestEarlyCloses:
    """CME shortens some days rather than closing them.

    Found by fetching real NQU6 bars: 2 of 58 gaps were unexplained by the two
    base rules, and both were 13:00 ET closes — 3 Jul 2026 (Independence Day,
    the 4th being a Saturday) and 7 Sep 2026 (Labor Day).

    **An early close is an earlier daily pause, not an early end to the day.**
    On Labor Day the market stopped at 13:00 and reopened at 18:00 like any
    evening — 24 real bars. The first implementation treated the early hour as
    "shut for the day" and swallowed all of them; that is what the
    `..._still_reopens...` test below pins. On a Friday the same rule ends the
    week early, because the weekly close is what follows the pause.
    """

    @pytest.fixture
    def profile(self):
        return SessionMarketProfile(
            DELTA, early_closes=SessionMarketProfile.CME_EARLY_CLOSES_2026)

    def test_it_closes_at_the_early_hour(self, profile):
        assert profile.is_open(at(2026, 9, 7, 12, 45))
        assert not profile.is_open(at(2026, 9, 7, 13, 0))
        assert not profile.is_open(at(2026, 9, 7, 16, 45))

    def test_a_shortened_weekday_still_reopens_that_evening(self, profile):
        """The regression guard. 24 real Labor Day bars were lost to this."""
        assert profile.is_open(at(2026, 9, 7, 18, 0))
        assert profile.is_open(at(2026, 9, 7, 23, 45))

    def test_a_shortened_friday_ends_the_week(self, profile):
        """3 Jul closed at 13:00 and did not reopen — the weekend follows."""
        assert profile.is_open(at(2026, 7, 3, 12, 45))
        assert not profile.is_open(at(2026, 7, 3, 13, 0))
        assert not profile.is_open(at(2026, 7, 3, 18, 0))
        assert profile.advance(at(2026, 7, 3, 12, 45), '15m', 1) == at(2026, 7, 5, 18, 0)

    def test_advance_steps_over_the_shortened_pause(self, profile):
        assert profile.advance(at(2026, 9, 7, 12, 45), '15m', 1) == at(2026, 9, 7, 18, 0)

    def test_an_ordinary_day_is_untouched(self, profile):
        assert profile.is_open(at(2026, 9, 8, 16, 45))
        assert profile.advance(at(2026, 9, 8, 16, 45), '15m', 1) == at(2026, 9, 8, 18, 0)

    def test_the_table_is_opt_in(self):
        """Not applied unless passed. A wrong early close swallows real bars."""
        assert SessionMarketProfile(DELTA).early_closes == {}
        assert SessionMarketProfile(DELTA).is_open(at(2026, 9, 7, 14, 0))


class TestAdvance:
    def test_inside_a_session_it_matches_the_continuous_profile(self, profile):
        cont = MarketProfile(DELTA)
        t = at(2026, 7, 1, 14, 0)
        assert profile.advance(t, '15m', 3) == cont.advance(t, '15m', 3)

    def test_it_steps_over_the_daily_pause(self, profile):
        """The whole point: +1 from the last bar before the pause is 18:00,
        not 17:00, which has no bar."""
        assert profile.advance(at(2026, 7, 1, 16, 45), '15m', 1) == at(2026, 7, 1, 18, 0)

    def test_it_steps_over_the_weekend(self, profile):
        assert profile.advance(at(2026, 7, 10, 16, 45), '15m', 1) == at(2026, 7, 12, 18, 0)

    def test_it_walks_backwards_too(self, profile):
        assert profile.advance(at(2026, 7, 12, 18, 0), '15m', -1) == at(2026, 7, 10, 16, 45)

    def test_zero_bars_is_identity(self, profile):
        t = at(2026, 7, 1, 14, 0)
        assert profile.advance(t, '15m', 0) == t

    def test_it_crosses_a_dst_boundary(self, profile):
        """US DST ended 2026-11-01. The pause is 17:00 ET on both sides, which is
        a different UTC offset — comparing UTC hours directly would drift."""
        before, after = at(2026, 10, 30, 16, 45), at(2026, 11, 2, 16, 45)
        assert not profile.is_open(at(2026, 10, 29, 17, 30))
        assert not profile.is_open(at(2026, 11, 3, 17, 30))
        assert profile.advance(before, '15m', 1) == at(2026, 11, 1, 18, 0)
        assert profile.advance(after, '15m', 1) == at(2026, 11, 2, 18, 0)

    def test_an_impossible_calendar_raises_rather_than_hangs(self):
        """Every day a holiday: the search must terminate and say so."""
        every_day = frozenset(dt.date(2026, 7, 1) + dt.timedelta(days=i) for i in range(30))
        p = SessionMarketProfile(DELTA, holidays=every_day)
        with pytest.raises(RuntimeError, match='no open session'):
            p.advance(at(2026, 7, 1, 14, 0), '15m', 1)


class TestBarSpan:
    def test_inside_a_session_it_counts_plainly(self, profile):
        a = at(2026, 7, 1, 14, 0)
        assert profile.bar_span(a, a + 4 * FIFTEEN, '15m') == 4

    def test_the_daily_pause_counts_as_one_bar(self, profile):
        """Continuous arithmetic says 5. There is one bar between them."""
        a, b = at(2026, 7, 1, 16, 45), at(2026, 7, 1, 18, 0)
        assert (b - a) // FIFTEEN == 5
        assert profile.bar_span(a, b, '15m') == 1

    def test_the_weekend_counts_as_one_bar(self, profile):
        a, b = at(2026, 7, 10, 16, 45), at(2026, 7, 12, 18, 0)
        # 49.25 h of wall clock: 197 bars of span, 196 of them absent.
        assert (b - a) // FIFTEEN == 197
        assert profile.bar_span(a, b, '15m') == 1

    def test_it_is_signed_like_the_continuous_profile(self, profile):
        a, b = at(2026, 7, 1, 14, 0), at(2026, 7, 1, 15, 0)
        assert profile.bar_span(a, b, '15m') == 4
        assert profile.bar_span(b, a, '15m') == -4

    def test_the_pause_is_invisible_above_1h(self, profile):
        """4 bars on 15m, 1 on 1h, 0 on 4h — which is why the daily pause only
        really matters on the timeframe we validate on."""
        a, b = at(2026, 7, 1, 16, 0), at(2026, 7, 1, 20, 0)
        assert profile.bar_span(a, b, '4h') == MarketProfile(DELTA).bar_span(a, b, '4h')


class TestGapValidation:
    def test_a_real_session_boundary_passes(self, profile):
        profile.assert_gap_explained(at(2026, 7, 1, 16, 45), at(2026, 7, 1, 18, 0), '15m')

    def test_a_dropped_bar_raises(self, profile):
        """A vendor dropping bars looks exactly like a closure in a gap listing.
        Yahoo's NQ feed drops enough for this to matter."""
        a, b = at(2026, 7, 1, 14, 0), at(2026, 7, 1, 15, 0)
        with pytest.raises(ValueError, match='fall inside a trading session'):
            profile.assert_gap_explained(a, b, '15m')

    def test_it_names_the_first_missing_bar(self, profile):
        a, b = at(2026, 7, 1, 14, 0), at(2026, 7, 1, 15, 0)
        with pytest.raises(ValueError, match='14:15'):
            profile.assert_gap_explained(a, b, '15m')


def test_profile_for_routes_by_product_code():
    """Wired 2026-09-10. NQ contracts get the session calendar; crypto does not.

    Prefix matching on the product code, so a new expiry needs no code change,
    and anything unrecognised falls through to continuous — a typo behaves like
    crypto rather than silently acquiring a trading calendar.
    """
    config = Config()
    for symbol in ('NQU6', 'NQZ6', 'nqz6', 'NQ'):
        assert config.profile_for(symbol).name == 'session', symbol
    for symbol in ('BTCUSDT', 'ETHUSDT', 'SOLUSDT', ''):
        assert config.profile_for(symbol).name == 'continuous', symbol


def test_routing_crypto_through_the_profile_changes_nothing():
    """The safety property that made wiring this safe to ship.

    `origins.py` and `zone_preprocessor.py` now call `profile.advance()` where
    they did `time + delta * n`. Under the continuous profile those are the same
    expression, so no crypto timestamp can move — proven here rather than
    asserted, across every timeframe and both directions.
    """
    config = Config()
    profile = config.profile_for('BTCUSDT')
    for timeframe, delta in config.delta_epoch.items():
        for anchor in (0, 1_782_864_000_000, 1_789_000_000_000):
            for n in range(-40, 41):
                assert profile.advance(anchor, timeframe, n) == anchor + delta * n


def test_the_session_profile_carries_the_known_early_closes():
    """`profile_for` hands out a profile that already knows about them.

    Found from real NQU6 bars, not assumed: 2 of 58 gaps were 13:00 ET closes.
    """
    session = Config().profile_for('NQU6')
    assert session.early_closes, 'the routed profile lost its early closes'
    assert not session.is_open(at(2026, 9, 7, 14, 0))
    assert session.is_open(at(2026, 9, 7, 18, 0))
