"""Which higher-timeframe bars a 1m bar closes — `windows_closed_by`.

`02` used to fire on the clock: a window closed when its final minute arrived,
and was built only if every minute in it existed. Both assumptions hold on
Binance, which prints a kline for every minute. Neither holds on NQ:

  * Massive prints only minutes that traded, so a quiet final minute meant the
    window never closed — and a quiet minute anywhere meant it was dropped;
  * the daily pause sat inside the 20:00 UTC 4h window, so that bar was dropped
    every day, and every daily and weekly bar with it.

Windows now come from the market profile (NQ's counted from the 18:00 ET session
open and cut at its close), and a window whose final minute printed nothing is
closed by the first minute after it. BTC's windows and triggers are unchanged —
pinned below against `calc_modulos`, the old trigger.
"""

import datetime as dt
from zoneinfo import ZoneInfo

from mgot_utils.core.configs import Config
from mgot_utils.core.timeframe_logic import calc_modulos, resample_bars, windows_closed_by

MINUTE = 60_000
HIGHER = ['3m', '15m', '1h', '4h', '1d', '1w']
NQ = Config().profile_for('NQU6')
BTC = Config().profile_for('BTCUSDT')


def at(y, m, d, hh, mm=0):
    return int(dt.datetime(y, m, d, hh, mm, tzinfo=ZoneInfo('America/New_York')).timestamp() * 1000)


def closed(profile, previous, current):
    return [(tf, opened) for tf, opened, _ in windows_closed_by(profile, previous, current, HIGHER)]


class TestContinuousIsUnchanged:
    def test_every_minute_triggers_exactly_what_calc_modulos_did(self):
        config = Config()
        start = 1_782_864_000_000                      # 2026-07-01 00:00 UTC
        for t in range(start, start + 8 * 86_400_000, MINUTE):
            got = [tf for tf, _ in closed(BTC, t - MINUTE, t)]
            want = [tf for tf in HIGHER if calc_modulos(t, tf, config) == 0]
            assert got == want, t

    def test_a_window_is_named_for_its_first_minute(self):
        t = 1_782_864_000_000 + 15 * MINUTE - MINUTE   # last minute of 00:00-00:15
        assert ('15m', 1_782_864_000_000) in closed(BTC, t - MINUTE, t)


class TestSessionWindows:
    def test_the_last_minute_before_the_pause_closes_the_4h_bar_and_the_day(self):
        assert closed(NQ, at(2026, 7, 8, 16, 58), at(2026, 7, 8, 16, 59)) == [
            ('3m', at(2026, 7, 8, 16, 57)), ('15m', at(2026, 7, 8, 16, 45)),
            ('1h', at(2026, 7, 8, 16)), ('4h', at(2026, 7, 8, 14)), ('1d', at(2026, 7, 7, 18))]

    def test_friday_close_also_closes_the_week(self):
        tfs = [tf for tf, _ in closed(NQ, at(2026, 7, 10, 16, 58), at(2026, 7, 10, 16, 59))]
        assert tfs == ['3m', '15m', '1h', '4h', '1d', '1w']

    def test_no_window_straddles_the_pause(self):
        """The first minute after the pause closes nothing: the 4h bar that
        used to span it now ends at 17:00 and was closed on time."""
        assert closed(NQ, at(2026, 7, 8, 16, 59), at(2026, 7, 8, 18)) == []

    def test_a_4h_bar_closes_on_the_hour_mid_session(self):
        assert ('4h', at(2026, 7, 8, 6)) in closed(NQ, at(2026, 7, 8, 9, 58), at(2026, 7, 8, 9, 59))


class TestQuietMinutes:
    """Massive prints only minutes that traded. A quiet minute is not lost data."""

    def test_a_quiet_final_minute_is_closed_by_the_next_one(self):
        """3 Jul 10:28 UTC printed nothing — and 10:29 existed, so on-time
        closing handled that one. Here the *final* minute is the quiet one."""
        got = closed(NQ, at(2026, 7, 8, 10, 28), at(2026, 7, 8, 10, 30))
        assert got == [('3m', at(2026, 7, 8, 10, 27)), ('15m', at(2026, 7, 8, 10, 15))]

    def test_a_quiet_minute_before_the_pause_still_closes_the_session(self):
        got = closed(NQ, at(2026, 7, 8, 16, 57), at(2026, 7, 8, 18))
        assert got == [('3m', at(2026, 7, 8, 16, 57)), ('15m', at(2026, 7, 8, 16, 45)),
                       ('1h', at(2026, 7, 8, 16)), ('4h', at(2026, 7, 8, 14)),
                       ('1d', at(2026, 7, 7, 18))]

    def test_late_windows_come_before_what_the_minute_closes_on_time(self):
        """Stream order is close order: 10:27's 3m bar closed at 10:30, before 10:32's."""
        got = closed(NQ, at(2026, 7, 8, 10, 28), at(2026, 7, 8, 10, 32))
        assert got == [('3m', at(2026, 7, 8, 10, 27)), ('15m', at(2026, 7, 8, 10, 15)),
                       ('3m', at(2026, 7, 8, 10, 30))]

    def test_a_window_with_no_minutes_has_no_bar(self):
        """10:30-10:44 all quiet: the 15m bar at 10:30 never existed to close."""
        got = closed(NQ, at(2026, 7, 8, 10, 29), at(2026, 7, 8, 10, 45))
        assert ('15m', at(2026, 7, 8, 10, 30)) not in got

    def test_the_weekend_leaves_nothing_behind(self):
        assert closed(NQ, at(2026, 7, 10, 16, 59), at(2026, 7, 12, 18)) == []

    def test_a_fresh_run_closes_only_on_time(self):
        assert closed(NQ, None, at(2026, 7, 8, 10, 30)) == []


class TestResampleNamesTheWindow:
    def test_a_quiet_first_minute_does_not_rename_the_bar(self):
        opened = at(2026, 7, 8, 14)
        minutes = [   # newest first, as 02 passes them; 14:00 printed nothing
            {'time': opened + 2 * MINUTE, 'open': 3, 'high': 5, 'low': 2, 'close': 4, 'volume': 1},
            {'time': opened + 1 * MINUTE, 'open': 2, 'high': 3, 'low': 1, 'close': 3, 'volume': 1},
        ]
        bar = resample_bars(minutes, 'NQU6', '4h', time=opened)
        assert bar['id'] == f'NQU6:4h:bar:{opened}' and bar['time'] == opened
        assert (bar['open'], bar['high'], bar['low'], bar['close']) == (2, 5, 1, 4)

    def test_without_it_the_oldest_minute_names_the_bar_as_before(self):
        minutes = [{'time': 1120, 'open': 1, 'high': 1, 'low': 1, 'close': 1, 'volume': 1},
                   {'time': 1000, 'open': 1, 'high': 1, 'low': 1, 'close': 1, 'volume': 1}]
        assert resample_bars(minutes, 'BTCUSDT', '3m')['time'] == 1000
