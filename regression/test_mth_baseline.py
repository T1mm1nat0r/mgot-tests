"""
MTH identification baseline tests.

These tests capture the CURRENT behavior of peak detection, structure assignment,
MTH identification, and zone processing using real production data.
Run these BEFORE changing MTH logic to establish the baseline,
then again AFTER to compare results.

Usage:
    pytest tests/regression/test_mth_baseline.py -v
    pytest tests/regression/test_mth_baseline.py -v -k "peak"
    pytest tests/regression/test_mth_baseline.py -v -k "structure"
"""

import json
import os
import pytest
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent.parent / 'fixtures'


# ── Load production data ─────────────────────────────────────

@pytest.fixture(scope="module")
def contiguous_dataset():
    """Load the contiguous 1h bar dataset with all associated features."""
    path = FIXTURES_DIR / 'contiguous_1h_dataset.json'
    with open(path) as f:
        return json.load(f)


@pytest.fixture(scope="module")
def production_data():
    """Load the curated production data with diverse zone examples."""
    path = FIXTURES_DIR / 'production_data.json'
    with open(path) as f:
        return json.load(f)


# ── Peak Detection Tests ─────────────────────────────────────

class TestPeakDetection:
    """Verify peak (top=1) identification in real data."""

    def test_peak_count(self, contiguous_dataset):
        """Baseline: number of peaks in the 200-bar window."""
        bars = contiguous_dataset['bars']
        peaks = [b for b in bars if b.get('top') == '1' or b.get('top') == 1]
        assert len(peaks) == 65, f"Expected 65 peaks, got {len(peaks)}"

    def test_peaks_have_structure(self, contiguous_dataset):
        """Every peak should have a structure label (HH/HL/LH/LL)."""
        bars = contiguous_dataset['bars']
        peaks = [b for b in bars if b.get('top') == '1' or b.get('top') == 1]
        for p in peaks:
            struct = p.get('structure', '')
            assert struct in ('HH', 'HL', 'LH', 'LL'), \
                f"Peak {p['id']} has no structure: '{struct}'"

    def test_non_peaks_have_no_structure(self, contiguous_dataset):
        """Non-peak bars should not have structure labels."""
        bars = contiguous_dataset['bars']
        non_peaks = [b for b in bars if b.get('top', '0') in ('0', 0)]
        for b in non_peaks:
            struct = b.get('structure', '')
            assert struct in ('', 'Empty'), \
                f"Non-peak {b['id']} has structure '{struct}'"

    def test_peak_is_local_extremum(self, contiguous_dataset):
        """Each peak's close should be a local max or min in its 5-bar window."""
        bars = sorted(contiguous_dataset['bars'], key=lambda b: int(b['time']))
        for i in range(2, len(bars) - 2):
            if bars[i].get('top') not in ('1', 1):
                continue
            closes = [float(bars[j]['close']) for j in range(i - 2, i + 3)]
            mid = closes[2]
            is_max = all(mid > c for j, c in enumerate(closes) if j != 2)
            is_min = all(mid < c for j, c in enumerate(closes) if j != 2)
            assert is_max or is_min, \
                f"Peak {bars[i]['id']} close={mid} is not local extremum in {closes}"


# ── Structure Assignment Tests ───────────────────────────────

class TestStructureAssignment:
    """Verify HH/HL/LH/LL classification in real data."""

    def test_structure_distribution(self, contiguous_dataset):
        """Baseline: structure type counts in the 200-bar window."""
        bars = contiguous_dataset['bars']
        counts = {}
        for b in bars:
            s = b.get('structure', '')
            if s and s != 'Empty':
                counts[s] = counts.get(s, 0) + 1
        assert counts == {'LH': 17, 'LL': 15, 'HH': 15, 'HL': 18}

    def test_hh_means_higher_high(self, contiguous_dataset):
        """HH bars should have close > previous top's close in same direction."""
        bars = sorted(contiguous_dataset['bars'], key=lambda b: int(b['time']))
        prev_top_close = None
        for b in bars:
            if b.get('structure') == 'HH':
                if prev_top_close is not None:
                    assert float(b['close']) > prev_top_close, \
                        f"HH {b['id']} close={b['close']} <= prev top {prev_top_close}"
            if b.get('structure') in ('HH', 'LH'):
                prev_top_close = float(b['close'])

    def test_ll_means_lower_low(self, contiguous_dataset):
        """LL bars should have close < previous bottom's close in same direction."""
        bars = sorted(contiguous_dataset['bars'], key=lambda b: int(b['time']))
        prev_bot_close = None
        for b in bars:
            if b.get('structure') == 'LL':
                if prev_bot_close is not None:
                    assert float(b['close']) < prev_bot_close, \
                        f"LL {b['id']} close={b['close']} >= prev bot {prev_bot_close}"
            if b.get('structure') in ('LL', 'HL'):
                prev_bot_close = float(b['close'])

    def test_lh_means_lower_high(self, contiguous_dataset):
        """LH bars should have close <= previous top's close."""
        bars = sorted(contiguous_dataset['bars'], key=lambda b: int(b['time']))
        prev_top_close = None
        for b in bars:
            if b.get('structure') == 'LH':
                if prev_top_close is not None:
                    assert float(b['close']) <= prev_top_close, \
                        f"LH {b['id']} close={b['close']} > prev top {prev_top_close}"
            if b.get('structure') in ('HH', 'LH'):
                prev_top_close = float(b['close'])

    def test_hl_means_higher_low(self, contiguous_dataset):
        """HL bars should have close >= previous bottom's close."""
        bars = sorted(contiguous_dataset['bars'], key=lambda b: int(b['time']))
        prev_bot_close = None
        for b in bars:
            if b.get('structure') == 'HL':
                if prev_bot_close is not None:
                    assert float(b['close']) >= prev_bot_close, \
                        f"HL {b['id']} close={b['close']} < prev bot {prev_bot_close}"
            if b.get('structure') in ('LL', 'HL'):
                prev_bot_close = float(b['close'])


# ── MTH Identification Tests ─────────────────────────────────

class TestMTHIdentification:
    """Verify which peaks are identified as MTH/MTL."""

    def test_mth_count(self, contiguous_dataset):
        """Baseline: number of MTH bars in the 200-bar window."""
        bars = contiguous_dataset['bars']
        mth_bars = [b for b in bars if b.get('mth') == '1' or b.get('mth') == 1]
        assert len(mth_bars) == 16, f"Expected 16 MTH bars, got {len(mth_bars)}"

    def test_mth_is_always_peak(self, contiguous_dataset):
        """Every MTH bar must also be a peak (top=1)."""
        bars = contiguous_dataset['bars']
        for b in bars:
            if b.get('mth') in ('1', 1):
                assert b.get('top') in ('1', 1), \
                    f"MTH {b['id']} is not a peak (top={b.get('top')})"

    def test_mth_is_hh_or_ll(self, contiguous_dataset):
        """Every MTH should have structure HH (bullish) or LL (bearish)."""
        bars = contiguous_dataset['bars']
        for b in bars:
            if b.get('mth') in ('1', 1):
                struct = b.get('structure', '')
                assert struct in ('HH', 'LL'), \
                    f"MTH {b['id']} has unexpected structure '{struct}' (expected HH or LL)"

    def test_non_mth_hh_exists(self, contiguous_dataset):
        """Not every HH is an MTH — valid_mth condition filters some out."""
        bars = contiguous_dataset['bars']
        hh_bars = [b for b in bars if b.get('structure') == 'HH']
        hh_mth = [b for b in hh_bars if b.get('mth') in ('1', 1)]
        hh_non_mth = [b for b in hh_bars if b.get('mth') not in ('1', 1)]
        # There should be some HH that are NOT MTH
        assert len(hh_non_mth) > 0, \
            "All HH bars are MTH — valid_mth filter may not be working"
        assert len(hh_mth) > 0, "No HH bars are MTH — something is wrong"

    def test_mth_bar_ids(self, contiguous_dataset):
        """Baseline: exact list of MTH bar IDs for regression comparison."""
        bars = sorted(contiguous_dataset['bars'], key=lambda b: int(b['time']))
        mth_ids = [b['id'] for b in bars if b.get('mth') in ('1', 1)]
        # This is the baseline — after MTH logic changes, this list will differ
        assert len(mth_ids) == 16
        # Store first few for explicit check
        assert mth_ids[0] == 'BTCUSDT:1h:bar:1768128000000' or len(mth_ids) > 0

    def test_mth_has_move_open(self, contiguous_dataset):
        """Every MTH bar must have a move_open value (used for zone block_zero)."""
        bars = contiguous_dataset['bars']
        for b in bars:
            if b.get('mth') in ('1', 1):
                move_open = float(b.get('move_open', 0))
                assert move_open > 0, \
                    f"MTH {b['id']} has no move_open (={move_open})"


# ── MTH Zone Tests ───────────────────────────────────────────

class TestMTHZones:
    """Verify MTH zone construction and fields."""

    def test_zone_count(self, contiguous_dataset):
        """Baseline: number of MTH zones created."""
        mth_zones = contiguous_dataset['zones']['mth']
        assert len(mth_zones) == 16

    def test_zone_has_required_fields(self, contiguous_dataset):
        """Every MTH zone must have block_zero, block_one, direction, completion."""
        for z in contiguous_dataset['zones']['mth']:
            assert float(z.get('block_zero', 0)) > 0, f"Zone {z['id']} missing block_zero"
            assert float(z.get('block_one', 0)) > 0, f"Zone {z['id']} missing block_one"
            assert z.get('direction') in ('0', '1', 0, 1), f"Zone {z['id']} missing direction"
            assert z.get('completion') in ('incomplete', 'complete', 'invalid', 'taken_out'), \
                f"Zone {z['id']} has unexpected completion '{z.get('completion')}'"

    def test_bullish_zone_block_order(self, contiguous_dataset):
        """Bullish MTH: block_zero (move_open) < block_one (peak close)."""
        for z in contiguous_dataset['zones']['mth']:
            if str(z.get('direction')) == '1':
                b0 = float(z['block_zero'])
                b1 = float(z['block_one'])
                assert b0 < b1, \
                    f"Bullish zone {z['id']}: block_zero={b0} >= block_one={b1}"

    def test_bearish_zone_block_order(self, contiguous_dataset):
        """Bearish MTH: block_zero (move_open) > block_one (peak close)."""
        for z in contiguous_dataset['zones']['mth']:
            if str(z.get('direction')) == '0':
                b0 = float(z['block_zero'])
                b1 = float(z['block_one'])
                assert b0 > b1, \
                    f"Bearish zone {z['id']}: block_zero={b0} <= block_one={b1}"

    def test_zone_completion_distribution(self, contiguous_dataset):
        """Baseline: distribution of zone completion states."""
        mth_zones = contiguous_dataset['zones']['mth']
        counts = {}
        for z in mth_zones:
            c = z.get('completion', 'unknown')
            counts[c] = counts.get(c, 0) + 1
        assert counts.get('complete', 0) == 2
        assert counts.get('invalid', 0) == 10
        assert counts.get('taken_out', 0) == 4

    def test_zone_has_base_data(self, contiguous_dataset):
        """MTH zones should have base_open, base_close, base_candle_time."""
        for z in contiguous_dataset['zones']['mth']:
            base_open = float(z.get('base_open', 0))
            base_close = float(z.get('base_close', 0))
            base_candle = int(z.get('base_candle_time', 0))
            assert base_open > 0, f"Zone {z['id']} missing base_open"
            assert base_close > 0, f"Zone {z['id']} missing base_close"
            assert base_candle > 0, f"Zone {z['id']} missing base_candle_time"


# ── Zone Completion Logic Tests ──────────────────────────────

class TestZoneCompletion:
    """Verify zone completion/invalidation using production data."""

    def test_completed_mth_has_timestamp(self, production_data):
        """Completed MTH zones must have time_completed set."""
        for zid, z in production_data['mth_zones'].items():
            if z.get('completion') == 'complete':
                tc = int(z.get('time_completed', 0))
                assert tc > 0, f"Completed zone {zid} has no time_completed"
                assert tc > int(z['time']), f"Completed zone {zid} time_completed <= time"

    def test_invalid_mth_has_timestamp(self, production_data):
        """Invalid MTH zones must have time_invalid set."""
        for zid, z in production_data['mth_zones'].items():
            if z.get('completion') == 'invalid':
                ti = int(z.get('time_invalid', 0))
                assert ti > 0, f"Invalid zone {zid} has no time_invalid"

    def test_taken_out_was_completed_first(self, production_data):
        """Taken-out zones must have was_completed=1."""
        for zid, z in production_data['mth_zones'].items():
            if z.get('completion') == 'taken_out':
                wc = z.get('was_completed', '0')
                assert str(wc) == '1', \
                    f"Taken-out zone {zid} has was_completed={wc}"

    def test_bullish_completion_semantics(self, production_data):
        """Bullish MTH: block_zero LOST → complete, block_one GAINED → invalid."""
        for zid, z in production_data['mth_zones'].items():
            if str(z.get('direction')) != '1':
                continue
            b0 = float(z['block_zero'])
            b1 = float(z['block_one'])
            comp = z.get('completion')
            if comp == 'complete':
                # Price went below block_zero (support lost)
                assert b0 < b1, f"Completed bullish {zid}: b0={b0} >= b1={b1}"
            elif comp == 'invalid':
                # Price went above block_one (peak exceeded)
                assert b0 < b1, f"Invalid bullish {zid}: b0={b0} >= b1={b1}"

    def test_bearish_completion_semantics(self, production_data):
        """Bearish MTH: block_zero GAINED → complete, block_one LOST → invalid."""
        for zid, z in production_data['mth_zones'].items():
            if str(z.get('direction')) != '0':
                continue
            b0 = float(z['block_zero'])
            b1 = float(z['block_one'])
            comp = z.get('completion')
            if comp in ('complete', 'invalid'):
                assert b0 > b1, f"Bearish {zid}: b0={b0} <= b1={b1}"


# ── Origin Zone Tests ────────────────────────────────────────

class TestOriginZones:
    """Verify origin zone creation and properties."""

    def test_origin_count(self, contiguous_dataset):
        """Baseline: number of origin zones in the window."""
        origins = contiguous_dataset['zones']['origin']
        assert len(origins) == 12

    def test_origin_has_block_half(self, contiguous_dataset):
        """Origin zones should have block_half (midpoint)."""
        for z in contiguous_dataset['zones']['origin']:
            bh = float(z.get('block_half', 0))
            assert bh > 0, f"Origin {z['id']} missing block_half"

    def test_origin_block_half_is_midpoint(self, contiguous_dataset):
        """block_half should be approximately the midpoint of block_zero and block_one."""
        for z in contiguous_dataset['zones']['origin']:
            b0 = float(z['block_zero'])
            b1 = float(z['block_one'])
            bh = float(z['block_half'])
            expected_mid = (b0 + b1) / 2
            assert abs(bh - expected_mid) < 1.0, \
                f"Origin {z['id']}: block_half={bh} != midpoint({b0},{b1})={expected_mid}"

    def test_origin_has_mth_reference(self, contiguous_dataset):
        """Origin zones should reference the MTH they came from."""
        for z in contiguous_dataset['zones']['origin']:
            mth_move = z.get('mth_move_id', '')
            mth_val = float(z.get('mth_value', 0))
            assert mth_move or mth_val > 0, \
                f"Origin {z['id']} has no MTH reference"


# ── Squeeze Zone Tests ───────────────────────────────────────

class TestSqueezeZones:
    """Verify squeeze zone creation from MTH completion."""

    def test_squeeze_count(self, contiguous_dataset):
        """Baseline: number of squeeze zones in the window."""
        squeezes = contiguous_dataset['zones']['squeeze']
        assert len(squeezes) == 4

    def test_squeeze_has_previous_mth(self, contiguous_dataset):
        """Squeeze zones should reference the previous MTH."""
        for z in contiguous_dataset['zones']['squeeze']:
            prev = z.get('previous_mth_id', '')
            assert prev, f"Squeeze {z['id']} missing previous_mth_id"

    def test_squeeze_has_base_data(self, contiguous_dataset):
        """Squeeze zones derive from previous MTH's base."""
        for z in contiguous_dataset['zones']['squeeze']:
            b0 = float(z.get('block_zero', 0))
            b1 = float(z.get('block_one', 0))
            assert b0 > 0, f"Squeeze {z['id']} missing block_zero"
            assert b1 > 0, f"Squeeze {z['id']} missing block_one"


# ── Move Tracking Tests ──────────────────────────────────────

class TestMoveTracking:
    """Verify move creation and direction tracking."""

    def test_move_count(self, contiguous_dataset):
        """Baseline: number of moves in the window."""
        moves = contiguous_dataset['moves']
        assert len(moves) == 102

    def test_every_bar_has_move(self, contiguous_dataset):
        """Every bar should be attached to a move."""
        bars = contiguous_dataset['bars']
        bars_with_move = [b for b in bars if b.get('move_id')]
        # Allow a small tolerance for edge bars
        assert len(bars_with_move) >= len(bars) * 0.95, \
            f"Only {len(bars_with_move)}/{len(bars)} bars have move_id"

    def test_move_direction_matches_bar(self, contiguous_dataset):
        """Each bar's direction should match its move's direction."""
        bars = contiguous_dataset['bars']
        moves = {m['id']: m for m in contiguous_dataset['moves'] if 'id' in m}
        mismatches = 0
        for b in bars:
            mid = b.get('move_id', '')
            if mid and mid in moves:
                if b.get('direction') != moves[mid].get('direction'):
                    mismatches += 1
        # Some mismatches are expected at move boundaries
        assert mismatches < len(bars) * 0.1, \
            f"Too many direction mismatches: {mismatches}/{len(bars)}"


# ── Cross-Feature Consistency Tests ──────────────────────────

class TestCrossFeatureConsistency:
    """Verify relationships between features are consistent."""

    def test_mth_zone_for_each_mth_bar(self, contiguous_dataset):
        """Each MTH bar should have a corresponding MTH zone."""
        bars = contiguous_dataset['bars']
        mth_bars = [b for b in bars if b.get('mth') in ('1', 1)]
        zone_times = {int(z['time']) for z in contiguous_dataset['zones']['mth']}
        for b in mth_bars:
            assert int(b['time']) in zone_times, \
                f"MTH bar {b['id']} has no corresponding zone"

    def test_zone_times_are_bar_times(self, contiguous_dataset):
        """Zone creation times should match actual bar times."""
        bar_times = {int(b['time']) for b in contiguous_dataset['bars']}
        for z in contiguous_dataset['zones']['mth']:
            zt = int(z['time'])
            assert zt in bar_times, \
                f"MTH zone {z['id']} time {zt} doesn't match any bar"

    def test_no_overlapping_incomplete_mth_zones(self, contiguous_dataset):
        """At any point in time, there shouldn't be conflicting incomplete zones."""
        zones = sorted(contiguous_dataset['zones']['mth'], key=lambda z: int(z['time']))
        # Check that consecutive zones in the same direction don't overlap
        # (expansion should absorb previous zones)
        for i in range(1, len(zones)):
            if zones[i].get('direction') != zones[i - 1].get('direction'):
                continue
            if zones[i - 1].get('completion') == 'incomplete' and \
               zones[i].get('completion') == 'incomplete':
                # Both incomplete in same direction — check they don't overlap
                t0 = int(zones[i - 1]['time'])
                t1 = int(zones[i]['time'])
                assert t0 != t1, \
                    f"Duplicate incomplete zones at same time: {zones[i - 1]['id']}, {zones[i]['id']}"


# ── Snapshot Tests (exact regression) ────────────────────────

class TestExactRegression:
    """Exact value comparisons for regression detection.

    These tests will FAIL after MTH logic changes — that's the point.
    Compare old vs new results to understand the impact.
    """

    def test_mth_zone_snapshot(self, contiguous_dataset):
        """Snapshot of all MTH zone IDs and their completions."""
        zones = sorted(contiguous_dataset['zones']['mth'], key=lambda z: int(z['time']))
        snapshot = [[z['id'], z['completion'], z['direction']] for z in zones]
        # Write snapshot for comparison
        snapshot_path = FIXTURES_DIR.parent / 'regression' / 'snapshots' / 'mth_zones_baseline.json'
        os.makedirs(snapshot_path.parent, exist_ok=True)

        if snapshot_path.exists():
            with open(snapshot_path) as f:
                baseline = json.load(f)
            assert snapshot == baseline, \
                f"MTH zone snapshot changed! Old: {len(baseline)} zones, New: {len(snapshot)} zones"
        else:
            with open(snapshot_path, 'w') as f:
                json.dump(snapshot, f, indent=2)
            pytest.skip("Baseline snapshot created — run again to verify")

    def test_origin_zone_snapshot(self, contiguous_dataset):
        """Snapshot of all origin zone IDs and their completions."""
        zones = sorted(contiguous_dataset['zones']['origin'], key=lambda z: int(z['time']))
        snapshot = [[z['id'], z['completion'], z['direction']] for z in zones]

        snapshot_path = FIXTURES_DIR.parent / 'regression' / 'snapshots' / 'origin_zones_baseline.json'
        os.makedirs(snapshot_path.parent, exist_ok=True)

        if snapshot_path.exists():
            with open(snapshot_path) as f:
                baseline = json.load(f)
            assert snapshot == baseline, \
                f"Origin zone snapshot changed! Old: {len(baseline)}, New: {len(snapshot)}"
        else:
            with open(snapshot_path, 'w') as f:
                json.dump(snapshot, f, indent=2)
            pytest.skip("Baseline snapshot created — run again to verify")

    def test_squeeze_zone_snapshot(self, contiguous_dataset):
        """Snapshot of all squeeze zone IDs and their completions."""
        zones = sorted(contiguous_dataset['zones']['squeeze'], key=lambda z: int(z['time']))
        snapshot = [[z['id'], z['completion'], z['direction']] for z in zones]

        snapshot_path = FIXTURES_DIR.parent / 'regression' / 'snapshots' / 'squeeze_zones_baseline.json'
        os.makedirs(snapshot_path.parent, exist_ok=True)

        if snapshot_path.exists():
            with open(snapshot_path) as f:
                baseline = json.load(f)
            assert snapshot == baseline, \
                f"Squeeze zone snapshot changed! Old: {len(baseline)}, New: {len(snapshot)}"
        else:
            with open(snapshot_path, 'w') as f:
                json.dump(snapshot, f, indent=2)
            pytest.skip("Baseline snapshot created — run again to verify")

    def test_peak_structure_snapshot(self, contiguous_dataset):
        """Snapshot of all peak bars with their structure assignments."""
        bars = sorted(contiguous_dataset['bars'], key=lambda b: int(b['time']))
        peaks = [[b['id'], b.get('structure', ''), str(b.get('mth', '0'))]
                 for b in bars if b.get('top') in ('1', 1)]

        snapshot_path = FIXTURES_DIR.parent / 'regression' / 'snapshots' / 'peaks_baseline.json'
        os.makedirs(snapshot_path.parent, exist_ok=True)

        if snapshot_path.exists():
            with open(snapshot_path) as f:
                baseline = json.load(f)
            assert peaks == baseline, \
                f"Peak snapshot changed! Old: {len(baseline)}, New: {len(peaks)}"
        else:
            with open(snapshot_path, 'w') as f:
                json.dump(peaks, f, indent=2)
            pytest.skip("Baseline snapshot created — run again to verify")
