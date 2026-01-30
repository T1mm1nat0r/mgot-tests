"""
Synthetic bar sequence generation for testing MGOT features.

This module provides functions to generate bar sequences with known patterns
that trigger specific features: moves, peaks, MTH, origins, etc.

All generated bars include realistic OHLCV data and proper timestamps.
"""

from typing import List, Dict


def create_trending_bars(
    symbol: str,
    timeframe: str,
    direction: int,
    count: int,
    start_price: float,
    start_time: int = 1700000000000
) -> List[Dict]:
    """
    Create bars moving in one direction (for move detection tests).

    Args:
        symbol: Trading symbol
        timeframe: Timeframe (1m, 1h, etc.)
        direction: 1 for bullish, 0 for bearish
        count: Number of bars to create
        start_price: Starting price
        start_time: Starting timestamp (ms)

    Returns:
        List of bar dictionaries
    """
    bars = []
    price = start_price

    # Timeframe deltas in milliseconds
    delta_ms = {
        '1m': 60_000,
        '3m': 180_000,
        '15m': 900_000,
        '1h': 3_600_000,
        '4h': 14_400_000,
        '1d': 86_400_000
    }
    time_delta = delta_ms.get(timeframe, 3_600_000)

    for i in range(count):
        # Trending move: consistent direction
        change = 10 if direction == 1 else -10
        bar_open = price
        bar_close = price + change

        bars.append({
            'id': f'{symbol}:{timeframe}:bar:{start_time + i * time_delta}',
            'symbol': symbol,
            'timeframe': timeframe,
            'time': start_time + i * time_delta,
            'open': bar_open,
            'close': bar_close,
            'high': max(bar_open, bar_close) + abs(change) * 0.1,  # Wick above
            'low': min(bar_open, bar_close) - abs(change) * 0.1,   # Wick below
            'volume': 1000.0,
            'direction': direction
        })
        price += change

    return bars


def create_hh_pattern(
    symbol: str = 'BTCUSDT',
    timeframe: str = '1h',
    start_time: int = 1700000000000
) -> List[Dict]:
    """
    Create sequence producing Higher High (tests MTH detection).

    Pattern:
    - Up move to 50k (first peak)
    - Down move to 49k (retracement)
    - Up move to 51k (HH!)

    Returns:
        List of bars forming HH pattern
    """
    bars = []

    # First up move to 50k
    bars += create_trending_bars(symbol, timeframe, 1, 3, 48000, start_time)

    # Down move to 49k
    time_offset = len(bars) * 3_600_000
    bars += create_trending_bars(symbol, timeframe, 0, 2, 50000, start_time + time_offset)

    # Second up move to 51k (creates HH)
    time_offset = len(bars) * 3_600_000
    bars += create_trending_bars(symbol, timeframe, 1, 3, 49000, start_time + time_offset)

    return bars


def create_ll_pattern(
    symbol: str = 'BTCUSDT',
    timeframe: str = '1h',
    start_time: int = 1700000000000
) -> List[Dict]:
    """
    Create sequence producing Lower Low (tests MTH detection).

    Pattern:
    - Down move to 48k (first trough)
    - Up move to 49k (retracement)
    - Down move to 47k (LL!)

    Returns:
        List of bars forming LL pattern
    """
    bars = []

    # First down move to 48k
    bars += create_trending_bars(symbol, timeframe, 0, 3, 50000, start_time)

    # Up move to 49k
    time_offset = len(bars) * 3_600_000
    bars += create_trending_bars(symbol, timeframe, 1, 2, 48000, start_time + time_offset)

    # Second down move to 47k (creates LL)
    time_offset = len(bars) * 3_600_000
    bars += create_trending_bars(symbol, timeframe, 0, 3, 49000, start_time + time_offset)

    return bars


def create_lh_pattern(
    symbol: str = 'BTCUSDT',
    timeframe: str = '1h',
    start_time: int = 1700000000000
) -> List[Dict]:
    """
    Create sequence producing Lower High (bearish structure).

    Pattern:
    - Up move to 50k (first peak)
    - Down move to 48k
    - Up move to 49.5k (LH - lower than first peak)

    Returns:
        List of bars forming LH pattern
    """
    bars = []

    # First up move to 50k
    bars += create_trending_bars(symbol, timeframe, 1, 3, 47000, start_time)

    # Down move to 48k
    time_offset = len(bars) * 3_600_000
    bars += create_trending_bars(symbol, timeframe, 0, 2, 50000, start_time + time_offset)

    # Second up move to 49.5k (LH)
    time_offset = len(bars) * 3_600_000
    bars += create_trending_bars(symbol, timeframe, 1, 2, 48000, start_time + time_offset)

    return bars


def create_origin_pattern(
    symbol: str = 'BTCUSDT',
    timeframe: str = '1h',
    start_time: int = 1700000000000
) -> List[Dict]:
    """
    Create sequence producing origin zone (tests origin creation).

    Pattern:
    - HH pattern (MTH created)
    - 4 alternating moves after MTH (confirms MTH holds)

    Returns:
        List of bars forming origin pattern
    """
    # Start with HH pattern
    bars = create_hh_pattern(symbol, timeframe, start_time)

    # Add 4 moves after MTH to trigger origin creation
    last_price = bars[-1]['close']
    time_offset = len(bars) * 3_600_000

    for i in range(4):
        # Alternate directions
        direction = 1 if i % 2 == 0 else 0
        bars += create_trending_bars(
            symbol,
            timeframe,
            direction,
            2,
            last_price,
            start_time + time_offset + i * 2 * 3_600_000
        )
        last_price = bars[-1]['close']

    return bars


def create_gap_sequence(
    symbol: str = 'BTCUSDT',
    timeframe: str = '1h',
    start_time: int = 1700000000000
) -> List[Dict]:
    """
    Create bar sequence with gap (tests stale tracker reset).

    Pattern:
    - 3 normal bars
    - 2 hour gap (missing bars)
    - 3 more bars

    Returns:
        List of bars with gap in middle
    """
    bars = []

    # First 3 bars
    bars += create_trending_bars(symbol, timeframe, 1, 3, 50000, start_time)

    # Gap: skip 2 hours (2 bars for 1h timeframe)
    time_offset = len(bars) * 3_600_000 + 2 * 3_600_000  # Add extra 2 hours

    # Next 3 bars after gap
    bars += create_trending_bars(
        symbol,
        timeframe,
        0,
        3,
        bars[-1]['close'],
        start_time + time_offset
    )

    return bars


def create_level_achievement_sequence(
    symbol: str = 'BTCUSDT',
    timeframe: str = '1h',
    level_value: float = 50000.0,
    start_time: int = 1700000000000
) -> List[Dict]:
    """
    Create bars that cross a specific level (tests level gains/losses).

    Pattern:
    - Start below level
    - Cross above (gain)
    - Cross below (loss)
    - Cross above again (gain)

    Returns:
        List of bars crossing level
    """
    bars = []
    time_delta = 3_600_000

    # Bar 1: Below level
    bars.append({
        'id': f'{symbol}:{timeframe}:bar:{start_time}',
        'symbol': symbol,
        'timeframe': timeframe,
        'time': start_time,
        'open': level_value - 200,
        'close': level_value - 100,
        'high': level_value - 50,
        'low': level_value - 250,
        'volume': 1000.0,
        'direction': 1
    })

    # Bar 2: Cross above (GAIN)
    bars.append({
        'id': f'{symbol}:{timeframe}:bar:{start_time + time_delta}',
        'symbol': symbol,
        'timeframe': timeframe,
        'time': start_time + time_delta,
        'open': level_value - 100,
        'close': level_value + 100,
        'high': level_value + 150,
        'low': level_value - 120,
        'volume': 1000.0,
        'direction': 1
    })

    # Bar 3: Cross below (LOSS)
    bars.append({
        'id': f'{symbol}:{timeframe}:bar:{start_time + 2 * time_delta}',
        'symbol': symbol,
        'timeframe': timeframe,
        'time': start_time + 2 * time_delta,
        'open': level_value + 100,
        'close': level_value - 100,
        'high': level_value + 150,
        'low': level_value - 150,
        'volume': 1000.0,
        'direction': 0
    })

    # Bar 4: Cross above again (GAIN)
    bars.append({
        'id': f'{symbol}:{timeframe}:bar:{start_time + 3 * time_delta}',
        'symbol': symbol,
        'timeframe': timeframe,
        'time': start_time + 3 * time_delta,
        'open': level_value - 100,
        'close': level_value + 100,
        'high': level_value + 150,
        'low': level_value - 120,
        'volume': 1000.0,
        'direction': 1
    })

    return bars


def create_bullish_retest_sequence(
    symbol: str = 'BTCUSDT',
    timeframe: str = '1h',
    level_value: float = 50000.0,
    start_time: int = 1700000000000
) -> List[Dict]:
    """
    Create bars that retest level from below (tests bl_retest).

    Pattern:
    - Start below level
    - Touch level from below but close above (bullish retest)

    Returns:
        List of bars with bullish retest
    """
    time_delta = 3_600_000

    return [{
        'id': f'{symbol}:{timeframe}:bar:{start_time}',
        'symbol': symbol,
        'timeframe': timeframe,
        'time': start_time,
        'open': level_value - 100,
        'close': level_value + 50,   # Close ABOVE level
        'high': level_value + 100,
        'low': level_value - 20,     # Low BELOW level (touched)
        'volume': 1000.0,
        'direction': 1
    }]


def create_bearish_retest_sequence(
    symbol: str = 'BTCUSDT',
    timeframe: str = '1h',
    level_value: float = 50000.0,
    start_time: int = 1700000000000
) -> List[Dict]:
    """
    Create bars that retest level from above (tests br_retest).

    Pattern:
    - Start above level
    - Touch level from above but close below (bearish retest)

    Returns:
        List of bars with bearish retest
    """
    time_delta = 3_600_000

    return [{
        'id': f'{symbol}:{timeframe}:bar:{start_time}',
        'symbol': symbol,
        'timeframe': timeframe,
        'time': start_time,
        'open': level_value + 100,
        'close': level_value - 50,   # Close BELOW level
        'high': level_value + 20,    # High ABOVE level (touched)
        'low': level_value - 100,
        'volume': 1000.0,
        'direction': 0
    }]


def create_zone_completion_sequence(
    symbol: str = 'BTCUSDT',
    timeframe: str = '1h',
    zone_type: str = 'mth',
    direction: int = 1,
    start_time: int = 1700000000000
) -> tuple[Dict, List[Dict]]:
    """
    Create zone and bar sequence that completes the zone.

    For MTH zones:
    - Bullish: block_one broken (top broken)
    - Bearish: block_zero broken (bottom broken)

    For Origin zones:
    - Bullish: block_one broken first (opposite level)
    - Bearish: block_zero broken first (opposite level)

    Returns:
        Tuple of (zone_dict, bars_list)
    """
    # Create zone
    if zone_type == 'mth':
        zone = {
            'id': f'{symbol}:{timeframe}:mth:{start_time}',
            'symbol': symbol,
            'timeframe': timeframe,
            'type': 'mth',
            'direction': direction,
            'completion': 'incomplete',
            'block_zero': 49500.0 if direction == 1 else 50500.0,
            'block_one': 50500.0 if direction == 1 else 49500.0,
        }
    else:  # origin
        zone = {
            'id': f'{symbol}:{timeframe}:origin:{start_time}',
            'symbol': symbol,
            'timeframe': timeframe,
            'type': 'origin',
            'direction': direction,
            'completion': 'incomplete',
            'block_zero': 50500.0 if direction == 1 else 49500.0,  # Swapped for origins
            'block_one': 49500.0 if direction == 1 else 50500.0,
            'block_half': 50000.0,
        }

    # Create bars that complete zone
    if zone_type == 'mth':
        # MTH completion: break base level
        target = zone['block_one'] if direction == 1 else zone['block_zero']
        cross_direction = 1 if direction == 1 else 0
    else:
        # Origin completion: break opposite level
        target = zone['block_one'] if direction == 1 else zone['block_zero']
        cross_direction = 0 if direction == 1 else 1

    bars = create_trending_bars(
        symbol,
        timeframe,
        cross_direction,
        3,
        target - 200 if cross_direction == 1 else target + 200,
        start_time + 7_200_000  # 2 hours after zone creation
    )

    return zone, bars
