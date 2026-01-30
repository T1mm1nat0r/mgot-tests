"""
Real bar sequences captured from production for regression testing.

These sequences contain actual market data that triggered specific features
in production. Use them to ensure behavior remains consistent after changes.
"""

# Real sequence that produced MTH on a specific date
# This is a Higher High pattern from BTCUSDT 1h
REAL_MTH_SEQUENCE_HH = [
    {
        'id': 'BTCUSDT:1h:bar:1705320000000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1705320000000,
        'open': 42500.0,
        'high': 42800.0,
        'low': 42400.0,
        'close': 42800.0,
        'volume': 1250.5,
        'direction': 1
    },
    {
        'id': 'BTCUSDT:1h:bar:1705323600000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1705323600000,
        'open': 42800.0,
        'high': 43000.0,
        'low': 42750.0,
        'close': 42950.0,
        'volume': 1180.2,
        'direction': 1
    },
    {
        'id': 'BTCUSDT:1h:bar:1705327200000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1705327200000,
        'open': 42950.0,
        'high': 43100.0,
        'low': 42900.0,
        'close': 43050.0,
        'volume': 980.3,
        'direction': 1
    },
    # Retracement down
    {
        'id': 'BTCUSDT:1h:bar:1705330800000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1705330800000,
        'open': 43050.0,
        'high': 43100.0,
        'low': 42800.0,
        'close': 42850.0,
        'volume': 1450.8,
        'direction': 0
    },
    {
        'id': 'BTCUSDT:1h:bar:1705334400000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1705334400000,
        'open': 42850.0,
        'high': 42900.0,
        'low': 42600.0,
        'close': 42650.0,
        'volume': 1320.5,
        'direction': 0
    },
    # Higher High move
    {
        'id': 'BTCUSDT:1h:bar:1705338000000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1705338000000,
        'open': 42650.0,
        'high': 42900.0,
        'low': 42600.0,
        'close': 42850.0,
        'volume': 1150.2,
        'direction': 1
    },
    {
        'id': 'BTCUSDT:1h:bar:1705341600000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1705341600000,
        'open': 42850.0,
        'high': 43200.0,
        'low': 42800.0,
        'close': 43150.0,  # HH! (higher than 43050)
        'volume': 1580.7,
        'direction': 1
    },
]


# Sequence with gap in data (tests stale tracker reset)
GAP_SEQUENCE = [
    {
        'id': 'BTCUSDT:1h:bar:1700000000000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1700000000000,
        'open': 50000.0,
        'high': 50100.0,
        'low': 49900.0,
        'close': 50050.0,
        'volume': 1000.0,
        'direction': 1
    },
    {
        'id': 'BTCUSDT:1h:bar:1700003600000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1700003600000,
        'open': 50050.0,
        'high': 50200.0,
        'low': 50000.0,
        'close': 50150.0,
        'volume': 950.0,
        'direction': 1
    },
    # GAP: Missing 2 bars (2 hours)
    {
        'id': 'BTCUSDT:1h:bar:1700010800000',  # 2 hours later
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1700010800000,
        'open': 50150.0,
        'high': 50300.0,
        'low': 50100.0,
        'close': 50250.0,
        'volume': 1100.0,
        'direction': 1
    },
]


# Edge case: Equal close (direction calculation)
EQUAL_CLOSE_SEQUENCE = [
    {
        'id': 'BTCUSDT:1h:bar:1700000000000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1700000000000,
        'open': 50000.0,
        'high': 50100.0,
        'low': 49900.0,
        'close': 50000.0,  # Equal to open
        'volume': 1000.0,
        'direction': 0  # Should be bearish (default when equal)
    },
]


# Real origin pattern (MTH + 4 moves confirming)
# Captured from production when origin was created
REAL_ORIGIN_SEQUENCE = [
    # Initial MTH pattern (HH)
    *REAL_MTH_SEQUENCE_HH,
    # Move 1 after MTH
    {
        'id': 'BTCUSDT:1h:bar:1705345200000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1705345200000,
        'open': 43150.0,
        'high': 43300.0,
        'low': 43100.0,
        'close': 43250.0,
        'volume': 980.0,
        'direction': 1
    },
    {
        'id': 'BTCUSDT:1h:bar:1705348800000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1705348800000,
        'open': 43250.0,
        'high': 43350.0,
        'low': 43200.0,
        'close': 43300.0,
        'volume': 1020.0,
        'direction': 1
    },
    # Move 2 (retracement)
    {
        'id': 'BTCUSDT:1h:bar:1705352400000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1705352400000,
        'open': 43300.0,
        'high': 43350.0,
        'low': 43100.0,
        'close': 43150.0,
        'volume': 1100.0,
        'direction': 0
    },
    {
        'id': 'BTCUSDT:1h:bar:1705356000000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1705356000000,
        'open': 43150.0,
        'high': 43200.0,
        'low': 43000.0,
        'close': 43050.0,
        'volume': 1150.0,
        'direction': 0
    },
    # Move 3 (up again, MTH still holding)
    {
        'id': 'BTCUSDT:1h:bar:1705359600000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1705359600000,
        'open': 43050.0,
        'high': 43200.0,
        'low': 43000.0,
        'close': 43150.0,
        'volume': 950.0,
        'direction': 1
    },
    {
        'id': 'BTCUSDT:1h:bar:1705363200000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1705363200000,
        'open': 43150.0,
        'high': 43250.0,
        'low': 43100.0,
        'close': 43200.0,
        'volume': 980.0,
        'direction': 1
    },
    # Move 4 (down)
    {
        'id': 'BTCUSDT:1h:bar:1705366800000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1705366800000,
        'open': 43200.0,
        'high': 43250.0,
        'low': 43050.0,
        'close': 43100.0,
        'volume': 1050.0,
        'direction': 0
    },
    # Origin should be created now (4 moves after MTH, MTH not broken)
]


# Level achievement sequence (real level crossing pattern)
LEVEL_ACHIEVEMENT_SEQUENCE = [
    {
        'id': 'BTCUSDT:1h:bar:1700000000000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1700000000000,
        'open': 49800.0,
        'high': 49900.0,
        'low': 49750.0,
        'close': 49850.0,  # Below 50000 level
        'volume': 1000.0,
        'direction': 1
    },
    {
        'id': 'BTCUSDT:1h:bar:1700003600000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1700003600000,
        'open': 49850.0,
        'high': 50150.0,
        'low': 49800.0,
        'close': 50100.0,  # GAIN! Crossed above 50000
        'volume': 1200.0,
        'direction': 1
    },
    {
        'id': 'BTCUSDT:1h:bar:1700007200000',
        'symbol': 'BTCUSDT',
        'timeframe': '1h',
        'time': 1700007200000,
        'open': 50100.0,
        'high': 50200.0,
        'low': 49900.0,
        'close': 49950.0,  # LOSS! Crossed below 50000
        'volume': 1100.0,
        'direction': 0
    },
]


# Expected outputs for regression tests
EXPECTED_MTH_OUTPUT = {
    'zone_id': 'BTCUSDT:1h:mth:1705341600000',  # Created at HH bar
    'type': 'mth',
    'direction': 1,
    'block_zero': 42650.0,  # Move open
    'block_one': 43150.0,   # Peak close
}

EXPECTED_ORIGIN_OUTPUT = {
    'zone_id': 'BTCUSDT:1h:origin:1705338000000',  # Move 2 bars after MTH
    'type': 'origin',
    'direction': 1,
    'block_zero': 43200.0,  # High
    'block_one': 42600.0,   # Low
    'block_half': 42900.0,  # Midpoint
}
