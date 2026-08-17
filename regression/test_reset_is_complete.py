"""
A reset must leave nothing behind, and `clear_all_data` twice did not.

The first miss left `complete_origins_index`, `legs_index` and every leg hash,
`origins_by_mth`, all ~31k `move:*` hashes, one `sweep_level` Level per MTH zone,
and 1,619 absorbed MTH zones — absorbed zones because they leave `zones_index`
while staying in `mth_index`, so deleting the index key alone orphaned the
hashes and the next run rediscovered them. That was fixed. Hours later the same
function was found to leave `dance_state`, `dance_index`, `expectation`,
`s2_watermark` and `squeezes_by_creation`: same bug class, same function, and no
test either time.

The shape of the bug is always the same. Someone adds a key family, the writer
works, nothing reads it until long after ingestion starts, and the reset that
should have removed it looks clean. So one test is not enough, and there are
three here, attacking it from different sides:

  * `test_clear_all_data_leaves_nothing_behind` seeds one key of every family
    and proves the keyspace is empty afterwards. It catches a cleaner that
    stopped working.
  * `test_every_key_family_is_cleared` derives the families from the *source* —
    every `{symbol}:{timeframe}:...` literal in the tree — and holds them
    against what `data_cleaner` names. It catches the actual failure mode, a new
    family nobody remembered to clean, because the author of that family would
    not have remembered to seed it here either.
  * `test_cleared_timeframes_match_the_config` guards the hardcoded timeframe
    list, which is the same omission one level up.

A second symbol is seeded alongside and must survive untouched — a "cleaner"
that flushed the db would otherwise pass every assertion here.
"""

import ast
import re
from pathlib import Path

import pytest

from mgot_utils.core.configs import Config
from mgot_utils.core.data_cleaner import clear_all_data

REPO = Path(__file__).resolve().parents[2]

SYMBOL = 'BTCUSDT'
BYSTANDER = 'ETHUSDT'
T = 1700000000000

CLEANER = REPO / 'utils' / 'src' / 'mgot_utils' / 'core' / 'data_cleaner.py'

# Directories whose Redis key literals count as production usage.
SOURCE_DIRS = ['01_retriever', '02_timeframe_creator', '03_levels_and_zones',
               '04_peaks_and_structure', '07_origins', '10_zone_processor',
               '11_state_snapshotter', '20_console', '21_houston', 'utils/src']

# `{<anything>symbol}:{<anything>tf}:` followed by literal text, up to the first
# interpolation. Matches `{symbol}`, `{bar.symbol}`, `{zone.symbol}` and the
# timeframe spellings `{timeframe}`, `{tf}`, `{htf}`.
TF_KEY_RE = re.compile(
    r"""f['"]\{[A-Za-z_.]*(?:symbol|sym)\}:\{[A-Za-z_.]*(?:timeframe|tf)\}:([^'"{]*)""")

# The same, for keys hanging directly off the symbol with no timeframe.
SYMBOL_KEY_RE = re.compile(
    r"""f['"]\{[A-Za-z_.]*(?:symbol|sym)\}:([a-z_]+)['"]""")

# Level keys hang off a zone id rather than off symbol/timeframe.
LEVEL_KEY_RE = re.compile(
    r"""f['"]\{(?:zone_id|zone\.id|self\.id|origin\.id)\}:([^'"{]*)""")

# Families whose members are hashes reachable only through an index, so the
# cleaner deletes them by draining that index rather than by naming them. Each
# maps to the index family that must itself be cleared — otherwise "cleaned via
# the index" would be an unfalsifiable excuse.
CLEANED_VIA_INDEX = {
    'bar': 'bars_index',
    'mth': 'mth_index',
    'origin': 'zones_index',
    'squeeze': 'zones_index',
    'move': 'sorted:moves',
    'leg': 'legs_index',
    'dance': 'dance_index',
}

# Suffixes hanging off a zone id that are *not* Redis keys — they are members of
# a sorted set, so there is nothing to delete but the index. That claim is
# checked in `test_event_entries_stay_index_members` rather than trusted.
ZONE_SUFFIX_NOT_A_KEY = {'sweep': 'sweeps_index', 'zone_test': 'zone_tests_index'}


# ── source scanning ──────────────────────────────────────────

def _production_sources():
    for directory in SOURCE_DIRS:
        for path in sorted((REPO / directory).rglob('*.py')):
            parts = path.parts
            # Each service vendors mgot_utils into its own .venv; a vendored
            # copy would satisfy every contract here no matter what the real
            # source says.
            if {'__pycache__', '.venv', 'site-packages', 'fixtures'} & set(parts):
                continue
            if path.name == 'conftest.py' or path.name.startswith('test_'):
                continue
            yield path


def _families(text, pattern=TF_KEY_RE):
    """Literal key families in one source file, e.g. `legs_index`, `last_dir`.

    A family is the literal text after the timeframe up to the first
    interpolation, so `last_dir:{direction}_time` reduces to `last_dir` and
    `dance:{time}` to `dance`. That is the granularity the cleaner works at.
    """
    out = set()
    for match in pattern.finditer(text):
        family = match.group(1).rstrip(':*')
        if family:
            out.add(family)
    return out


def _cleaner_families(pattern=TF_KEY_RE):
    return _families(CLEANER.read_text(), pattern)


def _covers(family, cleaned):
    """Is `family` named by the cleaner, directly or by its enumerated members?

    `last_mth:{direction}` reduces to `last_mth`, which the cleaner spells out as
    `last_mth:0` and `last_mth:1`; both count.
    """
    return family in cleaned or any(c.startswith(family + ':') for c in cleaned)


# ── seeding ──────────────────────────────────────────────────

def _seed(r, symbol):
    """Write one key of every family the system creates for `symbol`.

    Returns the keys written, so the assertion can be "these and only these".
    Sorted sets and sets are given a member and hashes a field: Redis has no
    empty collections, and a key seeded without one simply would not exist.
    """
    keys = set()
    pipe = r.pipeline()

    def hash_key(key, mapping):
        pipe.hset(key, mapping=mapping)
        keys.add(key)

    def zset(key, mapping):
        pipe.zadd(key, mapping)
        keys.add(key)

    def string(key, value):
        pipe.set(key, value)
        keys.add(key)

    for timeframe in Config().timeframes:
        p = f'{symbol}:{timeframe}'

        bar_id = f'{p}:bar:{T}'
        hash_key(bar_id, {'id': bar_id, 'time': T})
        zset(f'{p}:bars_index', {bar_id: T})

        move_id = f'{p}:move:{T}'
        hash_key(move_id, {'id': move_id, 'time': T})
        zset(f'{p}:sorted:moves', {move_id: T})

        origin_id, mth_id = f'{p}:origin:{T}', f'{p}:mth:{T}'
        squeeze_id = f'{p}:squeeze:{T}'
        # An absorbed MTH is the case that broke: it leaves `zones_index` but
        # stays in `mth_index` so squeeze lookups can still find it.
        absorbed_id = f'{p}:mth:{T + 1}'

        for zone_id in (origin_id, mth_id, squeeze_id):
            hash_key(zone_id, {'id': zone_id, 'time': T})
            zset(f'{p}:zones_index', {zone_id: T})
        hash_key(absorbed_id, {'id': absorbed_id, 'completion': 'absorbed'})
        zset(f'{p}:mth_index', {mth_id: T, absorbed_id: T + 1})

        for zone_id in (origin_id, mth_id, squeeze_id, absorbed_id):
            for name in ('block_zero', 'block_one', 'block_half', 'sweep_level'):
                level_id = f'{zone_id}:{name}'
                hash_key(level_id, {'id': level_id, 'value': 1.0})
                zset(f'{level_id}:log', {'event': T})

        zset(f'{p}:lvls:to_lose', {f'{origin_id}:block_zero': 1.0})
        zset(f'{p}:lvls:to_gain', {f'{origin_id}:block_one': 2.0})
        zset(f'{p}:temp_zones', {origin_id: T})

        zset(f'{p}:zone_lows', {origin_id: 1.0})
        zset(f'{p}:zone_highs', {origin_id: 2.0})
        pipe.sadd(f'{p}:price_in_zones', origin_id)
        keys.add(f'{p}:price_in_zones')

        hash_key(f'{p}:last_mth', {'id': mth_id})
        string(f'{p}:last_mth_direction', '1')
        hash_key(f'{p}:last_origin', {'id': origin_id, 'direction': '1',
                                      'time_completed': str(T)})
        zset(f'{p}:sorted:origin', {origin_id: T})
        string(f'{p}:last_mth:0', T)
        string(f'{p}:last_mth:1', T)

        string(f'move:id_{symbol}_{timeframe}', move_id)
        string(f'move:direction_{symbol}_{timeframe}', 1)
        string(f'move:start_time_{symbol}_{timeframe}', T)

        for name in ('previous_top', 'previous_bottom'):
            for direction in (0, 1):
                string(f'{p}:{name}:{direction}', T)
        for direction in (0, 1):
            for field in ('open', 'close', 'time'):
                string(f'{p}:move_close_{direction}_{field}', T)

        leg_id = f'{p}:leg:{T}'
        hash_key(leg_id, {'id': leg_id, 'start_time': T})
        zset(f'{p}:legs_index', {leg_id: T})
        zset(f'{p}:origins_by_mth', {origin_id: T})
        string(f'{p}:legs_built', 1)
        zset(f'{p}:complete_origins_index', {origin_id: T})

        zset(f'{p}:zone_tests_index', {f'{origin_id}:zone_test:{T}': T})
        zset(f'{p}:sweeps_index', {f'{mth_id}:sweep:{T}': T})
        string(f'{p}:last_dir:0_time', T)
        string(f'{p}:last_dir:1_time', T)

        for direction in (0, 1):
            hash_key(f'{p}:secondary_origin:{direction}',
                     {'invalidation_time': T, 'direction': direction})

        hash_key(f'{p}:dance_state', {'state': '1', 'direction': '0'})
        dance_id = f'{p}:dance:{T}'
        hash_key(dance_id, {'state': '1'})
        zset(f'{p}:dance_index', {dance_id: T})
        hash_key(f'{p}:expectation', {'rule': '10sk_half_achieved_expect_zero'})
        hash_key(f'{p}:s2_watermark', {'mth': T, 'squeeze': T})
        zset(f'{p}:squeezes_by_creation', {squeeze_id: T})

    zset(f'{symbol}:state_log', {f'{symbol}:state:{T}': T})
    hash_key(f'ingestion:{symbol}:status', {'mode': 'running'})

    pipe.execute()
    return keys


# ── the tests ────────────────────────────────────────────────

@pytest.mark.integration
def test_clear_all_data_leaves_nothing_behind(redis_client):
    """Every seeded key for the symbol is gone; the other symbol is untouched."""
    seeded = _seed(redis_client, SYMBOL)
    bystander = _seed(redis_client, BYSTANDER)

    # The seed has to actually be in Redis, or "nothing left behind" is vacuous.
    missing = {k for k in seeded if not redis_client.exists(k)}
    assert not missing, f'seeding failed for {len(missing)} key(s): {sorted(missing)[:5]}'

    clear_all_data(SYMBOL, redis_client)

    remaining = set(redis_client.keys('*'))
    leftover = remaining - bystander
    assert not leftover, (
        f'clear_all_data left {len(leftover)} key(s) behind for {SYMBOL}. '
        f'Add them to clear_all_data in {CLEANER.relative_to(REPO)}:\n  '
        + '\n  '.join(sorted(leftover)[:40])
    )

    lost = bystander - remaining
    assert not lost, (
        f'clear_all_data deleted {len(lost)} key(s) belonging to {BYSTANDER}, '
        f'e.g. {sorted(lost)[:5]}'
    )


@pytest.mark.integration
def test_clear_all_data_reports_what_it_deleted(redis_client):
    """The counts are the only feedback a Houston reset gives, so they must move."""
    _seed(redis_client, SYMBOL)
    deleted = clear_all_data(SYMBOL, redis_client)
    for bucket in ('bars', 'zones', 'levels', 'logs', 'other'):
        assert deleted.get(bucket, 0) > 0, \
            f'clear_all_data reported {bucket}=0 after a fully seeded keyspace'


def test_every_key_family_is_cleared():
    """Every `{symbol}:{timeframe}:...` family in the tree is named by the cleaner.

    This is the one that would have caught both real misses. The seeded test
    above only covers families someone thought to seed; whoever adds a family
    and forgets the cleaner would forget the seed too. Reading the families out
    of the source removes that dependency.
    """
    cleaned = _cleaner_families()
    uncovered = {}
    for path in _production_sources():
        if path == CLEANER:
            continue
        for family in _families(path.read_text()):
            if _covers(family, cleaned):
                continue
            index = CLEANED_VIA_INDEX.get(family)
            if index and _covers(index, cleaned):
                continue
            uncovered.setdefault(family, set()).add(str(path.relative_to(REPO)))

    assert not uncovered, (
        'these Redis key families are written but never cleared by '
        f'clear_all_data ({CLEANER.relative_to(REPO)}):\n  '
        + '\n  '.join(f'{{symbol}}:{{timeframe}}:{fam} — written in '
                      f'{", ".join(sorted(where))}'
                      for fam, where in sorted(uncovered.items()))
    )


def test_symbol_scoped_keys_without_a_timeframe_are_cleared():
    """`{symbol}:state_log` has no timeframe segment and is easy to miss."""
    cleaned = _cleaner_families(SYMBOL_KEY_RE)
    uncovered = {}
    for path in _production_sources():
        if path == CLEANER:
            continue
        for family in _families(path.read_text(), SYMBOL_KEY_RE):
            if family not in cleaned:
                uncovered.setdefault(family, set()).add(str(path.relative_to(REPO)))
    assert not uncovered, (
        f'`{{symbol}}:...` keys written but never cleared: '
        + ', '.join(f'{fam} ({", ".join(sorted(where))})'
                    for fam, where in sorted(uncovered.items()))
    )


def test_cleared_timeframes_match_the_config():
    """The cleaner hardcodes its timeframes, so a new one is silently skipped.

    `Config.timeframes` is the definition; `clear_all_data` keeps its own copy.
    Adding `5m` to the config without touching the cleaner would leave every 5m
    key behind on reset — the same bug one level up from a missing key family.
    """
    tree = ast.parse(CLEANER.read_text())
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == 'clear_all_data')
    literals = [n for n in ast.walk(fn) if isinstance(n, ast.List)]
    assert literals, 'clear_all_data no longer holds a timeframe list literal'
    hardcoded = [ast.literal_eval(literals[0])]
    assert hardcoded[0] == Config().timeframes, (
        f'clear_all_data clears {hardcoded[0]} but Config.timeframes is '
        f'{Config().timeframes} — keys on the difference survive a reset'
    )


def test_every_zone_level_suffix_is_deleted():
    """`sweep_level` was the miss here: one orphaned Level per MTH zone, forever.

    `_delete_zone_levels` carries its own list of level names. `Zone.get_lvl_ids`
    is the definition of which Levels a zone owns, and the two drifted apart the
    moment MTH zones gained a purpose-built sweep level outside the zone body.
    """
    zone_source = (REPO / 'utils' / 'src' / 'mgot_utils' / 'models' / 'zone.py').read_text()
    owned = _families(zone_source, LEVEL_KEY_RE)
    deleted = _families(CLEANER.read_text(), LEVEL_KEY_RE)
    assert owned <= deleted, (
        'Zone owns Levels that _delete_zone_levels does not delete: '
        f'{sorted(owned - deleted)} — one orphaned key per zone, every reset'
    )
    # Nothing in the tree may hang a *key* off a zone id that the cleaner has
    # not been told about. The two exceptions are sorted-set members.
    used = set()
    for path in _production_sources():
        used |= _families(path.read_text(), LEVEL_KEY_RE)
    unexplained = used - deleted - set(ZONE_SUFFIX_NOT_A_KEY)
    assert not unexplained, (
        f'`{{zone_id}}:{sorted(unexplained)}` is written but never deleted'
    )


@pytest.mark.parametrize('suffix,index', sorted(ZONE_SUFFIX_NOT_A_KEY.items()))
def test_event_entries_stay_index_members(suffix, index):
    """The excuse for not deleting these is that they are not keys — check it.

    `{zone_id}:sweep:{time}` and `{zone_id}:zone_test:{time}` are members of
    `sweeps_index` / `zone_tests_index`, never hashes. If one ever gains an
    `hset`, it becomes a real key and every reset starts orphaning one per
    event — the `move:*` miss again, at ~31k keys.
    """
    entry_var = None
    written_as_key = []
    for path in _production_sources():
        text = path.read_text()
        if f':{suffix}:' not in text:
            continue
        tree = ast.parse(text)
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Assign) and isinstance(node.value, ast.JoinedStr)):
                continue
            segment = ast.get_source_segment(text, node.value) or ''
            if f'}}:{suffix}:{{' not in segment:
                continue
            entry_var = node.targets[0].id
            for call in ast.walk(tree):
                if (isinstance(call, ast.Call)
                        and isinstance(call.func, ast.Attribute)
                        and call.func.attr in ('hset', 'set', 'sadd', 'lpush')
                        and call.args
                        and isinstance(call.args[0], ast.Name)
                        and call.args[0].id == entry_var):
                    written_as_key.append(f'{path.name}:{call.lineno}')

    assert entry_var, f'no `:{suffix}:` entry is built anywhere — has it been renamed?'
    assert not written_as_key, (
        f'`{{zone_id}}:{suffix}:{{time}}` is now written as a Redis key at '
        f'{written_as_key}, but clear_all_data only deletes the {index} it is '
        f'indexed by — every reset would now orphan one key per event'
    )
