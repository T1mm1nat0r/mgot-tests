"""
What one module writes into a hash must be what another module reads out of it.

Redis hashes are schemaless, so a misspelled field name is not an error
anywhere: the write succeeds, the read returns nothing, and the model supplies
its default. `04_peaks_and_structure.process_pso` wrote the original MTH value
into `og_move_value` for months. The `Move` model carries `og_mth_value`, and
that is what `origins._create_origin_zone` reads, so the value landed in a field
nothing reads and every origin chain lost its anchor at the first hop. Zero
origins in production carried a chained value; after the one-word fix, 94 of 311
on 15m did.

Nothing could have caught that at runtime. There is no exception to raise — 0.0
is a perfectly good `og_mth_value`. The only place the mistake is visible is in
the pairing of the two files, which is what these tests read.

Two shapes are covered:

  * a literal field name `hset` onto a hash that a **model** owns — the field
    must be declared on that model (`test_hset_fields_are_declared_on_the_model`)
  * a plain hash passed between modules with no model at all — every field the
    reader asks for must be one the writer supplies
    (`test_plain_hash_readers_only_ask_for_fields_the_writer_writes`)

The tests read source rather than running the pipeline on purpose: the bug's
whole character is that running it produces no complaint.
"""

import ast
import re
from functools import lru_cache
from pathlib import Path

import pytest

from mgot_utils.models import Zone
from mgot_utils.models.bar import Bar
from mgot_utils.models.leg import Leg
from mgot_utils.models.level import Level
from mgot_utils.models.move import Move
from mgot_utils.models.enums import Completion

REPO = Path(__file__).resolve().parents[2]

MODELS = {'Bar': Bar, 'Move': Move, 'Zone': Zone, 'Level': Level, 'Leg': Leg}


def _read(relative):
    return (REPO / relative).read_text()


def _tree(relative):
    return ast.parse(_read(relative))


# ── model-owned hashes ───────────────────────────────────────

# (writer file, hash target expression, field, model, reader file, reader expression)
#
# Each row is one cross-module handoff. The writer names the field as a string;
# the reader gets it through the model, where a typo becomes a silent default.
HSET_HANDOFFS = [
    pytest.param(
        '04_peaks_and_structure/main.py', 'top_row.move_id', 'og_mth_value',
        'Move', 'utils/src/mgot_utils/processing/origins.py',
        'peak_move.og_mth_value',
        id='og_mth_value: process_pso -> Move -> _create_origin_zone',
    ),
    pytest.param(
        'utils/src/mgot_utils/models/zone.py', 'temp_bar_id', 'origin',
        'Bar', 'utils/src/mgot_utils/core/console_tools.py',
        "create_markers(chart, 'origin'",
        id='origin flag: Zone.mark_complete -> Bar -> console markers',
    ),
    pytest.param(
        'utils/src/mgot_utils/processing/squeeze.py', 'zone.id', 'completion',
        'Zone', 'utils/src/mgot_utils/processing/s2.py',
        "('invalid', 'absorbed')",
        id='completion=absorbed: expand_block -> Zone -> s2 SS candidate scan',
    ),
]


@pytest.mark.parametrize(
    'writer,target,field,model_name,reader,reader_expr', HSET_HANDOFFS)
def test_hset_fields_are_declared_on_the_model(
        writer, target, field, model_name, reader, reader_expr):
    """The writer's literal, the model's field and the reader's attribute agree.

    All three legs are checked. Asserting only that the model has the field
    would pass while the writer spelled it `og_move_value`; asserting only the
    writer's literal would pass after someone renamed the model field.
    """
    model = MODELS[model_name]

    written = _hset_literal_fields(writer, target)
    assert field in written, (
        f'{writer} no longer writes {field!r} to {target} — it writes '
        f'{sorted(written)}. Either the handoff moved or the name drifted.'
    )

    assert field in model.model_fields, (
        f'{writer} writes {field!r} to a {model_name} hash, but {model_name} '
        f'has no such field, so every read of it returns the default and the '
        f'value is lost. {model_name} declares: {sorted(model.model_fields)}'
    )

    assert reader_expr in _read(reader), (
        f'{reader} no longer contains {reader_expr!r}; the read side of the '
        f'{field!r} handoff has moved and this contract is now checking nothing'
    )


def _hset_literal_fields(relative, target):
    """Field names `hset` as a literal onto `target` in one file.

    Matches the three-argument form `hset(key, 'field', value)`, which is the
    one that takes a bare string and is therefore the one a typo survives.
    """
    text = _read(relative)
    tree = ast.parse(text)
    fields = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == 'hset' and len(node.args) >= 2):
            continue
        if (ast.get_source_segment(text, node.args[0]) or '') != target:
            continue
        if isinstance(node.args[1], ast.Constant) and isinstance(node.args[1].value, str):
            fields.add(node.args[1].value)
    return fields


def test_og_mth_value_survives_every_hop_of_the_origin_chain():
    """The chained-origin value crosses four modules; hop three was the broken one.

    A secondary origin inherits the *original* MTH value rather than the MTH it
    was drawn from, so a chain of secondaries stays anchored to where the move
    began. The value travels:

        1. origins._create_origin_zone   peak_move.og_mth_value -> Zone
        2. post_process                  zone.og_mth_value -> secondary_origin hash
        3. 04.process_pso                secondary hash -> Move hash   <- typo lived here
        4. origins._create_origin_zone   Move -> the next Zone

    Hop 3 spelled it `og_move_value`. Nothing raised, nothing logged, and the
    chain simply restarted at every secondary: zero origins in production
    carried a chained value, against 94 of 311 on 15m once fixed. Each hop below
    is asserted separately so a break names the hop.
    """
    origins = _read('utils/src/mgot_utils/processing/origins.py')
    post_process = _read('utils/src/mgot_utils/processing/post_process.py')

    assert 'og_mth_value=peak_move.og_mth_value' in origins, \
        'hop 1: _create_origin_zone no longer carries the MTH move value onto the Zone'
    assert 'og_mth_value' in Zone.model_fields, 'hop 1: Zone lost the field'

    assert '"og_mth_value": zone.og_mth_value' in post_process, \
        'hop 2: the secondary_origin record no longer carries og_mth_value'
    for function, keys in _dict_literal_keys(
            'utils/src/mgot_utils/processing/post_process.py', 'secondary_origin'):
        assert 'og_mth_value' in keys, \
            f'hop 2: {function}() builds a secondary_origin without og_mth_value'

    written = _hset_literal_fields('04_peaks_and_structure/main.py', 'top_row.move_id')
    assert 'og_mth_value' in written, (
        f'hop 3: process_pso writes {sorted(written)} onto the Move hash, but '
        f'not og_mth_value — the chain is broken here again'
    )
    undeclared = written - set(Move.model_fields)
    assert not undeclared, (
        f'hop 3: process_pso writes {sorted(undeclared)} onto a Move hash and '
        f'Move declares no such field, so the value is discarded on every read. '
        f'This is the exact shape of the og_move_value bug.'
    )
    read = _hash_field_reads('04_peaks_and_structure/main.py', 'secondary_data')
    assert 'og_mth_value' in read, \
        'hop 3: process_pso no longer reads og_mth_value out of the secondary record'
    assert 'og_mth_value' in Move.model_fields, 'hop 3: Move lost the field'

    assert 'peak_move.og_mth_value' in origins, \
        'hop 4: the next origin no longer reads the chained value back off the Move'


def test_absorbed_is_a_real_completion_state():
    """`hset(zone.id, 'completion', 'absorbed')` bypasses the model entirely.

    Writing the literal skips pydantic's validation, so an invented state would
    be stored happily and then fail — or worse, silently mismatch — on the next
    `Zone.initiate_zone`. Absorption is how an MTH leaves `zones_index` while
    staying in `mth_index`, which is exactly the case a reset once got wrong.
    """
    squeeze = _read('utils/src/mgot_utils/processing/squeeze.py')
    assert "'completion', 'absorbed'" in squeeze
    assert 'absorbed' in {c.value for c in Completion}


# ── model-less hashes passed between modules ─────────────────

# (name, writer file, writer dict variable, reader file, reader dict variable)
#
# These hashes have no model, so nothing at all connects the two spellings —
# not even a default. The reader simply gets None.
PLAIN_HANDOFFS = [
    pytest.param(
        'last_origin',
        'utils/src/mgot_utils/processing/post_process.py', 'completed_org',
        'utils/src/mgot_utils/processing/structures.py', 'last_origin',
        id='last_origin: post_process -> origin_gate_open',
    ),
    pytest.param(
        'secondary_origin',
        'utils/src/mgot_utils/processing/post_process.py', 'secondary_origin',
        '04_peaks_and_structure/main.py', 'secondary_data',
        id='secondary_origin: post_process -> process_pso',
    ),
]


@pytest.mark.parametrize('name,writer,writer_var,reader,reader_var', PLAIN_HANDOFFS)
def test_plain_hash_readers_only_ask_for_fields_the_writer_writes(
        name, writer, writer_var, reader, reader_var):
    """Every field the reader asks for is written by *every live* write site.

    Per write site, not per union. `last_origin` is written from three places in
    `post_process`; a fourth that forgot `id` would leave `origin_gate_open`
    unable to resolve the previous origin, and the gate would silently fall
    through to "allowed" on every same-direction candidate.

    Write sites in functions nothing calls are skipped. `post_process` keeps a
    public `potential_secondary_origin` alongside the `_`-prefixed one the
    pipeline actually uses, and the copy has already drifted — it omits
    `og_mth_move_id`. Unreachable code cannot break a run, and failing on it
    here would only train people to stop reading this test.
    """
    written = _dict_literal_keys(writer, writer_var, reachable_only=True)
    assert written, f'{writer} no longer builds a live {writer_var} dict literal'

    read = _hash_field_reads(reader, reader_var)
    assert read, f'{reader} no longer reads fields off {reader_var}'

    for function, keys in written:
        missing = read - keys
        assert not missing, (
            f'{name}: the {writer_var} written by {function}() in {writer} omits '
            f'{sorted(missing)}, which {reader} reads off {reader_var}. '
            f'A missing field reads back as None, not as an error.'
        )


SOURCE_DIRS = ['03_levels_and_zones', '04_peaks_and_structure', '07_origins',
               '10_zone_processor', '11_state_snapshotter', 'utils/src']


def _production_sources():
    """Every first-party .py file.

    Each service vendors its own copy of `mgot_utils` into `.venv`, so skipping
    those is not tidiness — a vendored copy would make every function look
    called, and every model field look declared, no matter what the real source
    says.
    """
    for directory in SOURCE_DIRS:
        for path in sorted((REPO / directory).rglob('*.py')):
            parts = path.parts
            if '__pycache__' in parts or '.venv' in parts or 'site-packages' in parts:
                continue
            yield path


@lru_cache(maxsize=None)
def _is_called(name):
    """Is this function called anywhere outside its own definition?

    Word-boundaried so `potential_secondary_origin` does not match calls to
    `_potential_secondary_origin`, which is the whole distinction here.
    """
    pattern = re.compile(r'(?<![\w])' + re.escape(name) + r'\(')
    for path in _production_sources():
        for line in path.read_text().splitlines():
            if line.lstrip().startswith('def '):
                continue
            if pattern.search(line):
                return True
    return False


def _dict_literal_keys(relative, variable, reachable_only=False):
    """`(enclosing function, key set)` for every `variable = {...}` in one file."""
    text = _read(relative)
    tree = ast.parse(text)
    out = []
    for function in ast.walk(tree):
        if not isinstance(function, ast.FunctionDef):
            continue
        if reachable_only and not _is_called(function.name):
            continue
        for node in ast.walk(function):
            if not (isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict)):
                continue
            if variable not in [t.id for t in node.targets if isinstance(t, ast.Name)]:
                continue
            out.append((function.name,
                        {k.value for k in node.value.keys
                         if isinstance(k, ast.Constant) and isinstance(k.value, str)}))
    return out


def _hash_field_reads(relative, variable):
    """String literals used to look a field up on `variable`.

    Covers `x.get('f')`, `x['f']` and `'f' in x` — the three ways this codebase
    interrogates a raw hash.
    """
    text = _read(relative)
    fields = set()
    for node in ast.walk(ast.parse(text)):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == 'get' and node.args
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == variable
                and isinstance(node.args[0], ast.Constant)):
            fields.add(node.args[0].value)
        elif (isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name)
              and node.value.id == variable
              and isinstance(node.slice, ast.Constant)
              and isinstance(node.slice.value, str)):
            fields.add(node.slice.value)
        elif isinstance(node, ast.Compare) and len(node.ops) == 1 \
                and isinstance(node.ops[0], (ast.In, ast.NotIn)) \
                and isinstance(node.comparators[0], ast.Name) \
                and node.comparators[0].id == variable \
                and isinstance(node.left, ast.Constant) \
                and isinstance(node.left.value, str):
            fields.add(node.left.value)
    return fields


def test_origin_gate_reads_only_real_zone_fields():
    """The gate interrogates the previous origin's raw hash, not a Zone object.

    `prev.get('zone_tests')` and friends are Zone fields spelled as strings, so
    they carry the same typo risk as `og_mth_value` did — and a misspelling here
    reads as "never tested", which opens the gate on origins that should be
    blocked.
    """
    read = _hash_field_reads(
        'utils/src/mgot_utils/processing/structures.py', 'prev')
    assert read, 'origin gate no longer reads the previous origin hash'
    unknown = read - set(Zone.model_fields)
    assert not unknown, (
        f'the origin gate reads {sorted(unknown)} off an origin hash, but Zone '
        f'declares no such field — those reads return None on every bar'
    )


# ── fields written onto model hashes but never declared ──────

# Field names written onto a *bar* hash that the Bar model does not declare.
# Extra hash fields are legal — Redis is schemaless and `sync_with_db` merges
# rather than replaces — but each one is a name that only agrees with its reader
# by convention. `potential_secondary` is written by
# `04_peaks_and_structure.process_pso` and read only by `console_tools`, which
# pulls it straight out of the hash, so the names happen to match today.
# Empty as of 2026-08-18: `potential_secondary` was declared on Bar, which is
# what this ledger existed to prompt. Keep the set — an empty ledger that fails
# on the *first* re-divergence is the whole point, and is stricter than deleting
# the test once it happens to pass.
UNDECLARED_BAR_FIELDS: set[str] = set()


def test_no_new_undeclared_fields_appear_on_bar_hashes():
    """A field on the hash but not on the model is one rename from being dead.

    This is the ledger, not a prohibition: the set is allowed to be non-empty,
    but growing it should be a deliberate act. `og_move_value` was exactly this
    — a name agreed between two files and nowhere else — and it survived months
    because nothing anywhere enumerated the difference.
    """
    written = set()
    for target in ('top_row.id', 'bar.id', 'temp_bar_id'):
        for relative in ('04_peaks_and_structure/main.py',
                         'utils/src/mgot_utils/models/zone.py',
                         'utils/src/mgot_utils/processing/post_process.py',
                         'utils/src/mgot_utils/processing/structures.py'):
            written |= _hset_literal_fields(relative, target)

    undeclared = written - set(Bar.model_fields)
    assert undeclared == UNDECLARED_BAR_FIELDS, (
        f'undeclared bar-hash fields changed: expected {sorted(UNDECLARED_BAR_FIELDS)}, '
        f'found {sorted(undeclared)}. Either declare the new field on Bar or add '
        f'it here with a note on which reader agrees with the spelling.'
    )


# ── state machine hashes ─────────────────────────────────────

def test_s2_reads_only_dance_fields_the_dance_writes():
    """The dance's state dict is its whole interface to the S2 driver.

    `dance_state` has no model either. `s2.advance` steers on
    `state`, `direction`, `pending_iss_id`, `entered_at` and `trigger_zone_id`;
    a field the dance stopped writing reads back as None, and `on_iss_broken`
    would simply never fire — which is how state 3 stayed unreachable across the
    whole dataset once before.
    """
    produced = set()
    for _, keys in _dict_literal_keys('utils/src/mgot_utils/processing/dance.py', 'state'):
        produced |= keys
    defaults = _default_dance_keys()
    assert produced, 'dance.py no longer builds its state dict as a literal'

    consumed = set()
    for relative, variable in (
            ('utils/src/mgot_utils/processing/s2.py', 'state'),
            ('utils/src/mgot_utils/processing/s2.py', 'waiting'),
            ('utils/src/mgot_utils/processing/s2.py', 'triggered'),
            ('utils/src/mgot_utils/processing/expectation.py', 'state')):
        consumed |= _hash_field_reads(relative, variable)
    assert consumed, 'nothing reads the dance state any more'

    missing = consumed - produced - defaults
    assert not missing, (
        f'S2 reads dance fields {sorted(missing)} that the dance never writes; '
        f'they read back as None on every bar'
    )


def _default_dance_keys():
    """Keys of the fresh-state dict `read_dance` returns when nothing is stored."""
    text = _read('utils/src/mgot_utils/processing/dance.py')
    fn = next(n for n in ast.walk(ast.parse(text))
              if isinstance(n, ast.FunctionDef) and n.name == 'read_dance')
    for node in ast.walk(fn):
        if isinstance(node, ast.Dict):
            return {k.value for k in node.keys
                    if isinstance(k, ast.Constant) and isinstance(k.value, str)}
    return set()


def test_s2_watermark_round_trips_its_own_field_names():
    """Written and read in the same module, but through two separate literals.

    The watermark is what stops a completed MTH resetting the dance twice. A
    name written in `_write_watermarks` and not read in `_read_watermarks` reads
    back as 0 forever, which makes every MTH look new.
    """
    text = _read('utils/src/mgot_utils/processing/s2.py')
    tree = ast.parse(text)
    reader = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef) and n.name == '_read_watermarks')
    read = {node.args[0].value
            for node in ast.walk(reader)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr == 'get' and node.args
            and isinstance(node.args[0], ast.Constant)}
    assert read == {'mth', 'squeeze', 'last_mth_0', 'last_mth_1'}, (
        f'the watermark fields changed to {sorted(read)}; update the S2 driver '
        f'and this contract together'
    )
    # `advance` steers on the same names; a drift between the two is silent.
    for field in read:
        assert f"'{field}'" in text or f'{field}' in text, \
            f'{field} is read from the watermark but named nowhere in advance()'
