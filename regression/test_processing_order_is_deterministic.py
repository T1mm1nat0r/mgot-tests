"""
Zone processing order must not depend on PYTHONHASHSEED.

`post_process_zone` reads state that other zones write **in the same bar** — the
parent MTH's completion, the leg chain, `last_origin` — so the order zones are
processed in decides the outcome. Four call sites in `03_levels_and_zones`
built that order with `list(set(...))`, whose iteration order for strings is
salted per process.

Measured, 2026-08-18: replaying one 2976-bar 15m range twice, identical input,
only PYTHONHASHSEED differing, produced identical zone *sets* — nothing appeared
or disappeared — but

    og_mth_value          15 origins with different values
    potential_secondary    3 bars disagreeing
    achievements        1757 strings differing by member order alone

`og_mth_value` is the chained-origin anchor `process_pso` reads, so this was not
cosmetic. After routing every site through `_in_processing_order`, three
different seeds produced one identical digest.

This is a source-level test for the same reason the field-contract tests are:
there is nothing to raise at runtime. Both orders are perfectly good orders. The
defect is only visible in comparing two runs, which no assertion inside a single
run can do.
"""

import ast
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SERVICE = REPO / '03_levels_and_zones' / 'main.py'

# Functions whose return value feeds `post_process_zone`, directly or via
# `process_bar`. Each must hand back a deterministically ordered list.
ORDER_PRODUCERS = ('collect_affected_zones', 'process_level_sweeps')


def _tree():
    return ast.parse(SERVICE.read_text())


def _calls_in(node) -> list[str]:
    out = []
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name):
            out.append(sub.func.id)
    return out


def test_no_bare_list_of_set_survives():
    """`list(set(...))` and `list({...})` are the exact shape that caused this.

    Deliberately syntactic. A reviewer reading a diff cannot tell that
    `list(set(x))` is wrong — it looks like an ordinary dedupe — so the check
    has to be mechanical.
    """
    offenders = []
    for node in ast.walk(_tree()):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == 'list' and node.args):
            continue
        arg = node.args[0]
        is_set_call = isinstance(arg, ast.Call) and isinstance(arg.func, ast.Name) \
            and arg.func.id == 'set'
        is_set_literal = isinstance(arg, (ast.Set, ast.SetComp))
        if is_set_call or is_set_literal:
            offenders.append(node.lineno)
    assert not offenders, (
        f'{SERVICE.name} builds a list from a set at line(s) {offenders}. '
        'Set iteration order is salted per process; route it through '
        '_in_processing_order (or sorted) so replays are comparable.'
    )


@pytest.mark.parametrize('func_name', ORDER_PRODUCERS)
def test_order_producers_sort_before_returning(func_name):
    """Every producer of a zone-id list orders it explicitly."""
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            calls = _calls_in(node)
            assert '_in_processing_order' in calls or 'sorted' in calls, (
                f'{func_name} returns zone ids without ordering them. '
                'post_process_zone reads state its siblings write in the same '
                'bar, so an unordered list makes the outcome seed-dependent.'
            )
            return
    pytest.fail(f'{func_name} not found in {SERVICE.name}')


def test_combined_zone_list_is_ordered_before_processing():
    """The union at the `process_zone_updates` call site is the one that matters.

    The two producers can each be sorted and the union still arrive unordered —
    that was the original bug at what is now the `all_zone_ids` line.
    """
    for node in ast.walk(_tree()):
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == 'all_zone_ids' for t in node.targets):
            calls = _calls_in(node.value)
            assert '_in_processing_order' in calls or 'sorted' in calls, (
                'all_zone_ids is built without ordering; it is what '
                'process_zone_updates iterates.'
            )
            return
    pytest.fail('all_zone_ids assignment not found')


def test_ordering_is_oldest_first():
    """The chosen order, pinned. Zone ids end in their creation timestamp.

    Oldest-first so a zone that already exists settles before one created later
    can reference it. The order is a *choice* — determinism is the requirement,
    and this records which deterministic order shipped, so a change to it shows
    up as a failing test rather than as drifting counts in S4.
    """
    src = SERVICE.read_text()
    ns: dict = {}
    tree = _tree()
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == '_in_processing_order':
            exec(compile(ast.Module(body=[node], type_ignores=[]), '<t>', 'exec'), ns)
            break
    else:
        pytest.fail('_in_processing_order not found')

    order = ns['_in_processing_order']
    ids = [
        'BTCUSDT:15m:squeeze:1700000300000',
        'BTCUSDT:15m:origin:1700000100000',
        'BTCUSDT:15m:mth:1700000200000',
    ]
    assert order(set(ids)) == [
        'BTCUSDT:15m:origin:1700000100000',
        'BTCUSDT:15m:mth:1700000200000',
        'BTCUSDT:15m:squeeze:1700000300000',
    ]
    # Same input, any order in, one order out.
    assert order(set(reversed(ids))) == order(set(ids))
    # A malformed id must not raise — it sorts first and stays put.
    assert order({'weird', 'BTCUSDT:15m:origin:1700000100000'})[0] == 'weird'
