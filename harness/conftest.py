"""
The harness only runs safely in a pytest session that has not already imported
mgot_utils against the live keyspace.

`connect_to_redis` caches a module-level connection pool, and the services bind
their connection at *import* time. Once anything has imported mgot_utils with
`REDIS_DB` unset, that pool — and every module-level `r` built from it — points
at db 0. Setting the env afterwards changes nothing, and rebinding the global
pool would not rebind the module-level connections that already exist, so a
replay could still write zones into live state.

That is not recoverable in-process, so these tests skip rather than run against
the wrong database. Run them on their own:

    pytest harness/
"""

import sys

import pytest


def pytest_collection_modifyitems(config, items):
    # Inspect via sys.modules — importing mgot_utils here would itself create
    # the db-0 binding this is checking for.
    functions = sys.modules.get('mgot_utils.core.functions')
    if functions is None:
        return

    pool = getattr(functions, '_redis_pool', None)
    if pool is None or pool.connection_kwargs.get('db') == 9:
        return

    skip = pytest.mark.skip(reason=(
        f'mgot_utils is already bound to db {pool.connection_kwargs.get("db")}; '
        'the replay would write to live state. Run "pytest harness/" on its own.'
    ))
    # Every test under harness/ drives a Replay, so every one of them is unsafe
    # here — naming a single file would leave a later module unguarded, which is
    # the same omission this hook exists to prevent.
    for item in items:
        item.add_marker(skip)
