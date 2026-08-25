"""
HTF link records live beside the zone, not in it.

Measured 2026-08-24: the records are ~110KB on a 1m zone — 92-95% of the hash —
and no per-bar consumer reads them, so inlining them meant every
`Zone.fetch_many` in `process_zone_updates` dragged the blob across the wire.
HGETALL was 40% of Redis time at ~65 calls per bar.

These tests pin the arrangement rather than the speed:

  1. the blob is not a persisted zone field, so it cannot creep back onto the
     hot path by someone re-adding a model attribute;
  2. the scalar summaries stay on the zone, because `expectation.candidate_entry`
     reads them;
  3. the records survive the round trip and are reachable from the zone;
  4. the same-bar invariant holds — a zone claiming links has them.

What is deliberately NOT tested: regenerating links for an existing zone. That
is forbidden (business_rules_htf_links.md leakage rule), so there is no code
path to test.
"""

import json

import pytest

from mgot_utils.models import Zone
from mgot_utils.processing import htf_links


ZONE_ID = 'BTCUSDT:15m:mth:1784541600000'


class FakeRedis:
    """The three string ops this module uses, in memory.

    Deliberately not the `redis_client` fixture: that one needs a test server on
    port 6479 and skips without it, and a guard test that silently skips guards
    nothing. Real-Redis behaviour on this path is covered by the replay harness.
    """

    def __init__(self):
        self.store: dict[str, str] = {}

    def set(self, key, value):
        self.store[key] = value
        return True

    def get(self, key):
        return self.store.get(key)

    def delete(self, *keys):
        return sum(bool(self.store.pop(k, None)) for k in keys)


@pytest.fixture
def r() -> FakeRedis:
    return FakeRedis()


def _zone(**extra) -> Zone:
    data = {
        'id': ZONE_ID, 'symbol': 'BTCUSDT', 'timeframe': '15m', 'type': 'mth',
        'direction': 1, 'time': 1784541600000, 'process_time': 1784543400000,
    }
    data.update(extra)
    return Zone(**data)


class TestBlobIsOffTheHash:
    def test_htf_links_is_not_a_persisted_field(self):
        dumped = _zone().model_dump()
        assert 'htf_links' not in dumped, (
            'htf_links is back on the zone hash — it is ~110KB on a 1m zone and '
            'every Zone.fetch_many would carry it again'
        )

    def test_scalar_summaries_remain_on_the_zone(self):
        # expectation.candidate_entry reads these; they are cheap and must stay.
        dumped = _zone().model_dump()
        for field in ('htf_container_count', 'htf_max_tf', 'htf_nearest_id',
                      'htf_nearest_distance_pct', 'genesis_leg', 'genesis_leg_tf'):
            assert field in dumped, f'{field} is read by expectation.py and must stay on the zone'

    def test_unknown_field_does_not_resurrect_it(self):
        # A stored hash written before the move still carries htf_links. Loading
        # it must not put the value back onto the model.
        z = Zone.initiate_zone({
            'id': ZONE_ID, 'symbol': 'BTCUSDT', 'timeframe': '15m', 'type': 'mth',
            'direction': '1', 'time': '1784541600000', 'process_time': '1784543400000',
            'htf_links': '[{"tf":"1h"}]',
        })
        assert 'htf_links' not in z.model_dump()


class TestSideKeyRoundTrip:
    def test_key_is_derived_from_the_zone_id(self):
        assert htf_links.links_key(ZONE_ID) == f'{ZONE_ID}:htf_links'

    def test_records_round_trip(self, r):
        records = [{'tf': '1h', 'step': 1, 'id': 'BTCUSDT:1h:origin:1', 'type': 'origin',
                    'direction': 0, 'completion': 'complete', 'containment': 'full',
                    'semantic': 1, 'distance_pct': -0.42, 'width_ratio': 3.1}]
        r.set(htf_links.links_key(ZONE_ID), json.dumps(records, separators=(',', ':')))
        assert htf_links.fetch_htf_links(ZONE_ID, r) == records

    def test_reachable_from_the_zone(self, r):
        r.set(htf_links.links_key(ZONE_ID), '[{"tf":"4h"}]')
        assert _zone().fetch_htf_links(r) == [{'tf': '4h'}]

    def test_absent_links_are_empty_not_an_error(self, r):
        r.delete(htf_links.links_key(ZONE_ID))
        assert _zone().fetch_htf_links(r) == []

    def test_corrupt_json_degrades_to_empty(self, r):
        # A half-written key must not take down the per-bar path.
        r.set(htf_links.links_key(ZONE_ID), '{not json')
        assert htf_links.fetch_htf_links(ZONE_ID, r) == []


class TestSameBarInvariant:
    def test_a_zone_claiming_containers_has_records(self, r):
        """business_rules_htf_links.md: links and zone are written together, so
        no consumer can observe a zone whose scalars promise links it lacks."""
        zone = _zone()

        def fake_resolve(_zone_arg, _r):
            return {'_links': [{'tf': '1h', 'containment': 'full', 'distance_pct': 0.1}],
                    'htf_container_count': 1, 'htf_max_tf': '1h',
                    'htf_nearest_id': 'x', 'htf_nearest_distance_pct': 0.1,
                    'genesis_leg': 0, 'genesis_leg_tf': ''}

        original = htf_links.resolve_htf_links
        htf_links.resolve_htf_links = fake_resolve
        try:
            htf_links.apply_htf_links(zone, r)
        finally:
            htf_links.resolve_htf_links = original

        assert zone.htf_container_count == 1
        assert htf_links.fetch_htf_links(ZONE_ID, r), (
            'zone reports a container but its records were never written'
        )
