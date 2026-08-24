"""
SqueezeZone — the SS/ISS discriminator lives on the model.

business_rules_terminology.md makes SS vs ISS a mandatory distinction; these
tests pin the three properties that make it safe in code:

  1. the loader dispatches, so a squeeze hash never comes back as a plain Zone
     (which would silently drop `swing` — pydantic ignores unknown keys);
  2. legacy hashes without the field load as with-trend SS, because every
     squeeze in storage predates the discriminator and is one;
  3. the with-trend-only consumers refuse an inverted squeeze, so minting ISS
     zones later cannot leak them into rules never measured against them.
"""

import pytest

from mgot_utils.models import Zone, SqueezeZone
from mgot_utils.processing import ss_invalidation


def _hash(zone_type: str, **extra) -> dict:
    data = {
        'id': f'BTCUSDT:15m:{zone_type}:1784541600000',
        'symbol': 'BTCUSDT', 'timeframe': '15m', 'type': zone_type,
        'direction': '1', 'time': '1784541600000',
        'process_time': '1784543400000',
    }
    data.update(extra)
    return data


class TestLoaderDispatch:
    def test_squeeze_hash_loads_as_squeeze_zone(self):
        zone = Zone.initiate_zone(_hash('squeeze'))
        assert type(zone) is SqueezeZone

    def test_other_types_stay_plain_zone(self):
        for zone_type in ('mth', 'origin'):
            assert type(Zone.initiate_zone(_hash(zone_type))) is Zone

    def test_subclass_loader_is_not_overridden(self):
        # Loading through SqueezeZone directly keeps that class; the dispatch
        # only lifts plain-Zone loads.
        zone = SqueezeZone.initiate_zone(_hash('squeeze'))
        assert type(zone) is SqueezeZone


class TestSwingDiscriminator:
    def test_legacy_hash_defaults_to_with_trend(self):
        # Every squeeze in storage predates `swing` and is an SS.
        zone = Zone.initiate_zone(_hash('squeeze'))
        assert zone.swing == 'ss'
        assert zone.is_with_trend and not zone.is_inverted

    def test_iss_round_trips(self):
        zone = Zone.initiate_zone(_hash('squeeze', swing='iss'))
        assert zone.is_inverted and not zone.is_with_trend
        # sync_with_db persists model_dump, so the field must survive it.
        assert zone.model_dump()['swing'] == 'iss'

    def test_swing_is_strict(self):
        # A corrupted value must fail at load, not read as a silent 'ss'.
        with pytest.raises(Exception):
            Zone.initiate_zone(_hash('squeeze', swing='sideways'))


class TestWithTrendOnlyConsumers:
    def test_invalidation_refuses_an_iss(self):
        iss = Zone.initiate_zone(_hash('squeeze', swing='iss'))
        # r=None: the guard must answer before anything touches Redis.
        assert ss_invalidation.apply(iss, None) is False

    def test_invalidation_still_reaches_evaluate_for_ss(self, monkeypatch):
        seen = []
        monkeypatch.setattr(ss_invalidation, 'evaluate',
                            lambda squeeze, r: seen.append(squeeze.id) or {})
        ss = Zone.initiate_zone(_hash('squeeze'))
        ss_invalidation.apply(ss, None)
        assert seen, 'with-trend SS must still be evaluated'

    def test_plain_zone_keeps_legacy_behaviour(self, monkeypatch):
        # A plain Zone without the discriminator (constructed directly, not
        # loaded) must behave as before the model existed.
        monkeypatch.setattr(ss_invalidation, 'evaluate', lambda squeeze, r: {})
        plain = Zone(**{k: v for k, v in _hash('squeeze').items()})
        assert ss_invalidation.apply(plain, None) is False  # {} -> unchanged
