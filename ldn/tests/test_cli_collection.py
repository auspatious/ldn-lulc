from datetime import datetime

import pytest
from pystac import Extent, SpatialExtent, TemporalExtent

from ldn.cli_collection import _run_create_collection


@pytest.mark.parametrize("source_coop,host", [(True, "Source Cooperative"), (False, "Digital Earth Pacific")])
def test_geomad_collection_host_and_processor(source_coop, host):
    extent = Extent(SpatialExtent([[0, 0, 1, 1]]), TemporalExtent([[datetime(2020, 1, 1), None]]))
    providers = _run_create_collection("geomad", extent, False, source_coop).providers
    assert [p.name for p in providers if "host" in p.roles] == [host]
    if source_coop:
        assert providers[0].url == "https://source.coop/auspatious/geomad-sids"
    processor = next(p for p in providers if p.name == "Auspatious")
    assert processor.extra_fields["processing:facility"] == "Digital Earth Pacific"
