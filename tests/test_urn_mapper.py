from src.interfaces import UrnMapper
from src.urn_mapper import PassthroughMapper


class TestPassthroughMapper:
    def test_map_returns_same_urn(self):
        mapper = PassthroughMapper()
        urn = "urn:li:tag:abc-123"
        assert mapper.map(urn) == urn

    def test_map_all(self):
        mapper = PassthroughMapper()
        urns = ["urn:li:tag:a", "urn:li:tag:b", "urn:li:tag:c"]
        assert mapper.map_all(urns) == urns

    def test_map_all_empty(self):
        mapper = PassthroughMapper()
        assert mapper.map_all([]) == []

    def test_is_urn_mapper_subclass(self):
        mapper = PassthroughMapper()
        assert isinstance(mapper, UrnMapper)
