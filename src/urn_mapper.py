from src.interfaces import UrnMapper


class PassthroughMapper(UrnMapper):
    """Identity mapper. Used when source and target URNs are identical."""

    def map(self, urn: str) -> str:
        return urn


# Future extension points:
# class NameBasedMapper(UrnMapper):   -- Cluster 1: match by display name
# class PatternMapper(UrnMapper):     -- Cluster 3: regex-based URN transform
# class MappingFileMapper(UrnMapper): -- Cluster 3: pre-computed mapping file
