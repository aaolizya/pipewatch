"""Tag-based grouping and filtering for pipeline sources and metrics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set

from pipewatch.config import SourceConfig


@dataclass
class TagIndex:
    """Inverted index mapping tags to source names."""

    _index: Dict[str, Set[str]] = field(default_factory=dict)

    def add(self, source_name: str, tags: Iterable[str]) -> None:
        """Register *tags* for *source_name*."""
        for tag in tags:
            self._index.setdefault(tag, set()).add(source_name)

    def sources_for_tag(self, tag: str) -> Set[str]:
        """Return all source names carrying *tag*."""
        return set(self._index.get(tag, set()))

    def sources_for_tags(self, tags: Iterable[str], match_all: bool = False) -> Set[str]:
        """Return sources matching *any* (default) or *all* of the given tags."""
        tag_list = list(tags)
        if not tag_list:
            return set()
        sets = [self.sources_for_tag(t) for t in tag_list]
        if match_all:
            result = sets[0]
            for s in sets[1:]:
                result = result & s
            return result
        result: Set[str] = set()
        for s in sets:
            result |= s
        return result

    def all_tags(self) -> List[str]:
        """Return a sorted list of all known tags."""
        return sorted(self._index.keys())


def build_tag_index(sources: Iterable[SourceConfig]) -> TagIndex:
    """Build a :class:`TagIndex` from a collection of :class:`SourceConfig` objects.

    Tags are read from ``source.tags`` when present; sources without tags are
    silently skipped.
    """
    index = TagIndex()
    for source in sources:
        tags: Optional[List[str]] = getattr(source, "tags", None)
        if tags:
            index.add(source.name, tags)
    return index


def filter_sources_by_tag(
    sources: Iterable[SourceConfig],
    tags: Iterable[str],
    match_all: bool = False,
) -> List[SourceConfig]:
    """Return only those *sources* whose name appears in the tag-filtered set."""
    index = build_tag_index(sources)
    matching_names = index.sources_for_tags(tags, match_all=match_all)
    sources_list = list(sources) if not isinstance(sources, list) else sources
    return [s for s in sources_list if s.name in matching_names]
