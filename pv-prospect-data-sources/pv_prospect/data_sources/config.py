from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict

from .source_descriptor import SourceDescriptor


class DataSource(str, Enum):
    PV = 'pv'
    WEATHER = 'weather'

    def __str__(self) -> str:
        return self.value


@dataclass
class DataSourcesConfig:
    """Mirrors the ``data_sources`` block in config YAML."""

    pv: SourceDescriptor
    weather: SourceDescriptor

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataSourcesConfig':
        ds = data['data_sources']
        return cls(
            pv=SourceDescriptor(ds['pv']),
            weather=SourceDescriptor(ds['weather']),
        )

    def get_descriptor(self, source: DataSource) -> SourceDescriptor:
        """Look up the :class:`SourceDescriptor` for a :class:`DataSource`."""
        return getattr(self, source.value)
