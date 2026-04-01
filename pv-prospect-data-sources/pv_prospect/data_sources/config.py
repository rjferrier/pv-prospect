from dataclasses import dataclass
from typing import Any, Dict

from .data_source import DataSource, DataSourceType


@dataclass
class DataSourcesConfig:
    """Mirrors the ``data_sources`` block in config YAML."""

    pv: DataSource
    weather: DataSource

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataSourcesConfig':
        ds = data['data_sources']
        return cls(
            pv=DataSource(ds['pv']),
            weather=DataSource(ds['weather']),
        )

    def get_data_source(self, data_source_type: DataSourceType) -> DataSource:
        """Look up the :class:`DataSource` for a :class:`DataSourceType`."""
        return getattr(self, data_source_type.value)
