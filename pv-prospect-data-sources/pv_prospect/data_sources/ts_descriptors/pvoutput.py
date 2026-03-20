from dataclasses import dataclass


@dataclass(frozen=True)
class PVOutputTimeSeriesDescriptor:
    """Identifies a PV system by its integer system ID."""

    pv_system_id: int

    def __str__(self) -> str:
        return str(self.pv_system_id)
