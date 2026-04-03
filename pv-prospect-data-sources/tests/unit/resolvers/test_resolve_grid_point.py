from decimal import Decimal

import pytest
from pv_prospect.common.domain import GridPoint, Location
from pv_prospect.data_sources import resolve_grid_point


def _stub_get_location(pv_system_id: int) -> Location:
    return Location.from_coordinates(Decimal('50.49'), Decimal('-3.54'))


def test_from_location_str() -> None:
    result = resolve_grid_point(_stub_get_location, location_str='504900_-35400')

    assert result == GridPoint.from_id('504900_-35400')


def test_from_pv_system_id() -> None:
    result = resolve_grid_point(_stub_get_location, pv_system_id=89665)

    assert result == GridPoint(
        Location.from_coordinates(Decimal('50.49'), Decimal('-3.54'))
    )


def test_both_args_raises() -> None:
    with pytest.raises(ValueError, match='Ambiguous input'):
        resolve_grid_point(
            _stub_get_location,
            pv_system_id=89665,
            location_str='504900_-35400',
        )


def test_no_args_raises() -> None:
    with pytest.raises(ValueError, match='Either'):
        resolve_grid_point(_stub_get_location)
