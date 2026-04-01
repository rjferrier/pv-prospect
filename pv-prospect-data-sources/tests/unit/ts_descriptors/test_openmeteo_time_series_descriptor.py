from decimal import Decimal

from pv_prospect.data_sources import OpenMeteoTimeSeriesDescriptor


def test_str_returns_filename_friendly_coords():
    descriptor = OpenMeteoTimeSeriesDescriptor.from_str('504900_-35400')

    assert str(descriptor) == '504900_-35400'


def test_from_coordinates_positive_lat_lon():
    descriptor = OpenMeteoTimeSeriesDescriptor.from_coordinates(
        Decimal('52.6604'), Decimal('0.7808')
    )

    assert str(descriptor) == '526604_07808'


def test_from_coordinates_negative_longitude():
    descriptor = OpenMeteoTimeSeriesDescriptor.from_coordinates(
        Decimal('50.49'), Decimal('-3.54')
    )

    assert str(descriptor) == '504900_-35400'


def test_from_coordinates_rounds_to_four_decimals():
    descriptor = OpenMeteoTimeSeriesDescriptor.from_coordinates(
        Decimal('51.123456'), Decimal('-4.567891')
    )

    assert str(descriptor) == '511235_-45679'


def test_from_str_restores_coordinates():
    descriptor = OpenMeteoTimeSeriesDescriptor.from_str('504900_-35400')

    assert descriptor.location.latitude == Decimal('50.49')
    assert descriptor.location.longitude == Decimal('-3.54')


def test_from_str_roundtrips_with_from_coordinates():
    original = OpenMeteoTimeSeriesDescriptor.from_coordinates(
        Decimal('52.6604'), Decimal('0.7808')
    )
    roundtripped = OpenMeteoTimeSeriesDescriptor.from_str(str(original))

    assert roundtripped == original
