from datetime import date

from pv_prospect.data_sources import (
    OpenMeteoTimeSeriesDescriptor,
    PVOutputTimeSeriesDescriptor,
    SourceDescriptor,
    build_csv_file_path,
)


def test_builds_path_for_openmeteo_source():
    result = build_csv_file_path(
        'timeseries',
        SourceDescriptor.OPENMETEO_QUARTERHOURLY,
        OpenMeteoTimeSeriesDescriptor.from_str('526604_07808'),
        date(2025, 6, 24),
    )

    assert result == (
        'timeseries/openmeteo/quarterhourly/526604_07808/'
        'openmeteo-quarterhourly_526604_07808_20250624.csv'
    )


def test_builds_path_for_pvoutput_source():
    result = build_csv_file_path(
        'timeseries',
        SourceDescriptor.PVOUTPUT,
        PVOutputTimeSeriesDescriptor(89665),
        date(2025, 6, 1),
    )

    assert result == ('timeseries/pvoutput/89665/pvoutput_89665_20250601.csv')
