import textwrap

import pandas as pd
from pv_prospect.app.store import WINDOW_COLUMNS, parse_window


def test_single_site_returns_dict_keyed_by_int() -> None:
    csv_text = textwrap.dedent("""\
        system_id,time,temperature,plane_of_array_irradiance,power,power_max
        12345,2025-01-01,5.0,100.0,200.0,400.0
        12345,2025-01-02,6.0,110.0,210.0,420.0
    """)
    windows = parse_window(csv_text)
    assert list(windows.keys()) == [12345]
    assert isinstance(list(windows.keys())[0], int)


def test_multi_site_produces_separate_frames() -> None:
    csv_text = textwrap.dedent("""\
        system_id,time,temperature,plane_of_array_irradiance,power,power_max
        11111,2025-01-01,5.0,100.0,200.0,400.0
        22222,2025-01-01,6.0,110.0,210.0,420.0
    """)
    windows = parse_window(csv_text)
    assert set(windows.keys()) == {11111, 22222}
    assert len(windows[11111]) == 1
    assert len(windows[22222]) == 1


def test_window_columns_preserved() -> None:
    csv_text = textwrap.dedent("""\
        system_id,time,temperature,plane_of_array_irradiance,power,power_max
        12345,2025-01-01,5.0,100.0,200.0,400.0
    """)
    windows = parse_window(csv_text)
    df = windows[12345]
    for col in WINDOW_COLUMNS:
        assert col in df.columns


def test_time_parsed_as_datetime() -> None:
    csv_text = textwrap.dedent("""\
        system_id,time,temperature,plane_of_array_irradiance,power,power_max
        12345,2025-01-01,5.0,100.0,200.0,400.0
    """)
    windows = parse_window(csv_text)
    assert pd.api.types.is_datetime64_any_dtype(windows[12345]['time'])
