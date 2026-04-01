from pv_prospect.data_sources import DataSource


def test_str_returns_value():
    assert str(DataSource.PVOUTPUT) == 'pvoutput'


def test_str_returns_value_with_slashes():
    assert str(DataSource.OPENMETEO_QUARTERHOURLY) == 'openmeteo/quarterhourly'


def test_is_instance_of_str():
    assert isinstance(DataSource.PVOUTPUT, str)
