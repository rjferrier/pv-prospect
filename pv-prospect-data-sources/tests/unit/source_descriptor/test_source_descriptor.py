from pv_prospect.data_sources import SourceDescriptor


def test_str_returns_value():
    assert str(SourceDescriptor.PVOUTPUT) == 'pvoutput'


def test_str_returns_value_with_slashes():
    assert str(SourceDescriptor.OPENMETEO_QUARTERHOURLY) == 'openmeteo/quarterhourly'


def test_is_instance_of_str():
    assert isinstance(SourceDescriptor.PVOUTPUT, str)
