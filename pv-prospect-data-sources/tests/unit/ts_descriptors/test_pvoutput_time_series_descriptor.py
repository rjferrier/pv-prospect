from pv_prospect.data_sources import PVOutputTimeSeriesDescriptor


def test_str_returns_system_id():
    descriptor = PVOutputTimeSeriesDescriptor(89665)

    assert str(descriptor) == '89665'
