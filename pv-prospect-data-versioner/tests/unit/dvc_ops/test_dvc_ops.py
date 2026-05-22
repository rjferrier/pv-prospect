from pv_prospect.data_versioner.dvc_ops import inject_remote


def test_inject_remote_adds_field_to_each_out() -> None:
    dvc_data = {
        'outs': [
            {
                'md5': 'abc123',
                'size': 100,
                'path': 'data/prepared/weather/weather_2026-05-01_2026-05-15_0-07.csv',
            },
            {
                'md5': 'def456',
                'size': 200,
                'path': 'data/prepared/pv/89665/pv_89665_2026-05-01_2026-05-08.csv',
            },
        ]
    }

    inject_remote(dvc_data, 'versioned-feature')

    assert dvc_data['outs'][0]['remote'] == 'versioned-feature'
    assert dvc_data['outs'][1]['remote'] == 'versioned-feature'


def test_inject_remote_preserves_existing_fields() -> None:
    weather_path = 'data/prepared/weather/weather_2026-05-01_2026-05-15_0-07.csv'
    dvc_data = {
        'outs': [
            {'md5': 'abc123', 'size': 100, 'path': weather_path},
        ]
    }

    inject_remote(dvc_data, 'versioned-raw')

    out = dvc_data['outs'][0]
    assert out['md5'] == 'abc123'
    assert out['size'] == 100
    assert out['path'] == weather_path
    assert out['remote'] == 'versioned-raw'


def test_inject_remote_with_empty_outs() -> None:
    dvc_data: dict = {'outs': []}

    inject_remote(dvc_data, 'versioned-feature')

    assert dvc_data['outs'] == []


def test_inject_remote_with_missing_outs_key() -> None:
    dvc_data: dict = {}

    inject_remote(dvc_data, 'versioned-feature')

    assert dvc_data == {}
