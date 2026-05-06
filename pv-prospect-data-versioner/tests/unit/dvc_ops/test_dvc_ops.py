from pv_prospect.data_versioner.dvc_ops import inject_remote


def test_inject_remote_adds_field_to_each_out() -> None:
    dvc_data = {
        'outs': [
            {'md5': 'abc123', 'size': 100, 'path': 'data/prepared/weather.csv'},
            {'md5': 'def456', 'size': 200, 'path': 'data/prepared/pv/89665.csv'},
        ]
    }

    inject_remote(dvc_data, 'versioned-feature')

    assert dvc_data['outs'][0]['remote'] == 'versioned-feature'
    assert dvc_data['outs'][1]['remote'] == 'versioned-feature'


def test_inject_remote_preserves_existing_fields() -> None:
    dvc_data = {
        'outs': [
            {'md5': 'abc123', 'size': 100, 'path': 'data/prepared/weather.csv'},
        ]
    }

    inject_remote(dvc_data, 'versioned-raw')

    out = dvc_data['outs'][0]
    assert out['md5'] == 'abc123'
    assert out['size'] == 100
    assert out['path'] == 'data/prepared/weather.csv'
    assert out['remote'] == 'versioned-raw'


def test_inject_remote_with_empty_outs() -> None:
    dvc_data: dict = {'outs': []}

    inject_remote(dvc_data, 'versioned-feature')

    assert dvc_data['outs'] == []


def test_inject_remote_with_missing_outs_key() -> None:
    dvc_data: dict = {}

    inject_remote(dvc_data, 'versioned-feature')

    assert dvc_data == {}
