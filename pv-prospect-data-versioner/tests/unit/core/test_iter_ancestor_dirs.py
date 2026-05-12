from pv_prospect.data_versioner.core import iter_ancestor_dirs


def test_returns_empty_list_for_no_paths() -> None:
    assert iter_ancestor_dirs([]) == []


def test_excludes_root() -> None:
    assert iter_ancestor_dirs(['weather.csv']) == []


def test_returns_single_directory() -> None:
    assert iter_ancestor_dirs(['pv/89665.csv']) == ['pv']


def test_dedupes_siblings() -> None:
    paths = ['pv/89665.csv', 'pv/12345.csv']
    assert iter_ancestor_dirs(paths) == ['pv']


def test_orders_deepest_first() -> None:
    paths = ['a/b/c/file.csv', 'a/x.csv']
    assert iter_ancestor_dirs(paths) == ['a/b/c', 'a/b', 'a']
