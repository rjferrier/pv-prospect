import pytest
from pv_prospect.data_transformation.two_key_dict import TwoKeyDefaultDict


def test_init_empty():
    """Test initialization of an empty TwoKeyDefaultDict."""
    d = TwoKeyDefaultDict(list)
    assert len(d) == 0
    assert len(d.keys(0)) == 0
    assert len(d.keys(1)) == 0


def test_setitem_and_getitem():
    """Test setting and getting items."""
    d = TwoKeyDefaultDict(int)
    d[("a", "b")] = 10
    assert d[("a", "b")] == 10
    assert "a" in d.keys(0)
    assert "b" in d.keys(1)


def test_setitem_updates_keys():
    """Test that setting items properly updates _keys."""
    d = TwoKeyDefaultDict(list)
    d[("key1", "key2")] = [1, 2, 3]
    d[("key1", "key3")] = [4, 5, 6]
    d[("key4", "key2")] = [7, 8, 9]

    assert set(d.keys(0)) == {"key1", "key4"}
    assert set(d.keys(1)) == {"key2", "key3"}


def test_default_factory():
    """Test that default factory works correctly."""
    d = TwoKeyDefaultDict(list)
    d[("a", "b")].append(1)
    assert d[("a", "b")] == [1]
    assert "a" in d.keys(0)
    assert "b" in d.keys(1)


def test_delitem():
    """Test deleting items."""
    d = TwoKeyDefaultDict(int)
    d[("a", "b")] = 1
    d[("c", "d")] = 2

    del d[("a", "b")]
    assert ("a", "b") not in d
    assert set(d.keys(0)) == {"c"}
    assert set(d.keys(1)) == {"d"}


def test_delitem_multiple_same_key():
    """Test that deleting doesn't remove keys still in use."""
    d = TwoKeyDefaultDict(int)
    d[("a", "b")] = 1
    d[("a", "c")] = 2

    del d[("a", "b")]
    # "a" should still be in keys(0) because ("a", "c") exists
    assert "a" in d.keys(0)
    assert "b" not in d.keys(1)
    assert "c" in d.keys(1)


def test_pop():
    """Test pop method."""
    d = TwoKeyDefaultDict(int)
    d[("a", "b")] = 10
    d[("c", "d")] = 20

    value = d.pop(("a", "b"))
    assert value == 10
    assert ("a", "b") not in d
    assert set(d.keys(0)) == {"c"}
    assert set(d.keys(1)) == {"d"}


def test_pop_with_default():
    """Test pop with default value."""
    d = TwoKeyDefaultDict(int)
    value = d.pop(("nonexistent", "key"), 999)
    assert value == 999
    assert len(d) == 0


def test_popitem():
    """Test popitem method."""
    d = TwoKeyDefaultDict(int)
    d[("a", "b")] = 10

    key, value = d.popitem()
    assert key == ("a", "b")
    assert value == 10
    assert len(d) == 0
    assert len(d.keys(0)) == 0
    assert len(d.keys(1)) == 0


def test_clear():
    """Test clear method."""
    d = TwoKeyDefaultDict(int)
    d[("a", "b")] = 1
    d[("c", "d")] = 2

    d.clear()
    assert len(d) == 0
    assert len(d.keys(0)) == 0
    assert len(d.keys(1)) == 0


def test_update_with_dict():
    """Test update method with dictionary."""
    d = TwoKeyDefaultDict(int)
    d.update({("a", "b"): 1, ("c", "d"): 2})

    assert d[("a", "b")] == 1
    assert d[("c", "d")] == 2
    assert set(d.keys(0)) == {"a", "c"}
    assert set(d.keys(1)) == {"b", "d"}


def test_update_with_iterable():
    """Test update method with iterable of tuples."""
    d = TwoKeyDefaultDict(int)
    d.update([(("a", "b"), 1), (("c", "d"), 2)])

    assert d[("a", "b")] == 1
    assert d[("c", "d")] == 2
    assert set(d.keys(0)) == {"a", "c"}
    assert set(d.keys(1)) == {"b", "d"}


def test_setdefault_new_key():
    """Test setdefault with a new key."""
    d = TwoKeyDefaultDict(int)
    result = d.setdefault(("a", "b"), 10)

    assert result == 10
    assert d[("a", "b")] == 10
    assert "a" in d.keys(0)
    assert "b" in d.keys(1)


def test_setdefault_existing_key():
    """Test setdefault with an existing key."""
    d = TwoKeyDefaultDict(int)
    d[("a", "b")] = 5
    result = d.setdefault(("a", "b"), 10)

    assert result == 5
    assert d[("a", "b")] == 5


def test_keys_no_argument():
    """Test keys() method without arguments."""
    d = TwoKeyDefaultDict(int)
    d[("a", "b")] = 1
    d[("c", "d")] = 2

    keys = d.keys()
    assert set(keys) == {("a", "b"), ("c", "d")}


def test_keys_dimension_0():
    """Test keys(0) returns first dimension keys."""
    d = TwoKeyDefaultDict(int)
    d[("a", "b")] = 1
    d[("c", "d")] = 2
    d[("a", "e")] = 3

    keys_dim_0 = d.keys(0)
    assert set(keys_dim_0) == {"a", "c"}


def test_keys_dimension_1():
    """Test keys(1) returns second dimension keys."""
    d = TwoKeyDefaultDict(int)
    d[("a", "b")] = 1
    d[("c", "d")] = 2
    d[("e", "b")] = 3

    keys_dim_1 = d.keys(1)
    assert set(keys_dim_1) == {"b", "d"}


def test_map_values():
    """Test map_values method."""
    d = TwoKeyDefaultDict(int)
    d[("a", "b")] = 1
    d[("c", "d")] = 2

    result = d.map_values(lambda x: x * 2)
    assert result[("a", "b")] == 2
    assert result[("c", "d")] == 4
    # Original should be unchanged
    assert d[("a", "b")] == 1
    assert d[("c", "d")] == 2


def test_map_values_type_change():
    """Test map_values with type transformation."""
    d = TwoKeyDefaultDict(int)
    d[("a", "b")] = 10
    d[("c", "d")] = 20

    result = d.map_values(str)
    assert result[("a", "b")] == "10"
    assert result[("c", "d")] == "20"
    assert isinstance(result[("a", "b")], str)


def test_complex_type_keys():
    """Test with different key types (not just strings)."""
    d = TwoKeyDefaultDict(list)
    d[(1, 2)] = ["a", "b"]
    d[(3, 4)] = ["c", "d"]

    assert set(d.keys(0)) == {1, 3}
    assert set(d.keys(1)) == {2, 4}


def test_keys_integrity_after_multiple_operations():
    """Test that _keys remains consistent after multiple operations."""
    d = TwoKeyDefaultDict(int)

    # Add items
    d[("a", "b")] = 1
    d[("c", "d")] = 2
    d[("a", "e")] = 3

    # Update
    d.update({("f", "g"): 4})

    # Delete
    del d[("c", "d")]

    # Pop
    d.pop(("f", "g"))

    # Final state should have only ("a", "b") and ("a", "e")
    assert set(d.keys(0)) == {"a"}
    assert set(d.keys(1)) == {"b", "e"}


def test_empty_default_factory():
    """Test behavior when default_factory is None."""
    d = TwoKeyDefaultDict()
    d[("a", "b")] = 10

    assert d[("a", "b")] == 10
    # Accessing non-existent key should raise KeyError since no default_factory
    with pytest.raises(KeyError):
        _ = d[("x", "y")]


def test_rebuild_keys_maintains_integrity():
    """Test that _rebuild_keys correctly reconstructs key sets."""
    d = TwoKeyDefaultDict(int)
    d[("a", "b")] = 1
    d[("c", "d")] = 2
    d[("a", "d")] = 3

    # Manually corrupt _keys
    d._keys = (set(), set())

    # Rebuild should fix it
    d._rebuild_keys()

    assert set(d.keys(0)) == {"a", "c"}
    assert set(d.keys(1)) == {"b", "d"}
