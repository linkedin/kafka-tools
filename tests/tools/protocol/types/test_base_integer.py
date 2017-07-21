import six
import unittest

from kafka.tools.protocol.types.integers import Int8, _get_other_value


class BaseIntegerTests(unittest.TestCase):
    def test_get_other_value(self):
        val = _get_other_value(Int8(42))
        assert isinstance(val, six.integer_types)
        assert val == 42

    def test_get_other_value_int(self):
        val = _get_other_value(42)
        assert isinstance(val, six.integer_types)
        assert val == 42

    def test_get_bad_type(self):
        self.assertRaises(TypeError, _get_other_value, 'foo')

    def test_eq(self):
        val = Int8(123)
        val2 = Int8(123)
        assert val == val2
        assert val == 123
        assert 123 == val

    def test_ne(self):
        val = Int8(123)
        val2 = Int8(91)
        assert val != val2
        assert val != 91
        assert 91 != val

    def test_lt(self):
        val = Int8(91)
        val2 = Int8(123)
        assert val < val2
        assert val < 123
        assert 91 < val2

    def test_le(self):
        val = Int8(91)
        val2 = Int8(123)
        val3 = Int8(91)
        assert val <= val2
        assert val <= val3
        assert val <= 123
        assert val2 <= 123
        assert 91 <= val2
        assert 91 <= val

    def test_gt(self):
        val = Int8(91)
        val2 = Int8(123)
        assert val2 > val
        assert val2 > 91
        assert 123 > val

    def test_ge(self):
        val = Int8(91)
        val2 = Int8(123)
        val3 = Int8(91)
        assert val2 >= val
        assert val3 >= val
        assert val2 >= 91
        assert val >= 91
        assert 123 >= val
        assert 91 >= val

    def test_bool(self):
        val = Int8(0)
        val2 = Int8(1)
        assert not val
        assert val2

    def test_str(self):
        val = Int8(123)
        assert str(val) == "123 (int8)"
        assert repr(val) == "<Int8 123>"
