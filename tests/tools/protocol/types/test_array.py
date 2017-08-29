import random
import unittest

from kafka.tools.protocol.types.integers import Int8
from kafka.tools.protocol.types.sequences import Array


class ArrayTests(unittest.TestCase):
    def test_create(self):
        val = Array([1], 'int8')
        assert len(val) == 1

    def test_create_null(self):
        val = Array(None, 'int8')
        assert val.value() is None
        assert len(val) == 0

    def test_create_nonarray(self):
        self.assertRaises(ValueError, Array, 1, 'int8')

    def test_create_badtype(self):
        self.assertRaises(TypeError, Array, [1], 'nonexistant_type')

    def test_decode(self):
        (val, rest) = Array.decode(b'\x00\x00\x00\x01{', schema='int8')
        assert isinstance(val, Array)
        assert len(val) == 1
        assert isinstance(val.value()[0], Int8)
        assert val.value()[0].value() == 123
        assert rest == b''

    def test_decode_bad_type(self):
        self.assertRaises(TypeError, Array.decode, b'\x00\x00\x00\x01{', schema='nonexistant_type')

    def test_decode_two(self):
        (val, rest) = Array.decode(b'\x00\x00\x00\x02{[', schema='int8')
        assert isinstance(val, Array)
        assert len(val) == 2
        assert isinstance(val.value()[0], Int8)
        assert val.value()[0].value() == 123
        assert isinstance(val.value()[1], Int8)
        assert val.value()[1].value() == 91
        assert rest == b''

    def test_decode_remainder(self):
        (val, rest) = Array.decode(b'\x00\x00\x00\x01{abc', schema='int8')
        assert rest == b'abc'

    def test_decode_null(self):
        (val, rest) = Array.decode(b'\xff\xff\xff\xff', schema='int8')
        assert isinstance(val, Array)
        assert val.value() is None
        assert rest == b''

    def test_encode_two(self):
        val = Array([123, 91], 'int8')
        assert val.encode() == b'\x00\x00\x00\x02{['

    def test_encode(self):
        val = Array([123], 'int8')
        assert val.encode() == b'\x00\x00\x00\x01{'

    def test_encode_null(self):
        val = Array(None, 'int8')
        assert val.encode() == b'\xff\xff\xff\xff'

    def test_contains(self):
        i1 = Int8(123)
        i2 = Int8(91)
        i3 = Int8(32)
        val = Array([i1, i2], schema='int8', skip_validation=True)
        assert i1 in val
        assert i2 in val
        assert i3 not in val

    def test_contains_null_array(self):
        i1 = Int8(123)
        val = Array(None, schema='int8')
        assert i1 not in val

    def test_setitem_failure(self):
        obj = Array([123, 91], 'int8')
        self.assertRaises(NotImplementedError, obj.__setitem__, 1, 5)

    def test_delitem_failure(self):
        obj = Array([123, 91], 'int8')
        self.assertRaises(NotImplementedError, obj.__delitem__, 1)

    def test_iterator(self):
        values = [random.randrange(100000) for i in range(100)]
        obj = Array(values, schema='int32')
        t_iter = iter(obj)
        c = 0
        for item in t_iter:
            assert item.value() == values[c]
            c += 1
        assert c == 100

    def test_iterator_reverse(self):
        values = [random.randrange(100000) for i in range(100)]
        obj = Array(values, schema='int32')
        t_iter = reversed(obj)
        c = -1
        for item in t_iter:
            assert item.value() == values[c]
            c -= 1
        assert c == -101

    def test_str(self):
        obj = Array([123, 91], 'int8')
        assert str(obj) == "2 items (array)\n    123 (int8)\n    ---\n    91 (int8)"
        assert repr(obj) == "<Array length=2>"
