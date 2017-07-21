import unittest

from kafka.tools.protocol.types.integers import Int32


class Int32Tests(unittest.TestCase):
    def test_create(self):
        val = Int32(7623467)
        assert val.value() == 7623467

    def test_create_null(self):
        self.assertRaises(TypeError, Int32, None)

    def test_encode_bad_type(self):
        self.assertRaises(TypeError, Int32, 'abc')

    def test_encode_bad_range(self):
        self.assertRaises(ValueError, Int32, 3147483647)

    def test_decode(self):
        (val, rest) = Int32.decode(b'\x00tS+')
        assert isinstance(val, Int32)
        assert val.value() == 7623467
        assert rest == b''

    def test_decode_remainder(self):
        (val, rest) = Int32.decode(b'\x00tS+abc')
        assert val.value() == 7623467
        assert rest == b'abc'

    def test_decode_underflow(self):
        self.assertRaises(ValueError, Int32.decode, b'')

    def test_encode(self):
        val = Int32(7623467)
        assert val.encode() == b'\x00tS+'
