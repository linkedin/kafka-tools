import unittest

from kafka.tools.protocol.types.integers import Int8


class Int8Tests(unittest.TestCase):
    def test_create(self):
        val = Int8(123)
        assert val.value() == 123

    def test_create_null(self):
        self.assertRaises(TypeError, Int8, None)

    def test_encode_bad_type(self):
        self.assertRaises(TypeError, Int8, 'abc')

    def test_encode_bad_range(self):
        self.assertRaises(ValueError, Int8, 1023)

    def test_decode(self):
        (val, rest) = Int8.decode(b'{')
        assert isinstance(val, Int8)
        assert val.value() == 123
        assert rest == b''

    def test_decode_remainder(self):
        (val, rest) = Int8.decode(b'{abc')
        assert val.value() == 123
        assert rest == b'abc'

    def test_decode_underflow(self):
        self.assertRaises(ValueError, Int8.decode, b'')

    def test_encode(self):
        val = Int8(123)
        assert val.encode() == b'{'
