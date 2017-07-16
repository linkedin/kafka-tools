import unittest

from kafka.tools.protocol.types.integers import Int64


class Int64Tests(unittest.TestCase):
    def test_create(self):
        val = Int64(13147483647)
        assert val.value() == 13147483647

    def test_create_null(self):
        self.assertRaises(TypeError, Int64, None)

    def test_encode_bad_type(self):
        self.assertRaises(TypeError, Int64, 'abc')

    def test_encode_bad_range(self):
        self.assertRaises(ValueError, Int64, 9423372036854775807)

    def test_decode(self):
        (val, rest) = Int64.decode(b'\x00\x00\x00\x03\x0f\xa6\xad\xff')
        assert isinstance(val, Int64)
        assert val.value() == 13147483647
        assert rest == b''

    def test_decode_remainder(self):
        (val, rest) = Int64.decode(b'\x00\x00\x00\x03\x0f\xa6\xad\xffabc')
        assert val.value() == 13147483647
        assert rest == b'abc'

    def test_decode_underflow(self):
        self.assertRaises(ValueError, Int64.decode, b'')

    def test_encode(self):
        val = Int64(13147483647)
        assert val.encode() == b'\x00\x00\x00\x03\x0f\xa6\xad\xff'
