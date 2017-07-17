import unittest

from kafka.tools.protocol.types.bytes import Bytes


class BytesTests(unittest.TestCase):
    def test_create_upper(self):
        val = Bytes(b'87349DBC')
        assert val.value() == b'87349DBC'

    def test_create_null(self):
        val = Bytes(None)
        assert val.value() is None

    def test_encode_bad_type(self):
        self.assertRaises(TypeError, Bytes, 123)

    def test_encode_bad_length(self):
        self.assertRaises(ValueError, Bytes, b'87349DB')

    def test_decode(self):
        (val, rest) = Bytes.decode(b'\x00\x04\x874\x9d\xbc')
        assert isinstance(val, Bytes)
        assert val.value() == b'87349dbc'
        assert rest == b''

    def test_decode_null(self):
        (val, rest) = Bytes.decode(b'\xff\xff')
        assert isinstance(val, Bytes)
        assert val.value() is None
        assert rest == b''

    def test_decode_remainder(self):
        (val, rest) = Bytes.decode(b'\x00\x04\x874\x9d\xbcabc')
        assert val.value() == b'87349dbc'
        assert rest == b'abc'

    def test_decode_nosize(self):
        self.assertRaises(ValueError, Bytes.decode, b'')

    def test_decode_underflow(self):
        self.assertRaises(ValueError, Bytes.decode, b'\x00\x04\x874\x9d')

    def test_encode(self):
        val = Bytes(b'87349DBC')
        assert val.encode() == b'\x00\x04\x874\x9d\xbc'

    def test_encode_lower(self):
        val = Bytes(b'87349dbc')
        assert val.encode() == b'\x00\x04\x874\x9d\xbc'

    def test_encode_null(self):
        val = Bytes(None)
        assert val.encode() == b'\xff\xff'
