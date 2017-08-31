import unittest

from kafka.tools.protocol.types.bytes import Bytes


class BytesTests(unittest.TestCase):
    def test_from_string(self):
        val = Bytes.from_string("87349DBC")
        assert val.value() == b'\x87\x34\x9d\xbc'

    def test_from_string_lower(self):
        val = Bytes.from_string("87349dbc")
        assert val.value() == b'\x87\x34\x9d\xbc'

    def test_from_string_null(self):
        val = Bytes.from_string(None)
        assert val.value() is None

    def test_from_string_bad_type(self):
        self.assertRaises(TypeError, Bytes.from_string, 123)

    def test_from_string_bad_length(self):
        self.assertRaises(ValueError, Bytes.from_string, "87349DB")

    def test_create_bad_type(self):
        self.assertRaises(TypeError, Bytes, 123)

    def test_decode(self):
        (val, rest) = Bytes.decode(b'\x00\x00\x00\x04\x87\x34\x9d\xbc')
        assert isinstance(val, Bytes)
        assert val.value() == b'\x87\x34\x9d\xbc'
        assert rest == b''

    def test_decode_null(self):
        (val, rest) = Bytes.decode(b'\xff\xff\xff\xff')
        assert isinstance(val, Bytes)
        assert val.value() is None
        assert rest == b''

    def test_decode_remainder(self):
        (val, rest) = Bytes.decode(b'\x00\x00\x00\x04\x87\x34\x9d\xbcabc')
        assert rest == b'abc'

    def test_decode_nosize(self):
        self.assertRaises(ValueError, Bytes.decode, b'')

    def test_decode_underflow(self):
        self.assertRaises(ValueError, Bytes.decode, b'\x00\x00\x00\x04\x874\x9d')

    def test_encode(self):
        val = Bytes(b'\x87\x34\x9d\xbc')
        print(repr(val.encode()))
        assert val.encode() == b'\x00\x00\x00\x04\x87\x34\x9d\xbc'

    def test_encode_null(self):
        val = Bytes(None)
        assert val.encode() == b'\xff\xff\xff\xff'

    def test_str(self):
        val = Bytes(b'\x87\x34\x9d\xbc')
        print(str(val))
        assert str(val) == "87349dbc (bytes)"
        assert repr(val) == "<Bytes length=4>"
