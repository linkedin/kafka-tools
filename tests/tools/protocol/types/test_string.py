import unittest

from kafka.tools.protocol.types.string import String


class StringTests(unittest.TestCase):
    def test_create(self):
        val = String('foo')
        assert val.value() == 'foo'

    def test_create_null(self):
        val = String(None)
        assert val.value() is None

    def test_encode_bad_type(self):
        self.assertRaises(TypeError, String, 123)

    def test_decode(self):
        (val, rest) = String.decode(b'\x00\x03foo')
        assert isinstance(val, String)
        assert val.value() == 'foo'
        assert rest == b''

    def test_decode_null(self):
        (val, rest) = String.decode(b'\xff\xff')
        assert isinstance(val, String)
        assert val.value() is None
        assert rest == b''

    def test_decode_remainder(self):
        (val, rest) = String.decode(b'\x00\x03fooabc')
        assert val.value() == 'foo'
        assert rest == b'abc'

    def test_decode_nosize(self):
        self.assertRaises(ValueError, String.decode, b'')

    def test_decode_underflow(self):
        self.assertRaises(ValueError, String.decode, b'\x00\x03fo')

    def test_encode(self):
        val = String('foo')
        assert val.encode() == b'\x00\x03foo'

    def test_encode_null(self):
        val = String(None)
        assert val.encode() == b'\xff\xff'

    def test_str(self):
        val = String('foo')
        assert str(val) == "foo (string)"
        assert repr(val) == "<String length=3>"
