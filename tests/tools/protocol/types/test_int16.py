import unittest

from kafka.tools.protocol.types.integers import Int16


class Int16Tests(unittest.TestCase):
    def test_create(self):
        val = Int16(23865)
        assert val.value() == 23865

    def test_create_null(self):
        self.assertRaises(TypeError, Int16, None)

    def test_encode_bad_type(self):
        self.assertRaises(TypeError, Int16, 'abc')

    def test_encode_bad_range(self):
        self.assertRaises(ValueError, Int16, 57463)

    def test_decode(self):
        (val, rest) = Int16.decode(b']9')
        assert isinstance(val, Int16)
        assert val.value() == 23865
        assert rest == b''

    def test_decode_remainder(self):
        (val, rest) = Int16.decode(b']9abc')
        assert val.value() == 23865
        assert rest == b'abc'

    def test_decode_underflow(self):
        self.assertRaises(ValueError, Int16.decode, b'')

    def test_encode(self):
        val = Int16(23865)
        assert val.encode() == b']9'
