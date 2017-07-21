import unittest

from kafka.tools.protocol.types.boolean import Boolean


class BooleanTests(unittest.TestCase):
    def test_create_true(self):
        val = Boolean(True)
        assert val.value()

    def test_create_one(self):
        val = Boolean(1)
        assert val.value()

    def test_create_false(self):
        val = Boolean(False)
        assert not val.value()

    def test_create_zero(self):
        val = Boolean(0)
        assert not val.value()

    def test_decode_true(self):
        (val, rest) = Boolean.decode(b'\x01')
        assert isinstance(val, Boolean)
        assert val.value()
        assert rest == b''

    def test_decode_false(self):
        (val, rest) = Boolean.decode(b'\x00')
        assert isinstance(val, Boolean)
        assert not val.value()
        assert rest == b''

    def test_decode_remainder(self):
        (val, rest) = Boolean.decode(b'\x01abc')
        assert val.value()
        assert rest == b'abc'

    def test_decode_underflow(self):
        self.assertRaises(ValueError, Boolean.decode, b'')

    def test_encode_true(self):
        val = Boolean(True)
        assert val.encode() == b'\x01'

    def test_encode_false(self):
        val = Boolean(False)
        assert val.encode() == b'\x00'

    def test_boolean_context(self):
        val = Boolean(True)
        val2 = Boolean(False)
        assert val
        assert not val2

    def test_str(self):
        val = Boolean(True)
        assert str(val) == "True (boolean)"
        assert repr(val) == "<Boolean True>"
