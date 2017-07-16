import unittest

from kafka.tools.encoding import encode_boolean, encode_int8, encode_int16, encode_int32, encode_int64, encode_string, encode_bytes


class EncodingTests(unittest.TestCase):
    def test_encode_boolean_false(self):
        assert encode_boolean(False) == b'\x00'

    def test_encode_boolean_none(self):
        assert encode_boolean(None) == b'\x00'

    def test_encode_boolean_true(self):
        assert encode_boolean(True) == b'\x01'

    def test_encode_boolean_numeric_true(self):
        assert encode_boolean(1) == b'\x01'

    def test_encode_boolean_numeric_false(self):
        assert encode_boolean(0) == b'\x00'

    def test_encode_int8(self):
        assert encode_int8(123) == b'{'

    def test_encode_int8_nonnumeric(self):
        self.assertRaises(TypeError, encode_int8, 'abc')

    def test_encode_int16(self):
        assert encode_int16(123) == b'\x00{'

    def test_encode_int16_nonnumeric(self):
        self.assertRaises(TypeError, encode_int16, 'abc')

    def test_encode_int32(self):
        assert encode_int32(123) == b'\x00\x00\x00{'

    def test_encode_int32_nonnumeric(self):
        self.assertRaises(TypeError, encode_int32, 'abc')

    def test_encode_int64(self):
        assert encode_int64(123) == b'\x00\x00\x00\x00\x00\x00\x00{'

    def test_encode_int64_nonnumeric(self):
        self.assertRaises(TypeError, encode_int64, 'abc')

    def test_encode_string_null(self):
        assert encode_string(None) == b'\xff\xff'

    def test_encode_string_empty(self):
        assert encode_string(b'') == b'\x00\x00'

    def test_encode_string(self):
        assert encode_string(b'abc') == b'\x00\x03abc'

    def test_encode_string_nonstring(self):
        self.assertRaises(TypeError, encode_string, 123)

    def test_encode_bytes_null(self):
        assert encode_bytes(None) == b'\xff\xff'

    def test_encode_bytes_empty(self):
        assert encode_bytes(b'') == b'\x00\x00'

    def test_encode_bytes(self):
        assert encode_bytes(b'abcd') == b'\x00\x02\xab\xcd'

    def test_encode_bytes_nonstring(self):
        self.assertRaises(TypeError, encode_bytes, 123)

    def test_encode_bytes_odd(self):
        self.assertRaises(TypeError, encode_bytes, b'abc')
