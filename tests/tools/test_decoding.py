import unittest

from kafka.tools.decoding import decode_boolean, decode_int8, decode_int16, decode_int32, decode_int64, decode_string, decode_bytes


class DecodingTests(unittest.TestCase):
    def test_decode_boolean_false(self):
        (val, rest) = decode_boolean(b'\x00')
        assert val is False
        assert rest == b''

    def test_decode_boolean_true(self):
        (val, rest) = decode_boolean(b'\x01')
        assert val is True
        assert rest == b''

    def test_decode_boolean_remainder(self):
        (val, rest) = decode_boolean(b'\x00abc')
        assert val is False
        assert rest == b'abc'

    def test_decode_boolean_underflow(self):
        self.assertRaises(ValueError, decode_boolean, b'')

    def test_decode_int8(self):
        (val, rest) = decode_int8(b'{')
        assert val == 123
        assert rest == b''

    def test_decode_int8_remainder(self):
        (val, rest) = decode_int8(b'{abc')
        assert val == 123
        assert rest == b'abc'

    def test_decode_int8_underflow(self):
        self.assertRaises(ValueError, decode_int8, b'')

    def test_decode_int16(self):
        (val, rest) = decode_int16(b'\x00{')
        assert val == 123
        assert rest == b''

    def test_decode_int16_remainder(self):
        (val, rest) = decode_int16(b'\x00{abc')
        assert val == 123
        assert rest == b'abc'

    def test_decode_int16_underflow(self):
        self.assertRaises(ValueError, decode_int16, b'{')

    def test_decode_int32(self):
        (val, rest) = decode_int32(b'\x00\x00\x00{')
        assert val == 123
        assert rest == b''

    def test_decode_int32_remainder(self):
        (val, rest) = decode_int32(b'\x00\x00\x00{abc')
        assert val == 123
        assert rest == b'abc'

    def test_decode_int32_underflow(self):
        self.assertRaises(ValueError, decode_int32, b'\x00\x00{')

    def test_decode_int64(self):
        (val, rest) = decode_int64(b'\x00\x00\x00\x00\x00\x00\x00{')
        assert val == 123
        assert rest == b''

    def test_decode_int64_remainder(self):
        (val, rest) = decode_int64(b'\x00\x00\x00\x00\x00\x00\x00{abc')
        assert val == 123
        assert rest == b'abc'

    def test_decode_int64_underflow(self):
        self.assertRaises(ValueError, decode_int64, b'\x00\x00\x00\x00\x00\x00{')

    def test_decode_string_null(self):
        (val, rest) = decode_string(b'\xff\xff')
        assert val is None
        assert rest == b''

    def test_decode_string_null_remainder(self):
        (val, rest) = decode_string(b'\xff\xffabc')
        assert val is None
        assert rest == b'abc'

    def test_decode_string(self):
        (val, rest) = decode_string(b'\x00\x03abc')
        assert val == b'abc'
        assert rest == b''

    def test_decode_string_remainder(self):
        (val, rest) = decode_string(b'\x00\x03abcdef')
        assert val == b'abc'
        assert rest == b'def'

    def test_decode_string_empty(self):
        (val, rest) = decode_string(b'\x00\x00')
        assert val == b''
        assert rest == b''

    def test_decode_string_empty_remainder(self):
        (val, rest) = decode_string(b'\x00\x00abc')
        assert val == b''
        assert rest == b'abc'

    def test_decode_string_underflow_size(self):
        self.assertRaises(ValueError, decode_string, b'\x00')

    def test_decode_string_underflow_length(self):
        self.assertRaises(ValueError, decode_string, b'\x00\x04abc')

    def test_decode_bytes_null(self):
        (val, rest) = decode_bytes(b'\xff\xff')
        assert val is None
        assert rest == b''

    def test_decode_bytes_null_remainder(self):
        (val, rest) = decode_bytes(b'\xff\xffabc')
        assert val is None
        assert rest == b'abc'

    def test_decode_bytes(self):
        (val, rest) = decode_bytes(b'\x00\x02\xab\xcd')
        assert val == b'abcd'
        assert rest == b''

    def test_decode_bytes_remainder(self):
        (val, rest) = decode_bytes(b'\x00\x02\xab\xcd\xef')
        assert val == b'abcd'
        assert rest == b'\xef'

    def test_decode_bytes_empty(self):
        (val, rest) = decode_bytes(b'\x00\x00')
        assert val == b''
        assert rest == b''

    def test_decode_bytes_empty_remainder(self):
        (val, rest) = decode_bytes(b'\x00\x00abc')
        assert val == b''
        assert rest == b'abc'

    def test_decode_bytes_underflow_size(self):
        self.assertRaises(ValueError, decode_bytes, b'\x00')

    def test_decode_bytes_underflow_length(self):
        self.assertRaises(ValueError, decode_bytes, b'\x00\x03\xab\xcd')
