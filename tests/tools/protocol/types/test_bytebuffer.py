import unittest

from kafka.tools.protocol.types.bytebuffer import ByteBuffer


class ByteBufferTests(unittest.TestCase):
    def setUp(self):
        self.test_bb = ByteBuffer(b'\x03\x05\x07\x09\x11\x13\x15\x17\x19\x21')

    def test_create_allocate(self):
        val = ByteBuffer(4)
        assert val.position == 0
        assert val.capacity == 4
        assert val.remaining == 4

    def test_create_wrap_bytestring(self):
        val = ByteBuffer(b'\x03\x05\x07\x09')
        assert val.position == 0
        assert val.capacity == 4
        assert val.remaining == 4
        assert val.get(4) == bytearray(b'\x03\x05\x07\x09')

    def test_create_wrap_bytearray(self):
        ba = bytearray(b'\x03\x05\x07\x09')
        val = ByteBuffer(ba)
        assert val.position == 0
        assert val.capacity == 4
        assert val.remaining == 4
        assert val.get(4) == ba

    def test_create_bad_type(self):
        self.assertRaises(TypeError, ByteBuffer, [])

    def test_get_int8(self):
        assert self.test_bb.getInt8() == 3
        assert self.test_bb.position == 1

    def test_get_int8_at_position(self):
        assert self.test_bb.getInt8(2) == 7
        assert self.test_bb.position == 0

    def test_get_int16(self):
        assert self.test_bb.getInt16() == 773
        assert self.test_bb.position == 2

    def test_get_int16_at_position(self):
        assert self.test_bb.getInt16(2) == 1801
        assert self.test_bb.position == 0

    def test_get_int32(self):
        assert self.test_bb.getInt32() == 50661129
        assert self.test_bb.position == 4

    def test_get_int32_at_position(self):
        assert self.test_bb.getInt32(2) == 118034707
        assert self.test_bb.position == 0

    def test_get_int64(self):
        assert self.test_bb.getInt64() == 217587892519900439
        assert self.test_bb.position == 8

    def test_get_int64_at_position(self):
        assert self.test_bb.getInt64(2) == 506955206711777569
        assert self.test_bb.position == 0

    def test_get(self):
        assert self.test_bb.get(4) == bytearray(b'\x03\x05\x07\x09')

    def test_get_at_position(self):
        assert self.test_bb.get(4, 2) == bytearray(b'\x07\x09\x11\x13')

    def test_put_int8(self):
        self.test_bb.putInt8(8)
        assert self.test_bb.get(10, 0) == bytearray(b'\x08\x05\x07\x09\x11\x13\x15\x17\x19\x21')

    def test_put_int8_at_position(self):
        self.test_bb.putInt8(8, 2)
        assert self.test_bb.get(10, 0) == bytearray(b'\x03\x05\x08\x09\x11\x13\x15\x17\x19\x21')

    def test_put_int16(self):
        self.test_bb.putInt16(2342)
        assert self.test_bb.get(10, 0) == bytearray(b'\t&\x07\x09\x11\x13\x15\x17\x19\x21')

    def test_put_int16_at_position(self):
        self.test_bb.putInt16(2342, 2)
        assert self.test_bb.get(10, 0) == bytearray(b'\x03\x05\t&\x11\x13\x15\x17\x19\x21')

    def test_put_int32(self):
        self.test_bb.putInt32(897324)
        assert self.test_bb.get(10, 0) == bytearray(b'\x00\r\xb1,\x11\x13\x15\x17\x19\x21')

    def test_put_int32_at_position(self):
        self.test_bb.putInt32(897324, 2)
        assert self.test_bb.get(10, 0) == bytearray(b'\x03\x05\x00\r\xb1,\x15\x17\x19\x21')

    def test_put_int64(self):
        self.test_bb.putInt64(7992478585871677467)
        assert self.test_bb.get(10, 0) == bytearray(b'n\xea\xfc\xed\x89\xcc8\x1b\x19\x21')

    def test_put_int64_at_position(self):
        self.test_bb.putInt64(7992478585871677467, 2)
        assert self.test_bb.get(10, 0) == bytearray(b'\x03\x05n\xea\xfc\xed\x89\xcc8\x1b')

    def test_put(self):
        self.test_bb.put(b'\x21\x23\x25\x27')
        assert self.test_bb.get(10, 0) == bytearray(b'\x21\x23\x25\x27\x11\x13\x15\x17\x19\x21')

    def test_put_at_position(self):
        self.test_bb.put(b'\x21\x23\x25\x27', 2)
        assert self.test_bb.get(10, 0) == bytearray(b'\x03\x05\x21\x23\x25\x27\x15\x17\x19\x21')

    def test_put_bad_type(self):
        self.assertRaises(TypeError, self.test_bb.put, 56)

    def test_underflow(self):
        self.assertRaises(EOFError, self.test_bb.getInt64, 3)

    def test_position_setter(self):
        self.test_bb.position = 3
        assert self.test_bb.position == 3
        assert self.test_bb.getInt8() == 9

    def test_position_setter_out_of_range(self):
        with self.assertRaises(IndexError):
            self.test_bb.position = 10

    def test_position_setter_bad_type(self):
        with self.assertRaises(TypeError):
            self.test_bb.position = 'nonint'

    def test_position_setter_out_of_range_negative(self):
        with self.assertRaises(IndexError):
            self.test_bb.position = -1

    def test_capacity(self):
        assert self.test_bb.capacity == 10

    def test_limit(self):
        assert self.test_bb.limit == 9

    def test_limit_setter(self):
        self.test_bb.limit = 4
        self.assertRaises(EOFError, self.test_bb.getInt64)

    def test_limit_setter_out_of_range(self):
        with self.assertRaises(IndexError):
            self.test_bb.limit = 10

    def test_limit_setter_bad_type(self):
        with self.assertRaises(TypeError):
            self.test_bb.limit = 'nonint'

    def test_rewind(self):
        self.test_bb.position = 4
        self.test_bb.rewind()
        assert self.test_bb.position == 0

    def test_duplicate(self):
        val = self.test_bb.duplicate()
        assert val._buffer is self.test_bb._buffer
        assert val._first == self.test_bb._first
        assert val.limit == self.test_bb.limit
        assert val.position == self.test_bb.position

    def test_slice(self):
        self.test_bb.position = 4
        val = self.test_bb.slice()
        assert val._buffer is self.test_bb._buffer
        assert val._first == 4
        assert val.limit == self.test_bb.limit
        assert val.position == 4

    def test_slice_rewind(self):
        self.test_bb.position = 4
        val = self.test_bb.slice()
        val.rewind()
        assert val.position == 4
