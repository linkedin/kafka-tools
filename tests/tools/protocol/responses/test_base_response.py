import unittest

from kafka.tools.protocol.responses import _decode_plain_type, _decode_sequence, _decode_array
from kafka.tools.protocol.types.bytebuffer import ByteBuffer


class BaseResponseTests(unittest.TestCase):
    def test_decode_plain_value(self):
        assert _decode_plain_type('int8', ByteBuffer(bytearray(b'\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00'))) == 1
        assert _decode_plain_type('int16', ByteBuffer(bytearray(b'\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00'))) == 1
        assert _decode_plain_type('int32', ByteBuffer(bytearray(b'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00'))) == 1
        assert _decode_plain_type('int64', ByteBuffer(bytearray(b'\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00'))) == 1
        assert _decode_plain_type('boolean', ByteBuffer(bytearray(b'\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00'))) is True
        assert _decode_plain_type('string', ByteBuffer(bytearray(b'\x00\x03foo\x00\x00\x00\x00\x00'))) == 'foo'
        assert _decode_plain_type('string', ByteBuffer(bytearray(b'\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00'))) is None
        assert _decode_plain_type('bytes', ByteBuffer(bytearray(b'\x00\x00\x00\x02\x78\x23\x00\x00\x00\x00'))) == b'\x78\x23'
        assert _decode_plain_type('bytes', ByteBuffer(bytearray(b'\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00'))) is None

    def test_decode_plain_value_errors(self):
        self.assertRaises(NotImplementedError, _decode_plain_type, 'unknowntype', ByteBuffer(10))

    def test_decode_array(self):
        assert _decode_array('int8', ByteBuffer(bytearray(b'\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00'))) == [1]
        assert _decode_array('int8', ByteBuffer(bytearray(b'\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00'))) is None
        assert _decode_array([{'name': 'bar', 'type': 'int8'}], ByteBuffer(bytearray(b'\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00'))) == [{'bar': 1}]

    def test_decode_sequence(self):
        schema = [{'name': 'bar', 'type': 'int8'}]
        assert _decode_sequence(schema, ByteBuffer(bytearray(b'\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00'))) == {'bar': 1}

        schema = [{'name': 'bar', 'type': 'array', 'item_type': 'int8'}]
        assert _decode_sequence(schema, ByteBuffer(bytearray(b'\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00'))) == {'bar': [1]}
