import unittest
from mock import patch


from kafka.tools.protocol.requests import _evaluate_plain_value, _evaluate_sequence, _encode_plain_value, _encode_sequence
from kafka.tools.protocol.types.bytebuffer import ByteBuffer


class BaseRequestTests(unittest.TestCase):
    def test_evaluate_plain_value(self):
        _evaluate_plain_value(1, 'int8')
        _evaluate_plain_value(1, 'int16')
        _evaluate_plain_value(1, 'int32')
        _evaluate_plain_value(1, 'int64')
        _evaluate_plain_value('foo', 'string')
        _evaluate_plain_value(b'foo', 'bytes')
        _evaluate_plain_value(True, 'boolean')
        assert True

    @patch('kafka.tools.protocol.requests._evaluate_sequence')
    def test_evaluate_plain_value_sequence(self, mock_eval_seq):
        _evaluate_plain_value({'foo': 'bar'}, [{'name': 'foo', 'type': 'string'}])
        mock_eval_seq.assert_called_once_with({'foo': 'bar'}, [{'name': 'foo', 'type': 'string'}])

    def test_evaluate_plain_value_errors(self):
        self.assertRaises(TypeError, _evaluate_plain_value, 'foo', 'int8')
        self.assertRaises(TypeError, _evaluate_plain_value, 'foo', 'int16')
        self.assertRaises(TypeError, _evaluate_plain_value, 'foo', 'int32')
        self.assertRaises(TypeError, _evaluate_plain_value, 'foo', 'int64')
        self.assertRaises(TypeError, _evaluate_plain_value, 1, 'string')
        self.assertRaises(TypeError, _evaluate_plain_value, 1, 'bytes')
        self.assertRaises(NotImplementedError, _evaluate_plain_value, 1, 'unknowntype')

    def test_evaluate_sequence(self):
        _evaluate_sequence({'foo': 1}, [{'name': 'foo', 'type': 'int8'}])
        _evaluate_sequence({'foo': None}, [{'name': 'foo', 'type': 'array', 'item_type': 'int8'}])
        _evaluate_sequence({'foo': [1]}, [{'name': 'foo', 'type': 'array', 'item_type': 'int8'}])
        _evaluate_sequence({'foo': [{'bar': 1}]}, [{'name': 'foo', 'type': 'array', 'item_type': [{'name': 'bar', 'type': 'int8'}]}])

    def test_evaluate_sequence_errors(self):
        self.assertRaises(KeyError, _evaluate_sequence, {}, [{'name': 'foo', 'type': 'int8'}])
        self.assertRaises(TypeError, _evaluate_sequence, {'foo': 1}, [{'name': 'foo', 'type': 'array', 'item_type': 'int8'}])

    def test_encode_plain_value(self):
        buf = ByteBuffer(10)
        _encode_plain_value(1, 'int8', buf)
        assert buf.get(10, 0) == bytearray(b'\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00')

        buf = ByteBuffer(10)
        _encode_plain_value(1, 'int16', buf)
        assert buf.get(10, 0) == bytearray(b'\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00')

        buf = ByteBuffer(10)
        _encode_plain_value(1, 'int32', buf)
        assert buf.get(10, 0) == bytearray(b'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00')

        buf = ByteBuffer(10)
        _encode_plain_value(1, 'int64', buf)
        assert buf.get(10, 0) == bytearray(b'\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00')

        buf = ByteBuffer(10)
        _encode_plain_value(True, 'boolean', buf)
        assert buf.get(10, 0) == bytearray(b'\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00')

        buf = ByteBuffer(10)
        _encode_plain_value('foo', 'string', buf)
        assert buf.get(10, 0) == bytearray(b'\x00\x03foo\x00\x00\x00\x00\x00')

        buf = ByteBuffer(10)
        _encode_plain_value(None, 'string', buf)
        assert buf.get(10, 0) == bytearray(b'\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00')

        buf = ByteBuffer(10)
        _encode_plain_value(b'\x78\x23', 'bytes', buf)
        assert buf.get(10, 0) == bytearray(b'\x00\x00\x00\x02\x78\x23\x00\x00\x00\x00')

        buf = ByteBuffer(10)
        _encode_plain_value(None, 'bytes', buf)
        assert buf.get(10, 0) == bytearray(b'\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00')

    def test_encode_plain_value_errors(self):
        buf = ByteBuffer(10)
        self.assertRaises(NotImplementedError, _encode_plain_value, {'foo': 1}, {'name': 'foo', 'type': 'unknowntype'}, buf)

    def test_encode_sequence(self):
        buf = ByteBuffer(10)
        _encode_sequence({'foo': 1}, [{'name': 'foo', 'type': 'int8'}], buf)
        assert buf.get(10, 0) == bytearray(b'\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00')

        buf = ByteBuffer(10)
        _encode_sequence({'foo': None}, [{'name': 'foo', 'type': 'array', 'item_type': 'int8'}], buf)
        assert buf.get(10, 0) == bytearray(b'\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00')

        buf = ByteBuffer(10)
        _encode_sequence({'foo': [1]}, [{'name': 'foo', 'type': 'array', 'item_type': 'int8'}], buf)
        assert buf.get(10, 0) == bytearray(b'\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00')

        buf = ByteBuffer(10)
        _encode_sequence({'foo': [{'bar': 1}]}, [{'name': 'foo', 'type': 'array', 'item_type': [{'name': 'bar', 'type': 'int8'}]}], buf)
        assert buf.get(10, 0) == bytearray(b'\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00')
