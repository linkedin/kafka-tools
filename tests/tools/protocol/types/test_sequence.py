import random
import unittest
from mock import patch

from kafka.tools.protocol.types.integers import Int8, Int16
from kafka.tools.protocol.types.sequences import _validate_schema_entry_type, _validate_schema_entry, _validate_schema, _evaluate_value, Array, Sequence


class SequenceTests(unittest.TestCase):
    def test_validate_schema_entry_type(self):
        # These are all valid string types, and should not raise an error
        _validate_schema_entry_type('int8')
        _validate_schema_entry_type('int16')
        _validate_schema_entry_type('int32')
        _validate_schema_entry_type('int64')
        _validate_schema_entry_type('boolean')
        _validate_schema_entry_type('string')
        _validate_schema_entry_type('bytes')
        _validate_schema_entry_type('array')

    @patch('kafka.tools.protocol.types.sequences._validate_schema')
    def test_validate_schema_entry_type_sequence(self, mock_validate_schema):
        _validate_schema_entry_type(['a', 'b'])
        mock_validate_schema.assert_called_once_with(['a', 'b'])

    def test_validate_schema_entry_type_badtype(self):
        self.assertRaises(TypeError, _validate_schema_entry_type, 'nonexistant_type')
        self.assertRaises(TypeError, _validate_schema_entry_type, 1)

    def test_validate_schema_entry(self):
        _validate_schema_entry({'name': 'foo', 'type': 'int8'})

    def test_validate_schema_entry_list_type(self):
        _validate_schema_entry({'name': 'foo', 'type': []})

    def test_validate_schema_entry_name_missing(self):
        self.assertRaises(KeyError, _validate_schema_entry, {'type': 'int8'})

    def test_validate_schema_entry_bad_name(self):
        self.assertRaises(TypeError, _validate_schema_entry, {'name': 1, 'type': 'int8'})

    def test_validate_schema_entry_type_missing(self):
        self.assertRaises(KeyError, _validate_schema_entry, {'name': 'foo'})

    def test_validate_schema_entry_empty_dict(self):
        self.assertRaises(KeyError, _validate_schema_entry, {})

    def test_validate_schema_entry_empty_not_dict(self):
        self.assertRaises(TypeError, _validate_schema_entry, 'foo')

    @patch('kafka.tools.protocol.types.sequences._validate_schema_entry_type')
    @patch('kafka.tools.protocol.types.sequences._validate_schema_entry')
    def test_validate_schema(self, mock_entry, mock_entry_type):
        _validate_schema([{'name': 'foo', 'type': 'int8'}])
        mock_entry.assert_called_once_with({'name': 'foo', 'type': 'int8'})
        mock_entry_type.assert_called_once_with('int8')

    def test_validate_schema_array_without_type(self):
        self.assertRaises(KeyError, _validate_schema, [{'name': 'foo', 'type': 'array'}])

    @patch('kafka.tools.protocol.types.sequences._validate_schema_entry_type')
    @patch('kafka.tools.protocol.types.sequences._validate_schema_entry')
    def test_validate_schema_array(self, mock_entry, mock_entry_type):
        _validate_schema([{'name': 'foo', 'type': 'array', 'item_type': 'int8'}])
        mock_entry.assert_called_once_with({'name': 'foo', 'type': 'array', 'item_type': 'int8'})
        mock_entry_type.assert_called_once_with('int8')

    def test_evaluate_value(self):
        val = _evaluate_value({'foo': 3}, [{'name': 'foo', 'type': 'int8'}])
        assert isinstance(val, dict)
        assert len(val) == 1
        assert 'foo' in val
        assert isinstance(val['foo'], Int8)
        assert val['foo'].value() == 3

    def test_evaluate_value_array(self):
        val = _evaluate_value({'foo': [3]}, [{'name': 'foo', 'type': 'array', 'item_type': 'int8'}])
        assert isinstance(val, dict)
        assert len(val) == 1
        assert 'foo' in val
        assert isinstance(val['foo'], Array)
        assert len(val['foo']) == 1
        assert val['foo'][0].value() == 3

    def test_evaluate_value_sequence(self):
        val = _evaluate_value({'foo': {'bar': 3}}, [{'name': 'foo', 'type': [{'name': 'bar', 'type': 'int8'}]}])
        assert 'foo' in val
        assert len(val['foo']) == 1
        assert 'bar' in val['foo']
        assert val['foo']['bar'].value() == 3

    def test_evaluate_value_novalue(self):
        self.assertRaises(KeyError, _evaluate_value, {'bar': 3}, [{'name': 'foo', 'type': 'int8'}])

    @patch('kafka.tools.protocol.types.sequences._validate_schema')
    @patch('kafka.tools.protocol.types.sequences._evaluate_value')
    def test_create(self, mock_evaluate, mock_validate):
        mock_evaluate.return_value = {'foo': 'evaluated_value'}
        val = Sequence({'foo': 'original_value'}, schema=['fake_schema'])
        assert val._schema == ['fake_schema']
        assert val._value == {'foo': 'evaluated_value'}

    @patch('kafka.tools.protocol.types.sequences._evaluate_value')
    def test_create_bad_schema(self, mock_evaluate):
        self.assertRaises(TypeError, Sequence, {'foo': 3}, schema='not_a_list')

    @patch('kafka.tools.protocol.types.sequences._validate_schema')
    def test_create_bad_value(self, mock_validate):
        self.assertRaises(TypeError, Sequence, 'not_a_dict', schema=[{'name': 'foo', 'type': 'int8'}])

    def test_decode(self):
        obj, remaining_bytes = Sequence.decode(b'{', [{'name': 'foo', 'type': 'int8'}])
        assert isinstance(obj, Sequence)
        assert remaining_bytes == b''

        assert len(obj) == 1
        assert 'foo' in obj
        assert isinstance(obj['foo'], Int8)
        assert obj['foo'].value() == 123

    def test_decode_two(self):
        obj, remaining_bytes = Sequence.decode(b'{\x00[', [{'name': 'foo', 'type': 'int8'}, {'name': 'bar', 'type': 'int16'}])
        assert isinstance(obj, Sequence)
        assert remaining_bytes == b''

        assert len(obj) == 2
        assert 'foo' in obj
        assert isinstance(obj['foo'], Int8)
        assert obj['foo'].value() == 123
        assert 'bar' in obj
        assert isinstance(obj['bar'], Int16)
        assert obj['bar'].value() == 91

    def test_decode_remaining(self):
        obj, remaining_bytes = Sequence.decode(b'{\x00[abc', [{'name': 'foo', 'type': 'int8'}, {'name': 'bar', 'type': 'int16'}])
        assert remaining_bytes == b'abc'

    def test_decode_array(self):
        obj, remaining_bytes = Sequence.decode(b'\x00\x00\x00\x00\x00\x02{[abc',
                                               [{'name': 'foo', 'type': 'int16'}, {'name': 'bar', 'type': 'array', 'item_type': 'int8'}])
        assert len(obj) == 2
        assert len(obj['bar']) == 2
        assert isinstance(obj['bar'][0], Int8)
        assert obj['bar'][0].value() == 123
        assert isinstance(obj['bar'][1], Int8)
        assert obj['bar'][1].value() == 91

    def test_encode(self):
        obj, remaining_bytes = Sequence.decode(b'{', [{'name': 'foo', 'type': 'int8'}])
        assert remaining_bytes == b''

        val = obj.encode()
        assert val == b'{'

    def test_iterator(self):
        value_dict = {}
        schema_arr = []
        values = []
        for i in range(100):
            v = random.randrange(100000)
            k = 'knum_{0}'.format(i)
            schema_arr.append({'name': k, 'type': 'int32'})
            value_dict[k] = v
            values.append(v)

        obj = Sequence(value_dict, schema=schema_arr)
        t_iter = iter(obj)
        c = 0
        for item in t_iter:
            assert item.value() == values[c]
            c += 1
        assert c == 100

    def test_iterator_reverse(self):
        value_dict = {}
        schema_arr = []
        values = []
        for i in range(100):
            v = random.randrange(100000)
            k = 'knum_{0}'.format(i)
            schema_arr.append({'name': k, 'type': 'int32'})
            value_dict[k] = v
            values.append(v)

        obj = Sequence(value_dict, schema=schema_arr)
        t_iter = reversed(obj)
        c = -1
        for item in t_iter:
            assert item.value() == values[c]
            c -= 1
        assert c == -101

    def test_setitem_failure(self):
        obj = Sequence({'foo': 3}, schema=[{'name': 'foo', 'type': 'int8'}])
        self.assertRaises(NotImplementedError, obj.__setitem__, 'bar', 5)

    def test_delitem_failure(self):
        obj = Sequence({'foo': 3}, schema=[{'name': 'foo', 'type': 'int8'}])
        self.assertRaises(NotImplementedError, obj.__delitem__, 'foo')

    def test_str(self):
        obj = Sequence({'foo': 34, 'bar': [123, 91]}, [{'name': 'foo', 'type': 'int16'}, {'name': 'bar', 'type': 'array', 'item_type': 'int8'}])
        print(str(obj))
        assert str(obj) == "foo: 34 (int16)\nbar: 2 items (array)\n    123 (int8)\n    ---\n    91 (int8)"
        assert repr(obj) == "<Sequence length=2>"
