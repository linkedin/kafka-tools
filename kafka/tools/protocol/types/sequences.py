# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import collections
import six

from kafka.tools.protocol.types import BaseType
from kafka.tools.protocol.types.boolean import Boolean
from kafka.tools.protocol.types.bytes import Bytes
from kafka.tools.protocol.types.integers import Int8, Int16, Int32, Int64
from kafka.tools.protocol.types.string import String


# We need to define this early, but Array won't be in scope until later
schema_type_map = {}


def indent(text, amount, ch=' '):
    padding = amount * ch
    return ''.join(padding+line for line in text.splitlines(True))


def _validate_schema_entry_type(entry_type):
    if isinstance(entry_type, six.string_types):
        if entry_type.lower() not in schema_type_map:
            raise TypeError('Unknown schema data type: {0}'.format(entry_type))
    elif isinstance(entry_type, collections.Sequence):
        _validate_schema(entry_type)
    else:
        raise TypeError('Schema type must be a string type or a list, not {0}'.format(type(entry_type)))


def _validate_schema_entry(entry):
    if not isinstance(entry, dict):
        raise TypeError('Expected schema entries to be dicts, not {0}'.format(type(entry)))
    if not all(k in entry for k in ('name', 'type')):
        raise KeyError('Schema entries must have both name and type keys')
    if not isinstance(entry['name'], six.string_types):
        raise TypeError('Schema entry name must be a string, not {0}'.format(type(entry['name'])))


def _validate_schema(schema):
    for entry in schema:
        _validate_schema_entry(entry)
        if isinstance(entry['type'], six.string_types) and (entry['type'].lower() == 'array'):
            if 'item_type' not in entry:
                raise KeyError('Array schema entries must have an item_type key')
            _validate_schema_entry_type(entry['item_type'])
        else:
            _validate_schema_entry_type(entry['type'])


def _evaluate_value(value, schema):
    rv = {}
    for entry in schema:
        if entry['name'] not in value:
            raise KeyError('Value is missing an entry with key "{0}"'.format(entry['name']))
        entry_schema = entry['type']
        if isinstance(entry_schema, six.string_types):
            if entry_schema.lower() == 'array':
                entry_schema = entry['item_type']
            rv[entry['name']] = schema_type_map[entry['type'].lower()](value[entry['name']], entry_schema)
        else:
            rv[entry['name']] = Sequence(value[entry['name']], entry_schema)
    return rv


class SequenceIterator():
    def __init__(self, iter_obj, reversed=False):
        self._iter_obj = iter_obj
        self._reversed = reversed

        if self._reversed:
            # We're going to use negative indexing for reverse
            self._next_idx = 1
            self._last_idx = len(self._iter_obj._schema)
        else:
            self._next_idx = 0
            self._last_idx = len(self._iter_obj._schema) - 1

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        if self._next_idx > self._last_idx:
            raise StopIteration
        rv_idx = self._next_idx
        self._next_idx += 1
        if self._reversed:
            return self._iter_obj[self._iter_obj._schema[-rv_idx]['name']]
        else:
            return self._iter_obj[self._iter_obj._schema[rv_idx]['name']]


class Sequence(BaseType):
    _type = 'sequence'

    def _validate(self):
        if (not isinstance(self._schema, collections.Sequence)) or isinstance(self._schema, six.string_types):
            raise TypeError('Expected schema to be a list, not {0}'.format(type(self._schema)))
        _validate_schema(self._schema)

        if not isinstance(self._value, dict):
            raise TypeError('Expected value to be a dict, not {0}'.format(type(self._value)))
        self._value = _evaluate_value(self._value, self._schema)

    @classmethod
    def decode(cls, byte_array, schema=None):
        _validate_schema(schema)

        val = {}
        remaining_bytes = byte_array
        for entry in schema:
            if entry['type'].lower() == 'array':
                val[entry['name']], remaining_bytes = schema_type_map[entry['type']].decode(remaining_bytes, schema=entry['item_type'])
            else:
                val[entry['name']], remaining_bytes = schema_type_map[entry['type']].decode(remaining_bytes)

        return cls(val, schema=schema, skip_validation=True), remaining_bytes

    def encode(self):
        byte_array = b''
        for item in self._schema:
            byte_array += self._value[item['name']].encode()
        return byte_array

    def __len__(self):
        return len(self._value)

    def __contains__(self, k):
        return k in self._value

    def __getitem__(self, k):
        return self._value[k]

    def __setitem__(self, k, value):
        raise NotImplementedError

    def __delitem__(self, k):
        raise NotImplementedError

    def __iter__(self):
        return SequenceIterator(self)

    def __reversed__(self):
        return SequenceIterator(self, reversed=True)

    def __str__(self):
        lines = ["{0}: {1}".format(item['name'], self._value[item['name']]) for item in self._schema]
        return "\n".join(lines)

    def __repr__(self):
        return '<Sequence length={0}>'.format(len(self))


class ArrayIterator():
    def __init__(self, iter_obj, reversed=False):
        self._iter_obj = iter_obj
        self._reversed = reversed

        if self._reversed:
            # We're going to use negative indexing for reverse
            self._next_idx = 1
            self._last_idx = len(self._iter_obj)
        else:
            self._next_idx = 0
            self._last_idx = len(self._iter_obj) - 1

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        if self._next_idx > self._last_idx:
            raise StopIteration
        rv_idx = self._next_idx
        self._next_idx += 1
        if self._reversed:
            return self._iter_obj[-rv_idx]
        else:
            return self._iter_obj[rv_idx]


class Array(BaseType):
    _type = "array"

    def _evaluate_value(self):
        try:
            klass = schema_type_map[self._schema.lower()] if isinstance(self._schema, six.string_types) else Sequence
        except KeyError:
            raise TypeError('Unknown schema type {0}'.format(self._schema.lower()))
        self._value = [klass(item, schema=self._schema) for item in self._value]

    def _validate(self):
        if self._value is None:
            return
        if (not isinstance(self._value, collections.Sequence)) or isinstance(self._value, six.string_types):
            raise ValueError("The value must be None or a list, not {0}".format(type(self._value)))
        self._evaluate_value()

    @classmethod
    def decode(cls, byte_array, schema=None):
        array_len, remaining_bytes = Int32.decode(byte_array)
        if array_len.value() == -1:
            return cls(None, schema=schema), remaining_bytes

        try:
            klass = schema_type_map[schema.lower()] if isinstance(schema, six.string_types) else Sequence
        except KeyError:
            raise TypeError('Unknown schema type {0}'.format(schema.lower()))
        val = []
        for i in range(array_len.value()):
            item, remaining_bytes = klass.decode(remaining_bytes, schema)
            val.append(item)
        return cls(val, schema=schema, skip_validation=True), remaining_bytes

    def encode(self):
        if self._value is None:
            return Int32(-1).encode()

        byte_array = Int32(len(self._value)).encode()
        for item in self._value:
            byte_array += item.encode()
        return byte_array

    def __len__(self):
        # For the sake of interactions with code that treats this like an iterator, a null array is of zero length
        if self._value is None:
            return 0
        return len(self._value)

    def __contains__(self, item):
        if self._value is None:
            return False
        return any(item == x for x in self._value)

    def __getitem__(self, k):
        return self._value[k]

    def __setitem__(self, k, value):
        raise NotImplementedError

    def __delitem__(self, k):
        raise NotImplementedError

    def __iter__(self):
        return ArrayIterator(self)

    def __reversed__(self):
        return ArrayIterator(self, reversed=True)

    def __str__(self):
        lines = ["{0} items (array)".format(len(self))]
        lines.append("\n    ---\n".join([indent(str(item), 4) for item in self]))
        return "\n".join(lines)

    def __repr__(self):
        return "<Array length={0}>".format(len(self))


# Now we can fully declare this map
schema_type_map = {
    'array': Array,
    'boolean': Boolean,
    'bytes': Bytes,
    'int8': Int8,
    'int16': Int16,
    'int32': Int32,
    'int64': Int64,
    'string': String,
}
