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

import abc
import collections
import six
import struct


class ArgumentError(Exception):
    pass


def _evaluate_plain_value(value, value_type):
    if not isinstance(value_type, six.string_types):
        _evaluate_sequence(value, value_type)
    elif value_type in ('int8', 'int16', 'int32', 'int64'):
        if not isinstance(value, six.integer_types):
            raise TypeError("Expected an integer, got {0} instead".format(type(value)))
    elif value_type == 'string':
        if (value is not None) and (not isinstance(value, six.string_types)):
            raise TypeError("Expected a string, got {0} instead".format(type(value)))
    elif value_type == 'bytes':
        if (value is not None) and (not isinstance(value, six.binary_type)):
            raise TypeError("Expected a binary string, got {0} instead".format(type(value)))
    elif value_type == 'boolean':
        # Everything evaluates as boolean
        pass
    else:
        raise NotImplementedError("Reference to non-implemented type in schema: {0}".format(value_type))


def _evaluate_sequence(value, schema):
    for entry in schema:
        if entry['name'] not in value:
            raise KeyError('Value is missing an entry with key "{0}"'.format(entry['name']))
        entry_schema = entry['type']
        if isinstance(entry_schema, six.string_types) and entry_schema.lower() == 'array':
            if isinstance(value[entry['name']], collections.Sequence):
                for item in value[entry['name']]:
                    _evaluate_plain_value(item, entry['item_type'])
            elif value[entry['name']] is None:
                pass
            else:
                raise TypeError("entry '{0}' is not a list".format(entry['name']))
        else:
            _evaluate_plain_value(value[entry['name']], entry_schema)


def _encode_plain_value(value, value_type, buf):
    if value_type == 'int8':
        buf.putInt8(value)
    elif value_type == 'int16':
        buf.putInt16(value)
    elif value_type == 'int32':
        buf.putInt32(value)
    elif value_type == 'int64':
        buf.putInt64(value)
    elif value_type == 'string':
        if value is None:
            buf.putInt16(-1)
        else:
            buf.putInt16(len(value))
            buf.put(struct.pack('{0}s'.format(len(value)), value.encode("utf-8")))
    elif value_type == 'bytes':
        if value is None:
            buf.putInt32(-1)
        else:
            buf.putInt32(len(value))
            buf.put(value)
    elif value_type == 'boolean':
        buf.putInt8(1 if value else 0)
    elif isinstance(value_type, collections.Sequence):
        _encode_sequence(value, value_type, buf)
    else:
        raise NotImplementedError("Reference to non-implemented type in schema: {0}".format(value_type))


def _encode_sequence(value, schema, buf):
    for entry in schema:
        entry_schema = entry['type']
        if isinstance(entry_schema, six.string_types) and entry_schema.lower() == 'array':
            if value[entry['name']] is None:
                buf.putInt32(-1)
            else:
                buf.putInt32(len(value[entry['name']]))
                for item in value[entry['name']]:
                    _encode_plain_value(item, entry['item_type'], buf)
        else:
            _encode_plain_value(value[entry['name']], entry_schema, buf)


@six.add_metaclass(abc.ABCMeta)
class BaseRequest():  # pragma: no cover
    @abc.abstractproperty
    def schema(self):
        raise NotImplementedError

    @abc.abstractproperty
    def api_key(self):
        raise NotImplementedError

    @abc.abstractproperty
    def api_version(self):
        pass

    @abc.abstractproperty
    def cmd(self):
        raise NotImplementedError

    @abc.abstractproperty
    def response(self):
        raise NotImplementedError

    @abc.abstractproperty
    def help_string(self):
        raise NotImplementedError

    def __init__(self, value):
        _evaluate_sequence(value, self.schema)
        self._request = value

    def encode(self, buf):
        _encode_sequence(self._request, self.schema, buf)

    @abc.abstractmethod
    def process_arguments(cls, cmd_args):
        """IMPORTANT: this is class method, override it with @classmethod!"""
        raise NotImplementedError

    def __hash__(self):
        return id(self)

    def __len__(self):
        return len(self._request)

    def __contains__(self, k):
        return k in self._request

    def __getitem__(self, k):
        return self._request[k]

    def __setitem__(self, k):
        raise NotImplementedError

    def __delitem__(self, k):
        raise NotImplementedError
