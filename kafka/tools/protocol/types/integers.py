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
import six
import struct
from kafka.tools.protocol.types import BaseType


def _get_other_value(other):
    if isinstance(other, BaseIntegerType):
        return other._value
    elif isinstance(other, six.integer_types):
        return other
    else:
        raise TypeError("Cannot compare Integer type to {0}".format(type(other)))


class BaseIntegerType(BaseType):
    @abc.abstractproperty
    def _minimum_value(self):  # pragma: no cover
        raise NotImplementedError

    @abc.abstractproperty
    def _maximum_value(self):  # pragma: no cover
        raise NotImplementedError

    @abc.abstractproperty
    def _num_bytes(self):  # pragma: no cover
        raise NotImplementedError

    @abc.abstractproperty
    def _format_character(self):  # pragma: no cover
        raise NotImplementedError

    def _validate(self):
        if not isinstance(self._value, six.integer_types):
            raise TypeError('Expected "{0}" to be an integer, got: {1}'.format(type(self._value), repr(self._value)))
        if (self._value < self._minimum_value) or (self._value > self._maximum_value):
            raise ValueError("{0} is out of range ({1} ... {2})".format(self._value, self._minimum_value, self._maximum_value))
        return True

    @classmethod
    def decode(cls, byte_array, schema=None):
        if len(byte_array) < cls._num_bytes:
            raise ValueError('Expected {0} byte, only got {1}'.format(cls._num_bytes, len(byte_array)))
        items = struct.unpack(">{0}".format(cls._format_character), byte_array[0:cls._num_bytes])
        return cls(items[0], schema=cls._type), byte_array[cls._num_bytes:]

    def encode(self):
        return struct.pack(">{0}".format(self._format_character), self._value)

    def __str__(self):
        return "{0} ({1})".format(self._value, self._type)

    def __repr__(self):
        return "<{0} {1}>".format(self.__class__.__name__, self._value)

    def __eq__(self, other):
        return self._value == _get_other_value(other)

    def __ne__(self, other):
        return self._value != _get_other_value(other)

    def __lt__(self, other):
        return self._value < _get_other_value(other)

    def __le__(self, other):
        return self._value <= _get_other_value(other)

    def __gt__(self, other):
        return self._value > _get_other_value(other)

    def __ge__(self, other):
        return self._value >= _get_other_value(other)

    def __bool__(self):
        return self._value != 0

    def __nonzero__(self):
        return self.__bool__()


class Int8(BaseIntegerType):
    _type = "int8"
    _minimum_value = -128
    _maximum_value = 127
    _num_bytes = 1
    _format_character = 'b'


class Int16(BaseIntegerType):
    _type = "int16"
    _minimum_value = -32768
    _maximum_value = 32767
    _num_bytes = 2
    _format_character = 'h'


class Int32(BaseIntegerType):
    _type = "int32"
    _minimum_value = -2147483648
    _maximum_value = 2147483647
    _num_bytes = 4
    _format_character = 'i'


class Int64(BaseIntegerType):
    _type = "int64"
    _minimum_value = -9223372036854775808
    _maximum_value = 9223372036854775807
    _num_bytes = 8
    _format_character = 'q'


def decode_length(byte_array, length_cls=Int32):
    val_len, val_data = length_cls.decode(byte_array)
    if val_len.value() > len(val_data):
        raise ValueError('Expected {0} bytes, only got {1}'.format(val_len.value() + length_cls._num_bytes, len(byte_array)))
    return val_len, val_data
