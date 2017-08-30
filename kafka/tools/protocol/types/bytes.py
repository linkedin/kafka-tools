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

import binascii
import six
from kafka.tools.protocol.types import BaseType
from kafka.tools.protocol.types.integers import Int32, decode_length


class Bytes(BaseType):
    _type = "bytes"

    def _validate(self):
        if self._value is None:
            return
        if not isinstance(self._value, six.binary_type):
            raise TypeError('Expected {0}, got: {1}'.format(six.binary_type, type(self._value)))

    @classmethod
    def decode(cls, byte_array, schema=None):
        str_len, str_data = decode_length(byte_array, length_cls=Int32)
        if str_len.value() == -1:
            return cls(None, schema=cls._type), str_data
        return cls(str_data[0:str_len.value()], schema=cls._type), str_data[str_len.value():]

    @classmethod
    def from_string(cls, hex_str):
        if hex_str is None:
            return cls(None, schema=cls._type)
        if not isinstance(hex_str, six.string_types):
            raise TypeError('Expected {0}, got: {1}'.format(six.string_types, type(hex_str)))
        if len(hex_str) % 2 != 0:
            raise ValueError('String must be of even length, got {0} characters'.format(len(hex_str)))
        return cls(binascii.unhexlify(hex_str), schema=cls._type)

    def encode(self):
        if self._value is None:
            return Int32(-1).encode()
        str_len = Int32(len(self._value))
        return str_len.encode() + self._value

    def __str__(self):
        return '{0} (bytes)'.format(binascii.hexlify(self._value).decode("utf-8"))

    def __repr__(self):
        return '<Bytes length={0}>'.format(len(self._value))
