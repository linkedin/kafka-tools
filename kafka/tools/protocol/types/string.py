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

import six
import struct
from kafka.tools.protocol.types import BaseType
from kafka.tools.protocol.types.integers import Int16, decode_length


class String(BaseType):
    _type = "string"

    def _validate(self):
        if self._value is None:
            return
        if not isinstance(self._value, six.string_types):
            raise TypeError('Expected "{0}" to be {1}, got: {2}'.format(type(self._value), six.string_types, repr(self._value)))
        try:
            self._value = self._value.decode("utf-8")
        except AttributeError:
            pass

    @classmethod
    def decode(cls, byte_array, schema=None):
        str_len, str_data = decode_length(byte_array, length_cls=Int16)
        if str_len == -1:
            return cls(None, schema=cls._type), str_data
        return cls(str_data[0:str_len.value()].decode("utf-8"), schema=cls._type), str_data[str_len.value():]

    def encode(self):
        if self._value is None:
            return Int16(-1).encode()
        str_len = Int16(len(self._value))
        return str_len.encode() + struct.pack('{0}s'.format(str_len.value()), self._value.encode("utf-8"))

    def __str__(self):
        return '{0} (string)'.format(self._value)

    def __repr__(self):
        return '<String length={0}>'.format(len(self._value))
