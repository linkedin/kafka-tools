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
from kafka.tools.protocol.types.integers import Int16


class Bytes(BaseType):
    def _validate(self, val):
        if val is None:
            return True
        if not isinstance(val, six.binary_type):
            raise TypeError('Expected "{0}" to be {1}, got: {2}'.format(type(val), six.binary_type, repr(val)))
        if len(val) % 2 != 0:
            raise ValueError('String must be of even length, got {0} characters'.format(len(val)))
        return True

    @classmethod
    def decode(cls, byte_array):
        if len(byte_array) < 2:
            raise ValueError('Expected at least 2 bytes, only got {0}'.format(len(byte_array)))
        str_len, str_data = Int16.decode(byte_array)
        if str_len.value() == -1:
            return cls(None), str_data
        if str_len.value() > len(str_data):
            raise ValueError('Expected {0} bytes, only got {1}'.format(str_len.value() + 2, len(byte_array)))
        return cls(binascii.hexlify(str_data[0:str_len.value()])), str_data[str_len.value():]

    def encode(self):
        if self._value is None:
            return Int16(-1).encode()
        str_len = Int16(len(self._value) // 2)

        bytes = binascii.unhexlify(self._value)
        return str_len.encode() + bytes
