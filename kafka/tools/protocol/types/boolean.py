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

from kafka.tools.protocol.types import BaseType
from kafka.tools.protocol.types.integers import Int8


class Boolean(BaseType):
    _type = "boolean"

    def _validate(self):
        if self._value:
            self._value = True
            self._underlying_value = Int8(1)
        else:
            self._value = False
            self._underlying_value = Int8(0)

    @classmethod
    def decode(cls, byte_array, schema=None):
        underlying_value, remaining_bytes = Int8.decode(byte_array)
        return cls(underlying_value.value() == 1, schema=cls._type), remaining_bytes

    def encode(self):
        return self._underlying_value.encode()

    def __bool__(self):
        return self._value

    def __nonzero__(self):
        return self.__bool__()

    def __str__(self):
        return '{0} (boolean)'.format(self._value)

    def __repr__(self):
        return '<Boolean {0}>'.format(self._value)
