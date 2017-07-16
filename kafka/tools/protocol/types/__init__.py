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


@six.add_metaclass(abc.ABCMeta)
class BaseType():  # pragma: no cover
    @abc.abstractmethod
    def decode(cls, byte_array):
        """IMPORTANT: this is class method, override it with @classmethod!"""
        raise NotImplementedError

    def __init__(self, val):
        self._validate(val)
        self._value = val

    def value(self):
        return self._value

    @abc.abstractmethod
    def encode(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _validate(self, val):
        raise NotImplementedError


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

    def _validate(self, val):
        if not isinstance(val, six.integer_types):
            raise TypeError('Expected "{0}" to be an integer, got: {1}'.format(type(val), repr(val)))
        if (val < self._minimum_value) or (val > self._maximum_value):
            raise ValueError("{0} is out of range ({1} ... {2})".format(val, self._minimum_value, self._maximum_value))
        return True

    @classmethod
    def decode(cls, byte_array):
        if len(byte_array) < cls._num_bytes:
            raise ValueError('Expected {0} byte, only got {1}'.format(cls._num_bytes, len(byte_array)))
        items = struct.unpack(">{0}".format(cls._format_character), byte_array[0:cls._num_bytes])
        return cls(items[0]), byte_array[cls._num_bytes:]

    def encode(self):
        return struct.pack(">{0}".format(self._format_character), self._value)
