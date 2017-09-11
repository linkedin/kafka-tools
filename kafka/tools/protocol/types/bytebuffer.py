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


class ByteBuffer(object):
    def __init__(self, value):
        if isinstance(value, bytearray):
            self._buffer = value
        elif isinstance(value, six.binary_type) or isinstance(value, six.integer_types):
            self._buffer = bytearray(value)
        else:
            raise TypeError("initialization of ByteBuffer must be a bytearray, binary string, or integer")

        self._position = 0
        self._first = 0
        self._last = len(self._buffer) - 1

    @property
    def position(self):
        """The current position within the buffer"""
        return self._position

    @position.setter
    def position(self, value):
        if not isinstance(value, six.integer_types):
            raise TypeError("position must be an integer")
        self._check_position_value(value)
        self._position = value

    @property
    def limit(self):
        """The last position in the buffer"""
        return self._last

    @limit.setter
    def limit(self, value):
        if not isinstance(value, six.integer_types):
            raise TypeError("limit must be an integer")
        self._check_position_value(value, pos_type='limit')
        self._last = value

    @property
    def capacity(self):
        return self._last - self._first + 1

    @property
    def remaining(self):
        return self._last - self._position + 1

    def rewind(self):
        self._position = self._first

    def duplicate(self):
        new_bb = ByteBuffer(self._buffer)
        new_bb._first = self._first
        new_bb._position = self._position
        new_bb._last = self._last
        return new_bb

    def slice(self):
        new_bb = ByteBuffer(self._buffer)
        new_bb._first = self._position
        new_bb._position = self._position
        new_bb._last = self._last
        return new_bb

    def getInt8(self, position=None):
        return self._read_integer(position, 1, 'b')

    def putInt8(self, value, position=None):
        return self._write_integer(value, position, 1, 'b')

    def getInt16(self, position=None):
        return self._read_integer(position, 2, 'h')

    def putInt16(self, value, position=None):
        return self._write_integer(value, position, 2, 'h')

    def getInt32(self, position=None):
        return self._read_integer(position, 4, 'i')

    def putInt32(self, value, position=None):
        return self._write_integer(value, position, 4, 'i')

    def getInt64(self, position=None):
        return self._read_integer(position, 8, 'q')

    def putInt64(self, value, position=None):
        return self._write_integer(value, position, 8, 'q')

    def get(self, num_bytes, position=None):
        pos = self._get_and_check_position(position, num_bytes)
        return self._buffer[pos:pos+num_bytes]

    def put(self, byte_str, position=None):
        if not isinstance(byte_str, six.binary_type):
            raise TypeError("argument must be a binary string")

        num_bytes = len(byte_str)
        pos = self._get_and_check_position(position, num_bytes)
        self._buffer[pos:pos+num_bytes] = byte_str

    def _read_integer(self, position, num_bytes, struct_char):
        pos = self._get_and_check_position(position, num_bytes)
        return struct.unpack('>{0}'.format(struct_char), self._buffer[pos:pos+num_bytes])[0]

    def _write_integer(self, value, position, num_bytes, struct_char):
        pos = self._get_and_check_position(position, num_bytes)
        self.put(struct.pack('>{0}'.format(struct_char), value), position=pos)

    def _get_and_check_position(self, position, num_bytes):
        if position is None:
            pos = self._position
            self._position = min(self._position + num_bytes, self._last + 1)
        else:
            self._check_position_value(position)
            pos = position

        if (self._last - pos + 1) < num_bytes:
            raise EOFError("not enough data left in buffer")
        return pos

    def _check_position_value(self, position, pos_type='position'):
        if (position < self._first) or (position > self._last):
            raise IndexError("{0} is out of range".format(pos_type))
