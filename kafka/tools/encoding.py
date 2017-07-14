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
import struct


def encode_boolean(num):
    if num is None or (not num):
        return encode_int8(0)
    return encode_int8(1)


def encode_int8(num):
    if num is not None and not isinstance(num, six.integer_types):
        raise TypeError('Expected "{0}" to be an integer, got: {1}'.format(type(num), repr(num)))
    return struct.pack('>b', num)


def encode_int16(num):
    if num is not None and not isinstance(num, six.integer_types):
        raise TypeError('Expected "{0}" to be an integer, got: {1}'.format(type(num), repr(num)))
    return struct.pack('>h', num)


def encode_int32(num):
    if num is not None and not isinstance(num, six.integer_types):
        raise TypeError('Expected "{0}" to be an integer, got: {1}'.format(type(num), repr(num)))
    return struct.pack('>i', num)


def encode_int64(num):
    if num is not None and not isinstance(num, six.integer_types):
        raise TypeError('Expected "{0}" to be an integer, got: {1}'.format(type(num), repr(num)))
    return struct.pack('>q', num)


def encode_string(str):
    if str is None:
        return struct.pack('>h', -1)
    if not isinstance(str, six.string_types):
        raise TypeError('Expected "{0}" to be a string, got: {1}'.format(type(str), repr(str)))

    str_len = len(str)
    return struct.pack('>h{0}s'.format(str_len), str_len, str)


def encode_bytes(str):
    if str is None:
        return struct.pack('>h', -1)
    if not isinstance(str, six.string_types):
        raise TypeError('Expected "{0}" to be a string, got: {1}'.format(type(str), repr(str)))

    str_len = len(str)
    bytes = binascii.unhexlify(str)
    return encode_int16(str_len / 2) + bytes
