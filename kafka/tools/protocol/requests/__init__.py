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

from kafka.tools.encoding import encode_boolean, encode_int8, encode_int16, encode_int32, encode_int64, encode_string, encode_bytes


# Provide a mapping of schema names to encoder methods
basic_type_encoder = {
    'boolean': encode_boolean,
    'int8': encode_int8,
    'int16': encode_int16,
    'int32': encode_int32,
    'int64': encode_int64,
    'string': encode_string,
    'bytes': encode_bytes,
}


@six.add_metaclass(abc.ABCMeta)
class BaseRequest():  # pragma: no cover
    @abc.abstractproperty
    def request_format(self):
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

    def __init__(self, cmd_args):
        self.payload = self.process_arguments(cmd_args)

    def __hash__(self):
        return id(self)

    def encode(self):
        return encode_struct(self.payload, self.request_format)

    @abc.abstractmethod
    def process_arguments(self, cmd_args):
        raise NotImplementedError

    @abc.abstractmethod
    def response(self):
        raise NotImplementedError

    @abc.abstractmethod
    def show_help(self):
        raise NotImplementedError


def encode_object(obj, obj_def):
    if isinstance(obj_def['type'], six.string_types):
        return encode_basic_types(obj, obj_def)
    elif isinstance(obj_def['type'], collections.Sequence):
        return encode_struct(obj, obj_def['type'])
    else:
        raise TypeError("Request definition type must be a string or a sequence, not: ".format(obj_def['type']))
    pass


def encode_basic_types(obj, obj_def):
    if obj_def['type'] == 'array':
        return encode_array(obj, obj_def['item_type'])
    if obj_def['type'] in basic_type_encoder:
        return basic_type_encoder[obj_def['type']](obj)
    else:
        raise ValueError("Unknown protocol type: {0}".format(obj_def['type']))


def encode_struct(obj, obj_def):
    byte_array = b''
    for i, item_def in enumerate(obj_def):
        byte_array += encode_object(obj[i], item_def)
    return byte_array


def encode_array(item_array, item_def):
    if item_array is None:
        return encode_int32(-1)
    byte_array = encode_int32(len(item_array))
    for item in item_array:
        byte_array += encode_object(item, {'type': item_def})
    return byte_array
