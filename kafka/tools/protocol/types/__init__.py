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


@six.add_metaclass(abc.ABCMeta)
class BaseType():  # pragma: no cover
    @abc.abstractmethod
    def decode(cls, byte_array, schema=None):
        """IMPORTANT: this is class method, override it with @classmethod!"""
        raise NotImplementedError

    def __init__(self, val, schema=None, skip_validation=False):
        self._value = val
        self._schema = schema
        if not skip_validation:
            self._validate()

    def value(self):
        return self._value

    @abc.abstractmethod
    def encode(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _validate(self, val):
        raise NotImplementedError
