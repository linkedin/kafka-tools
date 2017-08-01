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

from kafka.tools.protocol.types.sequences import Sequence


class ArgumentError(Exception):
    pass


@six.add_metaclass(abc.ABCMeta)
class BaseRequest():  # pragma: no cover
    @abc.abstractproperty
    def schema(self):
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

    @abc.abstractproperty
    def response(self):
        raise NotImplementedError

    @abc.abstractproperty
    def help_string(self):
        raise NotImplementedError

    def __init__(self, value):
        self._request = Sequence(value, schema=self.schema)

    def encode(self):
        return self._request.encode()

    @abc.abstractmethod
    def process_arguments(cls, cmd_args):
        """IMPORTANT: this is class method, override it with @classmethod!"""
        raise NotImplementedError

    def __hash__(self):
        return id(self)

    def __len__(self):
        return len(self._request)

    def __contains__(self, k):
        return k in self._request

    def __getitem__(self, k):
        return self._request[k]

    def __setitem__(self, k):
        raise NotImplementedError

    def __delitem__(self, k):
        raise NotImplementedError
