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

from kafka.tools.protocol.requests import BaseRequest
from kafka.tools.protocol.responses.controlled_shutdown_v1 import ControlledShutdownV1Response


class ControlledShutdownV1Request(BaseRequest):
    api_key = 7
    api_version = 1
    cmd = "ControlledShutdown"

    request_format = [
        {'name': 'broker_id', 'type': 'int32'}
    ]

    def process_arguments(self, cmd_args):
        if (len(cmd_args) != 2) or (not isinstance(cmd_args[1], six.integer_types)):
            raise TypeError("ControlledShutdown requires exactly one argument that is a number")

        return [cmd_args[1]]

    def response(self, correlation_id):
        return ControlledShutdownV1Response(correlation_id)

    @classmethod
    def show_help(cls):
        print("Request:     {0}V{1}".format(cls.cmd, cls.api_version))
        print("Format:      {0}V{1} broker_id".format(cls.cmd, cls.api_version))
        print("Description: Request controlled shutdown for the specified broker ID")
