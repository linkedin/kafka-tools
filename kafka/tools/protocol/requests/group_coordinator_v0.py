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

from kafka.tools.protocol.requests import BaseRequest
from kafka.tools.protocol.responses.group_coordinator_v0 import GroupCoordinatorV0Response


class GroupCoordinatorV0Request(BaseRequest):
    api_key = 10
    api_version = 0
    cmd = "GroupCoordinator"

    request_format = [
        {'name': 'group_id', 'type': 'string'},
    ]

    def process_arguments(self, cmd_args):
        return [cmd_args[0]]

    def response(self, correlation_id):
        return GroupCoordinatorV0Response(correlation_id)

    @classmethod
    def show_help(cls):
        print("Request:     {0}V{1}".format(cls.cmd, cls.api_version))
        print("Format:      {0}V{1} group_id".format(cls.cmd, cls.api_version))
        print("Description: Retrieve the coordinator broker information for the specified group ID.")
