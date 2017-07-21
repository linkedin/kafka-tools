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

from kafka.tools.protocol.requests import BaseRequest, ArgumentError
from kafka.tools.protocol.responses.leave_group_v0 import LeaveGroupV0Response


class LeaveGroupV0Request(BaseRequest):
    api_key = 10
    api_version = 0
    cmd = "LeaveGroup"
    response = LeaveGroupV0Response

    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} group_id member_id\n".format(cmd, api_version) +
                   "Description: Remove the specified member from the specified group\n")

    schema = [
        {'name': 'group_id', 'type': 'string'},
        {'name': 'member_id', 'type': 'string'},
    ]

    @classmethod
    def process_arguments(cls, cmd_args):
        if len(cmd_args) != 2:
            raise ArgumentError("LeaveGroupV0 requires exactly two arguments")
        return {'group_id': cmd_args[0], 'member_id': cmd_args[1]}
