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
from kafka.tools.protocol.responses.sync_group_v0 import SyncGroupV0Response


class SyncGroupV0Request(BaseRequest):
    api_key = 14
    api_version = 0
    cmd = "SyncGroup"

    request_format = [
        {'name': 'group_id', 'type': 'string'},
        {'name': 'generation_id', 'type': 'int32'},
        {'name': 'member_id', 'type': 'string'},
        {'name': 'member_assignments', 'type': 'bytes'},
    ]

    def process_arguments(self, cmd_args):
        return [cmd_args[0], int(cmd_args[1]), cmd_args[2], cmd_args[3]]

    def response(self, correlation_id):
        return SyncGroupV0Response(correlation_id)

    @classmethod
    def show_help(cls):
        print("Request:     {0}V{1}".format(cls.cmd, cls.api_version))
        print("Format:      {0}V{1} group_id generation_id member_id member_assignments".format(cls.cmd, cls.api_version))
        print("Description: Sync group information after joining. Leader provides member assignments")
