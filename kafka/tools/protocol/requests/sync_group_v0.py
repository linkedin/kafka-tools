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

from kafka.tools.protocol.requests import BaseRequest, ArgumentError
from kafka.tools.protocol.responses.sync_group_v0 import SyncGroupV0Response


class SyncGroupV0Request(BaseRequest):
    api_key = 14
    api_version = 0
    cmd = "SyncGroup"
    response = SyncGroupV0Response

    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} group_id generation_id member_id member_assignments\n".format(cmd, api_version) +
                   "Description: Sync group information after joining. Leader provides member assignments\n")

    schema = [
        {'name': 'group_id', 'type': 'string'},
        {'name': 'generation_id', 'type': 'int32'},
        {'name': 'member_id', 'type': 'string'},
        {'name': 'member_assignments', 'type': 'bytes'},
    ]

    @classmethod
    def process_arguments(cls, cmd_args):
        if len(cmd_args) != 4:
            raise ArgumentError("SyncGroupV0 requires exactly 4 arguments")

        try:
            member_assignments = binascii.unhexlify(cmd_args[3])
        except:
            raise ArgumentError("member_assignments must be a hex string")

        try:
            return {'group_id': cmd_args[0],
                    'generation_id': int(cmd_args[1]),
                    'member_id': cmd_args[2],
                    'member_assignments': member_assignments}
        except ValueError:
            raise ArgumentError("The generation_id must be an integer")
