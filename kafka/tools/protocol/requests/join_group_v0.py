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
from kafka.tools.protocol.responses.join_group_v0 import JoinGroupV0Response


def _parse_group_protocol(protocol):
    pparts = protocol.split(',')
    if len(pparts) != 2:
        raise ArgumentError("Expected group_protocol_name,group_protocol_metadata")
    if pparts[1] == '':
        return {'protocol_name': pparts[0], 'protocol_metadata': None}
    else:
        try:
            return {'protocol_name': pparts[0], 'protocol_metadata': binascii.unhexlify(pparts[1])}
        except:
            raise ArgumentError("group_protocol_metadata must be empty or a hex string")


class JoinGroupV0Request(BaseRequest):
    api_key = 11
    api_version = 0
    cmd = "JoinGroup"
    response = JoinGroupV0Response

    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} group_id session_timeout member_id protocol_type ".format(cmd, api_version) +
                   "(group_protocol_name,group_protocol_metadata ...)\n" +
                   "Description: Join or create a consumer group\n")

    schema = [
        {'name': 'group_id', 'type': 'string'},
        {'name': 'session_timeout', 'type': 'int32'},
        {'name': 'member_id', 'type': 'string'},
        {'name': 'protocol_type', 'type': 'string'},
        {'name': 'group_protocols',
         'type': 'array',
         'item_type': [
             {'name': 'protocol_name', 'type': 'string'},
             {'name': 'protocol_metadata', 'type': 'bytes'},
         ]},
    ]

    @classmethod
    def process_arguments(cls, cmd_args):
        if len(cmd_args) < 5:
            raise ArgumentError("JoinGroupV0 requires at least 5 arguments")

        try:
            values = {'group_id': cmd_args[0],
                      'session_timeout': int(cmd_args[1]),
                      'member_id': cmd_args[2],
                      'protocol_type': cmd_args[3],
                      'group_protocols': []}
        except ValueError:
            raise ArgumentError("The session_timeout must be an integer")

        for protocol in cmd_args[4:]:
            values['group_protocols'].append(_parse_group_protocol(protocol))

        return values
