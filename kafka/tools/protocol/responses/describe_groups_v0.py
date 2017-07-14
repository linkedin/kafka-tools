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

from kafka.tools.protocol.responses import BaseResponse


class DescribeGroupsV0Response(BaseResponse):
    response_format = [
        {'name': 'groups',
         'type': 'array',
         'item_type': [
             {'name': 'error', 'type': 'int16'},
             {'name': 'group_id', 'type': 'string'},
             {'name': 'protocol_type', 'type': 'string'},
             {'name': 'protocol', 'type': 'string'},
             {'name': 'members',
              'type': 'array',
              'item_type': [
                  {'name': 'member_id', 'type': 'string'},
                  {'name': 'client_id', 'type': 'string'},
                  {'name': 'client_host', 'type': 'string'},
                  {'name': 'member_metadata', 'type': 'bytes'},
                  {'name': 'member_assignment', 'type': 'bytes'},
              ]},
         ]},
    ]

    def __str__(self):
        strs = []
        for group in self.response[0]:
            strs.append("Group: {0}".format(group[1]))
            strs.append("    error: {0}".format(group[0]))
            strs.append("    protocol_type: {0}".format(group[2]))
            strs.append("    protocol: {0}".format(group[3]))
            for member in group[4]:
                strs.append("    Member ID: {0}".format(member[0]))
                strs.append("        client_id: {0}".format(member[1]))
                strs.append("        client_host: {0}".format(member[2]))
                strs.append("        metadata: {0}".format(member[3]))
                strs.append("        assignment: {0}".format(member[4]))
        return "\n".join(strs)
