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


class JoinGroupV0Response(BaseResponse):
    schema = [
        {'name': 'error', 'type': 'int16'},
        {'name': 'generation_id', 'type': 'int32'},
        {'name': 'group_protocol', 'type': 'string'},
        {'name': 'leader_id', 'type': 'string'},
        {'name': 'member_id', 'type': 'string'},
        {'name': 'members',
         'type': 'array',
         'item_type': [
             {'name': 'member_id', 'type': 'string'},
             {'name': 'member_assignment', 'type': 'bytes'},
         ]},
    ]

    def __str__(self):
        strs = []
        strs.append("error: {0}".format(self.response[0]))
        strs.append("generation_id: {0}".format(self.response[1]))
        strs.append("group_protocol: {0}".format(self.response[2]))
        strs.append("leader_id: {0}".format(self.response[3]))
        strs.append("member_id: {0}".format(self.response[4]))
        for member in self.response[5]:
            strs.append("Member: {0}".format(member[0]))
            strs.append("    assignment: {0}".format(member[1]))
        return "\n".join(strs)
