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


class ListGroupsV0Response(BaseResponse):
    response_format = [
        {'name': 'error', 'type': 'int16'},
        {'name': 'groups',
         'type': 'array',
         'item_type': [
             {'name': 'group_id', 'type': 'string'},
             {'name': 'protocol_type', 'type': 'string'},
         ]},
    ]

    def __str__(self):
        strs = []
        strs.append("error: {0}".format(self.response[0]))
        strs.append("Groups:")
        for group in self.response[1]:
            strs.append("    id: {0}, protocol_type: {1}".format(group[0], group[1]))
        return "\n".join(strs)
