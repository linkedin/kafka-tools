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
from kafka.tools.protocol.responses.describe_groups_v0 import DescribeGroupsV0Response


class DescribeGroupsV0Request(BaseRequest):
    api_key = 15
    api_version = 0
    cmd = "DescribeGroups"
    response = DescribeGroupsV0Response

    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} [group_id ...]\n".format(cmd, api_version) +
                   "Description: Request metadata for the specified groups, or all groups if none are provided\n")

    schema = [
        {'name': 'group_ids', 'type': 'array', 'item_type': 'string'},
    ]

    @classmethod
    def process_arguments(cls, cmd_args):
        if len(cmd_args) > 0:
            return {'group_ids': cmd_args}
        else:
            return {'group_ids': None}
