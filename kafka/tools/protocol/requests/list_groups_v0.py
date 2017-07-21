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
from kafka.tools.protocol.responses.list_groups_v0 import ListGroupsV0Response


class ListGroupsV0Request(BaseRequest):
    api_key = 16
    api_version = 0
    cmd = "ListGroups"
    response = ListGroupsV0Response

    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1}\n".format(cmd, api_version) +
                   "Description: List all consumer groups for this cluster\n")

    schema = []

    @classmethod
    def process_arguments(cls, cmd_args):
        if len(cmd_args) != 0:
            raise ArgumentError("ListGroupsV0 takes no arguments")
        return {}
