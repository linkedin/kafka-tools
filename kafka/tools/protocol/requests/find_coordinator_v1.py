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

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.group_coordinator_v0 import GroupCoordinatorV0Request
from kafka.tools.protocol.responses.find_coordinator_v1 import FindCoordinatorV1Response


class FindCoordinatorV1Request(GroupCoordinatorV0Request):
    # FindCoordinator is the new name for GroupCoordinator, but it has the same API key
    api_version = 1
    cmd = "FindCoordinator"
    response = FindCoordinatorV1Response

    supports_cli = True
    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} type group_or_transaction_id\n".format(cmd, api_version) +
                   "Description: Retrieve the coordinator broker information for the specified type and ID.\n")

    schema = [
        {'name': 'coordinator_key', 'type': 'string'},
        {'name': 'coordinator_type', 'type': 'int8'},
    ]

    @classmethod
    def process_arguments(cls, cmd_args):
        if len(cmd_args) != 2:
            raise ArgumentError("FindCoordinator takes exactly two string arguments")
        if cmd_args[0] not in ('group', 'transaction'):
            raise ArgumentError("The first argument can only be 'group' or 'transaction'")
        return {'coordinator_type': 0 if cmd_args[0] == 'group' else 1, 'coordinator_key': cmd_args[1]}
