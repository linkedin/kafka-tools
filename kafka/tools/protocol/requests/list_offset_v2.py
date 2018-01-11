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
from kafka.tools.protocol.requests.list_offset_v1 import ListOffsetV1Request, _parse_next_topic
from kafka.tools.protocol.responses.list_offset_v2 import ListOffsetV2Response


class ListOffsetV2Request(ListOffsetV1Request):
    api_version = 2
    response = ListOffsetV2Response
    cmd = "ListOffset"

    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} replica_id committed (topic (partition,timestamp ...) ...)\n".format(cmd, api_version) +
                   "             replica_id should be -1 for normal consumers\n" +
                   "Description: Send replica information to broker\n" +
                   "Examples:    {0}V{1} -1 ExampleTopic 0,1477613177000 1,-1 ExampleTopic2 0,-2\n".format(cmd, api_version))

    schema = [
        {'name': 'replica_id', 'type': 'int32'},
        {'name': 'topics',
         'type': 'array',
         'item_type': [
             {'name': 'topic', 'type': 'string'},
             {'name': 'partitions',
              'type': 'array',
              'item_type': [
                  {'name': 'partition', 'type': 'int32'},
                  {'name': 'timestamp', 'type': 'int64'},
              ]},
         ]},
    ]

    @classmethod
    def process_arguments(cls, cmd_args):
        if len(cmd_args) < 4:
            raise ArgumentError("ListOffsetV2 requires at least 4 arguments")
        if cmd_args[1] not in ('true', 'false'):
            raise ArgumentError("committed must be 'true' or 'false'")

        try:
            replica_id = int(cmd_args.pop(0))
        except ValueError:
            raise ArgumentError("The replica_id must be an integer")
        committed = cmd_args.pop(0)

        values = {'replica_id': replica_id,
                  'isolation_level': 1 if committed == 'true' else 0,
                  'topics': []}

        while len(cmd_args) > 0:
            topic, cmd_args = _parse_next_topic(cmd_args)
            values['topics'].append(topic)

        return values
