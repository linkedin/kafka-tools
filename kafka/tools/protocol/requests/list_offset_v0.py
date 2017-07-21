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
from kafka.tools.protocol.responses.list_offset_v0 import ListOffsetV0Response


def _get_partition_map(partition):
    if len(partition) != 3:
        raise ArgumentError("Partition tuple must have exactly 3 fields")

    try:
        pmap = {'partition': int(partition[0]),
                'timestamp': int(partition[1]),
                'max_num_offsets': int(partition[2])}
    except ValueError:
        raise ArgumentError("Partition tuple must be exactly 3 integers")

    return pmap


def _parse_next_topic(cmd_args):
    topic = {'topic': cmd_args.pop(0), 'partitions': []}
    while True:
        try:
            cmd_args[0].index(',')
        except (IndexError, ValueError):
            if len(topic['partitions']) == 0:
                raise ArgumentError("Topic is missing partitions")
            return topic, cmd_args
        partition = cmd_args.pop(0).split(',')
        topic['partitions'].append(_get_partition_map(partition))


class ListOffsetV0Request(BaseRequest):
    api_key = 2
    api_version = 0
    cmd = "ListOffset"
    response = ListOffsetV0Response

    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} replica_id (topic (partition,timestamp,max_num_offsets ...) ...)\n".format(cmd, api_version) +
                   "             replica_id should be -1 for normal consumers\n" +
                   "Description: Send replica information to broker\n" +
                   "Examples:    {0}V{1} -1 ExampleTopic 0,1477613177000,1 1,-1,1 ExampleTopic2 0,-2,1\n".format(cmd, api_version))

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
                  {'name': 'max_num_offsets', 'type': 'int32'},
              ]},
         ]},
    ]

    @classmethod
    def process_arguments(cls, cmd_args):
        if len(cmd_args) < 3:
            raise ArgumentError("ListOffsetV0 requires at least 3 arguments")

        try:
            values = {'replica_id': int(cmd_args.pop(0)), 'topics': []}
        except ValueError:
            raise ArgumentError("The replica_id must be an integer")

        while len(cmd_args) > 0:
            topic, cmd_args = _parse_next_topic(cmd_args)
            values['topics'].append(topic)

        return values
