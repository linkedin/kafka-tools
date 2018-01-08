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
from kafka.tools.protocol.responses.offset_commit_v0 import OffsetCommitV0Response


def _get_partition_map(partition):
    if (len(partition) < 2) or (len(partition) > 3):
        raise ArgumentError("Partition tuple must have 3 or 4 fields")

    try:
        pmap = {'partition': int(partition[0]),
                'offset': int(partition[1]),
                'metadata': None}
    except ValueError:
        raise ArgumentError("Partition tuple must be exactly 2 integers and an optional string")

    if len(partition) == 3:
        pmap['metadata'] = partition[2]
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


class OffsetCommitV0Request(BaseRequest):
    api_key = 8
    api_version = 0
    cmd = "OffsetCommit"
    response = OffsetCommitV0Response

    supports_cli = True
    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} group_id (topic (partition,offset[,metadata] ...) ...)\n".format(cmd, api_version) +
                   "Description: Commit offsets for the specified consumer group\n")

    schema = [
        {'name': 'group_id', 'type': 'string'},
        {'name': 'topics',
         'type': 'array',
         'item_type': [
             {'name': 'topic', 'type': 'string'},
             {'name': 'partitions',
              'type': 'array',
              'item_type': [
                  {'name': 'partition', 'type': 'int32'},
                  {'name': 'offset', 'type': 'int64'},
                  {'name': 'metadata', 'type': 'string'},
              ]},
         ]},
    ]

    @classmethod
    def process_arguments(cls, cmd_args):
        if len(cmd_args) < 3:
            raise ArgumentError("OffsetCommitV0 requires at least 3 arguments")
        values = {'group_id': cmd_args.pop(0), 'topics': []}

        while len(cmd_args) > 0:
            topic, cmd_args = _parse_next_topic(cmd_args)
            values['topics'].append(topic)

        return values
