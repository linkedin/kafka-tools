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
from kafka.tools.protocol.responses.offset_fetch_v0 import OffsetFetchV0Response


def _parse_topic_set(topic_set):
    tparts = topic_set.split(',')
    if len(tparts) < 2:
        raise ArgumentError("Each topic must have at least one partition")

    try:
        return {'topic': tparts[0], 'partitions': [int(p) for p in tparts[1:]]}
    except ValueError:
        raise ArgumentError("partitions must be integers")


class OffsetFetchV0Request(BaseRequest):
    api_key = 9
    api_version = 0
    cmd = "OffsetFetch"
    response = OffsetFetchV0Response

    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} group_id (topic(,partition ...) ...)\n".format(cmd, api_version) +
                   "Description: Commit offsets for the specified consumer group\n" +
                   "Example:     {0}V{1} ExampleGroup ExampleTopic,0,1,2 ExampleTopic2,0\n".format(cmd, api_version))

    schema = [
        {'name': 'group_id', 'type': 'string'},
        {'name': 'topics',
         'type': 'array',
         'item_type': [
             {'name': 'topic', 'type': 'string'},
             {'name': 'partitions',
              'type': 'array',
              'item_type': 'int32'}]},
    ]

    @classmethod
    def process_arguments(cls, cmd_args):
        if len(cmd_args) < 2:
            raise ArgumentError("OffsetFetchV{0} requires at least 2 arguments".format(cls.api_version))
        values = {'group_id': cmd_args[0], 'topics': []}

        for topic_set in cmd_args[1:]:
            values['topics'].append(_parse_topic_set(topic_set))

        return values
