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
from kafka.tools.protocol.requests.offset_fetch_v0 import OffsetFetchV0Request, _parse_topic_set
from kafka.tools.protocol.responses.offset_fetch_v2 import OffsetFetchV2Response


class OffsetFetchV2Request(OffsetFetchV0Request):
    api_version = 2
    response = OffsetFetchV2Response
    cmd = "OffsetFetch"

    supports_cli = True
    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} group_id [topic(,partition ...) ...]\n".format(cmd, api_version) +
                   "Description: Fetch offsets for the specified topic partitions for the consumer group. If no\n" +
                   "             topic is specified, fetch for all topic partitions.\n" +
                   "Example:     {0}V{1} ExampleGroup ExampleTopic,0,1,2 ExampleTopic2,0\n".format(cmd, api_version) +
                   "             {0}V{1} ExampleGroup".format(cmd, api_version))

    @classmethod
    def process_arguments(cls, cmd_args):
        if len(cmd_args) == 1:
            return {'group_id': cmd_args[0], 'topics': None}
        elif len(cmd_args) == 0:
            raise ArgumentError("OffsetFetchV{0} requires at least 1 argument".format(cls.api_version))
        else:
            values = {'group_id': cmd_args[0], 'topics': []}

            for topic_set in cmd_args[1:]:
                values['topics'].append(_parse_topic_set(topic_set))

            return values
