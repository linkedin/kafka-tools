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
from kafka.tools.protocol.responses.offset_fetch_v1 import OffsetFetchV1Response


class OffsetFetchV1Request(BaseRequest):
    api_key = 9
    api_version = 1
    cmd = "OffsetFetch"

    request_format = [
        {'name': 'group_id', 'type': 'string'},
        {'name': 'topics',
         'type': 'array',
         'item_type': [
             {'name': 'topic', 'type': 'string'},
             {'name': 'partitions',
              'type': 'array',
              'item_type': [
                  {'name': 'partition', 'type': 'int32'},
              ]},
         ]},
    ]

    def process_arguments(self, cmd_args):
        topics = []
        for item in cmd_args[1:]:
            parts = item.split(",")
            if len(parts) > 2:
                partitions = [int(x) for x in parts[1:]]
                topics.append([parts[0], partitions])
            else:
                raise Exception("request format incorrect. check help.")

        return [cmd_args[0], topics]

    def response(self, correlation_id):
        return OffsetFetchV1Response(correlation_id)

    @classmethod
    def show_help(cls):
        print("Request:     {0}V{1}".format(cls.cmd, cls.api_version))
        print("Format:      {0}V{1} group_id (topic(,partition ...) ...)".format(cls.cmd, cls.api_version))
        print("Description: Commit offsets for the specified consumer group")
        print("Example:     {0}V{1} ExampleGroup ExampleTopic,0,1,2 ExampleTopic2,0".format(cls.cmd, cls.api_version))
