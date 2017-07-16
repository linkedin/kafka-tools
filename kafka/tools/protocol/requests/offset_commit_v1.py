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
from kafka.tools.protocol.responses.offset_commit_v1 import OffsetCommitV1Response


class OffsetCommitV1Request(BaseRequest):
    api_key = 8
    api_version = 1
    cmd = "OffsetCommit"

    schema = [
        {'name': 'group_id', 'type': 'string'},
        {'name': 'group_generation_id', 'type': 'int32'},
        {'name': 'member_id', 'type': 'string'},
        {'name': 'topics',
         'type': 'array',
         'item_type': [
             {'name': 'topic', 'type': 'string'},
             {'name': 'partitions',
              'type': 'array',
              'item_type': [
                  {'name': 'partition', 'type': 'int32'},
                  {'name': 'offset', 'type': 'int64'},
                  {'name': 'timestamp', 'type': 'int64'},
                  {'name': 'metadata', 'type': 'string'},
              ]},
         ]},
    ]

    def process_arguments(self, cmd_args):
        topic = None
        topics = []
        partitions = []
        for item in cmd_args[3:]:
            parts = item.split(",")
            if len(parts) == 1:
                if (topic is not None) and (len(partitions) > 0):
                    topics.append([topic, partitions])
                topic = parts[0]
                partitions = []
            elif len(parts) == 3:
                partitions.append([int(parts[0]), int(parts[1]), int(parts[2]), None])
            elif len(parts) == 4:
                partitions.append([int(parts[0]), int(parts[1]), int(parts[2]), parts[3]])
            else:
                raise Exception("request format incorrect. check help.")

        return [cmd_args[0], int(cmd_args[1]), cmd_args[2], topics]

    def response(self, correlation_id):
        return OffsetCommitV1Response(correlation_id)

    @classmethod
    def show_help(cls):
        print("Request:     {0}V{1}".format(cls.cmd, cls.api_version))
        print("Format:      {0}V{1} group_id group_generation_id member_id (topic (partition,offset,timestamp[,metadata] ...) ...)".format(
          cls.cmd, cls.api_version))
        print("Description: Commit offsets for the specified consumer group")
