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
from kafka.tools.protocol.responses.list_offset_v0 import ListOffsetV0Response


class ListOffsetV0Request(BaseRequest):
    api_key = 2
    api_version = 0
    cmd = "ListOffset"

    request_format = [
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

    def process_arguments(self, cmd_args):
        if (len(cmd_args) < 3) or (not cmd_args[0].isdigit()):
            raise TypeError("The first argument must be an integer, and at least one topic and partition must be specified")

        topic = None
        topics = []
        partitions = []
        for item in cmd_args[1:]:
            parts = item.split(",")
            if len(parts) == 1:
                if (topic is not None) and (len(partitions) > 0):
                    topics.append([topic, partitions])
                topic = parts[0]
                partitions = []
            elif len(parts) == 3:
                partitions.append([int(parts[0]), int(parts[1]), int(parts[2])])
            else:
                raise Exception("request format incorrect. check help.")

        return [int(cmd_args[0]), topics]

    def response(self, correlation_id):
        return ListOffsetV0Response(correlation_id)

    @classmethod
    def show_help(cls):
        print("Request:     {0}V{1}".format(cls.cmd, cls.api_version))
        print("Format:      {0}V{1} replica_id (topic (partition,timestamp,max_num_offsets ...) ...)".format(cls.cmd, cls.api_version))
        print("             replica_id should be -1 for normal consumers")
        print("Description: Send replica information to broker")
        print("Examples:    {0}V{1} -1 ExampleTopic 0,1477613177000,1 1,-1,1 ExampleTopic2 0,-2,1".format(cls.cmd, cls.api_version))
