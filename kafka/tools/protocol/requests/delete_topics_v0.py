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
from kafka.tools.protocol.responses.delete_topics_v0 import DeleteTopicsV0Response


class DeleteTopicsV0Request(BaseRequest):
    api_key = 20
    api_version = 0
    cmd = "DeleteTopics"

    request_format = [
        {'name': 'topics', 'type': 'array', 'item_type': 'string'},
        {'name': 'timeout', 'type': 'int32'},
    ]

    def process_arguments(self, cmd_args):
        if (len(cmd_args) < 2) or (not cmd_args[0].isdigit()):
            raise TypeError("The first argument must be an integer, and at least one topic must be provided")

        topic_set = set()
        for topic in cmd_args[1:]:
            topic_set.add(topic)
        return [list(topic_set), int(cmd_args[0])]

    def response(self, correlation_id):
        return DeleteTopicsV0Response(correlation_id)

    @classmethod
    def show_help(cls):
        print("Request:     {0}V{1}".format(cls.cmd, cls.api_version))
        print("Format:      {0}V{1} timeout (topic_name ...)".format(cls.cmd, cls.api_version))
        print("Description: Delete the specified topics.")
