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
from kafka.tools.protocol.responses.metadata_v0 import MetadataV0Response


class TopicMetadataV0Request(BaseRequest):
    api_key = 3
    api_version = 0
    cmd = "TopicMetadata"
    response = MetadataV0Response

    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} [topic_name ...]\n".format(cmd, api_version) +
                   "Description: Fetch metadata for the specified topics. If no topics are specified, all topics are requested\n")

    schema = [
        {'name': 'topics', 'type': 'array', 'item_type': 'string'}
    ]

    @classmethod
    def process_arguments(cls, cmd_args):
        # This looks weird, but it's correct. The list is the first item
        if len(cmd_args) == 0:
            return {'topics': None}
        else:
            return {'topics': cmd_args}
