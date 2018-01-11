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

from kafka.tools.protocol.requests.topic_metadata_v3 import TopicMetadataV3Request
from kafka.tools.protocol.responses.metadata_v4 import MetadataV4Response


class TopicMetadataV4Request(TopicMetadataV3Request):
    api_version = 4
    response = MetadataV4Response
    cmd = "TopicMetadata"

    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} [topic_name ...]\n".format(cmd, api_version) +
                   "Description: Fetch metadata for the specified topics. If no topics are specified, all topics\n" +
                   "             are requested. Note that auto-creation is not supported.\n")

    schema = [
        {'name': 'topics', 'type': 'array', 'item_type': 'string'},
        {'name': 'allow_auto_topic_creation', 'type': 'boolean'}
    ]

    @classmethod
    def process_arguments(cls, cmd_args):
        # This looks weird, but it's correct. The list is the first item
        if len(cmd_args) == 0:
            return {'allow_auto_topic_creation': False, 'topics': None}
        else:
            return {'allow_auto_topic_creation': False, 'topics': cmd_args}
