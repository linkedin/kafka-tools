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
from kafka.tools.protocol.requests.create_topics_v0 import CreateTopicsV0Request, _parse_remaining_args
from kafka.tools.protocol.responses.create_topics_v1 import CreateTopicsV1Response


class CreateTopicsV1Request(CreateTopicsV0Request):
    api_version = 1
    cmd = "CreateTopics"
    response = CreateTopicsV1Response

    supports_cli = True
    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} dry_run timeout (topic_name,num_partitions,".format(cmd, api_version) +
                   "replication_factor[,config=value]... )...\n" +
                   "             {0}V{1} dry_run timeout (topic_name,(partition_id=".format(cmd, api_version) +
                   "replica_id[|replica_id]...)...[,config=value]... )...\n"
                   "Description: Create the topics with the specified values. Note that EITHER num_partitions\n" +
                   "             and replication_factor, OR replica_assignment, MUST be provided for each topic\n" +
                   "             but not both. dry_run can only be 'true' or 'false'.\n" +
                   "Examples:    {0}V{1} true 30 ExampleTopicName,4,2 OtherExampleTopic,4,2\n".format(cmd, api_version) +
                   "             {0}V{1} false 30 ExampleTopicName,4,2,retention.ms=3600000\n".format(cmd, api_version) +
                   "             {0}V{1} false 30 ExampleTopicName,0=1|2,1=2|3,2=3|1,3=1|3\n".format(cmd, api_version))

    schema = [
        {'name': 'topics',
         'type': 'array',
         'item_type': [
             {'name': 'topic', 'type': 'string'},
             {'name': 'num_partitions', 'type': 'int32'},
             {'name': 'replication_factor', 'type': 'int16'},
             {'name': 'replica_assignment',
              'type': 'array',
              'item_type': [
                  {'name': 'partition_id', 'type': 'int32'},
                  {'name': 'replicas', 'type': 'array', 'item_type': 'int32'},
              ]},
             {'name': 'configs',
              'type': 'array',
              'item_type': [
                  {'name': 'config_key', 'type': 'string'},
                  {'name': 'config_value', 'type': 'string'},
              ]},
         ]},
        {'name': 'timeout', 'type': 'int32'},
        {'name': 'validate_only', 'type': 'boolean'},
    ]

    @classmethod
    def process_arguments(cls, cmd_args):
        if len(cmd_args) < 3:
            raise ArgumentError("At least one topic definition must be provided")
        if not cmd_args[0] in ('true', 'false'):
            raise ArgumentError("The first argument may only be true or false")
        if not cmd_args[1].isdigit():
            raise ArgumentError("The second argument must be an integer")

        values = {'validate_only': cmd_args[0] == 'true', 'topics': [], 'timeout': int(cmd_args[1])}
        for topic_def in cmd_args[2:]:
            tparts = topic_def.split(",")
            topic = {'topic': tparts[0],
                     'num_partitions': -1,
                     'replication_factor': -1,
                     'replica_assignment': [],
                     'configs': []}

            _parse_remaining_args(topic, tparts[1:])
            values['topics'].append(topic)

        return values
