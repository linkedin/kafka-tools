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
from kafka.tools.protocol.responses.create_topics_v0 import CreateTopicsV0Response


class CreateTopicsV0Request(BaseRequest):
    api_key = 19
    api_version = 0
    cmd = "CreateTopics"

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
    ]

    def process_arguments(self, cmd_args):
        if (len(cmd_args) < 2) or (not cmd_args[0].isdigit()):
            raise TypeError("The first argument must be an integer, and at least one topic definition must be provided")

        topics = []
        for topic_def in cmd_args[1:]:
            config_start_index = None
            tparts = topic_def.split(",")
            topic = [tparts[0]]
            if tparts[1].isdigit():
                topic.append(int(tparts[1]))
                topic.append(int(tparts[2]))
                topic.append([])
                if len(tparts) > 3:
                    config_start_index = 3
            else:
                partitions = []
                for i, partition_def in enumerate(tparts[1:]):
                    pparts = partition_def.split("=")
                    if len(pparts) != 2:
                        raise Exception("was expecting a 'key=value'")
                    if not pparts[0].isdigit():
                        config_start_index = i + 1
                        break
                    partitions.append([int(pparts[0]), [int(x) for x in pparts[1].split("|")]])
                topic.append(-1)
                topic.append(-1)
                topic.append(partitions)

            if config_start_index is not None:
                configs = []
                for config in tparts[config_start_index:]:
                    cparts = config.split("=")
                    if len(cparts) != 2:
                        raise Exception("was expecting a 'key=value'")
                    configs.append(cparts)
                topic.append(configs)
            else:
                topic.append([])

            topics.append(topic)

        return [topics, int(cmd_args[0])]

    def response(self, correlation_id):
        return CreateTopicsV0Response(correlation_id)

    @classmethod
    def show_help(cls):
        print("Request:     {0}V{1}".format(cls.cmd, cls.api_version))
        print("Format:      {0}V{1} timeout (topic_name,num_partitions,replication_factor[,config=value]... )...".format(cls.cmd, cls.api_version))
        print("             {0}V{1} timeout (topic_name,(partition_id=replica_id[|replica_id]...)...[,config=value]... )...".format(cls.cmd, cls.api_version))
        print("Description: Create the topics with the specified values. Note that EITHER num_partitions")
        print("             and replication_factor, OR replica_assignment, MUST be provided for each topic")
        print("             but not both")
        print("Examples:    {0}V{1} 30 ExampleTopicName,4,2 OtherExampleTopic,4,2".format(cls.cmd, cls.api_version))
        print("             {0}V{1} 30 ExampleTopicName,4,2,retention.ms=3600000".format(cls.cmd, cls.api_version))
        print("             {0}V{1} 30 ExampleTopicName,0=1|2,1=2|3,2=3|1,3=1|3".format(cls.cmd, cls.api_version))
