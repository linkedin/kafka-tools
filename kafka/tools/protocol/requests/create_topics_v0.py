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
from kafka.tools.protocol.responses.create_topics_v0 import CreateTopicsV0Response


def _parse_partition(topic, partition_id, replica_str):
    if topic['num_partitions'] != -1:
        raise ArgumentError("Cannot specify num_partitions and replication_factor when using an explicit partition and replica list")
    try:
        topic['replica_assignment'].append({'partition_id': partition_id, 'replicas': [int(bid) for bid in replica_str.split("|")]})
    except ValueError:
        raise ArgumentError("replica_id must be an integer")


def _parse_kv_args(topic, topic_args):
    for targ in topic_args:
        arg_parts = targ.split("=")
        if len(arg_parts) != 2:
            raise ArgumentError("was expecting a 'key=value'")

        try:
            partition_id = int(arg_parts[0])
            _parse_partition(topic, partition_id, arg_parts[1])
        except ValueError:
            topic['configs'].append({'config_key': arg_parts[0], 'config_value': arg_parts[1]})


def _parse_remaining_args(topic, targs):
    if targs[0].isdigit() and targs[1].isdigit():
        topic['num_partitions'] = int(targs[0])
        topic['replication_factor'] = int(targs[1])
        _parse_kv_args(topic, targs[2:])
    else:
        _parse_kv_args(topic, targs)

    if (topic['num_partitions'] == -1) and (len(topic['replica_assignment']) == 0):
        raise ArgumentError("You must specify either num_partitions and replication_factor OR an explicit replica_assignment")


class CreateTopicsV0Request(BaseRequest):
    api_key = 19
    api_version = 0
    cmd = "CreateTopics"
    response = CreateTopicsV0Response

    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} timeout (topic_name,num_partitions,replication_factor[,config=value]... )...\n".format(cmd, api_version) +
                   "             {0}V{1} timeout (topic_name,(partition_id=replica_id[|replica_id]...)...[,config=value]... )...\n".format(cmd, api_version) +
                   "Description: Create the topics with the specified values. Note that EITHER num_partitions\n" +
                   "             and replication_factor, OR replica_assignment, MUST be provided for each topic\n" +
                   "             but not both\n" +
                   "Examples:    {0}V{1} 30 ExampleTopicName,4,2 OtherExampleTopic,4,2\n".format(cmd, api_version) +
                   "             {0}V{1} 30 ExampleTopicName,4,2,retention.ms=3600000\n".format(cmd, api_version) +
                   "             {0}V{1} 30 ExampleTopicName,0=1|2,1=2|3,2=3|1,3=1|3\n".format(cmd, api_version))

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

    @classmethod
    def process_arguments(cls, cmd_args):
        if (len(cmd_args) < 2) or (not cmd_args[0].isdigit()):
            raise ArgumentError("The first argument must be an integer, and at least one topic definition must be provided")

        values = {'topics': [], 'timeout': int(cmd_args[0])}
        for topic_def in cmd_args[1:]:
            tparts = topic_def.split(",")
            topic = {'topic': tparts[0],
                     'num_partitions': -1,
                     'replication_factor': -1,
                     'replica_assignment': [],
                     'configs': []}

            _parse_remaining_args(topic, tparts[1:])
            values['topics'].append(topic)

        return values
