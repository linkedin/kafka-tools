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
from kafka.tools.protocol.responses.stop_replica_v0 import StopReplicaV0Response


def _parse_partition(item):
    parts = item.split(",")
    if len(parts) != 2:
        raise ArgumentError("partitions must be specified as topic,partitions")

    try:
        return {'topic': parts[0], 'partition': int(parts[1])}
    except ValueError:
        raise ArgumentError("The partition must be an integer")


class StopReplicaV0Request(BaseRequest):
    api_key = 5
    api_version = 0
    cmd = "StopReplica"
    response = StopReplicaV0Response

    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} controller_id controller_epoch delete_partitions (topic,partition ...)\n".format(cmd, api_version) +
                   "Description: Stop the specified replicas on the broker\n")

    schema = [
        {'name': 'controller_id', 'type': 'int32'},
        {'name': 'controller_epoch', 'type': 'int32'},
        {'name': 'delete_partitions', 'type': 'boolean'},
        {'name': 'partitions',
         'type': 'array',
         'item_type': [
             {'name': 'topic', 'type': 'string'},
             {'name': 'partition', 'type': 'int32'},
         ]},
    ]

    @classmethod
    def process_arguments(cls, cmd_args):
        if len(cmd_args) < 4:
            raise ArgumentError("StopReplicaV0 requires at least 4 arguments")

        try:
            values = {'controller_id': int(cmd_args[0]),
                      'controller_epoch': int(cmd_args[1]),
                      'delete_partitions': cmd_args[2].lower() in ['true', '1', 't', 'y', 'yes'],
                      'partitions': []}
        except ValueError:
            raise ArgumentError("The controller_id and controller_epoch must be integers")

        for item in cmd_args[3:]:
            values['partitions'].append(_parse_partition(item))

        return values
