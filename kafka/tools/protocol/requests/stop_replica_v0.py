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
from kafka.tools.protocol.responses.stop_replica_v0 import StopReplicaV0Response


class StopReplicaV0Request(BaseRequest):
    api_key = 5
    api_version = 0
    cmd = "StopReplica"

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

    def process_arguments(self, cmd_args):
        if (len(cmd_args) < 4) or (not cmd_args[0].isdigit()) or (not cmd_args[1].isdigit()):
            raise TypeError("The first two arguments must be integers, and at least one topic,partition must be provided")

        delete_partitions = False
        if cmd_args[2] in ['true', '1', 't', 'y', 'yes']:
            delete_partitions = True

        partitions = []
        for item in cmd_args[3:]:
            parts = item.split(",")
            if len(parts) == 2:
                partitions.append([parts[0], int(parts[0])])
            else:
                raise Exception("request format incorrect. check help.")

        return [int(cmd_args[0]), int(cmd_args[1]), delete_partitions, partitions]

    def response(self, correlation_id):
        return StopReplicaV0Response(correlation_id)

    @classmethod
    def show_help(cls):
        print("Request:     {0}V{1}".format(cls.cmd, cls.api_version))
        print("Format:      {0}V{1} controller_id controller_epoch delete_partitions (topic,partition ...)".format(cls.cmd, cls.api_version))
        print("Description: Stop the specified replicas on the broker")
