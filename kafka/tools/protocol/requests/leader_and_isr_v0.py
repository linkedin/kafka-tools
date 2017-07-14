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
from kafka.tools.protocol.responses.leader_and_isr_v0 import LeaderAndIsrV0Response


class LeaderAndIsrV0Request(BaseRequest):
    api_key = 4
    api_version = 0
    cmd = "LeaderAndIsr"

    request_format = [
        {'name': 'controller_id', 'type': 'int32'},
        {'name': 'controller_epoch', 'type': 'int32'},
        {'name': 'partition_states',
         'type': 'array',
         'item_type': [
             {'name': 'topic', 'type': 'string'},
             {'name': 'partition', 'type': 'int32'},
             {'name': 'controller_epoch', 'type': 'int32'},
             {'name': 'leader', 'type': 'int32'},
             {'name': 'leader_epoch', 'type': 'int32'},
             {'name': 'isr', 'type': 'array', 'item_type': 'int32'},
             {'name': 'zk_version', 'type': 'int32'},
             {'name': 'replicas', 'type': 'array', 'item_type': 'int32'},
         ]},
        {'name': 'live_leaders',
         'type': 'array',
         'item_type': [
             {'name': 'id', 'type': 'int32'},
             {'name': 'host', 'type': 'string'},
             {'name': 'port', 'type': 'int32'},
         ]},
    ]

    def process_arguments(self, cmd_args):
        if (len(cmd_args) < 4) or (not cmd_args[0].isdigit()) or (not cmd_args[1].isdigit()):
            raise TypeError("The first two arguments must be integers, and at least one each of partition and live brokers must be provided")

        partitions = []
        brokers = []
        for csv in cmd_args[2:]:
            cparts = csv.split(",")
            if len(cparts) == 8:
                isr = [int(x) for x in cparts[5].split("|")]
                replicas = [int(x) for x in cparts[7].split("|")]
                brokers.append([cparts[0], int(cparts[1]), int(cparts[2]), int(cparts[3]), int(cparts[4]), isr, int(cparts[6]), replicas])
            elif len(cparts) == 3:
                brokers.append([int(cparts[0]), cparts[1], int(cparts[2])])
            else:
                raise Exception("request format incorrect. check help.")

        return [int(cmd_args[0]), int(cmd_args[1]), partitions, brokers]

    def response(self, correlation_id):
        return LeaderAndIsrV0Response(correlation_id)

    @classmethod
    def show_help(cls):
        print("Request:     {0}V{1}".format(cls.cmd, cls.api_version))
        print("Format:      {0}V{1} controller_id controller_epoch (topic,partition,controller_epoch,leader,leader_epoch,isr,zk_version,replicas ...) " +
              "(broker_id,host,port ...)".format(cls.cmd, cls.api_version))
        print("             isr and replicas are a '|' separated list of broker IDs (e.g. '2|3')")
        print("Description: Send replica information to broker")
