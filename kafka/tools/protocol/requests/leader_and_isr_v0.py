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
from kafka.tools.protocol.responses.leader_and_isr_v0 import LeaderAndIsrV0Response


def _get_integer_list(delimited_str):
    return [int(x) for x in delimited_str.split("|")]


def _parse_argument(values, arg):
    cparts = arg.split(",")
    if len(cparts) == 8:
        try:
            values['partition_states'].append({'topic': cparts[0],
                                               'partition': int(cparts[1]),
                                               'controller_epoch': int(cparts[2]),
                                               'leader': int(cparts[3]),
                                               'leader_epoch': int(cparts[4]),
                                               'isr': _get_integer_list(cparts[5]),
                                               'zk_version': int(cparts[6]),
                                               'replicas': _get_integer_list(cparts[7])})
        except ValueError:
            raise ArgumentError("partition_states fields, except for topic, must be integers")
    elif len(cparts) == 3:
        try:
            values['live_leaders'].append({'id': int(cparts[0]), 'host': cparts[1], 'port': int(cparts[2])})
        except ValueError:
            raise ArgumentError("live_leaders broker_id and port fields must be integers")
    else:
        raise ArgumentError("partition_states or live_leaders format incorrect. check help.")


def _process_arguments(cmd_name, cmd_args):
    if len(cmd_args) < 4:
        raise ArgumentError("{0} requires at least 4 arguments".format(cmd_name))

    try:
        values = {'controller_id': int(cmd_args[0]),
                  'controller_epoch': int(cmd_args[1]),
                  'partition_states': [],
                  'live_leaders': []}
    except ValueError:
        raise ArgumentError("The controller_id and controller_epoch must be integers")

    for csv in cmd_args[2:]:
        _parse_argument(values, csv)

    return values


class LeaderAndIsrV0Request(BaseRequest):
    api_key = 4
    api_version = 0
    cmd = "LeaderAndIsr"
    response = LeaderAndIsrV0Response

    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} controller_id controller_epoch ".format(cmd, api_version) +
                   "(topic,partition,controller_epoch,leader,leader_epoch,isr,zk_version,replicas ...) (broker_id,host,port ...)\n" +
                   "             isr and replicas are a '|' separated list of broker IDs (e.g. '2|3')\n" +
                   "Description: Send replica information to broker\n")

    schema = [
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

    @classmethod
    def process_arguments(cls, cmd_args):
        return _process_arguments("LeaderAndIsrV0", cmd_args)
