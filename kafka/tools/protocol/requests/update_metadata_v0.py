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
from kafka.tools.protocol.responses.update_metadata_v0 import UpdateMetadataV0Response
from kafka.tools.protocol.requests.leader_and_isr_v0 import _process_arguments


class UpdateMetadataV0Request(BaseRequest):
    api_key = 6
    api_version = 0
    cmd = "UpdateMetadata"
    response = UpdateMetadataV0Response

    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} controller_id controller_epoch ".format(cmd, api_version) +
                   "(topic,partition,controller_epoch,leader,leader_epoch,isr,zk_version,replicas ...) (broker_id,host,port ...)\n" +
                   "             isr and replicas are a '|' separated list of broker IDs (e.g. '2|3')\n" +
                   "Description: Send updated metadata information to broker\n")

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
        return _process_arguments("UpdateMetadataV0", cmd_args)
