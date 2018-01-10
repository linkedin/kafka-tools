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
    response = StopReplicaV0Response

    help_string = ''

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
