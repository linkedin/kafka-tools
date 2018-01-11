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
from kafka.tools.protocol.responses.alter_replica_log_dirs_v0 import AlterReplicaLogDirsV0Response


class AlterReplicaLogDirsV0Request(BaseRequest):
    api_key = 34
    api_version = 0
    response = AlterReplicaLogDirsV0Response

    cmd = "AlterReplicaLogDirs"
    help_string = ''

    schema = [
        {'name': 'log_dirs',
         'type': 'array',
         'item_type': [
             {'name': 'log_dir', 'type': 'string'},
             {'name': 'topics',
              'type': 'array',
              'item_type': [
                  {'name': 'topic', 'type': 'string'},
                  {'name': 'partitions', 'type': 'array', 'item_type': 'int32'}
              ]},
         ]},
    ]
