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

from kafka.tools.protocol.responses.metadata_v0 import MetadataV0Response


class MetadataV3Response(MetadataV0Response):
    schema = [
        {'name': 'throttle_time_ms', 'type': 'int32'},
        {'name': 'brokers',
         'type': 'array',
         'item_type': [
             {'name': 'node_id', 'type': 'int32'},
             {'name': 'host', 'type': 'string'},
             {'name': 'port', 'type': 'int32'},
             {'name': 'rack', 'type': 'string'},
         ]},
        {'name': 'cluster_id', 'type': 'string'},
        {'name': 'controller_id', 'type': 'int32'},
        {'name': 'topics',
         'type': 'array',
         'item_type': [
             {'name': 'error', 'type': 'int16'},
             {'name': 'name', 'type': 'string'},
             {'name': 'internal', 'type': 'boolean'},
             {'name': 'partitions',
              'type': 'array',
              'item_type': [
                  {'name': 'error', 'type': 'int16'},
                  {'name': 'id', 'type': 'int32'},
                  {'name': 'leader', 'type': 'int32'},
                  {'name': 'replicas', 'type': 'array', 'item_type': 'int32'},
                  {'name': 'isrs', 'type': 'array', 'item_type': 'int32'},
              ]},
         ]},
    ]
