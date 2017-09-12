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

from kafka.tools.protocol.responses import BaseResponse


class MetadataV0Response(BaseResponse):
    schema = [
        {'name': 'brokers',
         'type': 'array',
         'item_type': [
             {'name': 'node_id', 'type': 'int32'},
             {'name': 'host', 'type': 'string'},
             {'name': 'port', 'type': 'int32'},
         ]},
        {'name': 'topics',
         'type': 'array',
         'item_type': [
             {'name': 'error', 'type': 'int16'},
             {'name': 'name', 'type': 'string'},
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

    def topic_names(self):
        return [t['name'] for t in self._response['topics']]

    def broker_ids(self):
        return [b['node_id'] for b in self._response['brokers']]
