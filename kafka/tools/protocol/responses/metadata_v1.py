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


class MetadataV1Response(BaseResponse):
    response_format = [
        {'name': 'brokers',
         'type': 'array',
         'item_type': [
             {'name': 'node_id', 'type': 'int32'},
             {'name': 'host', 'type': 'string'},
             {'name': 'port', 'type': 'int32'},
             {'name': 'rack', 'type': 'string'},
         ]},
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

    def __str__(self):
        strs = []
        strs.append("Brokers:")
        for broker in self.response[0]:
            strs.append("    id: {0}, host: {1}, port: {2}, rack: {3}".format(broker[0], broker[1], broker[2], broker[3]))
        strs.append("Controller: {0}".format(self.response[1]))
        for topic in self.response[2]:
            strs.append("Topic: {0}".format(topic[1]))
            strs.append("    error: {0}".format(topic[0]))
            strs.append("    internal: {0}".format(topic[2]))
            strs.append("    Partitions:")
            for partition in sorted(topic[3], key=lambda partition: partition[1]):
                strs.append("        id: {0}, error: {1}, leader: {2}, replicas: {3}, ISR: {4}".format(
                  partition[1], partition[0], partition[2], partition[3], partition[4]))
        return "\n".join(strs)
