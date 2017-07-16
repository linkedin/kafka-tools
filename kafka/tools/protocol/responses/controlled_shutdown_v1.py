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


class ControlledShutdownV1Response(BaseResponse):
    schema = [
        {'name': 'error', 'type': 'int16'},
        {'name': 'partitions_remaining',
         'type': 'array',
         'item_type': [
             {'name': 'topic', 'type': 'string'},
             {'name': 'partition', 'type': 'int32'},
         ]},
    ]

    def __str__(self):
        strs = []
        strs.append("Error: {0}".format(self.response[0]))
        for partition in self.response[1]:
            strs.append("Partitions Remaining:")
            strs.append("    topic: {0}, partition: {1}".format(partition[0], partition[1]))
        return "\n".join(strs)
