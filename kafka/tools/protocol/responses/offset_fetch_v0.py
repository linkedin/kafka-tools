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


class OffsetFetchV0Response(BaseResponse):
    schema = [
        {'name': 'responses',
         'type': 'array',
         'item_type': [
             {'name': 'topic', 'type': 'string'},
             {'name': 'partition_responses',
              'type': 'array',
              'item_type': [
                  {'name': 'partition', 'type': 'int32'},
                  {'name': 'offset', 'type': 'int64'},
                  {'name': 'metadata', 'type': 'string'},
                  {'name': 'error', 'type': 'int16'},
              ]},
         ]},
    ]

    def __str__(self):
        strs = []
        for topic in self.response[0]:
            strs.append("Topic: {0}".format(topic[0]))
            for partition in topic[1]:
                strs.append("    partition: {0}".format(partition[0]))
                strs.append("        offset: {0}".format(partition[1]))
                strs.append("        metadata: {0}".format(partition[2]))
                strs.append("        error: {0}".format(partition[3]))
        return "\n".join(strs)
