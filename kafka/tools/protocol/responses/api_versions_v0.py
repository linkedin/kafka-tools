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


class ApiVersionsV0Response(BaseResponse):
    schema = [
        {'name': 'error', 'type': 'int16'},
        {'name': 'api_versions',
         'type': 'array',
         'item_type': [
             {'name': 'api_key', 'type': 'int16'},
             {'name': 'min_version', 'type': 'int16'},
             {'name': 'max_version', 'type': 'int16'},
         ]},
    ]

    def __str__(self):
        strs = []
        strs.append("Error: {0}".format(self.response[0]))
        for api_version in self.response[1]:
            strs.append("API Key: {0}".format(api_version[0]))
            strs.append("    Min version: {0}".format(api_version[1]))
            strs.append("    Max version: {0}".format(api_version[2]))
        return "\n".join(strs)
