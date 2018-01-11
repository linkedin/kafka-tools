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
from kafka.tools.protocol.responses.describe_configs_v0 import DescribeConfigsV0Response


class DescribeConfigsV0Request(BaseRequest):
    api_key = 32
    api_version = 0
    response = DescribeConfigsV0Response

    cmd = "DescribeConfigs"
    help_string = ''

    schema = [
        {'name': 'resources',
         'type': 'array',
         'item_type': [
             {'name': 'resource_type', 'type': 'int8'},
             {'name': 'resource_name', 'type': 'string'},
             {'name': 'config_names', 'type': 'array', 'item_type': 'string'},
         ]},
    ]
