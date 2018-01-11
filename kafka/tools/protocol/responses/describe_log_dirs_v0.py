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


class DescribeLogDirsV0Response(BaseResponse):
    schema = [
        {'name': 'throttle_time_ms', 'type': 'int32'},
        {'name': 'log_dirs',
         'type': 'array',
         'item_type': [
             {'name': 'error_code', 'type': 'int16'},
             {'name': 'log_dir', 'type': 'string'},
             {'name': 'topics',
              'type': 'array',
              'item_type': [
                  {'name': 'topic', 'type': 'string'},
                  {'name': 'partitions',
                   'type': 'array',
                   'item_type': [
                       {'name': 'size', 'type': 'int64'},
                       {'name': 'offset_lag', 'type': 'int64'},
                       {'name': 'is_future', 'type': 'boolean'},
                   ]},
              ]},
         ]},
    ]
