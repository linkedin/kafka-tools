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

import time

from kafka.tools.models import BaseModel


class Group(BaseModel):
    equality_attrs = ['name']

    def __init__(self, name):
        self.name = name
        self.cluster = None
        self.coordinator = None
        self.protocol = None
        self.members = []
        self._last_updated = time.time()

    def updated_since(self, check_time):
        return check_time >= self._last_updated


class GroupMember(BaseModel):
    equality_attrs = ['name']

    def __init__(self, name, client_id=None, client_host=None, member_metadata=None, member_assignment=None):
        self.name = name
        self.client_id = client_id
        self.client_host = client_host
        self.member_metadata = member_metadata
        self.member_assignment = member_assignment
