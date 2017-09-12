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

from kafka.tools.models import BaseModel
from kafka.tools.protocol.responses.member_assignment_v0 import MemberAssignmentV0
from kafka.tools.protocol.types.bytebuffer import ByteBuffer


class Group(BaseModel):
    equality_attrs = ['name']

    def __init__(self, name):
        self.name = name
        self.cluster = None
        self.coordinator = None
        self.protocol = None
        self.protocol_type = None
        self.state = None
        self.members = []
        self._last_updated = 0.0

    def updated_since(self, check_time):
        return check_time <= self._last_updated

    def clear_members(self):
        self.members = []

    def add_member(self, name, client_id=None, client_host=None, metadata=None, assignment=None):
        new_member = GroupMember(name, client_id, client_host, metadata, assignment)
        new_member.group = self
        if (self.protocol_type == 'consumer') and (assignment is not None):
            new_member.set_assignment()
        self.members.append(new_member)

    def subscribed_topics(self):
        topics = set()
        for member in self.members:
            for topic in member.topics:
                topics.add(topic)
        return list(topics)


class GroupMember(BaseModel):
    equality_attrs = ['name']

    def __init__(self, name, client_id=None, client_host=None, metadata=None, assignment=None):
        self.name = name
        self.client_id = client_id
        self.client_host = client_host
        self.metadata = metadata
        self.assignment_data = assignment

        self.group = None
        self.user_data = None
        self.assignment_version = None
        self.topics = {}

    def set_assignment(self):
        assignment = MemberAssignmentV0.from_bytebuffer(0, ByteBuffer(self.assignment_data))
        self.assignment_version = assignment['version']
        self.user_data = assignment['user_data']
        for tp in assignment['partitions']:
            if tp['topic'] not in self.topics:
                self.topics[tp['topic']] = []
            for partition in tp['partitions']:
                self.topics[tp['topic']].append(partition)
