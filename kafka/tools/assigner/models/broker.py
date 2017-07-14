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

from __future__ import division

from kafka.tools.assigner.exceptions import ConfigurationException
from kafka.tools.assigner.models import BaseModel
from kafka.tools.assigner.tools import json_loads


class Broker(BaseModel):
    equality_attrs = ['hostname', 'id']

    def __init__(self, id, hostname):
        self.id = id
        self.hostname = hostname
        self.jmx_port = -1
        self.port = None
        self.rack = None
        self.version = None
        self.endpoints = None
        self.timestamp = None
        self.cluster = None
        self.partitions = {}

    @classmethod
    def create_from_json(cls, broker_id, jsondata):
        data = json_loads(jsondata)

        # These things are required, and we can't proceed if they're not there
        try:
            newbroker = cls(broker_id, data['host'])
        except KeyError:
            raise ConfigurationException("Cannot parse broker data in zookeeper. This version of Kafka may not be supported.")

        # These things are optional, and are pulled in for convenience or extra features
        for attr in ['jmx_port', 'port', 'rack', 'version', 'endpoints', 'timestamp']:
            try:
                setattr(newbroker, attr, data[attr])
            except KeyError:
                pass

        return newbroker

    # Shallow copy - do not copy partitions map over
    def copy(self):
        newbroker = Broker(self.id, self.hostname)
        newbroker.jmx_port = self.jmx_port
        newbroker.port = self.port
        newbroker.rack = self.rack
        newbroker.version = self.version
        newbroker.endpoints = self.endpoints
        newbroker.timestamp = self.timestamp
        newbroker.cluster = self.cluster
        return newbroker

    def num_leaders(self):
        return self.num_partitions_at_position(0)

    def num_partitions_at_position(self, pos=0):
        if pos in self.partitions:
            return len(self.partitions[pos])
        else:
            return pos

    def percent_leaders(self):
        if self.num_partitions() == 0:
            return 0.0
        return (self.num_leaders() / self.num_partitions()) * 100

    def total_size(self):
        return sum([p.size for pos in self.partitions for p in self.partitions[pos]], 0)

    def num_partitions(self):
        return sum([len(self.partitions[pos]) for pos in self.partitions], 0)
