from __future__ import division

import json

from kafka.tools.assigner.exceptions import ConfigurationException


class Broker:
    def __init__(self, id, hostname):
        self.id = id
        self.hostname = hostname
        self.jmx_port = -1
        self.port = None
        self.version = None
        self.endpoints = None
        self.timestamp = None
        self.cluster = None
        self.partitions = {}

    @classmethod
    def create_from_json(cls, broker_id, jsondata):
        data = json.loads(jsondata)

        # These things are required, and we can't proceed if they're not there
        try:
            newbroker = cls(broker_id, data['host'])
        except KeyError:
            raise ConfigurationException("Cannot parse broker data in zookeeper. This version of Kafka may not be supported.")

        # These things are optional, and are pulled in for convenience or extra features
        try:
            newbroker.jmx_port = data['jmx_port']
            newbroker.port = data['port']
            newbroker.version = data['version']
            newbroker.endpoints = data['endpoints']
            newbroker.timestamp = data['timestamp']
        except KeyError:
            pass

        return newbroker

    def __eq__(self, other):
        if not isinstance(other, Broker):
            raise TypeError
        return (self.hostname == other.hostname) and (self.id == other.id)

    # Shallow copy - do not copy partitions map over
    def copy(self):
        newbroker = Broker(self.id, self.hostname)
        newbroker.jmx_port = self.jmx_port
        newbroker.port = self.port
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
