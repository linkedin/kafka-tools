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

from kazoo.client import KazooClient
from kazoo.exceptions import KazooException

from kafka.tools import log
from kafka.tools.exceptions import ZookeeperException, ClusterConsistencyException
from kafka.tools.models import BaseModel
from kafka.tools.models.broker import Broker
from kafka.tools.models.topic import Topic
from kafka.tools.utilities import json_loads


def add_brokers_from_zk(cluster, zk):
    for b in zk.get_children("/brokers/ids"):
        broker_data, bstat = zk.get("/brokers/ids/{0}".format(b))
        cluster.add_broker(Broker.create_from_json(int(b), broker_data))
    if cluster.num_brokers() == 0:
        raise ZookeeperException("The cluster specified does not have any brokers")


def add_topic_with_replicas(cluster, topic, topic_data):
    newtopic = Topic(topic, len(topic_data['partitions']))
    for partition in topic_data['partitions']:
        for i, replica in enumerate(topic_data['partitions'][partition]):
            if replica not in cluster.brokers:
                # Hit a replica that's not in the ID list (which means it's dead)
                # We'll add it, but trying to get sizes will fail as we don't have a hostname
                cluster.add_broker(Broker(None, id=replica))
            newtopic.partitions[int(partition)].add_replica(cluster.brokers[replica], i)
    cluster.add_topic(newtopic)


def set_topic_retention(topic, zk):
    try:
        zdata, zstat = zk.get("/config/topics/{0}".format(topic.name))
        tdata = json_loads(zdata)
        topic.retention = int(tdata['config']['retention.ms'])
    except (KeyError, ValueError, KazooException):
        # If we can't get the config override for any reason, just stick with whatever the default is
        pass


class Cluster(BaseModel):
    equality_attrs = ['brokers', 'topics']

    def __init__(self, retention=1):
        self.brokers = {}
        self.topics = {}
        self.groups = {}
        self.retention = retention

    @classmethod
    def create_from_zookeeper(cls, zkconnect, default_retention=1, fetch_topics=True):
        log.info("Connecting to zookeeper {0}".format(zkconnect))
        try:
            zk = KazooClient(zkconnect)
            zk.start()
        except Exception as e:
            raise ZookeeperException("Cannot connect to Zookeeper: {0}".format(e))

        # Get broker list
        cluster = cls(retention=default_retention)
        add_brokers_from_zk(cluster, zk)

        # Get current partition state
        if fetch_topics:
            log.info("Getting partition list from Zookeeper")
            for topic in zk.get_children("/brokers/topics"):
                zdata, zstat = zk.get("/brokers/topics/{0}".format(topic))
                add_topic_with_replicas(cluster, topic, json_loads(zdata))
                set_topic_retention(cluster.topics[topic], zk)

            if cluster.num_topics() == 0:
                raise ZookeeperException("The cluster specified does not have any topics")

        log.info("Closing connection to zookeeper")
        zk.stop()
        zk.close()

        return cluster

    def clone(self):
        newcluster = Cluster()
        newcluster.retention = self.retention

        # We're not going to clone in the subclasses because we need to map partitions between topics and brokers
        # So we do shallow copies and populate the partition information ourselves
        for broker in self.brokers:
            newcluster.add_broker(self.brokers[broker].copy())

        for topic_name in self.topics:
            topic = self.topics[topic_name]
            newtopic = topic.copy()

            for i, partition in enumerate(topic.partitions):
                newpartition = partition.copy()
                newtopic.add_partition(newpartition)
                for pos, broker in enumerate(partition.replicas):
                    newpartition.add_replica(newcluster.brokers[broker.id])

            newcluster.add_topic(newtopic)

        return newcluster

    def add_broker(self, broker):
        broker.cluster = self
        self.brokers[broker.id] = broker

    def add_topic(self, topic):
        topic.cluster = self
        topic.retention = self.retention
        self.topics[topic.name] = topic

    def add_group(self, group):
        group.cluster = self
        self.groups[group.name] = group

    # Iterate over all the partitions in this cluster
    # Order is alphabetical by topic, numeric by partition
    def partitions(self, exclude_topics=[]):
        for topic in sorted(self.topics):
            if topic in exclude_topics:
                log.debug("Skipping topic {0} due to exclude-topics".format(topic))
                continue

            for partition in self.topics[topic].partitions:
                yield partition

    def num_brokers(self):
        return len(self.brokers)

    def num_topics(self):
        return len(self.topics)

    def max_replication_factor(self):
        max_pos = 0
        for broker in self.brokers:
            broker_max_pos = 0
            if len(self.brokers[broker].partitions) > 0:
                broker_max_pos = max(self.brokers[broker].partitions.keys())
            if broker_max_pos > max_pos:
                max_pos = broker_max_pos
        return max_pos + 1

    def changed_partitions(self, newcluster):
        moves = []
        try:
            for partition in newcluster.partitions([]):
                if partition.replicas != self.topics[partition.topic.name].partitions[partition.num].replicas:
                    moves.append(partition)
        except KeyError:
            raise ClusterConsistencyException("The new cluster does not match the existing cluster, either due to added topics or partitions")
        return moves

    def log_broker_summary(self):
        for broker_id in sorted(self.brokers.keys()):
            broker = self.brokers[broker_id]
            log.info("Broker {0}: partitions={1}/{2} ({3:.2f}%), size={4}".format(broker_id,
                                                                                  broker.num_leaders(),
                                                                                  broker.num_partitions(),
                                                                                  broker.percent_leaders(),
                                                                                  broker.total_size()))

    def to_dict(self):
        """
        Return cluster information as JSON
        """
        brokers = {}
        for broker in self.brokers.values():
            brokers[broker.id] = broker.to_dict()

        topics = {}
        for topic in self.topics.values():
            topics[topic.name] = topic.to_dict()

        return {'brokers': brokers, 'topics': topics}
