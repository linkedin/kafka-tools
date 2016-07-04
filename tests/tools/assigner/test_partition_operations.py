import unittest

from kafka.tools.assigner.exceptions import ReplicaNotFoundException, ClusterConsistencyException
from kafka.tools.assigner.models.broker import Broker
from kafka.tools.assigner.models.topic import Topic
from kafka.tools.assigner.models.cluster import Cluster


class PartitionOperationTests(unittest.TestCase):
    def setUp(self):
        self.cluster = Cluster()
        self.cluster.add_broker(Broker(1, "brokerhost1.example.com"))
        self.cluster.add_broker(Broker(2, "brokerhost2.example.com"))
        self.cluster.add_broker(Broker(3, "brokerhost3.example.com"))
        self.cluster.add_topic(Topic("testTopic1", 2))
        self.cluster.add_topic(Topic("testTopic2", 2))

    def add_topics(self):
        partition = self.cluster.topics['testTopic1'].partitions[0]
        partition.add_replica(self.cluster.brokers[1], 0)
        partition.add_replica(self.cluster.brokers[2], 1)
        partition = self.cluster.topics['testTopic1'].partitions[1]
        partition.add_replica(self.cluster.brokers[2], 0)
        partition.add_replica(self.cluster.brokers[1], 1)
        partition = self.cluster.topics['testTopic2'].partitions[0]
        partition.add_replica(self.cluster.brokers[2], 0)
        partition.add_replica(self.cluster.brokers[1], 1)
        partition = self.cluster.topics['testTopic2'].partitions[1]
        partition.add_replica(self.cluster.brokers[1], 0)
        partition.add_replica(self.cluster.brokers[2], 1)

    def test_partition_add_broker_partition(self):
        partition = self.cluster.topics['testTopic1'].partitions[0]
        partition._add_broker_partition(0, self.cluster.brokers[1])
        assert self.cluster.brokers[1].partitions[0] == [partition]

    def test_partition_add_broker_partition_two(self):
        partition = self.cluster.topics['testTopic1'].partitions[0]
        partition._add_broker_partition(0, self.cluster.brokers[1])

        partition2 = self.cluster.topics['testTopic2'].partitions[1]
        partition2._add_broker_partition(0, self.cluster.brokers[1])
        assert self.cluster.brokers[1].partitions[0] == [partition, partition2]

    def test_partition_add_replica(self):
        partition = self.cluster.topics['testTopic1'].partitions[0]
        partition.add_replica(self.cluster.brokers[1], 0)
        assert self.cluster.brokers[1].partitions[0] == [partition]
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [self.cluster.brokers[1]]

    def test_partition_add_replica_two(self):
        partition = self.cluster.topics['testTopic1'].partitions[0]
        partition.add_replica(self.cluster.brokers[1], 0)
        partition.add_replica(self.cluster.brokers[2], 1)
        assert self.cluster.brokers[1].partitions[0] == [partition]
        assert self.cluster.brokers[2].partitions[1] == [partition]
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [self.cluster.brokers[1], self.cluster.brokers[2]]

    def test_partition_remove_broker_partition(self):
        partition = self.cluster.topics['testTopic1'].partitions[0]
        partition.add_replica(self.cluster.brokers[1], 0)
        partition._remove_broker_partition(self.cluster.brokers[1])
        assert self.cluster.brokers[1].partitions[0] == []

    def test_partition_remove_broker_partition_two(self):
        partition = self.cluster.topics['testTopic1'].partitions[0]
        partition.add_replica(self.cluster.brokers[1], 0)
        partition2 = self.cluster.topics['testTopic2'].partitions[1]
        partition2.add_replica(self.cluster.brokers[1], 0)
        partition._remove_broker_partition(self.cluster.brokers[1])
        assert self.cluster.brokers[1].partitions[0] == [partition2]

    def test_partition_remove_replica(self):
        partition = self.cluster.topics['testTopic1'].partitions[0]
        partition.add_replica(self.cluster.brokers[1], 0)
        partition.remove_replica(self.cluster.brokers[1])
        assert self.cluster.brokers[1].partitions[0] == []
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == []

    def test_partition_remove_replica_single(self):
        partition = self.cluster.topics['testTopic1'].partitions[0]
        partition.add_replica(self.cluster.brokers[1], 0)
        partition.add_replica(self.cluster.brokers[2], 1)
        partition.remove_replica(self.cluster.brokers[1])
        assert self.cluster.brokers[1].partitions[0] == []
        assert self.cluster.brokers[2].partitions[1] == [partition]
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [self.cluster.brokers[2]]

    def test_partition_remove_replica_nonexistent(self):
        self.assertRaises(ReplicaNotFoundException, self.cluster.topics['testTopic1'].partitions[0].remove_replica, self.cluster.brokers[1])

    def test_partition_swap_replicas(self):
        partition = self.cluster.topics['testTopic1'].partitions[0]
        partition.add_replica(self.cluster.brokers[1], 0)
        partition.swap_replicas(self.cluster.brokers[1], self.cluster.brokers[2])
        assert self.cluster.brokers[2].partitions[0] == [partition]
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [self.cluster.brokers[2]]

    def test_partition_swap_replicas_nonexistent(self):
        self.assertRaises(ReplicaNotFoundException,
                          self.cluster.topics['testTopic1'].partitions[0].swap_replicas,
                          self.cluster.brokers[1],
                          self.cluster.brokers[2])

    def test_partition_swap_replica_positions(self):
        partition = self.cluster.topics['testTopic1'].partitions[0]
        partition.add_replica(self.cluster.brokers[1], 0)
        partition.add_replica(self.cluster.brokers[2], 1)
        partition.swap_replica_positions(self.cluster.brokers[1], self.cluster.brokers[2])
        assert self.cluster.brokers[2].partitions[0] == [partition]
        assert self.cluster.brokers[1].partitions[1] == [partition]
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [self.cluster.brokers[2], self.cluster.brokers[1]]

    def test_partition_swap_replica_positions_nonexistent(self):
        partition = self.cluster.topics['testTopic1'].partitions[0]
        partition.add_replica(self.cluster.brokers[1], 0)
        partition.add_replica(self.cluster.brokers[2], 1)
        self.assertRaises(ReplicaNotFoundException, partition.swap_replica_positions, self.cluster.brokers[1], self.cluster.brokers[3])

    def test_cluster_clone(self):
        # Should have a consistent cluster state
        self.add_topics()

        newcluster = self.cluster.clone()
        assert self.cluster is not newcluster
        for bid in newcluster.brokers:
            assert newcluster.brokers[bid] == self.cluster.brokers[bid]
        for tname in newcluster.topics:
            assert newcluster.topics[tname] == self.cluster.topics[tname]
        for partition in newcluster.partitions():
            assert partition == self.cluster.topics[partition.topic.name].partitions[partition.num]
            assert partition.replicas == self.cluster.topics[partition.topic.name].partitions[partition.num].replicas

    def test_cluster_changed_partitions(self):
        self.add_topics()
        newcluster = self.cluster.clone()
        newcluster.topics['testTopic1'].partitions[0].replicas.reverse()
        difference = self.cluster.changed_partitions(newcluster)
        assert difference == [newcluster.topics['testTopic1'].partitions[0]]

    def test_cluster_changed_partitions_inconsistent(self):
        self.add_topics()
        badcluster = Cluster()
        self.assertRaises(ClusterConsistencyException, badcluster.changed_partitions, self.cluster)
