import unittest

from kafka.tools.models.cluster import Cluster
from kafka.tools.models.broker import Broker
from kafka.tools.models.topic import Topic
from kafka.tools.models.partition import Partition


class TopicAndPartitionTests(unittest.TestCase):
    def setUp(self):
        self.cluster = Cluster(retention=500000)
        self.topic = Topic('testTopic', 1)
        self.topic.cluster = self.cluster
        self.topic.cluster.topics['testTopic'] = self.topic

    def test_topic_create(self):
        assert self.topic.name == 'testTopic'
        assert len(self.topic.partitions) == 1
        assert self.topic.cluster == self.cluster

    def test_partition_create(self):
        assert len(self.topic.partitions) == 1
        assert self.topic.partitions[0].topic == self.topic
        assert self.topic.partitions[0].num == 0
        assert self.topic.partitions[0].replicas == []
        assert self.topic.partitions[0].size == 0

    def test_topic_equality(self):
        topic2 = Topic('testTopic', 1)
        assert self.topic == topic2

    def test_topic_equality_with_different_partitions(self):
        topic2 = Topic('testTopic', 2)
        assert self.topic == topic2

    def test_topic_inequality(self):
        topic2 = Topic('anotherTestTopic', 1)
        assert self.topic != topic2

    def test_topic_equality_typeerror(self):
        self.assertRaises(TypeError, self.topic.__eq__, None)

    def test_topic_copy(self):
        topic2 = self.topic.copy()
        assert topic2.name == 'testTopic'
        assert len(topic2.partitions) == 0
        assert self.topic.cluster == topic2.cluster
        assert self.topic is not topic2

    def test_partition_equality(self):
        partition2 = Partition(self.topic, 0)
        assert self.topic.partitions[0] == partition2

    def test_partition_equality_with_different_replicas(self):
        partition2 = Partition(self.topic, 0)
        broker = Broker('testhost1', id=1)
        partition2.replicas = [broker]
        assert self.topic.partitions[0] == partition2

    def test_partition_inequality_on_topic_name(self):
        topic2 = Topic('anotherTestTopic', 1)
        assert self.topic.partitions[0] != topic2.partitions[0]

    def test_partition_inequality_on_partition_num(self):
        partition2 = Partition(self.topic, 1)
        assert self.topic.partitions[0] != partition2

    def test_partition_equality_typeerror(self):
        self.assertRaises(TypeError, self.topic.partitions[0].__eq__, None)

    def test_partition_copy_without_replicas(self):
        partition2 = self.topic.partitions[0].copy()
        assert self.topic.partitions[0] == partition2
        assert self.topic.partitions[0] is not partition2

    def test_partition_copy_with_replicas(self):
        broker = Broker('testhost1', id=1)
        self.topic.partitions[0].replicas = [broker]
        partition2 = self.topic.partitions[0].copy()
        assert self.topic.partitions[0] == partition2
        assert partition2.replicas == []
        assert self.topic.partitions[0] is not partition2

    def test_add_partition_to_topic(self):
        partition2 = Partition(self.topic, 1)
        self.topic.add_partition(partition2)
        assert len(self.topic.partitions) == 2
        assert self.topic.partitions[1].topic == self.topic
        assert self.topic.partitions[1].num == 1
        assert self.topic.partitions[1].replicas == []
        assert self.topic.partitions[1].size == 0

    def test_set_partition_size(self):
        self.topic.partitions[0].set_size(100)
        assert self.topic.partitions[0].size == 100

    def test_set_partition_size_skipped(self):
        self.topic.partitions[0].set_size(100)
        self.topic.partitions[0].set_size(99)
        assert self.topic.partitions[0].size == 100

    def test_set_partition_size_larger(self):
        self.topic.partitions[0].set_size(100)
        self.topic.partitions[0].set_size(200)
        assert self.topic.partitions[0].size == 200

    def test_set_partition_scaled_size_smaller(self):
        self.topic.retention = 1000000
        self.topic.partitions[0].set_size(100)
        assert self.topic.partitions[0].scaled_size == 50

    def test_set_partition_scaled_size_larger(self):
        self.topic.retention = 100000
        self.topic.partitions[0].set_size(100)
        assert self.topic.partitions[0].scaled_size == 500

    def test_partition_dict_for_reassignment_without_replicas(self):
        expected = {"topic": 'testTopic', "partition": 0, "replicas": []}
        assert self.topic.partitions[0].dict_for_reassignment() == expected

    def test_partition_dict_for_reassignment_with_replicas(self):
        broker = Broker('testhost1', id=1)
        self.topic.partitions[0].replicas = [broker]
        expected = {"topic": 'testTopic', "partition": 0, "replicas": [1]}
        assert self.topic.partitions[0].dict_for_reassignment() == expected

    def test_partition_dict_for_replica_election(self):
        expected = {"topic": 'testTopic', "partition": 0}
        assert self.topic.partitions[0].dict_for_replica_election() == expected
