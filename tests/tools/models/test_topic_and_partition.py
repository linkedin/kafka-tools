import unittest

from tests.tools.client.fixtures import list_offset, list_offset_error, offset_fetch, offset_fetch_error

from kafka.tools.exceptions import OffsetError
from kafka.tools.models.cluster import Cluster
from kafka.tools.models.broker import Broker
from kafka.tools.models.topic import Topic, TopicOffsets
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

    def test_updated_since(self):
        self.topic._last_updated = 100
        assert self.topic.updated_since(99)

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

    def test_add_or_update_replica_nochange(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        partition = Partition('topic1', 0)
        partition.add_replica(broker1)
        partition.add_replica(broker2)

        partition.add_or_update_replica(0, broker1)
        assert partition.replicas[0] == broker1

    def test_add_or_update_replica_new(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        partition = Partition('topic1', 0)
        partition.add_replica(broker1)
        partition.add_replica(broker2)

        partition.add_or_update_replica(2, broker1)
        assert partition.replicas[2] == broker1

    def test_add_or_update_replica_swap(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        partition = Partition('topic1', 0)
        partition.add_replica(broker1)
        partition.add_replica(broker2)

        partition.add_or_update_replica(1, broker1)
        assert partition.replicas[1] == broker1

    def test_assure_topic_has_partitions_nochange(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        topic = Topic('topic1', 2)
        topic.partitions[0].add_replica(broker1)
        topic.partitions[0].add_replica(broker2)
        topic.partitions[1].add_replica(broker2)
        topic.partitions[1].add_replica(broker1)

        assert len(topic.partitions) == 2
        topic.assure_has_partitions(2)
        assert len(topic.partitions) == 2

    def test_assure_topic_has_partitions_one(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        topic = Topic('topic1', 2)
        topic.partitions[0].add_replica(broker1)
        topic.partitions[0].add_replica(broker2)
        topic.partitions[1].add_replica(broker2)
        topic.partitions[1].add_replica(broker1)

        assert len(topic.partitions) == 2
        topic.assure_has_partitions(1)
        assert len(topic.partitions) == 1
        assert topic.partitions[0].num == 0

    def test_assure_topic_has_partitions_add_one(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        topic = Topic('topic1', 1)
        topic.partitions[0].add_replica(broker1)
        topic.partitions[0].add_replica(broker2)

        assert len(topic.partitions) == 1
        topic.assure_has_partitions(2)
        assert len(topic.partitions) == 2
        assert topic.partitions[1].num == 1

    def test_assure_topic_has_partitions_all(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        topic = Topic('topic1', 2)
        topic.partitions[0].add_replica(broker1)
        topic.partitions[0].add_replica(broker2)
        topic.partitions[1].add_replica(broker2)
        topic.partitions[1].add_replica(broker1)

        assert len(topic.partitions) == 2
        topic.assure_has_partitions(0)
        assert len(topic.partitions) == 0

    def test_delete_replicas_from_partition_nochange(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        topic = Topic('topic1', 1)
        topic.partitions[0].add_replica(broker1)
        topic.partitions[0].add_replica(broker2)

        partition = topic.partitions[0]
        assert len(partition.replicas) == 2
        partition.delete_replicas(2)
        assert len(partition.replicas) == 2

    def test_delete_replicas_from_partition_one(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        topic = Topic('topic1', 1)
        topic.partitions[0].add_replica(broker1)
        topic.partitions[0].add_replica(broker2)

        partition = topic.partitions[0]
        assert len(partition.replicas) == 2
        partition.delete_replicas(1)
        assert len(partition.replicas) == 1
        assert partition.replicas[0] == broker1

    def test_delete_replicas_from_partition_all(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        topic = Topic('topic1', 1)
        topic.partitions[0].add_replica(broker1)
        topic.partitions[0].add_replica(broker2)

        partition = topic.partitions[0]
        assert len(partition.replicas) == 2
        partition.delete_replicas(0)
        assert len(partition.replicas) == 0

    def test_topic_offsets_create(self):
        topic = Topic('topic1', 3)
        offsets = TopicOffsets(topic)
        assert offsets.topic == topic
        assert len(offsets.partitions) == 3
        for partition in offsets.partitions:
            assert partition == -1

    def test_set_offsets_from_list(self):
        topic = Topic('topic1', 2)
        offsets = TopicOffsets(topic)
        response = list_offset()

        offsets.set_offsets_from_list(response['responses'][0]['partition_responses'])
        assert offsets.partitions[0] == 4829
        assert offsets.partitions[1] == 8904

    def test_set_offsets_from_list_error(self):
        topic = Topic('topic1', 2)
        offsets = TopicOffsets(topic)
        response = list_offset_error()

        self.assertRaises(OffsetError, offsets.set_offsets_from_list, response['responses'][0]['partition_responses'])

    def test_set_offsets_from_fetch(self):
        topic = Topic('topic1', 2)
        offsets = TopicOffsets(topic)
        response = offset_fetch()

        offsets.set_offsets_from_fetch(response['responses'][0]['partition_responses'])
        assert offsets.partitions[0] == 4829
        assert offsets.partitions[1] == 8904

    def test_set_offsets_from_fetch_error(self):
        topic = Topic('topic1', 2)
        offsets = TopicOffsets(topic)
        response = offset_fetch_error()

        self.assertRaises(OffsetError, offsets.set_offsets_from_fetch, response['responses'][0]['partition_responses'])
