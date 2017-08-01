import time
import unittest
from mock import MagicMock

from tests.tools.client.fixtures import topic_metadata

from kafka.tools.client import Client
from kafka.tools.models.broker import Broker
from kafka.tools.models.topic import Topic
from kafka.tools.models.partition import Partition
from kafka.tools.protocol.requests.topic_metadata_v1 import TopicMetadataV1Request


def assert_cluster_has_topics(cluster, metadata):
    for mtopic in metadata['topics']:
        assert mtopic['name'].value() in cluster.topics
        topic = cluster.topics[mtopic['name'].value()]
        assert topic.name == mtopic['name'].value()
        assert topic.internal == mtopic['internal'].value()
        assert len(topic.partitions) == len(mtopic['partitions'])

        for i, tp in enumerate(mtopic['partitions']):
            partition = topic.partitions[i]
            assert partition.num == tp['id'].value()
            assert partition.leader.id == tp['leader'].value()
            assert len(partition.replicas) == len(tp['replicas'])

            for j, tp_replica in enumerate(tp['replicas']):
                assert partition.replicas[j].id == tp_replica


def assert_cluster_has_brokers(cluster, metadata):
    for b in metadata['brokers']:
        assert b['node_id'].value() in cluster.brokers
        broker = cluster.brokers[b['node_id'].value()]
        assert broker.hostname == b['host'].value()
        assert broker.port == b['port'].value()
        assert broker.rack == b['rack'].value()


class ClientTests(unittest.TestCase):
    def setUp(self):
        # Dummy client for testing - we're not going to connect that bootstrap broker
        self.client = Client()
        self.metadata_response = topic_metadata()

    def test_maybe_update_full_metadata_expired(self):
        self.client._send_any_broker = MagicMock()
        self.client._send_any_broker.return_value = 'metadata_response'
        self.client._update_from_metadata = MagicMock()

        fake_last_time = time.time() - (self.client.configuration.metadata_refresh * 2)
        self.client._last_full_metadata = fake_last_time
        self.client._maybe_update_full_metadata()

        assert self.client._last_full_metadata > fake_last_time
        self.client._send_any_broker.assert_called_once()
        arg = self.client._send_any_broker.call_args[0][0]
        assert isinstance(arg, TopicMetadataV1Request)
        assert arg['topics'].value() is None
        self.client._update_from_metadata.assert_called_once_with('metadata_response', delete=True)

    def test_maybe_update_full_metadata_nocache(self):
        self.client._send_any_broker = MagicMock()
        self.client._update_from_metadata = MagicMock()

        fake_last_time = time.time() - 1000
        self.client._last_full_metadata = fake_last_time
        self.client._maybe_update_full_metadata(cache=False)

        assert self.client._last_full_metadata > fake_last_time

    def test_maybe_update_full_metadata_usecache(self):
        self.client._send_any_broker = MagicMock()
        self.client._update_from_metadata = MagicMock()

        fake_last_time = time.time() - 1000
        self.client._last_full_metadata = fake_last_time
        self.client._maybe_update_full_metadata(cache=True)

        assert self.client._last_full_metadata == fake_last_time

    def test_update_from_metadata(self):
        self.client._update_brokers_from_metadata = MagicMock()
        self.client._update_topics_from_metadata = MagicMock()

        self.client._update_from_metadata('fake_metadata')
        self.client._update_brokers_from_metadata.assert_called_once_with('fake_metadata')
        self.client._update_topics_from_metadata.assert_called_once_with('fake_metadata', delete=False)

    def test_update_topics_from_metadata_create(self):
        # Don't want to test the broker update code here
        self.client.cluster.add_broker(Broker('host1.example.com', id=1, port=8031))
        self.client.cluster.add_broker(Broker('host2.example.com', id=101, port=8032))

        self.client._update_topics_from_metadata(self.metadata_response)
        assert_cluster_has_topics(self.client.cluster, self.metadata_response)

    def test_maybe_delete_topics_not_in_metadata(self):
        # Don't want to test the broker update code here
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        topic = Topic('topic2', 1)
        self.client.cluster.add_broker(broker1)
        self.client.cluster.add_broker(broker2)
        self.client.cluster.add_topic(topic)
        topic.partitions[0].add_replica(broker2)
        topic.partitions[0].add_replica(broker1)

        self.client._maybe_delete_topics_not_in_metadata(self.metadata_response, delete=True)
        assert 'topic2' not in self.client.cluster.topics

    def test_update_topics_from_metadata_update_replicas(self):
        # Don't want to test the broker update code here
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        broker3 = Broker('host3.example.com', id=304, port=8033)
        topic = Topic('topic1', 2)
        self.client.cluster.add_broker(broker1)
        self.client.cluster.add_broker(broker2)
        self.client.cluster.add_broker(broker3)
        self.client.cluster.add_topic(topic)
        topic.partitions[0].add_replica(broker3)
        topic.partitions[0].add_replica(broker1)
        topic.partitions[1].add_replica(broker2)
        topic.partitions[1].add_replica(broker1)

        self.client._update_topics_from_metadata(self.metadata_response)
        assert_cluster_has_topics(self.client.cluster, self.metadata_response)

    def test_update_topics_from_metadata_delete_replicas(self):
        # Don't want to test the broker update code here
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        broker3 = Broker('host3.example.com', id=304, port=8033)
        topic = Topic('topic1', 2)
        self.client.cluster.add_broker(broker1)
        self.client.cluster.add_broker(broker2)
        self.client.cluster.add_broker(broker3)
        self.client.cluster.add_topic(topic)
        topic.partitions[0].add_replica(broker2)
        topic.partitions[0].add_replica(broker1)
        topic.partitions[0].add_replica(broker3)
        topic.partitions[1].add_replica(broker2)
        topic.partitions[1].add_replica(broker1)

        self.client._update_topics_from_metadata(self.metadata_response)
        assert_cluster_has_topics(self.client.cluster, self.metadata_response)

    def test_add_or_update_replica_nochange(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        partition = Partition('topic1', 0)
        partition.add_replica(broker1)
        partition.add_replica(broker2)

        self.client._add_or_update_replica(partition, 0, broker1)
        assert partition.replicas[0] == broker1

    def test_add_or_update_replica_new(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        partition = Partition('topic1', 0)
        partition.add_replica(broker1)
        partition.add_replica(broker2)

        self.client._add_or_update_replica(partition, 2, broker1)
        assert partition.replicas[2] == broker1

    def test_add_or_update_replica_swap(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        partition = Partition('topic1', 0)
        partition.add_replica(broker1)
        partition.add_replica(broker2)

        self.client._add_or_update_replica(partition, 1, broker1)
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
        self.client._assure_topic_has_partitions(topic, 2)
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
        self.client._assure_topic_has_partitions(topic, 1)
        assert len(topic.partitions) == 1
        assert topic.partitions[0].num == 0

    def test_assure_topic_has_partitions_add_one(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        topic = Topic('topic1', 1)
        topic.partitions[0].add_replica(broker1)
        topic.partitions[0].add_replica(broker2)

        assert len(topic.partitions) == 1
        self.client._assure_topic_has_partitions(topic, 2)
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
        self.client._assure_topic_has_partitions(topic, 0)
        assert len(topic.partitions) == 0

    def test_delete_replicas_from_partition_nochange(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        topic = Topic('topic1', 1)
        topic.partitions[0].add_replica(broker1)
        topic.partitions[0].add_replica(broker2)

        partition = topic.partitions[0]
        assert len(partition.replicas) == 2
        self.client._delete_replicas_from_partition(partition, 2)
        assert len(partition.replicas) == 2

    def test_delete_replicas_from_partition_one(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker2 = Broker('host2.example.com', id=101, port=8032)
        topic = Topic('topic1', 1)
        topic.partitions[0].add_replica(broker1)
        topic.partitions[0].add_replica(broker2)

        partition = topic.partitions[0]
        assert len(partition.replicas) == 2
        self.client._delete_replicas_from_partition(partition, 1)
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
        self.client._delete_replicas_from_partition(partition, 0)
        assert len(partition.replicas) == 0

    def test_update_brokers_from_metadata(self):
        self.client._update_brokers_from_metadata(self.metadata_response)
        assert_cluster_has_brokers(self.client.cluster, self.metadata_response)

    def test_update_brokers_from_metadata_update_rack(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker1.rack = 'wrongrack'
        self.client.cluster.add_broker(broker1)

        self.client._update_brokers_from_metadata(self.metadata_response)
        assert_cluster_has_brokers(self.client.cluster, self.metadata_response)

    def test_update_brokers_from_metadata_update_host(self):
        broker1 = Broker('wronghost.example.com', id=1, port=8031)
        self.client.cluster.add_broker(broker1)
        broker1.close = MagicMock()

        self.client._update_brokers_from_metadata(self.metadata_response)
        assert_cluster_has_brokers(self.client.cluster, self.metadata_response)
        broker1.close.assert_called_once()
