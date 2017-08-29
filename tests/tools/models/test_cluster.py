import json
import unittest
from testfixtures import LogCapture

from kafka.tools.models.broker import Broker
from kafka.tools.models.topic import Topic
from kafka.tools.models.cluster import Cluster


class SimpleClusterTests(unittest.TestCase):
    def setUp(self):
        self.cluster = Cluster()

    def add_brokers(self, num):
        for i in range(1, num + 1):
            broker = Broker("brokerhost{0}.example.com".format(i), id=i)
            self.cluster.add_broker(broker)

    def add_topics(self, num, partition_count=2):
        for i in range(1, num + 1):
            topic = Topic("testTopic{0}".format(i), partition_count)
            self.cluster.add_topic(topic)

    def add_partitions_to_broker(self, broker_id, pos, num):
        topic = Topic('testTopic', num)
        self.cluster.brokers[broker_id].partitions[pos] = []
        for i in range(num):
            self.cluster.brokers[broker_id].partitions[pos].append(topic.partitions[i])

    def test_cluster_create(self):
        assert self.cluster.brokers == {}
        assert self.cluster.topics == {}

    def test_cluster_add_broker(self):
        self.add_brokers(1)
        assert len(self.cluster.brokers) == 1
        for bid in self.cluster.brokers.keys():
            assert self.cluster.brokers[bid].cluster is self.cluster

    def test_cluster_num_brokers(self):
        self.add_brokers(2)
        assert self.cluster.num_brokers() == 2

    def test_cluster_add_topic(self):
        self.add_topics(1)
        assert len(self.cluster.topics) == 1
        for tname in self.cluster.topics.keys():
            assert self.cluster.topics[tname].cluster is self.cluster

    def test_cluster_num_topics(self):
        self.add_topics(2)
        assert self.cluster.num_topics() == 2

    def test_cluster_partition_iterator(self):
        self.add_topics(2)
        seen_partitions = {}
        for partition in self.cluster.partitions([]):
            seen_partitions["{0}:{1}".format(partition.topic.name, partition.num)] = 1
        assert seen_partitions == {'testTopic1:0': 1, 'testTopic1:1': 1, 'testTopic2:0': 1, 'testTopic2:1': 1}

    def test_cluster_partition_iterator_with_exclude(self):
        self.add_topics(2)
        seen_partitions = {}
        for partition in self.cluster.partitions(['testTopic1']):
            seen_partitions["{0}:{1}".format(partition.topic.name, partition.num)] = 1
        assert seen_partitions == {'testTopic2:0': 1, 'testTopic2:1': 1}

    def test_cluster_max_replication_factor(self):
        self.add_brokers(2)
        self.add_partitions_to_broker(1, 0, 1)
        self.add_partitions_to_broker(1, 1, 1)
        self.add_partitions_to_broker(2, 2, 2)
        assert self.cluster.max_replication_factor() == 3

    def test_cluster_log_info(self):
        self.add_brokers(2)
        with LogCapture() as l:
            self.cluster.log_broker_summary()
            l.check(('kafka-tools', 'INFO', 'Broker 1: partitions=0/0 (0.00%), size=0'),
                    ('kafka-tools', 'INFO', 'Broker 2: partitions=0/0 (0.00%), size=0'))

    def test_cluster_output_json(self):
        self.add_topics(2)
        self.add_brokers(2)
        self.cluster.topics['testTopic1'].partitions[0].add_replica(self.cluster.brokers[1], 0)
        self.cluster.topics['testTopic1'].partitions[0].add_replica(self.cluster.brokers[2], 1)
        self.cluster.topics['testTopic1'].partitions[1].add_replica(self.cluster.brokers[1], 1)
        self.cluster.topics['testTopic1'].partitions[1].add_replica(self.cluster.brokers[2], 0)
        cluster = self.cluster.to_dict()
        cluster_json = json.dumps(cluster, sort_keys=True)
        assert cluster_json == json.dumps({
            'brokers': {
                '1': {
                    'hostname': 'brokerhost1.example.com',
                    'id': 1,
                    'jmx_port': -1,
                    'port': 9092,
                    'rack': None,
                    'version': None
                },
                '2': {
                    'hostname': 'brokerhost2.example.com',
                    'id': 2,
                    'jmx_port': -1,
                    'port': 9092,
                    'rack': None,
                    'version': None
                }
            },
            'topics': {
                'testTopic1': {
                    'partitions': {
                        '0': {
                            'replicas': [1, 2],
                            'size': 0
                        },
                        '1': {
                            'replicas': [2, 1],
                            'size': 0
                        }
                    },
                    'retention': 1
                },
                'testTopic2': {
                    'partitions': {
                        '0': {
                            'replicas': [],
                            'size': 0
                        },
                        '1': {
                            'replicas': [],
                            'size': 0
                        }
                    },
                    'retention': 1
                }
            }
        }, sort_keys=True)
