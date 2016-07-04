import unittest
from mock import patch
from kazoo.client import KazooClient

from kafka.tools.assigner.exceptions import ZookeeperException
from kafka.tools.assigner.models.cluster import Cluster


class ClusterZookeeperTests(unittest.TestCase):
    def setUp(self):
        self.patcher_start = patch.object(KazooClient, 'start', autospec=True)
        self.patcher_children = patch.object(KazooClient, 'get_children', autospec=True)
        self.patcher_get = patch.object(KazooClient, 'get', autospec=True)
        self.patcher_stop = patch.object(KazooClient, 'stop', autospec=True)
        self.mock_start = self.patcher_start.start()
        self.mock_children = self.patcher_children.start()
        self.mock_get = self.patcher_get.start()
        self.mock_stop = self.patcher_stop.start()

    def tearDown(self):
        self.patcher_start.stop()
        self.patcher_children.stop()
        self.patcher_get.stop()
        self.patcher_stop.stop()

    def test_cluster_create_connect_failure(self):
        self.mock_start.side_effect = Exception()
        self.assertRaises(ZookeeperException, Cluster.create_from_zookeeper, 'zkconnect')

    def test_cluster_create_no_brokers(self):
        self.mock_children.side_effect = [[]]
        self.assertRaises(ZookeeperException, Cluster.create_from_zookeeper, 'zkconnect')

    def test_cluster_create_no_topics(self):
        self.mock_children.side_effect = [['1', '2'], []]
        self.mock_get.side_effect = [(('{"jmx_port":7667,"timestamp":"1465289114807","endpoints":["PLAINTEXT://brokerhost1.example.com:9223",'
                                      '"SSL://brokerhost1.example.com:9224"],"host":"brokerhost1.example.com","version":1,"port":9223}'), None),
                                     (('{"jmx_port":7667,"timestamp":"1465289114807","endpoints":["PLAINTEXT://brokerhost2.example.com:9223",'
                                      '"SSL://brokerhost2.example.com:9224"],"host":"brokerhost2.example.com","version":1,"port":9223}'), None)]
        self.assertRaises(ZookeeperException, Cluster.create_from_zookeeper, 'zkconnect')

    def test_cluster_create_missing_broker(self):
        self.mock_children.side_effect = [['1', '2'], ['testTopic1', 'testTopic2']]
        self.mock_get.side_effect = [(('{"jmx_port":7667,"timestamp":"1465289114807","endpoints":["PLAINTEXT://brokerhost1.example.com:9223",'
                                      '"SSL://brokerhost1.example.com:9224"],"host":"brokerhost1.example.com","version":1,"port":9223}'), None),
                                     (('{"jmx_port":7667,"timestamp":"1465289114807","endpoints":["PLAINTEXT://brokerhost2.example.com:9223",'
                                      '"SSL://brokerhost2.example.com:9224"],"host":"brokerhost2.example.com","version":1,"port":9223}'), None),
                                     ('{"version":1,"partitions":{"0":[1,2],"1":[3,1]}}', None),
                                     ('{"version":1,"partitions":{"0":[3,1],"1":[1,2]}}', None)]
        cluster = Cluster.create_from_zookeeper('zkconnect')

        assert len(cluster.brokers) == 3
        b1 = cluster.brokers[1]
        b3 = cluster.brokers[3]
        assert cluster.brokers[3].hostname is None
        assert cluster.topics['testTopic1'].partitions[1].replicas == [b3, b1]
        assert cluster.topics['testTopic2'].partitions[0].replicas == [b3, b1]

    def test_cluster_create_from_zookeeper(self):
        self.mock_children.side_effect = [['1', '2'], ['testTopic1', 'testTopic2']]
        self.mock_get.side_effect = [(('{"jmx_port":7667,"timestamp":"1465289114807","endpoints":["PLAINTEXT://brokerhost1.example.com:9223",'
                                     '"SSL://brokerhost1.example.com:9224"],"host":"brokerhost1.example.com","version":1,"port":9223}'), None),
                                     (('{"jmx_port":7667,"timestamp":"1465289114807","endpoints":["PLAINTEXT://brokerhost2.example.com:9223",'
                                      '"SSL://brokerhost2.example.com:9224"],"host":"brokerhost2.example.com","version":1,"port":9223}'), None),
                                     ('{"version":1,"partitions":{"0":[1,2],"1":[2,1]}}', None),
                                     ('{"version":1,"partitions":{"0":[2,1],"1":[1,2]}}', None)]
        cluster = Cluster.create_from_zookeeper('zkconnect')

        assert len(cluster.brokers) == 2
        b1 = cluster.brokers[1]
        b2 = cluster.brokers[2]
        assert b1.hostname == 'brokerhost1.example.com'
        assert b2.hostname == 'brokerhost2.example.com'

        assert len(cluster.topics) == 2
        assert 'testTopic1' in cluster.topics
        assert 'testTopic2' in cluster.topics

        t1 = cluster.topics['testTopic1']
        assert len(t1.partitions) == 2
        assert t1.partitions[0].replicas == [b1, b2]
        assert t1.partitions[1].replicas == [b2, b1]

        t2 = cluster.topics['testTopic2']
        assert len(t2.partitions) == 2
        assert t2.partitions[0].replicas == [b2, b1]
        assert t2.partitions[1].replicas == [b1, b2]
