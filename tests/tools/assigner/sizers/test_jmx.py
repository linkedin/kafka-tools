import unittest

from argparse import Namespace
from mock import patch, Mock

from kafka.tools.assigner.exceptions import UnknownBrokerException
from kafka.tools.assigner.models.broker import Broker
from kafka.tools.assigner.models.cluster import Cluster
from kafka.tools.assigner.models.topic import Topic
from kafka.tools.assigner.sizers.jmx import SizerJMX, _validate_broker


class MockLong:
    def __init__(self, value):
        self.value = value


class MockMBeanServerConnection:
    def getAttribute(self, bean, attribute):
        return MockLong(1234)

    def queryNames(self, name, query):
        return [MockObjectName("testTopic1", "0")]


class MockJMXConnector:
    def getMBeanServerConnection(self):
        return MockMBeanServerConnection()

    def close(self):
        return


class MockObjectName:
    def __init__(self, topic, partition):
        self.topic = topic
        self.partition = partition

    def getKeyProperty(self, key):
        if key == "topic":
            return self.topic
        if key == "partition":
            return self.partition
        return None


class SizerJMXTests(unittest.TestCase):
    def setUp(self):
        self.args = Namespace()
        self.args.property = ['libjvm=/path/to/libjvm']
        self.cluster = self.create_cluster_onehost()

    def create_cluster_onehost(self):
        cluster = Cluster()
        cluster.add_broker(Broker(1, "brokerhost1.example.com"))
        cluster.brokers[1].jmx_port = 1099
        cluster.add_topic(Topic("testTopic1", 2))
        cluster.add_topic(Topic("testTopic2", 2))
        partition = cluster.topics['testTopic1'].partitions[0]
        partition.add_replica(cluster.brokers[1], 0)
        partition = cluster.topics['testTopic1'].partitions[1]
        partition.add_replica(cluster.brokers[1], 0)
        partition = cluster.topics['testTopic2'].partitions[0]
        partition.add_replica(cluster.brokers[1], 0)
        partition = cluster.topics['testTopic2'].partitions[1]
        partition.add_replica(cluster.brokers[1], 0)
        return cluster

    @patch('kafka.tools.assigner.sizers.jmx.jpype.startJVM')
    def test_sizer_create(self, mock_startJVM):
        sizer = SizerJMX(self.args, self.cluster)
        assert isinstance(sizer, SizerJMX)
        mock_startJVM.assert_called_with("/path/to/libjvm")

    @patch('kafka.tools.assigner.sizers.jmx.jpype.startJVM')
    def test_sizer_create_default(self, mock_startJVM):
        self.args.property = []
        sizer = SizerJMX(self.args, self.cluster)
        assert isinstance(sizer, SizerJMX)
        mock_startJVM.assert_called_with("/export/apps/jdk/JDK-1_8_0_72/jre/lib/amd64/server/libjvm.so")

    def test_sizer_run_missing_host(self):
        self.cluster.brokers[1].hostname = None
        self.assertRaises(UnknownBrokerException, _validate_broker, self.cluster.brokers[1])

    def test_sizer_run_missing_jmx_port(self):
        self.cluster.brokers[1].jmx_port = -1
        self.assertRaises(UnknownBrokerException, _validate_broker, self.cluster.brokers[1])

    def test_sizer_fetch_bean(self):
        mock_jpype = Mock()
        sizer = SizerJMX(self.args, self.cluster, mock_jpype)

        bean = MockObjectName("testTopic1", "0")
        connection = MockMBeanServerConnection()
        sizer._fetch_bean(connection, bean)
        assert self.cluster.topics['testTopic1'].partitions[0].size == 1234

    def test_sizer_get_partition_sizes(self):
        mock_jpype = Mock()
        mock_jpype.javax.management.remote.JMXConnectorFactory.connect.return_value = MockJMXConnector()
        sizer = SizerJMX(self.args, self.cluster, mock_jpype)

        sizer.get_partition_sizes()
        mock_jpype.javax.management.remote.JMXServiceURL.assert_called_with("service:jmx:rmi:///jndi/rmi://brokerhost1.example.com:1099/jmxrmi")
        assert self.cluster.topics['testTopic1'].partitions[0].size == 1234
