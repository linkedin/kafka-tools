import unittest

from argparse import Namespace
from mock import patch, Mock, call

from kafka.tools.exceptions import UnknownBrokerException, ConfigurationException
from kafka.tools.models.broker import Broker
from kafka.tools.models.cluster import Cluster
from kafka.tools.models.topic import Topic
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


class MockHashMap:
    def __init__(self):
        self._hashmap = {}

    def put(self, key, value):
        self._hashmap[key] = value

    def __getitem__(self, key):
        if key in self._hashmap:
            return self._hashmap[key]
        return None


class SizerJMXTests(unittest.TestCase):
    def setUp(self):
        self.args = Namespace()
        self.args.property = ['libjvm=/path/to/libjvm']
        self.cluster = self.create_cluster_onehost()

        self.mock_jpype = Mock()
        self.mock_hashmap = MockHashMap()
        self.mock_jpype.javax.management.remote.JMXConnectorFactory.connect.return_value = MockJMXConnector()
        self.mock_jpype.javax.management.remote.JMXServiceURL.return_value = "testURL"
        self.mock_jpype.java.util.HashMap.return_value = self.mock_hashmap
        self.mock_jpype.JArray.return_value = list
        self.mock_jpype.javax.management.remote.JMXConnector.CREDENTIALS = "CREDENTIALS"

    def create_cluster_onehost(self):
        cluster = Cluster()
        cluster.add_broker(Broker("brokerhost1.example.com", id=1))
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

    @patch('kafka.tools.assigner.sizers.jmx.jpype')
    def test_sizer_create(self, mock_jpype_module):
        sizer = SizerJMX(self.args, self.cluster)
        assert isinstance(sizer, SizerJMX)
        mock_jpype_module.startJVM.assert_called_with("/path/to/libjvm")
        mock_jpype_module.java.util.HashMap.assert_called_once()

    @patch('kafka.tools.assigner.sizers.jmx.jpype')
    def test_sizer_create_default(self, mock_jpype_module):
        self.args.property = []
        sizer = SizerJMX(self.args, self.cluster)
        assert isinstance(sizer, SizerJMX)
        mock_jpype_module.startJVM.assert_called_with("/export/apps/jdk/JDK-1_8_0_72/jre/lib/amd64/server/libjvm.so")
        mock_jpype_module.java.util.HashMap.assert_called_once()

    def test_sizer_missing_username(self):
        self.args.property.append("jmxpass=somepass")
        self.assertRaises(ConfigurationException, SizerJMX, self.args, self.cluster, self.mock_jpype)

    def test_sizer_missing_password(self):
        self.args.property.append("jmxuser=someuser")
        self.assertRaises(ConfigurationException, SizerJMX, self.args, self.cluster, self.mock_jpype)

    def test_sizer_missing_truststore(self):
        self.args.property.append("truststorepass=sometrustpass")
        self.assertRaises(ConfigurationException, SizerJMX, self.args, self.cluster, self.mock_jpype)

    def test_sizer_missing_truststorepass(self):
        self.args.property.append("truststore=somefile")
        self.assertRaises(ConfigurationException, SizerJMX, self.args, self.cluster, self.mock_jpype)

    def test_sizer_set_username_password(self):
        self.args.property.append("jmxuser=someuser")
        self.args.property.append("jmxpass=somepass")
        sizer = SizerJMX(self.args, self.cluster, self.mock_jpype)
        assert sizer._envhash["CREDENTIALS"] == ["someuser", "somepass"]

    def test_sizer_set_truststore(self):
        self.args.property.append("truststore=somefile")
        self.args.property.append("truststorepass=sometrustpass")
        SizerJMX(self.args, self.cluster, self.mock_jpype)
        self.mock_jpype.java.lang.System.setProperty.assert_has_calls(
            [call("javax.net.ssl.trustStore", "somefile"), call("javax.net.ssl.trustStorePassword", "sometrustpass")],
            any_order=True)

    def test_sizer_run_missing_host(self):
        self.cluster.brokers[1].hostname = None
        self.assertRaises(UnknownBrokerException, _validate_broker, self.cluster.brokers[1])

    def test_sizer_run_missing_jmx_port(self):
        self.cluster.brokers[1].jmx_port = -1
        self.assertRaises(UnknownBrokerException, _validate_broker, self.cluster.brokers[1])

    def test_sizer_fetch_bean(self):
        sizer = SizerJMX(self.args, self.cluster, self.mock_jpype)

        bean = MockObjectName("testTopic1", "0")
        connection = MockMBeanServerConnection()
        sizer._fetch_bean(connection, bean)
        assert self.cluster.topics['testTopic1'].partitions[0].size == 1234

    def test_sizer_get_partition_sizes(self):
        sizer = SizerJMX(self.args, self.cluster, self.mock_jpype)

        sizer.get_partition_sizes()
        self.mock_jpype.javax.management.remote.JMXConnectorFactory.connect.assert_called_with("testURL", self.mock_hashmap)
        self.mock_jpype.javax.management.remote.JMXServiceURL.assert_called_with("service:jmx:rmi:///jndi/rmi://brokerhost1.example.com:1099/jmxrmi")
        assert self.cluster.topics['testTopic1'].partitions[0].size == 1234
