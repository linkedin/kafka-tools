import unittest
from mock import MagicMock, patch, call

from tests.tools.client.fixtures import topic_metadata

from kafka.tools.client import Client
from kafka.tools.configuration import ClientConfiguration
from kafka.tools.exceptions import ConfigurationError, ConnectionError
from kafka.tools.models.broker import Broker
from kafka.tools.models.cluster import Cluster
from kafka.tools.protocol.requests.topic_metadata_v1 import TopicMetadataV1Request


class GenericInterfaceTests(unittest.TestCase):
    def setUp(self):
        # Dummy client for testing - we're not going to connect that bootstrap broker
        self.client = Client(broker_list='broker1.example.com:9091,broker2.example.com:9092')
        self.client._connected = True
        self.metadata_response = topic_metadata()

    def test_close(self):
        # Two brokers for the client
        broker = Broker('host1.example.com', id=1, port=8031)
        broker.rack = 'rack1'
        broker.close = MagicMock()
        self.client.cluster.add_broker(broker)

        broker = Broker('host2.example.com', id=101, port=8032)
        broker.rack = 'rack1'
        broker.close = MagicMock()
        self.client.cluster.add_broker(broker)

        self.client.close()
        for broker_id in self.client.cluster.brokers:
            self.client.cluster.brokers[broker_id].close.assert_called_once()

    def test_create(self):
        assert self.client.configuration.broker_list == [('broker1.example.com', 9091), ('broker2.example.com', 9092)]

    def test_create_with_configuration(self):
        config = ClientConfiguration(zkconnect='zk.example.com:2181/kafka-cluster')
        client = Client(configuration=config)
        assert client.configuration == config

    def test_create_config_zkconnect(self):
        test_client = Client(zkconnect='zk.example.com:2181/kafka-cluster')
        assert test_client.configuration.zkconnect == 'zk.example.com:2181/kafka-cluster'

    def test_create_config_bad_arg(self):
        self.assertRaises(ConfigurationError, Client, invalidconfig='foo')

    def test_create_config_bad_config_object(self):
        self.assertRaises(ConfigurationError, Client, configuration='foo')

    @patch.object(Cluster, 'create_from_zookeeper')
    def test_connect_zookeeper(self, mock_create):
        mock_create.return_value = Cluster()
        test_client = Client(zkconnect='zk.example.com:2181/kafka-cluster')
        test_client.connect()
        mock_create.assert_called_once_with(zkconnect='zk.example.com:2181/kafka-cluster', fetch_topics=False)

    @patch.object(Broker, 'connect')
    @patch.object(Broker, 'send')
    @patch.object(Broker, 'close')
    def test_connect_broker_list(self, mock_close, mock_send, mock_connect):
        def add_brokers_with_mocks(metadata):
            self.client._update_brokers_from_metadata(metadata)
            for broker_id in self.client.cluster.brokers:
                self.client.cluster.brokers[broker_id].connect = MagicMock()

        mock_connect.side_effect = [ConnectionError, None]
        mock_send.return_value = (1, self.metadata_response)
        self.client._update_from_metadata = MagicMock()
        self.client._update_from_metadata.side_effect = add_brokers_with_mocks

        self.client.connect()
        mock_connect.assert_has_calls([call(), call()])
        mock_send.assert_called_once()
        assert isinstance(mock_send.call_args[0][0], TopicMetadataV1Request)
        mock_close.assert_called_once()

        # Metadata is updated and brokers are connected
        self.client._update_from_metadata.assert_called_once_with(self.metadata_response)
        for broker_id in self.client.cluster.brokers:
            self.client.cluster.brokers[broker_id].connect.assert_called_once()

    def test_connect_broker_list_exhausted(self):
        self.client._maybe_bootstrap_cluster = MagicMock()
        self.client._maybe_bootstrap_cluster.return_value = False
        self.assertRaises(ConnectionError, self.client.connect)
