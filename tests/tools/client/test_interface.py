import unittest
from mock import MagicMock, patch

from tests.tools.client.fixtures import topic_metadata

from kafka.tools.client import Client
from kafka.tools.models.cluster import Cluster
from kafka.tools.models.broker import Broker
from kafka.tools.protocol.requests.topic_metadata_v1 import TopicMetadataV1Request


class GenericInterfaceTests(unittest.TestCase):
    def setUp(self):
        # Dummy client for testing - we're not going to connect that bootstrap broker
        self.client = Client()
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

    def test_connect_bootstrap(self):
        def add_brokers_with_mocks(metadata):
            self.client._update_brokers_from_metadata(metadata)
            for broker_id in self.client.cluster.brokers:
                self.client.cluster.brokers[broker_id].connect = MagicMock()

        bootstrap = self.client._bootstrap_broker
        bootstrap.connect = MagicMock()
        bootstrap.send = MagicMock()
        bootstrap.send.return_value = (1, self.metadata_response)
        bootstrap.close = MagicMock()

        self.client._update_from_metadata = MagicMock()
        self.client._update_from_metadata.side_effect = add_brokers_with_mocks

        self.client.connect()

        # Bootstrap broker is used for a single metadata request and then closed
        bootstrap.connect.assert_called_once()
        bootstrap.send.assert_called_once()
        assert isinstance(bootstrap.send.call_args[0][0], TopicMetadataV1Request)
        bootstrap.close.assert_called_once()
        assert self.client._bootstrap_broker is None

        # Metadata is updated and brokers are connected
        self.client._update_from_metadata.assert_called_once_with(self.metadata_response)
        for broker_id in self.client.cluster.brokers:
            self.client.cluster.brokers[broker_id].connect.assert_called_once()

    @patch.object(Cluster, 'create_from_zookeeper')
    def test_create_with_zkconnect(self, mock_create):
        mock_create.return_value = self.client.cluster

        test_client = Client(zkconnect='zk.example.com:2181/kafka-cluster')

        assert test_client.cluster == self.client.cluster
        mock_create.assert_called_once_with(zkconnect='zk.example.com:2181/kafka-cluster')
