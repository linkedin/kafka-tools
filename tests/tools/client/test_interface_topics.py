import unittest
from mock import MagicMock

from tests.tools.client.fixtures import topic_metadata, topic_metadata_error

from kafka.tools.client import Client
from kafka.tools.exceptions import TopicError
from kafka.tools.models.broker import Broker
from kafka.tools.models.topic import Topic
from kafka.tools.protocol.requests.topic_metadata_v1 import TopicMetadataV1Request


class InterfaceTopicsTests(unittest.TestCase):
    def setUp(self):
        # Dummy client for testing - we're not going to connect that bootstrap broker
        self.client = Client()
        self.client._connected = True

        # Two brokers for the client
        broker = Broker('host1.example.com', id=1, port=8031)
        broker.rack = 'rack1'
        self.client.cluster.add_broker(broker)
        broker = Broker('host2.example.com', id=101, port=8032)
        broker.rack = 'rack1'
        self.client.cluster.add_broker(broker)

        self.metadata = topic_metadata()
        self.metadata_error = topic_metadata_error()

    def test_list_topics(self):
        self.client.cluster.add_topic(Topic('topic1', 1))

        self.client._maybe_update_full_metadata = MagicMock()

        topics = self.client.list_topics()
        self.client._maybe_update_full_metadata.assert_called_once_with(True)
        assert topics == ['topic1']

    def test_list_topics_cache(self):
        self.client._maybe_update_full_metadata = MagicMock()

        self.client.list_topics(cache=False)
        self.client._maybe_update_full_metadata.assert_called_once_with(False)

    def test_get_topic(self):
        self.client._send_any_broker = MagicMock()
        self.client._send_any_broker.return_value = self.metadata

        val = self.client.get_topic('topic1')

        self.client._send_any_broker.assert_called_once()
        assert isinstance(self.client._send_any_broker.call_args[0][0], TopicMetadataV1Request)

        assert isinstance(val, Topic)
        assert val.name == 'topic1'
        assert len(val.partitions) == 2

    def test_get_topic_existing_cached(self):
        topic = Topic('topic1', 2)
        self.client.cluster.add_topic(topic)

        self.client._send_any_broker = MagicMock()
        self.client._send_any_broker.return_value = self.metadata
        self.client._update_from_metadata = MagicMock()
        val = self.client.get_topic('topic1')

        self.client._send_any_broker.assert_not_called()
        self.client._update_from_metadata.assert_not_called()

        assert isinstance(val, Topic)
        assert val.name == 'topic1'

    def test_get_topic_force_cache(self):
        topic = Topic('topic1', 1)
        self.client.cluster.add_topic(topic)

        self.client._send_any_broker = MagicMock()
        self.client._send_any_broker.return_value = self.metadata

        val = self.client.get_topic('topic1', cache=False)

        self.client._send_any_broker.assert_called_once()
        assert isinstance(self.client._send_any_broker.call_args[0][0], TopicMetadataV1Request)

        assert isinstance(val, Topic)
        assert val.name == 'topic1'
        assert len(val.partitions) == 2

    def test_get_topic_expired(self):
        topic = Topic('topic1', 1)
        topic._last_updated = 100
        self.client.cluster.add_topic(topic)

        self.client._send_any_broker = MagicMock()
        self.client._send_any_broker.return_value = self.metadata

        val = self.client.get_topic('topic1')

        self.client._send_any_broker.assert_called_once()
        assert isinstance(self.client._send_any_broker.call_args[0][0], TopicMetadataV1Request)

        assert isinstance(val, Topic)
        assert val.name == 'topic1'
        assert len(val.partitions) == 2

    def test_get_topic_error(self):
        topic = Topic('topic1', 1)
        self.client.cluster.add_topic(topic)

        self.client._send_any_broker = MagicMock()
        self.client._send_any_broker.return_value = self.metadata_error

        self.assertRaises(TopicError, self.client.get_topic, 'topic1', cache=False)
        self.client._send_any_broker.assert_called_once()
        assert isinstance(self.client._send_any_broker.call_args[0][0], TopicMetadataV1Request)
