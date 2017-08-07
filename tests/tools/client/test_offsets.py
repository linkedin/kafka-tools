import unittest
from mock import MagicMock

from tests.tools.client.fixtures import list_offset, list_offset_error, topic_metadata

from kafka.tools.client import Client
from kafka.tools.exceptions import ConnectionError, OffsetError
from kafka.tools.models.topic import TopicOffsets


class OffsetTests(unittest.TestCase):
    def setUp(self):
        # Dummy client for testing - we're not going to connect that bootstrap broker
        self.client = Client()

        # Get the broker and topic from a metadata update
        self.client._update_from_metadata(topic_metadata())

        self.list_offset = list_offset()
        self.list_offset_error = list_offset_error()
        self.list_offset_request = {1: {'replica_id': -1,
                                        'topics': [{'topic': 'topic1',
                                                    'partitions': [{'partition': 0,
                                                                    'timestamp': Client.OFFSET_LATEST,
                                                                    'max_num_offsets': 1},
                                                                   {'partition': 1,
                                                                    'timestamp': Client.OFFSET_EARLIEST,
                                                                    'max_num_offsets': 1}]}]}}

    def test_send_list_offsets_to_brokers(self):
        self.client.cluster.brokers[1].send = MagicMock()
        self.client.cluster.brokers[1].send.return_value = (1, self.list_offset)

        val = self.client._send_list_offsets_to_brokers(self.list_offset_request)

        self.client.cluster.brokers[1].send.assert_called_once()
        req = self.client.cluster.brokers[1].send.call_args[0][0]
        assert req['replica_id'] == -1
        assert len(req['topics']) == 1
        assert req['topics'][0]['topic'].value() == 'topic1'
        assert len(req['topics'][0]['partitions']) == 2
        assert req['topics'][0]['partitions'][0]['partition'] == 0
        assert req['topics'][0]['partitions'][0]['timestamp'] == Client.OFFSET_LATEST
        assert req['topics'][0]['partitions'][0]['max_num_offsets'] == 1
        assert req['topics'][0]['partitions'][1]['partition'] == 1
        assert req['topics'][0]['partitions'][1]['timestamp'] == Client.OFFSET_EARLIEST
        assert req['topics'][0]['partitions'][1]['max_num_offsets'] == 1

        assert len(val) == 1
        assert 'topic1' in val
        assert isinstance(val['topic1'], TopicOffsets)
        assert val['topic1'].topic == self.client.cluster.topics['topic1']
        assert len(val['topic1'].partition) == 2
        assert val['topic1'].partition[0] == 4829
        assert val['topic1'].partition[1] == 8904

    def test_send_list_offsets_to_brokers_connection_error(self):
        self.client.cluster.brokers[1].send = MagicMock()
        self.client.cluster.brokers[1].send.side_effect = ConnectionError
        self.assertRaises(ConnectionError, self.client._send_list_offsets_to_brokers, self.list_offset_request)

    def test_send_list_offsets_to_brokers_offsets_error(self):
        self.client.cluster.brokers[1].send = MagicMock()
        self.client.cluster.brokers[1].send.return_value = (1, self.list_offset_error)
        self.assertRaises(OffsetError, self.client._send_list_offsets_to_brokers, self.list_offset_request)
