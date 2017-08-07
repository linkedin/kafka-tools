import unittest
from mock import MagicMock

from tests.tools.client.fixtures import topic_metadata

from kafka.tools.client import Client


class InterfaceOffsetsTests(unittest.TestCase):
    def setUp(self):
        # Dummy client for testing - we're not going to connect that bootstrap broker
        self.client = Client()

        # Get the broker and topic from a metadata update
        self.client._update_from_metadata(topic_metadata())

    def test_get_offsets_for_topics(self):
        self.client._maybe_update_metadata_for_topics = MagicMock()
        self.client._send_list_offsets_to_brokers = MagicMock()
        self.client._send_list_offsets_to_brokers.return_value = {'topic1': 'responseobj'}

        val = self.client.get_offsets_for_topics(['topic1'])
        self.client._maybe_update_metadata_for_topics.assert_called_once_with(['topic1'])
        self.client._send_list_offsets_to_brokers.assert_called_once()

        values = self.client._send_list_offsets_to_brokers.call_args[0][0]
        assert len(values) == 2
        for broker_id in (1, 101):
            assert values[broker_id]['replica_id'] == -1
            assert len(values[broker_id]['topics']) == 1
            assert values[broker_id]['topics'][0]['topic'] == 'topic1'
            assert len(values[broker_id]['topics'][0]['partitions']) == 1
            assert values[broker_id]['topics'][0]['partitions'][0]['timestamp'] == Client.OFFSET_LATEST
            assert values[broker_id]['topics'][0]['partitions'][0]['max_num_offsets'] == 1
        assert values[1]['topics'][0]['partitions'][0]['partition'] == 0
        assert values[101]['topics'][0]['partitions'][0]['partition'] == 1

        assert val == {'topic1': 'responseobj'}

    def test_get_offsets_for_topic(self):
        self.client.get_offsets_for_topics = MagicMock()
        self.client.get_offsets_for_topics.return_value = {'topic1': 'thingitreturns'}
        val = self.client.get_offsets_for_topic('topic1')
        self.client.get_offsets_for_topics.assert_called_once_with(['topic1'], Client.OFFSET_LATEST)
        assert val == 'thingitreturns'

    def test_get_offsets_for_topic_bad_timestamp(self):
        self.assertRaises(TypeError, self.client.get_offsets_for_topics, ['topic1'], timestamp='notanint')
