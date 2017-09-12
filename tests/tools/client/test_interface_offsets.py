import unittest
from mock import MagicMock

from tests.tools.client.fixtures import topic_metadata, offset_fetch

from kafka.tools.client import Client
from kafka.tools.exceptions import GroupError
from kafka.tools.models.group import Group
from kafka.tools.models.topic import TopicOffsets
from kafka.tools.protocol.requests.offset_fetch_v1 import OffsetFetchV1Request


class InterfaceOffsetsTests(unittest.TestCase):
    def setUp(self):
        # Dummy client for testing - we're not going to connect that bootstrap broker
        self.client = Client()
        self.client._connected = True

        # Get the broker and topic from a metadata update
        self.client._update_from_metadata(topic_metadata())

        self.group = Group('testgroup')
        self.offset_fetch = offset_fetch()

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

    def test_get_topics_for_group_string(self):
        val = self.client._get_topics_for_group(self.group, 'testtopic')
        assert val == ['testtopic']

    def test_get_topics_for_group_list(self):
        val = self.client._get_topics_for_group(self.group, ['topica', 'topicb'])
        assert val == ['topica', 'topicb']

    def test_get_topics_for_group_empty(self):
        self.assertRaises(GroupError, self.client._get_topics_for_group, self.group, [])

    def test_get_topics_for_group_subscribed(self):
        self.group.subscribed_topics = MagicMock()
        self.group.subscribed_topics.return_value = ['topica']
        val = self.client._get_topics_for_group(self.group, None)

        self.group.subscribed_topics.assert_called_once_with()
        assert val == ['topica']

    def test_get_offsets_for_group(self):
        self.client.get_group = MagicMock()
        self.client.get_group.return_value = self.group
        self.client._get_topics_for_group = MagicMock()
        self.client._get_topics_for_group.return_value = ['topic1']
        self.client._maybe_update_metadata_for_topics = MagicMock()
        self.client._send_group_aware_request = MagicMock()
        self.client._send_group_aware_request.return_value = self.offset_fetch

        val = self.client.get_offsets_for_group('testgroup')

        self.client.get_group.assert_called_once_with('testgroup')
        self.client._get_topics_for_group.assert_called_once_with(self.group, None)
        self.client._maybe_update_metadata_for_topics.assert_called_once_with(['topic1'])

        self.client._send_group_aware_request.assert_called_once()
        assert self.client._send_group_aware_request.call_args[0][0] == 'testgroup'
        req = self.client._send_group_aware_request.call_args[0][1]
        assert isinstance(req, OffsetFetchV1Request)
        assert req['group_id'] == 'testgroup'
        assert len(req['topics']) == 1
        assert req['topics'][0]['topic'] == 'topic1'
        assert len(req['topics'][0]['partitions']) == 2
        assert req['topics'][0]['partitions'][0] == 0
        assert req['topics'][0]['partitions'][1] == 1

        assert 'topic1' in val
        assert isinstance(val['topic1'], TopicOffsets)
        assert val['topic1'].partitions == [4829, 8904]

    def test_set_offsets_for_group_bad_offsets(self):
        self.assertRaises(TypeError, self.client.set_offsets_for_group, 'testgroup', 'notalist')

    def test_set_offsets_for_group_offsets_none(self):
        self.assertRaises(TypeError, self.client.set_offsets_for_group, 'testgroup', None)

    def test_set_offsets_for_group_not_empty(self):
        self.group.state = 'Stable'

        self.client.get_group = MagicMock()
        self.client.get_group.return_value = self.group

        self.assertRaises(GroupError, self.client.set_offsets_for_group, 'testgroup', [])
        self.client.get_group.assert_called_once_with('testgroup')

    def test_set_offsets_for_group(self):
        self.client.get_group = MagicMock()
        self.client.get_group.return_value = self.group
        self.client._send_set_offset_request = MagicMock()
        self.client._send_set_offset_request.return_value = 'sendresponse'
        self.client._parse_set_offset_response = MagicMock()
        self.client._parse_set_offset_response.return_value = {'topic1': [0, 0]}

        offsets = TopicOffsets(self.client.cluster.topics['topic1'])
        offsets.partitions[0] = 2342
        offsets.partitions[1] = 8793
        val = self.client.set_offsets_for_group('testgroup', [offsets])

        assert val == {'topic1': [0, 0]}
        self.client.get_group.assert_called_once_with('testgroup')
        self.client._send_set_offset_request.assert_called_once_with('testgroup', [offsets])
        self.client._parse_set_offset_response.assert_called_once_with('sendresponse')
