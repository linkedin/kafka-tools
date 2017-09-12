import unittest
from mock import MagicMock

from tests.tools.client.fixtures import list_offset, list_offset_error, topic_metadata, offset_commit_response

from kafka.tools.client import Client
from kafka.tools.exceptions import ConnectionError, OffsetError
from kafka.tools.models.topic import TopicOffsets
from kafka.tools.protocol.requests.offset_commit_v2 import OffsetCommitV2Request


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
        assert req['topics'][0]['topic'] == 'topic1'
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
        assert len(val['topic1'].partitions) == 2
        assert val['topic1'].partitions[0] == 4829
        assert val['topic1'].partitions[1] == 8904

    def test_send_list_offsets_to_brokers_connection_error(self):
        self.client.cluster.brokers[1].send = MagicMock()
        self.client.cluster.brokers[1].send.side_effect = ConnectionError
        self.assertRaises(ConnectionError, self.client._send_list_offsets_to_brokers, self.list_offset_request)

    def test_send_list_offsets_to_brokers_offsets_error(self):
        self.client.cluster.brokers[1].send = MagicMock()
        self.client.cluster.brokers[1].send.return_value = (1, self.list_offset_error)
        self.assertRaises(OffsetError, self.client._send_list_offsets_to_brokers, self.list_offset_request)

    def test_send_set_offset_request_bad_offsets(self):
        self.assertRaises(TypeError, self.client._send_set_offset_request, 'testgroup', ['notatopicoffsets'])

    def test_send_set_offset_request_bad_topic(self):
        offsets = TopicOffsets(self.client.cluster.topics['topic1'])
        offsets.topic = None
        self.assertRaises(TypeError, self.client._send_set_offset_request, 'testgroup', [offsets])

    def test_send_set_offset_request(self):
        offsets = TopicOffsets(self.client.cluster.topics['topic1'])
        offsets.partitions[0] = 2342
        offsets.partitions[1] = 8793

        self.client._send_group_aware_request = MagicMock()
        self.client._send_group_aware_request.return_value = 'responseobject'
        val = self.client._send_set_offset_request('testgroup', [offsets])
        assert val == 'responseobject'

        self.client._send_group_aware_request.assert_called_once()
        assert self.client._send_group_aware_request.call_args[0][0] == 'testgroup'
        req = self.client._send_group_aware_request.call_args[0][1]
        assert isinstance(req, OffsetCommitV2Request)
        assert req['group_id'] == 'testgroup'
        assert req['group_generation_id'] == -1
        assert req['member_id'] == ''
        assert req['retention_time'] == -1
        assert len(req['topics']) == 1
        assert req['topics'][0]['topic'] == 'topic1'
        assert len(req['topics'][0]['partitions']) == 2
        assert req['topics'][0]['partitions'][0]['partition'] == 0
        assert req['topics'][0]['partitions'][0]['offset'] == 2342
        assert req['topics'][0]['partitions'][0]['metadata'] is None
        assert req['topics'][0]['partitions'][1]['partition'] == 1
        assert req['topics'][0]['partitions'][1]['offset'] == 8793
        assert req['topics'][0]['partitions'][1]['metadata'] is None

    def test_parse_set_offset_response(self):
        response = offset_commit_response()
        val = self.client._parse_set_offset_response(response)
        assert val == {'topic1': [0, 16]}
