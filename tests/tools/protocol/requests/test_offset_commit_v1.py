import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.offset_commit_v1 import _get_partition_map, _parse_next_topic, OffsetCommitV1Request


class OffsetCommitV1RequestTests(unittest.TestCase):
    def test_get_partition_map(self):
        val = _get_partition_map([4, 2, 8])
        assert val == {'partition': 4, 'offset': 2, 'timestamp': 8, 'metadata': None}

    def test_get_partition_map_metadata(self):
        val = _get_partition_map([4, 2, 8, 'somemetadata'])
        assert val == {'partition': 4, 'offset': 2, 'timestamp': 8, 'metadata': 'somemetadata'}

    def test_get_partition_map_short(self):
        self.assertRaises(ArgumentError, _get_partition_map, ['foo', 2])

    def test_get_partition_map_extra(self):
        self.assertRaises(ArgumentError, _get_partition_map, ['foo', 2, 8, 'somemetadata', 'foo'])

    def test_get_partition_map_nonnumeric(self):
        self.assertRaises(ArgumentError, _get_partition_map, ['foo', 2, 8])
        self.assertRaises(ArgumentError, _get_partition_map, [4, 'foo', 8])
        self.assertRaises(ArgumentError, _get_partition_map, [4, 2, 'foo'])

    def test_parse_next_topic(self):
        val, rest = _parse_next_topic(['topicname', '4,2,8'])
        assert val == {'topic': 'topicname', 'partitions': [{'partition': 4, 'offset': 2, 'timestamp': 8, 'metadata': None}]}
        assert rest == []

    def test_parse_next_topic_multiple(self):
        val, rest = _parse_next_topic(['topicname', '4,2,8', '9,3,4'])
        assert val == {'topic': 'topicname', 'partitions': [{'partition': 4, 'offset': 2, 'timestamp': 8, 'metadata': None},
                       {'partition': 9, 'offset': 3, 'timestamp': 4, 'metadata': None}]}
        assert rest == []

    def test_parse_next_topic_remainder(self):
        val, rest = _parse_next_topic(['topicname', '4,2,8', 'nexttopic', '9,3,4'])
        assert val == {'topic': 'topicname', 'partitions': [{'partition': 4, 'offset': 2, 'timestamp': 8, 'metadata': None}]}
        assert rest == ['nexttopic', '9,3,4']

    def test_parse_next_topic_nopartitions(self):
        self.assertRaises(ArgumentError, _parse_next_topic, ['topicname'])

    def test_process_arguments(self):
        val = OffsetCommitV1Request.process_arguments(['groupname', '16', 'membername', 'topicname', '4,2,8', 'nexttopic', '9,3,4'])
        assert val == {'group_id': 'groupname',
                       'group_generation_id': 16,
                       'member_id': 'membername',
                       'topics': [{'topic': 'topicname', 'partitions': [{'partition': 4, 'offset': 2, 'timestamp': 8, 'metadata': None}]},
                                  {'topic': 'nexttopic', 'partitions': [{'partition': 9, 'offset': 3, 'timestamp': 4, 'metadata': None}]}]}

    def test_process_arguments_notenough(self):
        self.assertRaises(ArgumentError, OffsetCommitV1Request.process_arguments, ['groupname', '16', 'membername', 'topicname'])

    def test_process_arguments_nonnumeric(self):
        self.assertRaises(ArgumentError, OffsetCommitV1Request.process_arguments, ['groupname', 'foo', 'membername', 'topicname', '4,2,8'])

    def test_schema(self):
        validate_schema(OffsetCommitV1Request.schema)
