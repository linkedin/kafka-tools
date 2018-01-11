import unittest

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.offset_commit_v0 import _get_partition_map, _parse_next_topic, OffsetCommitV0Request


class OffsetCommitV0RequestTests(unittest.TestCase):
    def test_get_partition_map(self):
        val = _get_partition_map([4, 2])
        assert val == {'partition': 4, 'offset': 2, 'metadata': None}

    def test_get_partition_map_metadata(self):
        val = _get_partition_map([4, 2, 'somemetadata'])
        assert val == {'partition': 4, 'offset': 2, 'metadata': 'somemetadata'}

    def test_get_partition_map_short(self):
        self.assertRaises(ArgumentError, _get_partition_map, [4])

    def test_get_partition_map_extra(self):
        self.assertRaises(ArgumentError, _get_partition_map, [4, 2, 'somemetadata', 'foo'])

    def test_get_partition_map_nonnumeric(self):
        self.assertRaises(ArgumentError, _get_partition_map, ['foo', 2, 'somemetadata'])
        self.assertRaises(ArgumentError, _get_partition_map, [4, 'foo', 'somemetadata'])

    def test_parse_next_topic(self):
        val, rest = _parse_next_topic(['topicname', '4,2'])
        assert val == {'topic': 'topicname', 'partitions': [{'partition': 4, 'offset': 2, 'metadata': None}]}
        assert rest == []

    def test_parse_next_topic_multiple(self):
        val, rest = _parse_next_topic(['topicname', '4,2', '9,3'])
        assert val == {'topic': 'topicname', 'partitions': [{'partition': 4, 'offset': 2, 'metadata': None},
                                                            {'partition': 9, 'offset': 3, 'metadata': None}]}
        assert rest == []

    def test_parse_next_topic_remainder(self):
        val, rest = _parse_next_topic(['topicname', '4,2', 'nexttopic', '9,3'])
        assert val == {'topic': 'topicname', 'partitions': [{'partition': 4, 'offset': 2, 'metadata': None}]}
        assert rest == ['nexttopic', '9,3']

    def test_parse_next_topic_nopartitions(self):
        self.assertRaises(ArgumentError, _parse_next_topic, ['topicname'])

    def test_process_arguments(self):
        val = OffsetCommitV0Request.process_arguments(['groupname', 'topicname', '4,2,somemetadata', 'nexttopic', '9,3,moremetadata'])
        assert val == {'group_id': 'groupname',
                       'topics': [{'topic': 'topicname', 'partitions': [{'partition': 4, 'offset': 2, 'metadata': 'somemetadata'}]},
                                  {'topic': 'nexttopic', 'partitions': [{'partition': 9, 'offset': 3, 'metadata': 'moremetadata'}]}]}

    def test_process_arguments_notenough(self):
        self.assertRaises(ArgumentError, OffsetCommitV0Request.process_arguments, ['groupname', 'topicname'])
