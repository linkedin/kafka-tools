import unittest

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.list_offset_v1 import _parse_next_topic, ListOffsetV1Request


class ListOffsetV1RequestTest(unittest.TestCase):
    def test_parse_next_topic(self):
        val, rest = _parse_next_topic(['topicname', '4,2'])
        assert val == {'topic': 'topicname', 'partitions': [{'partition': 4, 'timestamp': 2}]}
        assert rest == []

    def test_parse_next_topic_multiple(self):
        val, rest = _parse_next_topic(['topicname', '4,2', '9,3'])
        assert val == {'topic': 'topicname', 'partitions': [{'partition': 4, 'timestamp': 2},
                                                            {'partition': 9, 'timestamp': 3}]}
        assert rest == []

    def test_parse_next_topic_remainder(self):
        val, rest = _parse_next_topic(['topicname', '4,2', 'nexttopic', '9,3'])
        assert val == {'topic': 'topicname', 'partitions': [{'partition': 4, 'timestamp': 2}]}
        assert rest == ['nexttopic', '9,3']

    def test_parse_next_topic_nopartitions(self):
        self.assertRaises(ArgumentError, _parse_next_topic, ['topicname'])

    def test_parse_next_topic_short_partitions(self):
        self.assertRaises(ArgumentError, _parse_next_topic, ['topicname', '4'])

    def test_parse_next_topic_extra_partitions(self):
        self.assertRaises(ArgumentError, _parse_next_topic, ['topicname', '4,2,8'])

    def test_parse_next_topic_nonnumeric(self):
        self.assertRaises(ArgumentError, _parse_next_topic, ['topicname', 'foo,2'])
        self.assertRaises(ArgumentError, _parse_next_topic, ['topicname', '4,foo'])

    def test_process_arguments(self):
        val = ListOffsetV1Request.process_arguments(['-1', 'topicname', '4,2', 'nexttopic', '9,3'])
        assert val == {'replica_id': -1,
                       'topics': [{'topic': 'topicname', 'partitions': [{'partition': 4, 'timestamp': 2}]},
                                  {'topic': 'nexttopic', 'partitions': [{'partition': 9, 'timestamp': 3}]}]}

    def test_process_arguments_nonnumeric(self):
        self.assertRaises(ArgumentError, ListOffsetV1Request.process_arguments, ['foo', 'topicname', '4,2'])

    def test_process_arguments_notenough(self):
        self.assertRaises(ArgumentError, ListOffsetV1Request.process_arguments, ['-1', 'topicname'])
