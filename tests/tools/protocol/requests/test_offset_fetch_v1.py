import unittest

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.offset_fetch_v1 import _parse_topic_set, OffsetFetchV1Request


class OffsetFetchV1RequestTests(unittest.TestCase):
    def test_parse_topic_set(self):
        val = _parse_topic_set('topicname,4')
        assert val == {'topic': 'topicname', 'partitions': [{'partition': 4}]}

    def test_parse_topic_set_multiple(self):
        val = _parse_topic_set('topicname,4,2,8')
        assert val == {'topic': 'topicname', 'partitions': [{'partition': 4}, {'partition': 2}, {'partition': 8}]}

    def test_parse_topic_set_nopartitions(self):
        self.assertRaises(ArgumentError, _parse_topic_set, 'topicname')

    def test_parse_topic_set_nonnumeric(self):
        self.assertRaises(ArgumentError, _parse_topic_set, 'topicname,foo,2,8')
        self.assertRaises(ArgumentError, _parse_topic_set, 'topicname,4,foo')

    def test_process_arguments(self):
        val = OffsetFetchV1Request.process_arguments(['groupname', 'topicname,4', 'nexttopic,9'])
        assert val == {'group_id': 'groupname',
                       'topics': [{'topic': 'topicname', 'partitions': [{'partition': 4}]},
                                  {'topic': 'nexttopic', 'partitions': [{'partition': 9}]}]}

    def test_process_arguments_notenough(self):
        self.assertRaises(ArgumentError, OffsetFetchV1Request.process_arguments, ['groupname'])
