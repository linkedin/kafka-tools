import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.offset_fetch_v0 import _parse_topic_set, OffsetFetchV0Request


class OffsetFetchV0RequestTests(unittest.TestCase):
    def test_parse_topic_set(self):
        val = _parse_topic_set('topicname,4')
        assert val == {'topic': 'topicname', 'partitions': [4]}

    def test_parse_topic_set_multiple(self):
        val = _parse_topic_set('topicname,4,2,8')
        assert val == {'topic': 'topicname', 'partitions': [4, 2, 8]}

    def test_parse_topic_set_nopartitions(self):
        self.assertRaises(ArgumentError, _parse_topic_set, 'topicname')

    def test_parse_topic_set_nonnumeric(self):
        self.assertRaises(ArgumentError, _parse_topic_set, 'topicname,foo,2,8')
        self.assertRaises(ArgumentError, _parse_topic_set, 'topicname,4,foo')

    def test_process_arguments(self):
        val = OffsetFetchV0Request.process_arguments(['groupname', 'topicname,4', 'nexttopic,9'])
        assert val == {'group_id': 'groupname',
                       'topics': [{'topic': 'topicname', 'partitions': [4]},
                                  {'topic': 'nexttopic', 'partitions': [9]}]}

    def test_process_arguments_notenough(self):
        self.assertRaises(ArgumentError, OffsetFetchV0Request.process_arguments, ['groupname'])

    def test_schema(self):
        validate_schema(OffsetFetchV0Request.schema)
