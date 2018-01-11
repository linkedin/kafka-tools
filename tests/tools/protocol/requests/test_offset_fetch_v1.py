import unittest

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.offset_fetch_v2 import OffsetFetchV2Request


class OffsetFetchV2RequestTests(unittest.TestCase):
    def test_process_arguments(self):
        val = OffsetFetchV2Request.process_arguments(['groupname', 'topicname,4', 'nexttopic,9'])
        assert val == {'group_id': 'groupname',
                       'topics': [{'topic': 'topicname', 'partitions': [4]},
                                  {'topic': 'nexttopic', 'partitions': [9]}]}

    def test_process_arguments_alltopics(self):
        val = OffsetFetchV2Request.process_arguments(['groupname'])
        assert val == {'group_id': 'groupname',
                       'topics': None}

    def test_process_arguments_notenough(self):
        self.assertRaises(ArgumentError, OffsetFetchV2Request.process_arguments, [])
