import unittest

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.offset_commit_v2 import OffsetCommitV2Request


class OffsetCommitV2RequestTests(unittest.TestCase):
    def test_process_arguments(self):
        val = OffsetCommitV2Request.process_arguments(['groupname', '16', 'membername', '76', 'topicname', '4,2', 'nexttopic', '9,3'])
        assert val == {'group_id': 'groupname',
                       'group_generation_id': 16,
                       'member_id': 'membername',
                       'retention_time': 76,
                       'topics': [{'topic': 'topicname', 'partitions': [{'partition': 4, 'offset': 2, 'metadata': None}]},
                                  {'topic': 'nexttopic', 'partitions': [{'partition': 9, 'offset': 3, 'metadata': None}]}]}

    def test_process_arguments_notenough(self):
        self.assertRaises(ArgumentError, OffsetCommitV2Request.process_arguments, ['groupname', '16', 'membername', '76', 'topicname'])

    def test_process_arguments_nonnumeric(self):
        self.assertRaises(ArgumentError, OffsetCommitV2Request.process_arguments, ['groupname', 'foo', 'membername', '76', 'topicname', '4,2'])
        self.assertRaises(ArgumentError, OffsetCommitV2Request.process_arguments, ['groupname', '16', 'membername', 'foo', 'topicname', '4,2'])
