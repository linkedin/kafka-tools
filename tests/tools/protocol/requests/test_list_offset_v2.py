import unittest

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.list_offset_v2 import ListOffsetV2Request


class ListOffsetV1RequestTest(unittest.TestCase):
    def test_process_arguments(self):
        val = ListOffsetV2Request.process_arguments(['-1', 'false', 'topicname', '4,2', 'nexttopic', '9,3'])
        assert val == {'replica_id': -1,
                       'isolation_level': 0,
                       'topics': [{'topic': 'topicname', 'partitions': [{'partition': 4, 'timestamp': 2}]},
                                  {'topic': 'nexttopic', 'partitions': [{'partition': 9, 'timestamp': 3}]}]}

    def test_process_arguments_nonnumeric(self):
        self.assertRaises(ArgumentError, ListOffsetV2Request.process_arguments, ['foo', 'true', 'topicname', '4,2'])

    def test_process_arguments_nonbool(self):
        self.assertRaises(ArgumentError, ListOffsetV2Request.process_arguments, ['-1', 'notboolean', 'topicname', '4,2'])

    def test_process_arguments_notenough(self):
        self.assertRaises(ArgumentError, ListOffsetV2Request.process_arguments, ['-1', 'true', 'topicname'])
