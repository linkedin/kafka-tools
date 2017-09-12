import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.offset_commit_v0 import OffsetCommitV0Request


class OffsetCommitV0RequestTests(unittest.TestCase):
    def test_process_arguments(self):
        val = OffsetCommitV0Request.process_arguments(['groupname', 'topicname', '4,2,somemetadata', 'nexttopic', '9,3,moremetadata'])
        assert val == {'group_id': 'groupname',
                       'topics': [{'topic': 'topicname', 'partitions': [{'partition': 4, 'offset': 2, 'metadata': 'somemetadata'}]},
                                  {'topic': 'nexttopic', 'partitions': [{'partition': 9, 'offset': 3, 'metadata': 'moremetadata'}]}]}

    def test_process_arguments_notenough(self):
        self.assertRaises(ArgumentError, OffsetCommitV0Request.process_arguments, ['groupname', 'topicname'])

    def test_schema(self):
        validate_schema(OffsetCommitV0Request.schema)
