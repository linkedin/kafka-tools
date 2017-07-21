import unittest

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.offset_commit_v0 import _parse_next_topic, OffsetCommitV0Request


class OffsetCommitV0RequestTests(unittest.TestCase):
    def test_parse_next_topic(self):
        val, rest = _parse_next_topic(['topicname', '4,2,othermetadata'])
        assert val == {'topic': 'topicname', 'partitions': [{'partition': 4, 'offset': 2, 'metadata': 'othermetadata'}]}
        assert rest == []

    def test_parse_next_topic_multiple(self):
        val, rest = _parse_next_topic(['topicname', '4,2,somemetadata', '9,3,moremetadata'])
        assert val == {'topic': 'topicname', 'partitions': [{'partition': 4, 'offset': 2, 'metadata': 'somemetadata'},
                       {'partition': 9, 'offset': 3, 'metadata': 'moremetadata'}]}
        assert rest == []

    def test_parse_next_topic_remainder(self):
        val, rest = _parse_next_topic(['topicname', '4,2,somemetadata', 'nexttopic', '9,3,moremetadata'])
        assert val == {'topic': 'topicname', 'partitions': [{'partition': 4, 'offset': 2, 'metadata': 'somemetadata'}]}
        assert rest == ['nexttopic', '9,3,moremetadata']

    def test_parse_next_topic_nopartitions(self):
        self.assertRaises(ArgumentError, _parse_next_topic, ['topicname'])

    def test_parse_next_topic_short(self):
        self.assertRaises(ArgumentError, _parse_next_topic, ['topicname', '4,2'])

    def test_parse_next_topic_extra(self):
        self.assertRaises(ArgumentError, _parse_next_topic, ['topicname', '4,2,somemetadata,foo'])

    def test_parse_next_topic_nonnumeric(self):
        self.assertRaises(ArgumentError, _parse_next_topic, ['topicname', 'foo,2,somemetadata'])
        self.assertRaises(ArgumentError, _parse_next_topic, ['topicname', '4,foo,somemetadata'])

    def test_process_arguments(self):
        val = OffsetCommitV0Request.process_arguments(['groupname', 'topicname', '4,2,somemetadata', 'nexttopic', '9,3,moremetadata'])
        assert val == {'group_id': 'groupname',
                       'topics': [{'topic': 'topicname', 'partitions': [{'partition': 4, 'offset': 2, 'metadata': 'somemetadata'}]},
                                  {'topic': 'nexttopic', 'partitions': [{'partition': 9, 'offset': 3, 'metadata': 'moremetadata'}]}]}

    def test_process_arguments_notenough(self):
        self.assertRaises(ArgumentError, OffsetCommitV0Request.process_arguments, ['groupname', 'topicname'])
