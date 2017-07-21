import unittest

from kafka.tools.protocol.requests.topic_metadata_v0 import TopicMetadataV0Request


class TopicMetadataV0RequestTest(unittest.TestCase):
    def test_process_arguments(self):
        val = TopicMetadataV0Request.process_arguments(['topicname', 'anothertopic'])
        assert val == {'topics': ['topicname', 'anothertopic']}

    def test_process_arguments_all(self):
        val = TopicMetadataV0Request.process_arguments([])
        assert val == {'topics': None}
