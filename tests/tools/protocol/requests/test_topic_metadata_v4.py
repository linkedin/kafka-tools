import unittest

from kafka.tools.protocol.requests.topic_metadata_v4 import TopicMetadataV4Request


class TopicMetadataV1RequestTest(unittest.TestCase):
    def test_process_arguments(self):
        val = TopicMetadataV4Request.process_arguments(['topicname', 'anothertopic'])
        assert val == {'topics': ['topicname', 'anothertopic'], 'allow_auto_topic_creation': False}

    def test_process_arguments_all(self):
        val = TopicMetadataV4Request.process_arguments([])
        assert val == {'topics': None, 'allow_auto_topic_creation': False}
