import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests.topic_metadata_v1 import TopicMetadataV1Request


class TopicMetadataV1RequestTest(unittest.TestCase):
    def test_process_arguments(self):
        val = TopicMetadataV1Request.process_arguments(['topicname', 'anothertopic'])
        assert val == {'topics': ['topicname', 'anothertopic']}

    def test_process_arguments_all(self):
        val = TopicMetadataV1Request.process_arguments([])
        assert val == {'topics': None}

    def test_schema(self):
        validate_schema(TopicMetadataV1Request.schema)
