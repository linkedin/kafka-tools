import unittest

from tests.tools.client.fixtures import topic_metadata_v0


class MetadataV0ResponseTest(unittest.TestCase):
    def setUp(self):
        self.metadata = topic_metadata_v0()

    def test_topic_names(self):
        val = self.metadata.topic_names()
        assert val == ['topic1']

    def test_broker_ids(self):
        val = self.metadata.broker_ids()
        assert val == [1, 101]
