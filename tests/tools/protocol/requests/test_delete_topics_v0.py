import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.delete_topics_v0 import DeleteTopicsV0Request


class DeleteTopicsV0RequestTests(unittest.TestCase):
    def test_process_arguments(self):
        val = DeleteTopicsV0Request.process_arguments(['3296', 'topicname', 'anothertopic'])
        assert val == {'timeout': 3296, 'topics': ['topicname', 'anothertopic']}

    def test_process_arguments_notimeout(self):
        self.assertRaises(ArgumentError, DeleteTopicsV0Request.process_arguments, ['topicname', 'anothertopic'])

    def test_process_arguments_notopic(self):
        self.assertRaises(ArgumentError, DeleteTopicsV0Request.process_arguments, ['3296'])

    def test_schema(self):
        validate_schema(DeleteTopicsV0Request.schema)
