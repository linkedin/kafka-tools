import unittest
from mock import patch

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.create_topics_v1 import CreateTopicsV1Request


class CreateTopicsV1RequestTests(unittest.TestCase):
    def setUp(self):
        self.topic = {'topic': 'topicname',
                      'num_partitions': -1,
                      'replication_factor': -1,
                      'replica_assignment': [],
                      'configs': []}

    @patch('kafka.tools.protocol.requests.create_topics_v1._parse_remaining_args')
    def test_process_arguments(self, mock_parse):
        val = CreateTopicsV1Request.process_arguments(['true', '3296', 'topicname,someargs'])
        mock_parse.assert_called_once_with(self.topic, ['someargs'])
        assert val == {'timeout': 3296, 'topics': [self.topic], 'validate_only': True}

    def test_process_arguments_nodryrun(self):
        self.assertRaises(ArgumentError, CreateTopicsV1Request.process_arguments, ['3296', 'topicname,someargs', 'anothertopic,moreargs'])

    def test_process_arguments_notimeout(self):
        self.assertRaises(ArgumentError, CreateTopicsV1Request.process_arguments, ['true', 'topicname,someargs', 'anothertopic,moreargs'])

    def test_process_arguments_notopic(self):
        self.assertRaises(ArgumentError, CreateTopicsV1Request.process_arguments, ['true', '3296'])
