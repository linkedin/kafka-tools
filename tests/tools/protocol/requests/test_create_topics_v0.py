import unittest
from mock import patch
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.create_topics_v0 import _parse_partition, _parse_kv_args, _parse_remaining_args, CreateTopicsV0Request


class CreateTopicsV0RequestTests(unittest.TestCase):
    def setUp(self):
        self.topic = {'topic': 'topicname',
                      'num_partitions': -1,
                      'replication_factor': -1,
                      'replica_assignment': [],
                      'configs': []}
        self.target_topic = {'topic': 'topicname',
                             'num_partitions': -1,
                             'replication_factor': -1,
                             'replica_assignment': [],
                             'configs': []}

    def test_parse_partition(self):
        _parse_partition(self.topic, 1, "3|4")

        self.target_topic['replica_assignment'] = [{'partition_id': 1, 'replicas': [3, 4]}]
        assert self.topic == self.target_topic

    def test_parse_partition_conflict(self):
        self.topic['num_partitions'] = 8
        self.topic['replication_factor'] = 3
        self.assertRaises(ArgumentError, _parse_partition, self.topic, 1, "3|4")

    def test_parse_partition_nonnumeric(self):
        self.assertRaises(ArgumentError, _parse_partition, self.topic, 1, "3|foo")

    def test_parse_kv_args_partition(self):
        _parse_kv_args(self.topic, ["1=3|4"])

        self.target_topic['replica_assignment'] = [{'partition_id': 1, 'replicas': [3, 4]}]
        assert self.topic == self.target_topic

    def test_parse_kv_args_config(self):
        _parse_kv_args(self.topic, ["configkey=configvalue"])

        self.target_topic['configs'] = [{'config_key': 'configkey', 'config_value': 'configvalue'}]
        assert self.topic == self.target_topic

    def test_parse_kv_args_noequals(self):
        self.topic['num_partitions'] = 8
        self.assertRaises(ArgumentError, _parse_kv_args, self.topic, "foo")

    def test_parse_remaining_args_count_and_factor(self):
        _parse_remaining_args(self.topic, ["8", "3", "configkey=configvalue"])

        self.target_topic['num_partitions'] = 8
        self.target_topic['replication_factor'] = 3
        self.target_topic['configs'] = [{'config_key': 'configkey', 'config_value': 'configvalue'}]
        assert self.topic == self.target_topic

    def test_parse_remaining_args_replica_list(self):
        _parse_remaining_args(self.topic, ["1=3|4", "configkey=configvalue"])

        self.target_topic['replica_assignment'] = [{'partition_id': 1, 'replicas': [3, 4]}]
        self.target_topic['configs'] = [{'config_key': 'configkey', 'config_value': 'configvalue'}]
        assert self.topic == self.target_topic

    def test_parse_remaining_args_no_partitions(self):
        self.assertRaises(ArgumentError, _parse_remaining_args, self.topic, ["configkey=configvalue"])

    @patch('kafka.tools.protocol.requests.create_topics_v0._parse_remaining_args')
    def test_process_arguments(self, mock_parse):
        val = CreateTopicsV0Request.process_arguments(['3296', 'topicname,someargs'])
        mock_parse.assert_called_once_with(self.topic, ['someargs'])
        assert val == {'timeout': 3296, 'topics': [self.topic]}

    def test_process_arguments_notimeout(self):
        self.assertRaises(ArgumentError, CreateTopicsV0Request.process_arguments, ['topicname,someargs', 'anothertopic,moreargs'])

    def test_process_arguments_notopic(self):
        self.assertRaises(ArgumentError, CreateTopicsV0Request.process_arguments, ['3296'])

    def test_schema(self):
        validate_schema(CreateTopicsV0Request.schema)
