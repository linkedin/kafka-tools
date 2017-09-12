import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.stop_replica_v0 import _parse_partition, StopReplicaV0Request


class StopReplicaV0RequestTests(unittest.TestCase):
    def test_parse_partition(self):
        val = _parse_partition('topicname,3')
        assert val == {'topic': 'topicname', 'partition': 3}

    def test_parse_partition_nocomma(self):
        self.assertRaises(ArgumentError, _parse_partition, "topicname")

    def test_parse_partition_twocomma(self):
        self.assertRaises(ArgumentError, _parse_partition, "topicname,3,foo")

    def test_parse_partition_nonnumeric(self):
        self.assertRaises(ArgumentError, _parse_partition, "topicname,foo")

    def test_process_arguments(self):
        val = StopReplicaV0Request.process_arguments(['3', '53', 'true', 'topicname,3', 'anothertopic,7'])
        assert val == {'controller_id': 3,
                       'controller_epoch': 53,
                       'delete_partitions': True,
                       'partitions': [{'topic': 'topicname', 'partition': 3},
                                      {'topic': 'anothertopic', 'partition': 7}]}

    def test_process_arguments_false(self):
        val = StopReplicaV0Request.process_arguments(['3', '53', 'no', 'topicname,3', 'anothertopic,7'])
        assert val == {'controller_id': 3,
                       'controller_epoch': 53,
                       'delete_partitions': False,
                       'partitions': [{'topic': 'topicname', 'partition': 3},
                                      {'topic': 'anothertopic', 'partition': 7}]}

    def test_process_arguments_missing(self):
        self.assertRaises(ArgumentError, StopReplicaV0Request.process_arguments, [])

    def test_process_arguments_notenough(self):
        self.assertRaises(ArgumentError, StopReplicaV0Request.process_arguments, ['3', '53', 'no'])

    def test_process_arguments_nonnumeric(self):
        self.assertRaises(ArgumentError, StopReplicaV0Request.process_arguments, ['foo', '53', 'no', 'topicname,3'])
        self.assertRaises(ArgumentError, StopReplicaV0Request.process_arguments, ['3', 'foo', 'no', 'topicname,3'])

    def test_schema(self):
        validate_schema(StopReplicaV0Request.schema)
