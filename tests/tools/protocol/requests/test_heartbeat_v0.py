import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.heartbeat_v0 import HeartbeatV0Request


class HeartbeatV0RequestTests(unittest.TestCase):
    def test_process_arguments(self):
        val = HeartbeatV0Request.process_arguments(['groupname', '4', 'membername'])
        assert val == {'group_id': 'groupname', 'group_generation_id': 4, 'member_id': 'membername'}

    def test_process_arguments_missing(self):
        self.assertRaises(ArgumentError, HeartbeatV0Request.process_arguments, [])

    def test_process_arguments_two(self):
        self.assertRaises(ArgumentError, HeartbeatV0Request.process_arguments, ['groupname', 'anothergroup'])

    def test_process_arguments_four(self):
        self.assertRaises(ArgumentError, HeartbeatV0Request.process_arguments, ['groupname', 'foo', 'membername', 'extraarg'])

    def test_process_arguments_nonnumeric(self):
        self.assertRaises(ArgumentError, HeartbeatV0Request.process_arguments, ['groupname', 'foo', 'membername'])

    def test_schema(self):
        validate_schema(HeartbeatV0Request.schema)
