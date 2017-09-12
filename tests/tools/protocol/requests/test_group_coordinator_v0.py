import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.group_coordinator_v0 import GroupCoordinatorV0Request


class GroupCoordinatorV0RequestTests(unittest.TestCase):
    def test_process_arguments(self):
        val = GroupCoordinatorV0Request.process_arguments(['groupname'])
        assert val == {'group_id': 'groupname'}

    def test_process_arguments_missing(self):
        self.assertRaises(ArgumentError, GroupCoordinatorV0Request.process_arguments, [])

    def test_process_arguments_two(self):
        self.assertRaises(ArgumentError, GroupCoordinatorV0Request.process_arguments, ['groupname', 'anothergroup'])

    def test_schema(self):
        validate_schema(GroupCoordinatorV0Request.schema)
