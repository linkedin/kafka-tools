import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.leave_group_v0 import LeaveGroupV0Request


class LeaveGroupV0RequestTests(unittest.TestCase):
    def test_process_arguments(self):
        val = LeaveGroupV0Request.process_arguments(['groupname', 'membername'])
        assert val == {'group_id': 'groupname', 'member_id': 'membername'}

    def test_process_arguments_missing(self):
        self.assertRaises(ArgumentError, LeaveGroupV0Request.process_arguments, [])

    def test_process_arguments_three(self):
        self.assertRaises(ArgumentError, LeaveGroupV0Request.process_arguments, ['groupname', 'membername', 'extraarg'])

    def test_schema(self):
        validate_schema(LeaveGroupV0Request.schema)
