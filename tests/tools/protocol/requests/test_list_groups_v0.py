import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.list_groups_v0 import ListGroupsV0Request


class ListGroupsV0RequestTests(unittest.TestCase):
    def test_process_arguments(self):
        assert ListGroupsV0Request.process_arguments([]) == {}

    def test_process_arguments_extra(self):
        self.assertRaises(ArgumentError, ListGroupsV0Request.process_arguments, ['foo'])

    def test_schema(self):
        validate_schema(ListGroupsV0Request.schema)
