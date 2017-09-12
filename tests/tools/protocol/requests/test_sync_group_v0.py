import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.sync_group_v0 import SyncGroupV0Request


class SyncGroupV0RequestTests(unittest.TestCase):
    def test_process_arguments(self):
        val = SyncGroupV0Request.process_arguments(['groupname', '53', 'membername', '86723ba0cf'])
        print(val)
        assert val == {'group_id': 'groupname',
                       'generation_id': 53,
                       'member_id': 'membername',
                       'member_assignments': b'\x86\x72\x3b\xa0\xcf'}

    def test_process_arguments_missing(self):
        self.assertRaises(ArgumentError, SyncGroupV0Request.process_arguments, [])

    def test_process_arguments_notenough(self):
        self.assertRaises(ArgumentError, SyncGroupV0Request.process_arguments, ['groupname', '53', 'membername'])

    def test_process_arguments_nonnumeric(self):
        self.assertRaises(ArgumentError, SyncGroupV0Request.process_arguments, ['groupname', 'foo', 'membername', '86723ba0cf'])

    def test_process_arguments_nonhex(self):
        self.assertRaises(ArgumentError, SyncGroupV0Request.process_arguments, ['groupname', '53', 'membername', 'not_a_hex_string'])

    def test_schema(self):
        validate_schema(SyncGroupV0Request.schema)
