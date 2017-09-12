import unittest
from mock import patch
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.join_group_v0 import _parse_group_protocol, JoinGroupV0Request


class JoinGroupV0RequestTests(unittest.TestCase):
    def test_parse_group_protocol(self):
        val = _parse_group_protocol('protocoltype,891234abdb')
        assert val == {'protocol_name': 'protocoltype', 'protocol_metadata': b'\x89\x12\x34\xab\xdb'}

    def test_parse_group_protocol_blank_metadata(self):
        val = _parse_group_protocol('protocoltype,')
        assert val == {'protocol_name': 'protocoltype', 'protocol_metadata': None}

    def test_parse_group_protocol_nocomma(self):
        self.assertRaises(ArgumentError, _parse_group_protocol, "foo")

    def test_parse_group_protocol_twocomma(self):
        self.assertRaises(ArgumentError, _parse_group_protocol, "foo,bar,baz")

    def test_parse_group_protocol_nonhex(self):
        self.assertRaises(ArgumentError, _parse_group_protocol, "protocoltype,notahexstring")

    @patch('kafka.tools.protocol.requests.join_group_v0._parse_group_protocol')
    def test_process_arguments(self, mock_parse):
        mock_parse.return_value = {'protocol_name': 'protocoltype', 'protocol_metadata': b'\x89\x12\x34\xab\xdb'}
        val = JoinGroupV0Request.process_arguments(['groupname', '3296', 'membername', 'protocolname', 'somegroupprotocols'])

        mock_parse.assert_called_once_with('somegroupprotocols')
        assert val == {'group_id': 'groupname',
                       'session_timeout': 3296,
                       'member_id': 'membername',
                       'protocol_type': 'protocolname',
                       'group_protocols': [{'protocol_name': 'protocoltype', 'protocol_metadata': b'\x89\x12\x34\xab\xdb'}]}

    def test_process_arguments_missing(self):
        self.assertRaises(ArgumentError, JoinGroupV0Request.process_arguments, [])

    def test_process_arguments_notenough(self):
        self.assertRaises(ArgumentError, JoinGroupV0Request.process_arguments, ['foo', '23412', 'bar'])

    def test_process_arguments_nonnumeric(self):
        self.assertRaises(ArgumentError, JoinGroupV0Request.process_arguments, ['groupname', 'foo', 'membername', 'protocolname', 'somegroupprotocols'])

    def test_schema(self):
        validate_schema(JoinGroupV0Request.schema)
