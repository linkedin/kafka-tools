import unittest
from mock import patch

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.join_group_v1 import JoinGroupV1Request


class JoinGroupV0RequestTests(unittest.TestCase):
    @patch('kafka.tools.protocol.requests.join_group_v1._parse_group_protocol')
    def test_process_arguments(self, mock_parse):
        mock_parse.return_value = {'protocol_name': 'protocoltype', 'protocol_metadata': b'\x89\x12\x34\xab\xdb'}
        val = JoinGroupV1Request.process_arguments(['groupname', '3296', '89243', 'membername',
                                                    'protocolname', 'somegroupprotocols'])

        mock_parse.assert_called_once_with('somegroupprotocols')
        assert val == {'group_id': 'groupname',
                       'session_timeout': 3296,
                       'rebalance_timeout': 89243,
                       'member_id': 'membername',
                       'protocol_type': 'protocolname',
                       'group_protocols': [{'protocol_name': 'protocoltype', 'protocol_metadata': b'\x89\x12\x34\xab\xdb'}]}

    def test_process_arguments_missing(self):
        self.assertRaises(ArgumentError, JoinGroupV1Request.process_arguments, [])

    def test_process_arguments_notenough(self):
        self.assertRaises(ArgumentError, JoinGroupV1Request.process_arguments, ['foo', '23412', '89243', 'bar'])

    def test_process_arguments_nonnumeric(self):
        self.assertRaises(ArgumentError, JoinGroupV1Request.process_arguments, ['groupname', 'foo', '89243',
                                                                                'membername', 'protocolname',
                                                                                'somegroupprotocols'])
