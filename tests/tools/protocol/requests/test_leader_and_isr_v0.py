import unittest
from mock import patch
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.leader_and_isr_v0 import _parse_argument, _process_arguments, LeaderAndIsrV0Request


class LeaderAndIsrV0RequestTests(unittest.TestCase):
    def setUp(self):
        self.values = {'controller_id': 5,
                       'controller_epoch': 42,
                       'partition_states': [],
                       'live_leaders': []}
        self.target = {'controller_id': 5,
                       'controller_epoch': 42,
                       'partition_states': [],
                       'live_leaders': []}

    def test_parse_argument_state(self):
        _parse_argument(self.values, 'topicname,3,53,2,64,6|7,23,8|9')

        self.target['partition_states'] = [{'topic': 'topicname',
                                            'partition': 3,
                                            'controller_epoch': 53,
                                            'leader': 2,
                                            'leader_epoch': 64,
                                            'isr': [6, 7],
                                            'zk_version': 23,
                                            'replicas': [8, 9]}]
        assert self.values == self.target

    def test_parse_argument_state_nonnumeric(self):
        self.assertRaises(ArgumentError, _parse_argument, self.values, 'topicname,foo,53,2,64,6|7,23,8|9')
        self.assertRaises(ArgumentError, _parse_argument, self.values, 'topicname,3,foo,2,64,6|7,23,8|9')
        self.assertRaises(ArgumentError, _parse_argument, self.values, 'topicname,3,53,foo,64,6|7,23,8|9')
        self.assertRaises(ArgumentError, _parse_argument, self.values, 'topicname,3,53,2,foo,6|7,23,8|9')
        self.assertRaises(ArgumentError, _parse_argument, self.values, 'topicname,3,53,2,64,foo|7,23,8|9')
        self.assertRaises(ArgumentError, _parse_argument, self.values, 'topicname,3,53,2,64,6|7,foo,8|9')
        self.assertRaises(ArgumentError, _parse_argument, self.values, 'topicname,3,53,2,64,6|7,23,foo|9')

    def test_parse_argument_live(self):
        _parse_argument(self.values, '3,examplehost,4398')

        self.target['live_leaders'] = [{'id': 3,
                                        'host': 'examplehost',
                                        'port': 4398}]
        assert self.values == self.target

    def test_parse_argument_live_nonnumeric(self):
        self.assertRaises(ArgumentError, _parse_argument, self.values, 'foo,examplehost,4398')
        self.assertRaises(ArgumentError, _parse_argument, self.values, '3,examplehost,foo')

    def test_parse_argument_notenough(self):
        self.assertRaises(ArgumentError, _parse_argument, self.values, '3,examplehost')

    def test_parse_argument_toomany(self):
        self.assertRaises(ArgumentError, _parse_argument, self.values, 'topicname,foo,53,2,64,6|7,23,8|9,foo')

    def test_process_arguments(self):
        val = _process_arguments("LeaderAndIsrV0", ['5', '42', 'topicname,3,53,2,64,6|7,23,8|9', '3,examplehost,4398'])

        self.target['partition_states'] = [{'topic': 'topicname',
                                            'partition': 3,
                                            'controller_epoch': 53,
                                            'leader': 2,
                                            'leader_epoch': 64,
                                            'isr': [6, 7],
                                            'zk_version': 23,
                                            'replicas': [8, 9]}]
        self.target['live_leaders'] = [{'id': 3,
                                        'host': 'examplehost',
                                        'port': 4398}]
        assert val == self.target

    def test_process_arguments_nonnumeric(self):
        self.assertRaises(ArgumentError, _process_arguments, "LeaderAndIsrV0", ['foo', '42', 'topicname,3,53,2,64,6|7,23,8|9', '3,examplehost,4398'])
        self.assertRaises(ArgumentError, _process_arguments, "LeaderAndIsrV0", ['5', 'foo', 'topicname,3,53,2,64,6|7,23,8|9', '3,examplehost,4398'])

    def test_process_arguments_notenough(self):
        self.assertRaises(ArgumentError, _process_arguments, "LeaderAndIsrV0", ['foo', '42', 'topicname,3,53,2,64,6|7,23,8|9'])

    @patch('kafka.tools.protocol.requests.leader_and_isr_v0._process_arguments')
    def test_process_arguments_class(self, mock_process):
        mock_process.return_value = 'fake_return_value'
        val = LeaderAndIsrV0Request.process_arguments(['fake_arguments'])
        mock_process.assert_called_once_with("LeaderAndIsrV0", ['fake_arguments'])
        assert val == 'fake_return_value'

    def test_schema(self):
        validate_schema(LeaderAndIsrV0Request.schema)
