import argparse
import timeout_decorator
import unittest
from mock import patch, MagicMock

from kafka.tools.models.broker import Broker
from kafka.tools.protocol.__main__ import _get_request_classes, _get_request_commands, _print_errors, _parse_command, _cli_loop, main


class TestKlass1:
    cmd = "TestKlass"
    api_version = 1


class TestKlass2:
    cmd = "TestKlass"
    api_version = 2


class TestKlassy1:
    cmd = "TestKlassy"
    api_version = 1


class MainTests(unittest.TestCase):
    def setUp(self):
        self.broker = Broker('testhost', port=3945)
        self.request_classes = {'testklass': {1: TestKlass1, 2: TestKlass2}, 'testklassy': {1: TestKlassy1}}
        self.request_cmds = {'testklass': TestKlass2,
                             'testklassv1': TestKlass1,
                             'testklassv2': TestKlass2,
                             'testklassy': TestKlassy1,
                             'testklassyv1': TestKlassy1}

    @patch('kafka.tools.protocol.__main__.get_modules')
    def test_get_request_classes(self, mock_modules):
        mock_modules.return_value = [TestKlass1, TestKlass2, TestKlassy1]
        val = _get_request_classes()
        assert val == self.request_classes

    def test_get_request_commands(self):
        val = _get_request_commands({'testklass': {1: TestKlass1, 2: TestKlass2}, 'testklassy': {1: TestKlassy1}})
        print(val)
        assert val == self.request_cmds

    def test_print_errors(self):
        # This just outputs to the console, so just make sure it doesn't fail
        _print_errors()
        assert True

    def test_parse_command_exit(self):
        self.assertRaises(EOFError, _parse_command, self.broker, self.request_classes, self.request_cmds, 'exit', [])
        self.assertRaises(EOFError, _parse_command, self.broker, self.request_classes, self.request_cmds, 'quit', [])
        self.assertRaises(EOFError, _parse_command, self.broker, self.request_classes, self.request_cmds, 'q', [])

    @patch('kafka.tools.protocol.__main__.show_help')
    def test_parse_command_help(self, mock_help):
        _parse_command(self.broker, self.request_classes, self.request_cmds, 'help', [])
        mock_help.assert_called_once_with(self.request_classes, self.request_cmds, [])

    @patch('kafka.tools.protocol.__main__.show_help')
    def test_parse_command_help_pass_args(self, mock_help):
        _parse_command(self.broker, self.request_classes, self.request_cmds, 'help', ['foo'])
        mock_help.assert_called_once_with(self.request_classes, self.request_cmds, ['foo'])

    @patch('kafka.tools.protocol.__main__._print_errors')
    def test_parse_command_errors(self, mock_errors):
        _parse_command(self.broker, self.request_classes, self.request_cmds, 'errors', [])
        mock_errors.assert_called_once_with()

    def test_parse_command_unknown(self):
        # Should just not raise an error, as it prints out
        _parse_command(self.broker, self.request_classes, self.request_cmds, 'unknown_command', [])
        assert True

    def test_parse_command_request(self):
        mock_klass = MagicMock()
        mock_broker = MagicMock()
        self.request_cmds['testklass'] = mock_klass
        self.broker = mock_broker

        mock_klass.process_arguments.return_value = 'fake_request_dict'
        mock_klass.return_value = 'fake_request'
        mock_broker.send.return_value = (8129, 'fake_response')
        _parse_command(self.broker, self.request_classes, self.request_cmds, 'testklass', ['someargs'])

        mock_klass.process_arguments.assert_called_once_with(['someargs'])
        mock_klass.assert_called_once_with('fake_request_dict')
        mock_broker.send.assert_called_once_with('fake_request')

    @patch('kafka.tools.protocol.__main__._get_request_classes')
    @patch('kafka.tools.protocol.__main__._get_request_commands')
    @patch('kafka.tools.protocol.__main__._parse_command')
    @patch('kafka.tools.protocol.__main__.input')
    @timeout_decorator.timeout(5)
    def test_cli_loop_command(self, mock_input, mock_parse, mock_commands, mock_classes):
        mock_classes.return_value = self.request_classes
        mock_commands.return_value = self.request_cmds
        mock_input.side_effect = ['testcommand arg1 arg2', EOFError]

        # This will loop twice. First loop should call _parse_command, making sure we pass the args
        # Second loop triggers exit
        _cli_loop(self.broker)
        mock_parse.assert_called_once_with(self.broker, self.request_classes, self.request_cmds, 'testcommand', ['arg1', 'arg2'])

    @patch('kafka.tools.protocol.__main__._get_request_classes')
    @patch('kafka.tools.protocol.__main__._get_request_commands')
    @patch('kafka.tools.protocol.__main__.input')
    @timeout_decorator.timeout(5)
    def test_cli_loop_exit(self, mock_input, mock_commands, mock_classes):
        mock_classes.return_value = self.request_classes
        mock_commands.return_value = self.request_cmds
        mock_input.return_value = 'exit'

        # This will should just exit the loop after 1 run
        _cli_loop(self.broker)

    @patch('kafka.tools.protocol.__main__._get_request_classes')
    @patch('kafka.tools.protocol.__main__._get_request_commands')
    @patch('kafka.tools.protocol.__main__.input')
    @timeout_decorator.timeout(5)
    def test_cli_loop_empty(self, mock_input, mock_commands, mock_classes):
        mock_classes.return_value = self.request_classes
        mock_commands.return_value = self.request_cmds
        mock_input.side_effect = ['', EOFError]

        # This should loop twice. The first loop is the test (empty input), the second triggers an exit of the loop
        _cli_loop(self.broker)

    @patch('kafka.tools.protocol.__main__._cli_loop')
    @patch('kafka.tools.protocol.__main__.set_up_arguments')
    @patch('kafka.tools.protocol.__main__.Broker')
    def test_main(self, mock_broker, mock_args, mock_cli_loop):
        mock_args.return_value = argparse.Namespace(broker='testhost', port=9328)
        mock_broker_inst = MagicMock()
        mock_broker.return_value = mock_broker_inst
        rv = main()
        assert rv == 0

        mock_args.assert_called_once_with()
        mock_broker.assert_called_once_with('testhost', port=9328)
        mock_broker_inst.connect.assert_called_once_with()
        mock_cli_loop.assert_called_once_with(mock_broker_inst)
        mock_broker_inst.close.assert_called_once_with()
