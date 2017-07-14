import argparse
import inspect
import sys
import unittest

from contextlib import contextmanager

import kafka.tools.assigner.actions

from kafka.tools.assigner.arguments import set_up_arguments, CSVAction
from kafka.tools.modules import get_modules
from kafka.tools.assigner.plugins import PluginModule


@contextmanager
def redirect_err_output():
    current_err = sys.stderr
    try:
        sys.stderr = sys.stdout
        yield
    finally:
        sys.stderr = current_err


class ArgumentTests(unittest.TestCase):
    def setUp(self):
        self.null_plugin = PluginModule()

    def create_action_map(self):
        self.action_map = dict((cls.name, cls) for cls in get_modules(kafka.tools.assigner.actions, kafka.tools.assigner.actions.ActionModule))

    def test_get_arguments_none(self):
        sys.argv = ['kafka-assigner']
        with redirect_err_output():
            self.assertRaises(SystemExit, set_up_arguments, {}, {}, [self.null_plugin])

    def test_get_modules(self):
        self.create_action_map()
        assert 'elect' in self.action_map
        assert inspect.isclass(self.action_map['elect'])

    def test_get_arguments_minimum(self):
        self.create_action_map()
        sys.argv = ['kafka-assigner', '--zookeeper', 'zkhost1.example.com:2181', 'elect']
        args = set_up_arguments(self.action_map, {}, [self.null_plugin])
        assert args.action == 'elect'
        assert args.zookeeper == 'zkhost1.example.com:2181'
        assert args.leadership is False
        assert args.generate is False
        assert args.execute is False
        assert args.property == []
        assert args.size is False
        assert args.skip_ple is False
        assert args.sizer == 'ssh'
        assert args.moves == 10
        assert args.ple_size == 2000
        assert args.ple_wait == 120

    def test_csv_action_single(self):
        aparser = argparse.ArgumentParser()
        aparser.add_argument('--foo', action=CSVAction)
        args = aparser.parse_args(['--foo', '1'])
        assert args.foo == ['1']

    def test_csv_action_nargs(self):
        aparser = argparse.ArgumentParser()
        self.assertRaises(ValueError, aparser.add_argument, '--foo', action=CSVAction, nargs='*')

    def test_csv_action_two_args(self):
        aparser = argparse.ArgumentParser()
        aparser.add_argument('--foo', action=CSVAction)
        args = aparser.parse_args(['--foo', '1', '--foo', '2'])
        assert args.foo == ['1', '2']

    def test_csv_action_commas(self):
        aparser = argparse.ArgumentParser()
        aparser.add_argument('--foo', action=CSVAction)
        args = aparser.parse_args(['--foo', '1,2'])
        assert args.foo == ['1', '2']
