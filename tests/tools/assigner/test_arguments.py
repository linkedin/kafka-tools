import inspect
import sys
import unittest

from contextlib import contextmanager

import kafka.tools.assigner.actions

from kafka.tools.assigner.arguments import set_up_arguments
from kafka.tools.assigner.modules import get_modules
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
