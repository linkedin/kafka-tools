import inspect
import sys
import unittest
from contextlib import contextmanager
from StringIO import StringIO

import kafka.tools.assigner.actions
from kafka.tools.assigner.arguments import set_up_arguments
from kafka.tools.assigner.modules import get_modules
from kafka.tools.assigner.plugins import PluginModule


@contextmanager
def capture_sys_output():
    capture_out, capture_err = StringIO(), StringIO()
    current_out, current_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = capture_out, capture_err
        yield capture_out, capture_err
    finally:
        sys.stdout, sys.stderr = current_out, current_err


class ArgumentTests(unittest.TestCase):
    def setUp(self):
        self.null_plugin = PluginModule()

    def create_action_map(self):
        self.action_map = {cls.name: cls for cls in get_modules(kafka.tools.assigner.actions, kafka.tools.assigner.actions.ActionModule)}

    def test_get_arguments_none(self):
        sys.argv = ['kafka-assigner']
        with self.assertRaises(SystemExit):
            with capture_sys_output() as (stdout, stderr):
                set_up_arguments({}, {}, [self.null_plugin])

    def test_get_modules(self):
        self.create_action_map()
        assert 'elect' in self.action_map
        assert inspect.isclass(self.action_map['elect'])

    def test_get_arguments_minimum(self):
        self.create_action_map()
        sys.argv = ['kafka-assigner', '--zookeeper', 'zkhost1.example.com:2181', 'elect']
        args = set_up_arguments(self.action_map, {}, [self.null_plugin])
        assert args.action == 'elect'
