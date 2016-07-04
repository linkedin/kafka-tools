import unittest

from argparse import Namespace
from .fixtures import set_up_cluster

from kafka.tools.assigner.actions import ActionModule, ActionBalanceModule


class ActionModuleTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster()
        self.args = Namespace()

    def test_create_class(self):
        action = ActionModule(self.args, self.cluster)
        assert isinstance(action, ActionModule)

    def test_configure_args(self):
        self.assertRaises(Exception, ActionModule.configure_args, None)

    def test_add_args(self):
        action = ActionModule(self.args, self.cluster)
        action._add_args(None)
        assert True

    def test_process_cluster(self):
        action = ActionModule(self.args, self.cluster)
        action.process_cluster()
        assert True

    def test_create_balance_class(self):
        ActionBalanceModule.configure_args(None)
