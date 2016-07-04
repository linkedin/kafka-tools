import sys
import unittest

from mock import patch
from argparse import Namespace
from .fixtures import set_up_cluster, set_up_subparser

from kafka.tools.assigner.actions.balance import ActionBalance
from kafka.tools.assigner.actions.balancemodules.count import ActionBalanceCount


class ActionBalanceTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster()
        (self.parser, self.subparsers) = set_up_subparser()
        self.args = Namespace()

    def test_create_class(self):
        self.args.types = ['count']
        action = ActionBalance(self.args, self.cluster)
        assert isinstance(action, ActionBalance)

    def test_configure_args(self):
        ActionBalance.configure_args(self.subparsers)
        sys.argv = ['kafka-assigner', 'balance', '-t', 'count']
        parsed_args = self.parser.parse_args()
        assert parsed_args.action == 'balance'

    @patch.object(ActionBalanceCount, 'process_cluster')
    def test_process_cluster(self, mock_process):
        self.args.types = ['count']
        action = ActionBalance(self.args, self.cluster)
        action.process_cluster()
        mock_process.assert_called_once_with()
