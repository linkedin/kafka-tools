import sys
import unittest

from mock import patch
from argparse import Namespace
from ..fixtures import set_up_cluster, set_up_subparser

from kafka.tools.assigner.actions.balance import ActionBalance
from kafka.tools.assigner.actions.reorder import ActionReorder
from kafka.tools.assigner.actions.balancemodules.leader import ActionBalanceLeader


class ActionBalanceLeaderTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster()
        (self.parser, self.subparsers) = set_up_subparser()
        self.args = Namespace()

    def test_configure_args(self):
        ActionBalance.configure_args(self.subparsers)
        sys.argv = ['kafka-assigner', 'balance', '-t', 'leader']
        parsed_args = self.parser.parse_args()
        assert parsed_args.action == 'balance'

    def test_create_class(self):
        action = ActionBalanceLeader(self.args, self.cluster)
        assert isinstance(action, ActionBalanceLeader)
        assert isinstance(action._reorder, ActionReorder)

    @patch.object(ActionReorder, 'process_cluster')
    def test_process_cluster(self, mock_reorder):
        self.args.types = ['leader']
        action = ActionBalanceLeader(self.args, self.cluster)
        action.process_cluster()
        mock_reorder.assert_called_once_with()
