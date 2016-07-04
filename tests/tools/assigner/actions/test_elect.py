import sys
import unittest

from argparse import Namespace
from .fixtures import set_up_cluster, set_up_subparser

from kafka.tools.assigner.actions.elect import ActionElect


class ActionElectTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster()
        (self.parser, self.subparsers) = set_up_subparser()
        self.args = Namespace()

    def test_create_class(self):
        action = ActionElect(self.args, self.cluster)
        assert isinstance(action, ActionElect)

    def test_configure_args(self):
        ActionElect.configure_args(self.subparsers)
        sys.argv = ['kafka-assigner', 'elect']
        parsed_args = self.parser.parse_args()
        assert parsed_args.action == 'elect'

    def test_process_cluster(self):
        action = ActionElect(self.args, self.cluster)
        action.process_cluster()
        assert True
