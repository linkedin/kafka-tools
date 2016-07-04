import sys
import unittest

from argparse import Namespace
from .fixtures import set_up_cluster, set_up_subparser

from kafka.tools.assigner.actions.reorder import ActionReorder


class ActionReorderTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster()
        (self.parser, self.subparsers) = set_up_subparser()
        self.args = Namespace()

    def test_create_class(self):
        action = ActionReorder(self.args, self.cluster)
        assert isinstance(action, ActionReorder)

    def test_configure_args(self):
        ActionReorder.configure_args(self.subparsers)
        sys.argv = ['kafka-assigner', 'reorder']
        parsed_args = self.parser.parse_args()
        assert parsed_args.action == 'reorder'

    def test_process_cluster_no_change(self):
        action = ActionReorder(self.args, self.cluster)
        action.process_cluster()

        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [b1, b2]
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[1].replicas == [b1, b2]

    def test_process_cluster(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        self.cluster.topics['testTopic1'].partitions[0].swap_replica_positions(b1, b2)

        action = ActionReorder(self.args, self.cluster)
        action.process_cluster()

        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [b2, b1]
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b1, b2]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[1].replicas == [b1, b2]
