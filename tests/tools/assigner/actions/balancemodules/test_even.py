import sys
import unittest

from argparse import Namespace
from ..fixtures import set_up_cluster, set_up_subparser

from kafka.tools.assigner.models.topic import Topic
from kafka.tools.assigner.actions.balance import ActionBalance
from kafka.tools.assigner.actions.balancemodules.even import ActionBalanceEven


class ActionBalanceEvenTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster()
        (self.parser, self.subparsers) = set_up_subparser()
        self.args = Namespace()

    def test_configure_args(self):
        ActionBalance.configure_args(self.subparsers)
        sys.argv = ['kafka-assigner', 'balance', '-t', 'even']
        parsed_args = self.parser.parse_args()
        assert parsed_args.action == 'balance'

    def test_create_class(self):
        action = ActionBalanceEven(self.args, self.cluster)
        assert isinstance(action, ActionBalanceEven)

    def test_process_cluster_no_change(self):
        action = ActionBalanceEven(self.args, self.cluster)
        action.process_cluster()

        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [b1, b2]
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[1].replicas == [b1, b2]

    def test_process_cluster_one_move(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        self.cluster.topics['testTopic1'].partitions[0].swap_replica_positions(b1, b2)

        action = ActionBalanceEven(self.args, self.cluster)
        action.process_cluster()

        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [b1, b2]
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[1].replicas == [b1, b2]

    def test_process_cluster_odd_count(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        self.cluster.add_topic(Topic("testTopic3", 3))
        partition = self.cluster.topics['testTopic3'].partitions[0]
        partition.add_replica(b1, 0)
        partition.add_replica(b2, 1)
        partition = self.cluster.topics['testTopic3'].partitions[1]
        partition.add_replica(b1, 0)
        partition.add_replica(b2, 1)
        partition = self.cluster.topics['testTopic3'].partitions[2]
        partition.add_replica(b1, 0)
        partition.add_replica(b2, 1)
        self.cluster.topics['testTopic1'].partitions[0].swap_replica_positions(b1, b2)

        action = ActionBalanceEven(self.args, self.cluster)
        action.process_cluster()

        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [b1, b2]
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[1].replicas == [b1, b2]
        assert self.cluster.topics['testTopic3'].partitions[0].replicas == [b1, b2]
        assert self.cluster.topics['testTopic3'].partitions[1].replicas == [b1, b2]
        assert self.cluster.topics['testTopic3'].partitions[2].replicas == [b1, b2]

    def test_process_cluster_different_rf(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        self.cluster.add_topic(Topic("testTopic3", 2))
        partition = self.cluster.topics['testTopic3'].partitions[0]
        partition.add_replica(b1, 0)
        partition.add_replica(b2, 1)
        partition = self.cluster.topics['testTopic3'].partitions[1]
        partition.add_replica(b1, 0)
        self.cluster.topics['testTopic1'].partitions[0].swap_replica_positions(b1, b2)

        action = ActionBalanceEven(self.args, self.cluster)
        action.process_cluster()

        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [b1, b2]
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[1].replicas == [b1, b2]
        assert self.cluster.topics['testTopic3'].partitions[0].replicas == [b1, b2]
        assert self.cluster.topics['testTopic3'].partitions[1].replicas == [b1]
