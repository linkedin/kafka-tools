import sys
import unittest

from argparse import Namespace
from ..fixtures import set_up_cluster, set_up_subparser

from kafka.tools.models.broker import Broker
from kafka.tools.models.topic import Topic
from kafka.tools.assigner.actions.balance import ActionBalance
from kafka.tools.assigner.actions.balancemodules.rate import ActionBalanceRate


class ActionBalanceRateTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster()
        self.cluster.topics['testTopic1'].partitions[0].set_size(1000)
        self.cluster.topics['testTopic1'].partitions[1].set_size(1000)
        self.cluster.topics['testTopic2'].partitions[0].set_size(2000)
        self.cluster.topics['testTopic2'].partitions[1].set_size(2000)

        (self.parser, self.subparsers) = set_up_subparser()
        self.args = Namespace(exclude_topics=[])

    def test_configure_args(self):
        ActionBalance.configure_args(self.subparsers)
        sys.argv = ['kafka-assigner', 'balance', '-t', 'rate']
        parsed_args = self.parser.parse_args()
        assert parsed_args.action == 'balance'

    def test_create_class(self):
        action = ActionBalanceRate(self.args, self.cluster)
        assert isinstance(action, ActionBalanceRate)

    # This is a duplicate of the size test, but we need at least one test to make sure it does work
    def test_process_cluster_one_move(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        self.cluster.topics['testTopic1'].partitions[0].swap_replica_positions(b1, b2)

        action = ActionBalanceRate(self.args, self.cluster)
        action.process_cluster()

        assert sum([p.scaled_size for p in self.cluster.brokers[1].partitions[0]], 0) == 3000
        assert sum([p.scaled_size for p in self.cluster.brokers[1].partitions[1]], 0) == 3000
        assert sum([p.scaled_size for p in self.cluster.brokers[2].partitions[0]], 0) == 3000
        assert sum([p.scaled_size for p in self.cluster.brokers[2].partitions[1]], 0) == 3000

    def test_process_cluster_zero_partitions(self):
        self.cluster.add_broker(Broker("brokerhost3.example.com", id=3))
        self.cluster.brokers[1].rack = "c"
        self.cluster.add_topic(Topic("testTopic3", 1))
        partition = self.cluster.topics['testTopic3'].partitions[0]
        partition.add_replica(self.cluster.brokers[1], 0)
        partition.add_replica(self.cluster.brokers[2], 1)
        partition.add_replica(self.cluster.brokers[3], 2)
        self.cluster.topics['testTopic3'].partitions[0].set_size(2000)

        self.args.exclude_topics = ['testTopic3']
        action = ActionBalanceRate(self.args, self.cluster)
        action.process_cluster()

        assert sum([p.scaled_size for p in self.cluster.brokers[1].partitions[0]], 0) == 3000
        assert sum([p.scaled_size for p in self.cluster.brokers[1].partitions[1]], 0) == 2000
        assert sum([p.scaled_size for p in self.cluster.brokers[2].partitions[0]], 0) == 3000
        assert sum([p.scaled_size for p in self.cluster.brokers[2].partitions[1]], 0) == 4000
        assert sum([p.scaled_size for p in self.cluster.brokers[3].partitions[0]], 0) == 2000
        assert sum([p.scaled_size for p in self.cluster.brokers[3].partitions[1]], 0) == 2000
        assert sum([p.scaled_size for p in self.cluster.brokers[3].partitions[2]], 0) == 2000
