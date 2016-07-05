import sys
import unittest

from argparse import Namespace
from ..fixtures import set_up_cluster, set_up_subparser, set_up_cluster_4broker

from kafka.tools.assigner.models.broker import Broker
from kafka.tools.assigner.models.topic import Topic
from kafka.tools.assigner.actions.balance import ActionBalance
from kafka.tools.assigner.actions.balancemodules.count import ActionBalanceCount


class ActionBalanceCountTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster()
        (self.parser, self.subparsers) = set_up_subparser()
        self.args = Namespace()

    def test_configure_args(self):
        ActionBalance.configure_args(self.subparsers)
        sys.argv = ['kafka-assigner', 'balance', '-t', 'count']
        parsed_args = self.parser.parse_args()
        assert parsed_args.action == 'balance'

    def test_create_class(self):
        action = ActionBalanceCount(self.args, self.cluster)
        assert isinstance(action, ActionBalanceCount)

    def test_process_cluster_no_change(self):
        action = ActionBalanceCount(self.args, self.cluster)
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
        self.cluster.add_topic(Topic("testTopic3", 2))
        partition = self.cluster.topics['testTopic3'].partitions[0]
        partition.add_replica(b1, 0)
        partition.add_replica(b2, 1)
        partition = self.cluster.topics['testTopic3'].partitions[1]
        partition.add_replica(b2, 0)
        partition.add_replica(b1, 1)
        self.cluster.topics['testTopic1'].partitions[0].swap_replica_positions(b1, b2)

        action = ActionBalanceCount(self.args, self.cluster)
        action.process_cluster()

        assert len(b1.partitions[0]) == 3
        assert len(b1.partitions[1]) == 3
        assert len(b2.partitions[0]) == 3
        assert len(b2.partitions[1]) == 3

    def test_process_cluster_empty_broker(self):
        self.cluster.add_broker(Broker(3, 'brokerhost3.example.com'))
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        b3 = self.cluster.brokers[3]
        self.cluster.add_topic(Topic("testTopic3", 2))
        partition = self.cluster.topics['testTopic3'].partitions[0]
        partition.add_replica(b1, 0)
        partition.add_replica(b2, 1)
        partition = self.cluster.topics['testTopic3'].partitions[1]
        partition.add_replica(b2, 0)
        partition.add_replica(b1, 1)
        self.cluster.topics['testTopic1'].partitions[0].swap_replica_positions(b1, b2)

        action = ActionBalanceCount(self.args, self.cluster)
        action.process_cluster()

        assert len(b1.partitions[0]) == 2
        assert len(b1.partitions[1]) == 2
        assert len(b2.partitions[0]) == 2
        assert len(b2.partitions[1]) == 2
        assert len(b3.partitions[0]) == 2
        assert len(b3.partitions[1]) == 2

    def test_process_cluster_empty_one(self):
        self.cluster.add_broker(Broker(3, 'brokerhost3.example.com'))
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        b3 = self.cluster.brokers[3]
        self.cluster.add_topic(Topic("testTopic3", 2))
        partition = self.cluster.topics['testTopic3'].partitions[0]
        partition.add_replica(b3, 0)
        partition.add_replica(b2, 1)
        partition = self.cluster.topics['testTopic3'].partitions[1]
        partition.add_replica(b2, 0)
        partition.add_replica(b3, 1)
        self.cluster.topics['testTopic1'].partitions[0].swap_replicas(b1, b3)
        self.cluster.topics['testTopic1'].partitions[1].swap_replicas(b1, b3)
        self.cluster.topics['testTopic2'].partitions[0].swap_replicas(b1, b3)
        self.cluster.topics['testTopic2'].partitions[1].swap_replicas(b1, b3)

        action = ActionBalanceCount(self.args, self.cluster)
        action.process_cluster()

        assert len(b1.partitions[0]) == 2
        assert len(b1.partitions[1]) == 2
        assert len(b2.partitions[0]) == 2
        assert len(b2.partitions[1]) == 2
        assert len(b3.partitions[0]) == 2
        assert len(b3.partitions[1]) == 2

    def test_process_cluster_rf3(self):
        self.cluster.add_broker(Broker(3, 'brokerhost3.example.com'))
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        b3 = self.cluster.brokers[3]
        self.cluster.add_topic(Topic("testTopic3", 2))
        partition = self.cluster.topics['testTopic3'].partitions[0]
        partition.add_replica(b3, 0)
        partition.add_replica(b2, 1)
        partition.add_replica(b1, 2)
        partition = self.cluster.topics['testTopic3'].partitions[1]
        partition.add_replica(b2, 0)
        partition.add_replica(b3, 1)
        partition.add_replica(b1, 2)

        action = ActionBalanceCount(self.args, self.cluster)
        action.process_cluster()
        assert len(b1.partitions[0]) == 2
        assert len(b1.partitions[1]) == 2
        assert len(b2.partitions[0]) == 2
        assert len(b2.partitions[1]) == 2
        assert len(b3.partitions[0]) == 2
        assert len(b3.partitions[1]) == 2

    def test_process_cluster_odd_count(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        self.cluster.add_topic(Topic("testTopic3", 1))
        partition = self.cluster.topics['testTopic3'].partitions[0]
        partition.add_replica(b1, 0)
        partition.add_replica(b2, 1)
        self.cluster.topics['testTopic1'].partitions[0].swap_replica_positions(b1, b2)

        action = ActionBalanceCount(self.args, self.cluster)
        action.process_cluster()

        b1_0 = len(b1.partitions[0])
        b1_1 = len(b1.partitions[1])
        assert b1_0 == len(b2.partitions[1])
        assert b1_1 == len(b2.partitions[0])
        assert b1_0 >= 2 and b1_0 <= 3
        assert b1_1 >= 2 and b1_1 <= 3

    def test_process_cluster_unbalanced(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        self.cluster.topics['testTopic1'].partitions[1].swap_replica_positions(b1, b2)
        self.cluster.topics['testTopic2'].partitions[0].swap_replica_positions(b1, b2)

        action = ActionBalanceCount(self.args, self.cluster)
        action.process_cluster()

        b1_0 = len(b1.partitions[0])
        b1_1 = len(b1.partitions[1])
        assert b1_0 == 2
        assert b1_1 == 2

    def test_process_cluster_4broker(self):
        self.cluster = set_up_cluster_4broker()
        action = ActionBalanceCount(self.args, self.cluster)
        action.process_cluster()

        for broker_id in self.cluster.brokers:
            for pos in range(2):
                assert len(self.cluster.brokers[broker_id].partitions[pos]) == 3
