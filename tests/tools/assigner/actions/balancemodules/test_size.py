import sys
import unittest

from argparse import Namespace
from ..fixtures import set_up_cluster, set_up_subparser

from kafka.tools.assigner.models.broker import Broker
from kafka.tools.assigner.models.topic import Topic
from kafka.tools.assigner.actions.balance import ActionBalance
from kafka.tools.assigner.actions.balancemodules.size import ActionBalanceSize


class ActionBalanceSizeTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster()
        self.cluster.topics['testTopic1'].partitions[0].size = 1000
        self.cluster.topics['testTopic1'].partitions[1].size = 1000
        self.cluster.topics['testTopic2'].partitions[0].size = 2000
        self.cluster.topics['testTopic2'].partitions[1].size = 2000

        (self.parser, self.subparsers) = set_up_subparser()
        self.args = Namespace()

    def test_configure_args(self):
        ActionBalance.configure_args(self.subparsers)
        sys.argv = ['kafka-assigner', 'balance', '-t', 'size']
        parsed_args = self.parser.parse_args()
        assert parsed_args.action == 'balance'

    def test_create_class(self):
        action = ActionBalanceSize(self.args, self.cluster)
        assert isinstance(action, ActionBalanceSize)

    def test_process_cluster_no_change(self):
        action = ActionBalanceSize(self.args, self.cluster)
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

        action = ActionBalanceSize(self.args, self.cluster)
        action.process_cluster()

        assert sum([p.size for p in self.cluster.brokers[1].partitions[0]], 0) == 3000
        assert sum([p.size for p in self.cluster.brokers[1].partitions[1]], 0) == 3000
        assert sum([p.size for p in self.cluster.brokers[2].partitions[0]], 0) == 3000
        assert sum([p.size for p in self.cluster.brokers[2].partitions[1]], 0) == 3000

    def test_process_cluster_empty_broker(self):
        self.cluster.add_broker(Broker(3, 'brokerhost3.example.com'))
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        self.cluster.add_topic(Topic("testTopic3", 2))
        partition = self.cluster.topics['testTopic3'].partitions[0]
        partition.size = 1000
        partition.add_replica(b1, 0)
        partition.add_replica(b2, 1)
        partition = self.cluster.topics['testTopic3'].partitions[1]
        partition.add_replica(b2, 0)
        partition.add_replica(b1, 1)
        partition.size = 2000

        action = ActionBalanceSize(self.args, self.cluster)
        action.process_cluster()

        assert sum([p.size for p in self.cluster.brokers[1].partitions[0]], 0) == 3000
        assert sum([p.size for p in self.cluster.brokers[1].partitions[1]], 0) == 3000
        assert sum([p.size for p in self.cluster.brokers[2].partitions[0]], 0) == 3000
        assert sum([p.size for p in self.cluster.brokers[2].partitions[1]], 0) == 3000
        assert sum([p.size for p in self.cluster.brokers[3].partitions[0]], 0) == 3000
        assert sum([p.size for p in self.cluster.brokers[3].partitions[1]], 0) == 3000

    def test_process_cluster_odd_partitions(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        self.cluster.add_topic(Topic("testTopic3", 3))
        partition = self.cluster.topics['testTopic3'].partitions[0]
        partition.size = 1000
        partition.add_replica(b1, 0)
        partition.add_replica(b2, 1)
        partition = self.cluster.topics['testTopic3'].partitions[1]
        partition.add_replica(b2, 0)
        partition.add_replica(b1, 1)
        partition.size = 2000
        partition = self.cluster.topics['testTopic3'].partitions[2]
        partition.add_replica(b2, 0)
        partition.add_replica(b1, 1)
        partition.size = 1000

        action = ActionBalanceSize(self.args, self.cluster)
        action.process_cluster()

        assert sum([p.size for p in self.cluster.brokers[1].partitions[0]], 0) == 5000
        assert sum([p.size for p in self.cluster.brokers[1].partitions[1]], 0) == 5000
        assert sum([p.size for p in self.cluster.brokers[2].partitions[0]], 0) == 5000
        assert sum([p.size for p in self.cluster.brokers[2].partitions[1]], 0) == 5000

    def test_process_cluster_large_partition(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        self.cluster.add_topic(Topic("testTopic3", 3))
        partition = self.cluster.topics['testTopic3'].partitions[0]
        partition.size = 1000
        partition.add_replica(b1, 0)
        partition.add_replica(b2, 1)
        partition = self.cluster.topics['testTopic3'].partitions[1]
        partition.add_replica(b1, 0)
        partition.add_replica(b2, 1)
        partition.size = 2000
        partition = self.cluster.topics['testTopic3'].partitions[2]
        partition.add_replica(b1, 0)
        partition.add_replica(b2, 1)
        partition.size = 8000

        action = ActionBalanceSize(self.args, self.cluster)
        action.process_cluster()

        b1_0 = sum([p.size for p in self.cluster.brokers[1].partitions[0]], 0)
        b1_1 = sum([p.size for p in self.cluster.brokers[1].partitions[1]], 0)
        b2_0 = sum([p.size for p in self.cluster.brokers[2].partitions[0]], 0)
        b2_1 = sum([p.size for p in self.cluster.brokers[2].partitions[1]], 0)
        assert b1_0 >= 8000 and b1_0 <= 9000
        assert b1_1 >= 8000 and b1_1 <= 9000
        assert b2_0 >= 8000 and b2_0 <= 9000
        assert b2_1 >= 8000 and b2_1 <= 9000

    def test_process_cluster_large_partition_early(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        self.cluster.add_topic(Topic("testTopic3", 3))
        partition = self.cluster.topics['testTopic3'].partitions[0]
        partition.size = 1000
        partition.add_replica(b1, 0)
        partition.add_replica(b2, 1)
        partition = self.cluster.topics['testTopic3'].partitions[1]
        partition.add_replica(b1, 0)
        partition.add_replica(b2, 1)
        partition.size = 2000
        partition = self.cluster.topics['testTopic3'].partitions[2]
        partition.add_replica(b1, 0)
        partition.add_replica(b2, 1)
        partition.size = 1000
        self.cluster.topics['testTopic1'].partitions[0].size = 8000

        action = ActionBalanceSize(self.args, self.cluster)
        action.process_cluster()

        b1_0 = sum([p.size for p in self.cluster.brokers[1].partitions[0]], 0)
        b1_1 = sum([p.size for p in self.cluster.brokers[1].partitions[1]], 0)
        b2_0 = sum([p.size for p in self.cluster.brokers[2].partitions[0]], 0)
        b2_1 = sum([p.size for p in self.cluster.brokers[2].partitions[1]], 0)
        assert b1_0 >= 8000 and b1_0 <= 9000
        assert b1_1 >= 8000 and b1_1 <= 9000
        assert b2_0 >= 8000 and b2_0 <= 9000
        assert b2_1 >= 8000 and b2_1 <= 9000
