import sys
import unittest

from argparse import Namespace
from ..fixtures import set_up_cluster, set_up_subparser, set_up_cluster_4broker

from kafka.tools.assigner.models.topic import Topic
from kafka.tools.assigner.actions.balance import ActionBalance
from kafka.tools.assigner.actions.balancemodules.even import ActionBalanceEven, pmap_matches_target


class ActionBalanceEvenTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster()
        (self.parser, self.subparsers) = set_up_subparser()
        self.args = Namespace()

    def is_cluster_even(self, skip_topics=[]):
        for topic_name in self.cluster.topics:
            topic = self.cluster.topics[topic_name]
            if topic_name in skip_topics:
                continue

            # Create broker map for this topic.
            pmap = [dict.fromkeys(self.cluster.brokers.keys(), 0) for pos in range(len(topic.partitions[0].replicas))]
            for partition in topic.partitions:
                for i, replica in enumerate(partition.replicas):
                    pmap[i][replica.id] += 1

            if not pmap_matches_target(pmap, len(topic.partitions) / len(self.cluster.brokers)):
                print("Position map for topic {0}: {1}".format(topic_name, pmap))
                return False
        return True

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
        assert self.is_cluster_even()

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
        assert self.is_cluster_even(skip_topics=['testTopic3'])

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
        assert self.is_cluster_even(skip_topics=['testTopic3'])

    def test_skip_topic_ok(self):
        action = ActionBalanceEven(self.args, self.cluster)
        assert action.check_topic_ok(self.cluster.topics['testTopic1'])

    def test_skip_topic_different_rf(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        self.cluster.add_topic(Topic("testTopic3", 2))
        partition = self.cluster.topics['testTopic3'].partitions[0]
        partition.add_replica(b1, 0)
        partition.add_replica(b2, 1)
        partition = self.cluster.topics['testTopic3'].partitions[1]
        partition.add_replica(b1, 0)

        action = ActionBalanceEven(self.args, self.cluster)
        assert not action.check_topic_ok(self.cluster.topics['testTopic3'])

    def test_skip_topic_odd_count(self):
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

        action = ActionBalanceEven(self.args, self.cluster)
        assert not action.check_topic_ok(self.cluster.topics['testTopic3'])

    def test_pmap_matches_target(self):
        target = 2
        pmap = [{1: 2, 2: 2, 3: 2, 4: 2}, {1: 2, 2: 2, 3: 2, 4: 2}]
        assert pmap_matches_target(pmap, target)

    def test_pmap_matches_target_fails(self):
        target = 1
        pmap = [{1: 1, 2: 2, 3: 0, 4: 1}, {1: 1, 2: 1, 3: 2, 4: 0}]
        assert not pmap_matches_target(pmap, target)

    def test_process_cluster_4broker(self):
        self.cluster = set_up_cluster_4broker()

        action = ActionBalanceEven(self.args, self.cluster)
        action.process_cluster()
        assert self.is_cluster_even(skip_topics=['testTopic3'])
