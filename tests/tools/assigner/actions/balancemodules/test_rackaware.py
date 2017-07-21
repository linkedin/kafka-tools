import sys
import unittest

from argparse import Namespace
from collections import deque
from mock import call, patch
from ..fixtures import set_up_cluster, set_up_subparser

from kafka.tools.exceptions import BalanceException
from kafka.tools.models.broker import Broker
from kafka.tools.assigner.actions.balance import ActionBalance
from kafka.tools.assigner.actions.balancemodules.rackaware import (ActionBalanceRackAware, check_partition_swappable, racks_for_replica_list,
                                                                   difference_in_size_to_last_partition)


class ActionBalanceRackAwareTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster()
        self.cluster.topics['testTopic1'].partitions[0].size = 1000
        self.cluster.topics['testTopic1'].partitions[1].size = 1000
        self.cluster.topics['testTopic2'].partitions[0].size = 2000
        self.cluster.topics['testTopic2'].partitions[1].size = 2000

        (self.parser, self.subparsers) = set_up_subparser()
        self.args = Namespace(exclude_topics=[])

    def test_configure_args(self):
        ActionBalance.configure_args(self.subparsers)
        sys.argv = ['kafka-assigner', 'balance', '-t', 'rackaware']
        parsed_args = self.parser.parse_args()
        assert parsed_args.action == 'balance'

    def test_init_broker_deque(self):
        action = ActionBalanceRackAware(self.args, self.cluster)
        assert set(action._random_brokers) == set(self.cluster.brokers.values())

    def test_check_partition_swappable_already_exists(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        replicas_a = [b1, b2]
        replicas_b = [b2, b1]
        assert check_partition_swappable(replicas_a, replicas_b, 0) is False
        assert check_partition_swappable(replicas_b, replicas_a, 0) is False

    def test_check_partition_swappable_racks_collide(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        b3 = Broker('brokerhost3.example.com', id=3)
        b4 = Broker('brokerhost4.example.com', id=4)
        b2.rack = "a"
        b3.rack = "a"
        b4.rack = "b"
        replicas_a = [b1, b2]
        replicas_b = [b3, b4]
        assert check_partition_swappable(replicas_a, replicas_b, 0) is False
        assert check_partition_swappable(replicas_b, replicas_a, 0) is False

    def test_check_partition_swappable_racks_ok(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        b3 = Broker('brokerhost3.example.com', id=3)
        b4 = Broker('brokerhost4.example.com', id=4)
        b2.rack = "a"
        b3.rack = "b"
        b4.rack = "b"
        replicas_a = [b1, b2]
        replicas_b = [b3, b4]
        assert check_partition_swappable(replicas_a, replicas_b, 0) is True
        assert check_partition_swappable(replicas_b, replicas_a, 0) is True

    def test_racks_for_replica_list_nopos(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        assert racks_for_replica_list([b1, b2]) == ["a", "b"]

    def test_racks_for_replica_list_pos(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        assert racks_for_replica_list([b1, b2], 1) == ["a"]

    def test_difference_in_size_larger(self):
        partitions = [self.cluster.topics['testTopic1'].partitions[0],
                      self.cluster.topics['testTopic1'].partitions[1]]
        partitions[0].size = 1000
        partitions[1].size = 2000
        p = self.cluster.topics['testTopic2'].partitions[0]
        p.size = 3000
        assert difference_in_size_to_last_partition(p, partitions) == 1000

    def test_difference_in_size_smaller(self):
        partitions = [self.cluster.topics['testTopic1'].partitions[0],
                      self.cluster.topics['testTopic1'].partitions[1]]
        partitions[0].size = 1000
        partitions[1].size = 2000
        p = self.cluster.topics['testTopic2'].partitions[0]
        p.size = 1000
        assert difference_in_size_to_last_partition(p, partitions) == 1000

    def test_difference_in_size_empty_list(self):
        partitions = []
        p = self.cluster.topics['testTopic2'].partitions[0]
        p.size = 1000
        assert difference_in_size_to_last_partition(p, partitions) == float("inf")

    def test_try_pick_new_broker_skipself(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        b3 = Broker('brokerhost3.example.com', id=3)
        self.cluster.add_broker(b3)
        b3.rack = "a"
        action = ActionBalanceRackAware(self.args, self.cluster)

        # Firmly order the deque
        action._random_brokers = deque([b1, b2, b3])
        newbroker = action._try_pick_new_broker(self.cluster.topics['testTopic1'].partitions[0], 0)
        assert newbroker == b3
        assert action._random_brokers == deque([b1, b2, b3])

    def test_try_pick_new_broker_failed(self):
        action = ActionBalanceRackAware(self.args, self.cluster)
        self.assertRaises(BalanceException, action._try_pick_new_broker, self.cluster.topics['testTopic1'].partitions[0], 0)

    def test_try_pick_new_broker(self):
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        b3 = Broker('brokerhost3.example.com', id=3)
        self.cluster.add_broker(b3)
        b3.rack = "b"
        action = ActionBalanceRackAware(self.args, self.cluster)

        # Firmly order the deque
        action._random_brokers = deque([b3, b1, b2])
        newbroker = action._try_pick_new_broker(self.cluster.topics['testTopic1'].partitions[0], 1)
        assert newbroker == b3
        assert action._random_brokers == deque([b1, b2, b3])

    def test_try_pick_swap_partition_none(self):
        action = ActionBalanceRackAware(self.args, self.cluster)
        small_partitions = [self.cluster.topics['testTopic1'].partitions[0],
                            self.cluster.topics['testTopic1'].partitions[1]]
        large_partitions = [self.cluster.topics['testTopic2'].partitions[0]]
        p = self.cluster.topics['testTopic2'].partitions[1]
        small_partitions[0].size = 1000
        small_partitions[1].size = 2000
        large_partitions[0].size = 4000
        p.size = 3000
        assert action._try_pick_swap_partition(p, 0, small_partitions, large_partitions) is None

    @patch('kafka.tools.assigner.actions.balancemodules.rackaware.check_partition_swappable')
    def test_try_pick_swap_partitions_nosmall(self, mock_check):
        action = ActionBalanceRackAware(self.args, self.cluster)
        small_partitions = []
        large_partitions = [self.cluster.topics['testTopic2'].partitions[0]]
        p = self.cluster.topics['testTopic2'].partitions[1]
        large_partitions[0].size = 4000
        p.size = 3000

        mock_check.return_value = True
        assert action._try_pick_swap_partition(p, 0, small_partitions, large_partitions) == self.cluster.topics['testTopic2'].partitions[0]

    @patch('kafka.tools.assigner.actions.balancemodules.rackaware.check_partition_swappable')
    def test_try_pick_swap_partitions_nolarge(self, mock_check):
        action = ActionBalanceRackAware(self.args, self.cluster)
        large_partitions = []
        small_partitions = [self.cluster.topics['testTopic2'].partitions[0]]
        p = self.cluster.topics['testTopic2'].partitions[1]
        small_partitions[0].size = 1000
        p.size = 3000

        mock_check.return_value = True
        assert action._try_pick_swap_partition(p, 0, small_partitions, large_partitions) == self.cluster.topics['testTopic2'].partitions[0]

    @patch('kafka.tools.assigner.actions.balancemodules.rackaware.check_partition_swappable')
    def test_try_pick_swap_partition_full(self, mock_check):
        action = ActionBalanceRackAware(self.args, self.cluster)
        small_partitions = [self.cluster.topics['testTopic1'].partitions[0],
                            self.cluster.topics['testTopic1'].partitions[1]]
        large_partitions = [self.cluster.topics['testTopic2'].partitions[0]]
        p = self.cluster.topics['testTopic2'].partitions[1]
        small_partitions[0].size = 1000
        small_partitions[1].size = 2000
        large_partitions[0].size = 4000
        p.size = 3000

        mock_check.side_effect = [False, False, True]
        target = action._try_pick_swap_partition(p, 0, small_partitions, large_partitions)

        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        mock_check.assert_has_calls([call([b1, b2], [b2, b1], 0), call([b1, b2], [b2, b1], 0), call([b1, b2], [b1, b2], 0)])
        assert target == self.cluster.topics['testTopic1'].partitions[0]

    def test_get_sorted_list(self):
        action = ActionBalanceRackAware(self.args, self.cluster)
        p1 = self.cluster.topics['testTopic1'].partitions[0]
        p2 = self.cluster.topics['testTopic1'].partitions[1]
        p3 = self.cluster.topics['testTopic2'].partitions[0]
        p4 = self.cluster.topics['testTopic2'].partitions[1]
        p1.size = 1000
        p2.size = 6000
        p3.size = 3000
        p4.size = 4000
        assert action._get_sorted_partition_list_at_pos(0) == [p1, p3, p4, p2]

    def test_get_sorted_list_missing_replica(self):
        action = ActionBalanceRackAware(self.args, self.cluster)
        p1 = self.cluster.topics['testTopic1'].partitions[0]
        p2 = self.cluster.topics['testTopic1'].partitions[1]
        p3 = self.cluster.topics['testTopic2'].partitions[0]
        p4 = self.cluster.topics['testTopic2'].partitions[1]
        p1.size = 1000
        p2.size = 6000
        p3.size = 3000
        p4.size = 4000
        p1.replicas = [self.cluster.brokers[1]]
        assert action._get_sorted_partition_list_at_pos(0) == [p3, p4, p2]

    @patch.object(ActionBalanceRackAware, '_try_pick_swap_partition')
    def test_process_partitions_at_pos_nochange(self, mock_pick):
        action = ActionBalanceRackAware(self.args, self.cluster)
        action._process_partitions_at_pos(0)
        mock_pick.assert_not_called()

    @patch.object(ActionBalanceRackAware, '_try_pick_swap_partition')
    def test_process_partitions_at_pos_swap_partition(self, mock_pick):
        action = ActionBalanceRackAware(self.args, self.cluster)
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        b3 = Broker('brokerhost3.example.com', id=3)
        b4 = Broker('brokerhost4.example.com', id=4)
        self.cluster.add_broker(b3)
        self.cluster.add_broker(b4)
        b3.rack = "a"
        b4.rack = "c"
        self.cluster.topics['testTopic2'].partitions[0].swap_replicas(b2, b3)
        self.cluster.topics['testTopic1'].partitions[1].swap_replicas(b1, b4)
        mock_pick.return_value = self.cluster.topics['testTopic1'].partitions[1]

        action._process_partitions_at_pos(0)
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b3, b4]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2, b1]

    @patch.object(ActionBalanceRackAware, '_try_pick_swap_partition')
    @patch.object(ActionBalanceRackAware, '_try_pick_new_broker')
    def test_process_partitions_at_pos_swap_broker(self, mock_broker, mock_pick):
        action = ActionBalanceRackAware(self.args, self.cluster)
        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        b3 = Broker('brokerhost3.example.com', id=3)
        b4 = Broker('brokerhost4.example.com', id=4)
        self.cluster.add_broker(b3)
        self.cluster.add_broker(b4)
        b3.rack = "a"
        b4.rack = "c"
        self.cluster.topics['testTopic2'].partitions[0].swap_replicas(b2, b3)
        self.cluster.topics['testTopic1'].partitions[1].swap_replicas(b1, b4)
        mock_pick.return_value = None
        mock_broker.return_value = b2

        action._process_partitions_at_pos(0)
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b2, b4]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2, b1]

    def test_process_cluster_single_rack(self):
        action = ActionBalanceRackAware(self.args, self.cluster)
        self.cluster.brokers[2].rack = "a"
        self.assertRaises(BalanceException, action.process_cluster)

    @patch.object(ActionBalanceRackAware, '_process_partitions_at_pos')
    def test_process_cluster(self, mock_process):
        action = ActionBalanceRackAware(self.args, self.cluster)
        action.process_cluster()
        mock_process.assert_has_calls([call(0), call(1)])
