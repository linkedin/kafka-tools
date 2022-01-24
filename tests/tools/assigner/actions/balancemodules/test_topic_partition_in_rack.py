import sys
import unittest
from argparse import Namespace
from ..fixtures import set_up_subparser, set_up_cluster_9broker
from kafka.tools.assigner.actions.balance import ActionBalance
from kafka.tools.assigner.actions.balancemodules.topic_partition_in_rack import ActionBalanceTopicPartitionInRack


class ActionBalanceTopicPartitionInRackTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster_9broker()
        self.parser, self.subparser = set_up_subparser()
        self.args = Namespace(exclude_topics=[])

    def test_configure_args(self):
        ActionBalance.configure_args(self.subparser)
        sys.argv = ['kafka-assigner', 'balance', '-t', 'topic_partition_in_rack']
        parsed_args = self.parser.parse_args()
        assert parsed_args.action == 'balance'

    def test_create_class(self):
        action = ActionBalanceTopicPartitionInRack(self.args, self.cluster)
        assert isinstance(action, ActionBalanceTopicPartitionInRack)

    def test_process_cluster_partition_unskew_leader_skew(self):
        self.args.exclude_topics = ["topic2"]
        action = ActionBalanceTopicPartitionInRack(self.args, self.cluster)


        broker_partitions_before_and_after_in_topic1 = {
            1: [3,2],
            2: [2,2],
            3: [1,2]
        }

        action.process_cluster()
        broker_partition_count = {}
        for p in self.cluster.topics["topic1"].partitions:
            for i, b in enumerate(p.replicas):
                if b.id not in broker_partition_count:
                    broker_partition_count[b.id] = 0
                broker_partition_count[b.id] += 1

        for i in broker_partitions_before_and_after_in_topic1:
            assert (broker_partitions_before_and_after_in_topic1[i][1] ==\
                broker_partition_count[i])

    def test_process_cluster_partition_unskew_leader_unskew(self):
        self.args.exclude_topics = ["topic1"]
        action = ActionBalanceTopicPartitionInRack(self.args, self.cluster)
        """
        BEFORE 
        {'partitions':
            {
            0: {'size': 0, 'replicas': [7, 8, 1]},
            1: {'size': 0, 'replicas': [7, 3, 6]},
            2: {'size': 0, 'replicas': [7, 9, 5]},
            3: {'size': 0, 'replicas': [5, 2, 1]},
            4: {'size': 0, 'replicas': [5, 1, 8]}}
        }

        AFTER RE-ARRANGE
        {'partitions':
            {
            0: {'size': 0, 'replicas': [9, 8, 2]},
            1: {'size': 0, 'replicas': [7, 3, 6]},
            2: {'size': 0, 'replicas': [7, 9, 6]},
            3: {'size': 0, 'replicas': [5, 2, 1]},
            4: {'size': 0, 'replicas': [5, 1, 8]}}
        }

        AFTER LEADER RE-ARRANGE
        {'partitions':
            {
            0: {'size': 0, 'replicas': [9, 7, 2]},
            1: {'size': 0, 'replicas': [8, 3, 5]},
            2: {'size': 0, 'replicas': [7, 9, 6]},
            3: {'size': 0, 'replicas': [6, 2, 1]},
            4: {'size': 0, 'replicas': [5, 1, 8]}}
        }        
        """

        # input & expected output for topic: topic2
        broker_partitions_before_and_after_in_topic2 = {
            1: [3,2],
            5: [3,2],
            6: [1,2],
            7: [3,2],
            8: [2,2],
            9: [1,2]
        }

        broker_partitions_leader_before_and_after_in_topic2 = {
            1: [0,0],
            5: [2,1],
            6: [0,1],
            7: [3,1],
            8: [0,1],
            9: [0,1]
        }

        action.process_cluster()

        broker_partition_count_leader_count = {}
        for p in self.cluster.topics["topic2"].partitions:
            for i, b in enumerate(p.replicas):
                if b.id not in broker_partition_count_leader_count:
                    broker_partition_count_leader_count[b.id] = [0, 0]

                if i == 0:
                    broker_partition_count_leader_count[b.id][1] += 1
                broker_partition_count_leader_count[b.id][0] += 1

        for i in broker_partitions_before_and_after_in_topic2:
            assert (broker_partitions_leader_before_and_after_in_topic2[i][1] ==\
                broker_partition_count_leader_count[i][1])
            assert (broker_partitions_before_and_after_in_topic2[i][1] ==\
                broker_partition_count_leader_count[i][0])

    def tearDown(self):
        pass
