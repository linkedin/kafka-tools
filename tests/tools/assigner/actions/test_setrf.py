import sys
import unittest

from argparse import Namespace
from .fixtures import set_up_cluster, set_up_subparser

from kafka.tools.assigner.exceptions import ConfigurationException
from kafka.tools.assigner.actions.setrf import ActionSetRF
from kafka.tools.assigner.models.broker import Broker


class ActionSetRFTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster()
        (self.parser, self.subparsers) = set_up_subparser()
        self.args = Namespace()

    def test_create_class(self):
        self.args.topics = ['testTopic1']
        self.args.replication_factor = 1
        action = ActionSetRF(self.args, self.cluster)
        assert isinstance(action, ActionSetRF)

    def test_create_class_zero(self):
        self.args.topics = ['testTopic1']
        self.args.replication_factor = 0
        self.assertRaises(ConfigurationException, ActionSetRF, self.args, self.cluster)

    def test_create_class_too_high(self):
        self.args.topics = ['testTopic1']
        self.args.replication_factor = 3
        self.assertRaises(ConfigurationException, ActionSetRF, self.args, self.cluster)

    def test_configure_args(self):
        ActionSetRF.configure_args(self.subparsers)
        sys.argv = ['kafka-assigner', 'set-replication-factor', '-t', 'testTopic1', '-r', '1']
        parsed_args = self.parser.parse_args()
        assert parsed_args.action == 'set-replication-factor'

    def test_process_cluster_topic_nonexistent(self):
        self.args.topics = ['testTopic3']
        self.args.replication_factor = 1
        action = ActionSetRF(self.args, self.cluster)
        action.process_cluster()

        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [b1, b2]
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[1].replicas == [b1, b2]

    def test_process_cluster_decrease(self):
        self.args.topics = ['testTopic1']
        self.args.replication_factor = 1
        action = ActionSetRF(self.args, self.cluster)
        action.process_cluster()

        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [b1]
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b2]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[1].replicas == [b1, b2]

    def test_process_cluster_increase(self):
        self.cluster.add_broker(Broker(3, "brokerhost3.example.com"))
        self.args.topics = ['testTopic1']
        self.args.replication_factor = 3
        action = ActionSetRF(self.args, self.cluster)
        action.process_cluster()

        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        b3 = self.cluster.brokers[3]
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [b1, b2, b3]
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b2, b1, b3]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[1].replicas == [b1, b2]
