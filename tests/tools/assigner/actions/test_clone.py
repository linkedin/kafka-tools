import sys
import unittest

from argparse import Namespace
from .fixtures import set_up_cluster, set_up_subparser

from kafka.tools.assigner.exceptions import ConfigurationException
from kafka.tools.assigner.actions.clone import ActionClone
from kafka.tools.assigner.models.broker import Broker


class ActionCloneTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster()
        (self.parser, self.subparsers) = set_up_subparser()
        self.args = Namespace()

    def test_create_class(self):
        self.args.brokers = [1]
        self.args.to_broker = 2
        action = ActionClone(self.args, self.cluster)
        assert isinstance(action, ActionClone)

    def test_create_class_bad_target(self):
        self.args.brokers = [1]
        self.args.to_broker = 3
        self.assertRaises(ConfigurationException, ActionClone, self.args, self.cluster)

    def test_create_class_bad_source(self):
        self.args.brokers = [3]
        self.args.to_broker = 2
        self.assertRaises(ConfigurationException, ActionClone, self.args, self.cluster)

    def test_configure_args(self):
        ActionClone.configure_args(self.subparsers)
        sys.argv = ['kafka-assigner', 'clone', '-b', '1', '-t', '2']
        parsed_args = self.parser.parse_args()
        assert parsed_args.action == 'clone'

    def test_process_cluster_clean_target(self):
        self.cluster.add_broker(Broker(3, "brokerhost3.example.com"))
        self.args.brokers = [1]
        self.args.to_broker = 3
        action = ActionClone(self.args, self.cluster)
        action.process_cluster()

        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        b3 = self.cluster.brokers[3]
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [b3, b1, b2]
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b2, b3, b1]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2, b3, b1]
        assert self.cluster.topics['testTopic2'].partitions[1].replicas == [b3, b1, b2]

    def test_process_cluster_duplicates(self):
        self.args.brokers = [1]
        self.args.to_broker = 2
        action = ActionClone(self.args, self.cluster)
        action.process_cluster()

        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [b2, b1]
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[1].replicas == [b2, b1]

    def test_process_cluster_no_change(self):
        self.cluster.add_broker(Broker(3, "brokerhost3.example.com"))
        self.args.brokers = [3]
        self.args.to_broker = 1
        action = ActionClone(self.args, self.cluster)
        action.process_cluster()

        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [b1, b2]
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[1].replicas == [b1, b2]
