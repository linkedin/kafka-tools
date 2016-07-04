import sys
import unittest

from argparse import Namespace
from .fixtures import set_up_cluster, set_up_subparser

from kafka.tools.assigner.exceptions import ConfigurationException, NotEnoughReplicasException
from kafka.tools.assigner.actions.trim import ActionTrim
from kafka.tools.assigner.models.broker import Broker


class ActionTrimTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster()
        (self.parser, self.subparsers) = set_up_subparser()
        self.args = Namespace()

    def test_create_class(self):
        self.args.brokers = [1]
        action = ActionTrim(self.args, self.cluster)
        assert isinstance(action, ActionTrim)

    def test_create_class_bad_source(self):
        self.args.brokers = [3]
        self.assertRaises(ConfigurationException, ActionTrim, self.args, self.cluster)

    def test_configure_args(self):
        ActionTrim.configure_args(self.subparsers)
        sys.argv = ['kafka-assigner', 'trim', '-b', '1']
        parsed_args = self.parser.parse_args()
        assert parsed_args.action == 'trim'

    def test_process_cluster_no_change(self):
        self.cluster.add_broker(Broker(3, "brokerhost3.example.com"))
        self.args.brokers = [3]
        action = ActionTrim(self.args, self.cluster)
        action.process_cluster()

        b1 = self.cluster.brokers[1]
        b2 = self.cluster.brokers[2]
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [b1, b2]
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2, b1]
        assert self.cluster.topics['testTopic2'].partitions[1].replicas == [b1, b2]

    def test_process_cluster(self):
        self.args.brokers = [1]
        action = ActionTrim(self.args, self.cluster)
        action.process_cluster()

        b2 = self.cluster.brokers[2]
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [b2]
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b2]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2]
        assert self.cluster.topics['testTopic2'].partitions[1].replicas == [b2]

    def test_process_cluster_error(self):
        self.args.brokers = [1, 2]
        action = ActionTrim(self.args, self.cluster)
        self.assertRaises(NotEnoughReplicasException, action.process_cluster)
