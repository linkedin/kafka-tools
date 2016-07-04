import sys
import unittest

from argparse import Namespace
from .fixtures import set_up_cluster, set_up_subparser

from kafka.tools.assigner.exceptions import ConfigurationException, NotEnoughReplicasException
from kafka.tools.assigner.actions.remove import ActionRemove
from kafka.tools.assigner.models.broker import Broker


class ActionRemoveTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster()
        (self.parser, self.subparsers) = set_up_subparser()
        self.args = Namespace()

    def test_create_class(self):
        self.args.brokers = [1]
        self.args.to_brokers = [2]
        action = ActionRemove(self.args, self.cluster)
        assert isinstance(action, ActionRemove)

    def test_create_class_bad_target(self):
        self.args.brokers = [1]
        self.args.to_brokers = [3]
        self.assertRaises(ConfigurationException, ActionRemove, self.args, self.cluster)

    def test_create_class_bad_source(self):
        self.args.brokers = [3]
        self.args.to_brokers = [2]
        self.assertRaises(ConfigurationException, ActionRemove, self.args, self.cluster)

    def test_create_class_source_in_target(self):
        self.args.brokers = [1]
        self.args.to_brokers = [1]
        self.assertRaises(ConfigurationException, ActionRemove, self.args, self.cluster)

    def test_create_class_no_target(self):
        self.args.brokers = [1]
        self.args.to_brokers = None
        action = ActionRemove(self.args, self.cluster)
        assert action.to_brokers == [2]

    def test_configure_args(self):
        ActionRemove.configure_args(self.subparsers)
        sys.argv = ['kafka-assigner', 'remove', '-b', '1', '-t', '2']
        parsed_args = self.parser.parse_args()
        assert parsed_args.action == 'remove'

    def test_process_cluster(self):
        self.cluster.add_broker(Broker(3, "brokerhost3.example.com"))
        self.args.brokers = [1]
        self.args.to_brokers = None
        action = ActionRemove(self.args, self.cluster)
        action.process_cluster()

        b2 = self.cluster.brokers[2]
        b3 = self.cluster.brokers[3]
        assert self.cluster.topics['testTopic1'].partitions[0].replicas == [b3, b2]
        assert self.cluster.topics['testTopic1'].partitions[1].replicas == [b2, b3]
        assert self.cluster.topics['testTopic2'].partitions[0].replicas == [b2, b3]
        assert self.cluster.topics['testTopic2'].partitions[1].replicas == [b3, b2]

    def test_process_cluster_error(self):
        self.args.brokers = [1]
        self.args.to_brokers = None
        action = ActionRemove(self.args, self.cluster)
        self.assertRaises(NotEnoughReplicasException, action.process_cluster)
