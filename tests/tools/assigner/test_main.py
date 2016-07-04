import argparse
import unittest

from mock import patch

from kafka.tools.assigner.__main__ import main, get_plugins_list, check_and_get_sizes, run_preferred_replica_elections
from kafka.tools.assigner.actions.balance import ActionBalance
from kafka.tools.assigner.models.broker import Broker
from kafka.tools.assigner.models.cluster import Cluster
from kafka.tools.assigner.models.topic import Topic
from kafka.tools.assigner.models.replica_election import ReplicaElection
from kafka.tools.assigner.plugins import PluginModule
from kafka.tools.assigner.sizers.ssh import SizerSSH


def set_up_cluster():
    cluster = Cluster()
    cluster.add_broker(Broker(1, "brokerhost1.example.com"))
    cluster.add_broker(Broker(2, "brokerhost2.example.com"))
    cluster.add_topic(Topic("testTopic1", 2))
    cluster.add_topic(Topic("testTopic2", 2))
    partition = cluster.topics['testTopic1'].partitions[0]
    partition.add_replica(cluster.brokers[1], 0)
    partition.add_replica(cluster.brokers[2], 1)
    partition = cluster.topics['testTopic1'].partitions[1]
    partition.add_replica(cluster.brokers[2], 0)
    partition.add_replica(cluster.brokers[1], 1)
    partition = cluster.topics['testTopic2'].partitions[0]
    partition.add_replica(cluster.brokers[2], 0)
    partition.add_replica(cluster.brokers[1], 1)
    partition = cluster.topics['testTopic2'].partitions[1]
    partition.add_replica(cluster.brokers[1], 0)
    partition.add_replica(cluster.brokers[2], 1)
    return cluster


class MainTests(unittest.TestCase):
    def setUp(self):
        self.patcher_args = patch('kafka.tools.assigner.__main__.set_up_arguments')
        self.patcher_tools_path = patch('kafka.tools.assigner.__main__.get_tools_path')
        self.patcher_java_home = patch('kafka.tools.assigner.__main__.check_java_home')
        self.patcher_cluster = patch.object(Cluster, 'create_from_zookeeper')

        self.mock_args = self.patcher_args.start()
        self.mock_tools_path = self.patcher_tools_path.start()
        self.mock_java_home = self.patcher_java_home.start()
        self.mock_cluster = self.patcher_cluster.start()

        self.mock_tools_path.return_value = '/path/to/tools'
        self.mock_cluster.return_value = set_up_cluster()

    def tearDown(self):
        self.patcher_args.stop()
        self.patcher_tools_path.stop()
        self.patcher_java_home.stop()
        self.patcher_cluster.stop()

    @patch('kafka.tools.assigner.__main__.get_plugins_list')
    def test_main(self, mock_plugins):
        mock_plugins.return_value = [PluginModule]
        self.mock_args.return_value = argparse.Namespace(zookeeper='zkconnect',
                                                         action='elect',
                                                         tools_path='/path/to/tools',
                                                         moves=10,
                                                         execute=False,
                                                         generate=False,
                                                         size=False,
                                                         skip_ple=False,
                                                         ple_size=2000,
                                                         ple_wait=120,
                                                         sizer='ssh',
                                                         leadership=True)
        assert main() == 0

    def test_get_plugins(self):
        plugin_list = get_plugins_list()
        assert plugin_list == []

    @patch('kafka.tools.assigner.sizers.ssh.paramiko.SSHClient.load_system_host_keys')
    @patch.object(SizerSSH, 'get_partition_sizes')
    def test_get_sizes(self, mock_sizes, mock_load_keys):
        args = argparse.Namespace(sizer='ssh', size=True)
        check_and_get_sizes(ActionBalance, args, set_up_cluster(), {'ssh': SizerSSH})

    def test_ple(self):
        cluster = set_up_cluster()
        args = argparse.Namespace(ple_wait=0, zookeeper='zkconnect', tools_path='/path/to/tools')
        batches = [ReplicaElection(cluster.brokers[1].partitions, args.ple_wait),
                   ReplicaElection(cluster.brokers[2].partitions, args.ple_wait)]
        run_preferred_replica_elections(batches, args, args.tools_path, [], True)
