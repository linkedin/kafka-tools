import unittest

from argparse import Namespace
from mock import call, patch, ANY
from subprocess import PIPE
from testfixtures import compare
from testfixtures.popen import MockPopen

from kafka.tools.assigner.exceptions import UnknownBrokerException
from kafka.tools.assigner.models.broker import Broker
from kafka.tools.assigner.models.cluster import Cluster
from kafka.tools.assigner.models.topic import Topic
from kafka.tools.assigner.sizers.ssh import SizerSSH


class SizerSSHTests(unittest.TestCase):
    def setUp(self):
        self.args = Namespace()
        self.args.datadir = "/path/to/data"

        self.patcher_ssh = patch('kafka.tools.assigner.models.reassignment.subprocess.Popen', new_callable=MockPopen)
        self.mock_ssh = self.patcher_ssh.start()

    def tearDown(self):
        self.patcher_ssh.stop()

    def create_cluster_onehost(self):
        self.cluster = Cluster()
        self.cluster.add_broker(Broker(1, "brokerhost1.example.com"))
        self.cluster.add_topic(Topic("testTopic1", 2))
        self.cluster.add_topic(Topic("testTopic2", 2))
        partition = self.cluster.topics['testTopic1'].partitions[0]
        partition.add_replica(self.cluster.brokers[1], 0)
        partition = self.cluster.topics['testTopic1'].partitions[1]
        partition.add_replica(self.cluster.brokers[1], 0)
        partition = self.cluster.topics['testTopic2'].partitions[0]
        partition.add_replica(self.cluster.brokers[1], 0)
        partition = self.cluster.topics['testTopic2'].partitions[1]
        partition.add_replica(self.cluster.brokers[1], 0)

    def create_cluster_twohost(self):
        self.cluster = Cluster()
        self.cluster.add_broker(Broker(1, "brokerhost1.example.com"))
        self.cluster.add_broker(Broker(2, "brokerhost2.example.com"))
        self.cluster.add_topic(Topic("testTopic1", 2))
        self.cluster.add_topic(Topic("testTopic2", 2))
        partition = self.cluster.topics['testTopic1'].partitions[0]
        partition.add_replica(self.cluster.brokers[1], 0)
        partition.add_replica(self.cluster.brokers[2], 1)
        partition = self.cluster.topics['testTopic1'].partitions[1]
        partition.add_replica(self.cluster.brokers[2], 0)
        partition.add_replica(self.cluster.brokers[1], 1)
        partition = self.cluster.topics['testTopic2'].partitions[0]
        partition.add_replica(self.cluster.brokers[2], 0)
        partition.add_replica(self.cluster.brokers[1], 1)
        partition = self.cluster.topics['testTopic2'].partitions[1]
        partition.add_replica(self.cluster.brokers[1], 0)
        partition.add_replica(self.cluster.brokers[2], 1)

    def test_sizer_create(self):
        self.create_cluster_onehost()
        sizer = SizerSSH(self.args, self.cluster)
        assert isinstance(sizer, SizerSSH)

    def test_sizer_run_missing_host(self):
        self.create_cluster_onehost()
        self.cluster.brokers[1].hostname = None
        sizer = SizerSSH(self.args, self.cluster)
        self.assertRaises(UnknownBrokerException, sizer.get_partition_sizes)

    def test_sizer_run_badinput(self):
        self.create_cluster_onehost()
        m_stdout = "foo\nbar\nbaz\n"
        self.mock_ssh.set_default(stdout=m_stdout.encode('utf-8'))

        sizer = SizerSSH(self.args, self.cluster)
        sizer.get_partition_sizes()

        compare([call.Popen(['ssh', 'brokerhost1.example.com', 'du -sk /path/to/data/*'], stdout=PIPE, stderr=ANY)], self.mock_ssh.mock.method_calls)
        assert self.cluster.topics['testTopic1'].partitions[0].size == 0
        assert self.cluster.topics['testTopic1'].partitions[1].size == 0
        assert self.cluster.topics['testTopic2'].partitions[0].size == 0
        assert self.cluster.topics['testTopic2'].partitions[1].size == 0

    def test_sizer_run_setsizes_singlehost(self):
        self.create_cluster_onehost()
        m_stdout = ("1001\t/path/to/data/testTopic1-0\n"
                    "1002\t/path/to/data/testTopic1-1\n"
                    "2001\t/path/to/data/testTopic2-0\n"
                    "2002\t/path/to/data/testTopic2-1\n")
        self.mock_ssh.set_default(stdout=m_stdout.encode('utf-8'))

        sizer = SizerSSH(self.args, self.cluster)
        sizer.get_partition_sizes()

        compare([call.Popen(['ssh', 'brokerhost1.example.com', 'du -sk /path/to/data/*'], stdout=PIPE, stderr=ANY)], self.mock_ssh.mock.method_calls)
        assert self.cluster.topics['testTopic1'].partitions[0].size == 1001
        assert self.cluster.topics['testTopic1'].partitions[1].size == 1002
        assert self.cluster.topics['testTopic2'].partitions[0].size == 2001
        assert self.cluster.topics['testTopic2'].partitions[1].size == 2002

    def test_sizer_run_setsizes_badtopic(self):
        self.create_cluster_onehost()
        m_stdout = ("1001\t/path/to/data/testTopic1-0\n"
                    "1002\t/path/to/data/testTopic1-1\n"
                    "5002\t/path/to/data/badTopic1-1\n"
                    "2001\t/path/to/data/testTopic2-0\n"
                    "2002\t/path/to/data/testTopic2-1\n")
        self.mock_ssh.set_default(stdout=m_stdout.encode('utf-8'))

        sizer = SizerSSH(self.args, self.cluster)
        sizer.get_partition_sizes()

        compare([call.Popen(['ssh', 'brokerhost1.example.com', 'du -sk /path/to/data/*'], stdout=PIPE, stderr=ANY)], self.mock_ssh.mock.method_calls)
        assert self.cluster.topics['testTopic1'].partitions[0].size == 1001
        assert self.cluster.topics['testTopic1'].partitions[1].size == 1002
        assert self.cluster.topics['testTopic2'].partitions[0].size == 2001
        assert self.cluster.topics['testTopic2'].partitions[1].size == 2002

    def test_sizer_run_setsizes_badpartition(self):
        self.create_cluster_onehost()
        m_stdout = ("1001\t/path/to/data/testTopic1-0\n"
                    "1002\t/path/to/data/testTopic1-1\n"
                    "5002\t/path/to/data/testTopic1-2\n"
                    "2001\t/path/to/data/testTopic2-0\n"
                    "2002\t/path/to/data/testTopic2-1\n")
        self.mock_ssh.set_default(stdout=m_stdout.encode('utf-8'))

        sizer = SizerSSH(self.args, self.cluster)
        sizer.get_partition_sizes()

        compare([call.Popen(['ssh', 'brokerhost1.example.com', 'du -sk /path/to/data/*'], stdout=PIPE, stderr=ANY)], self.mock_ssh.mock.method_calls)
        assert self.cluster.topics['testTopic1'].partitions[0].size == 1001
        assert self.cluster.topics['testTopic1'].partitions[1].size == 1002
        assert self.cluster.topics['testTopic2'].partitions[0].size == 2001
        assert self.cluster.topics['testTopic2'].partitions[1].size == 2002
