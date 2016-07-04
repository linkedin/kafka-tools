import unittest

from argparse import Namespace
from mock import call, patch, ANY

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

from kafka.tools.assigner.exceptions import UnknownBrokerException
from kafka.tools.assigner.models.broker import Broker
from kafka.tools.assigner.models.cluster import Cluster
from kafka.tools.assigner.models.topic import Topic
from kafka.tools.assigner.sizers.ssh import SizerSSH


class SizerSSHTests(unittest.TestCase):
    def setUp(self):
        self.args = Namespace()
        self.args.datadir = "/path/to/data"

        self.patcher_load_keys = patch('kafka.tools.assigner.sizers.ssh.paramiko.SSHClient.load_system_host_keys', autospec=True)
        self.patcher_connect = patch('kafka.tools.assigner.sizers.ssh.paramiko.SSHClient.connect', autospec=True)
        self.patcher_exec_command = patch('kafka.tools.assigner.sizers.ssh.paramiko.SSHClient.exec_command', autospec=True)
        self.mock_load_keys = self.patcher_load_keys.start()
        self.mock_connect = self.patcher_connect.start()
        self.mock_exec_command = self.patcher_exec_command.start()

    def tearDown(self):
        self.patcher_load_keys.stop()
        self.patcher_connect.stop()
        self.patcher_exec_command.stop()

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
        self.mock_load_keys.assert_called_with(ANY)
        assert isinstance(sizer, SizerSSH)

    def test_sizer_run_missing_host(self):
        self.create_cluster_onehost()
        self.cluster.brokers[1].hostname = None
        sizer = SizerSSH(self.args, self.cluster)
        self.assertRaises(UnknownBrokerException, sizer.get_partition_sizes)

    def test_sizer_run_badinput(self):
        self.create_cluster_onehost()
        m_stdout = StringIO(u'foo\nbar\nbaz\n')
        m_stderr = StringIO()
        m_stdin = StringIO()
        self.mock_exec_command.return_value = (m_stdin, m_stdout, m_stderr)

        sizer = SizerSSH(self.args, self.cluster)
        sizer.get_partition_sizes()

        self.mock_connect.assert_called_once_with(ANY, "brokerhost1.example.com", allow_agent=True)
        self.mock_exec_command.assert_called_once_with(ANY, "du -sk /path/to/data/*")
        assert self.cluster.topics['testTopic1'].partitions[0].size == 0
        assert self.cluster.topics['testTopic1'].partitions[1].size == 0
        assert self.cluster.topics['testTopic2'].partitions[0].size == 0
        assert self.cluster.topics['testTopic2'].partitions[1].size == 0

    def test_sizer_run_setsizes_singlehost(self):
        self.create_cluster_onehost()
        m_stdout = StringIO(u'1001\t/path/to/data/testTopic1-0\n'
                            "1002\t/path/to/data/testTopic1-1\n"
                            "2001\t/path/to/data/testTopic2-0\n"
                            "2002\t/path/to/data/testTopic2-1\n")
        m_stderr = StringIO()
        m_stdin = StringIO()
        self.mock_exec_command.return_value = (m_stdin, m_stdout, m_stderr)

        sizer = SizerSSH(self.args, self.cluster)
        sizer.get_partition_sizes()

        self.mock_connect.assert_called_once_with(ANY, "brokerhost1.example.com", allow_agent=True)
        self.mock_exec_command.assert_called_once_with(ANY, "du -sk /path/to/data/*")
        assert self.cluster.topics['testTopic1'].partitions[0].size == 1001
        assert self.cluster.topics['testTopic1'].partitions[1].size == 1002
        assert self.cluster.topics['testTopic2'].partitions[0].size == 2001
        assert self.cluster.topics['testTopic2'].partitions[1].size == 2002

    def test_sizer_run_setsizes_twohost(self):
        self.create_cluster_twohost()
        m_stdout_1 = StringIO(u'1001\t/path/to/data/testTopic1-0\n'
                              u'1002\t/path/to/data/testTopic1-1\n'
                              u'2001\t/path/to/data/testTopic2-0\n'
                              u'2002\t/path/to/data/testTopic2-1\n')
        m_stdout_2 = StringIO(u'1001\t/path/to/data/testTopic1-0\n'
                              u'4002\t/path/to/data/testTopic1-1\n'
                              u'1011\t/path/to/data/testTopic2-0\n'
                              u'2002\t/path/to/data/testTopic2-1\n')
        m_stderr = StringIO()
        m_stdin = StringIO()
        self.mock_exec_command.side_effect = [(m_stdin, m_stdout_1, m_stderr), (m_stdin, m_stdout_2, m_stderr)]

        sizer = SizerSSH(self.args, self.cluster)
        sizer.get_partition_sizes()

        self.mock_connect.assert_has_calls([call(ANY, "brokerhost1.example.com", allow_agent=True), call(ANY, "brokerhost2.example.com", allow_agent=True)])
        self.mock_exec_command.assert_has_calls([call(ANY, "du -sk /path/to/data/*"), call(ANY, "du -sk /path/to/data/*")])
        assert self.cluster.topics['testTopic1'].partitions[0].size == 1001
        assert self.cluster.topics['testTopic1'].partitions[1].size == 4002
        assert self.cluster.topics['testTopic2'].partitions[0].size == 2001
        assert self.cluster.topics['testTopic2'].partitions[1].size == 2002

    def test_sizer_run_setsizes_badtopic(self):
        self.create_cluster_onehost()
        m_stdout = StringIO(u'1001\t/path/to/data/testTopic1-0\n'
                            u'1002\t/path/to/data/testTopic1-1\n'
                            u'5002\t/path/to/data/badTopic1-1\n'
                            u'2001\t/path/to/data/testTopic2-0\n'
                            u'2002\t/path/to/data/testTopic2-1\n')
        m_stderr = StringIO()
        m_stdin = StringIO()
        self.mock_exec_command.return_value = (m_stdin, m_stdout, m_stderr)

        sizer = SizerSSH(self.args, self.cluster)
        sizer.get_partition_sizes()

        self.mock_connect.assert_called_once_with(ANY, "brokerhost1.example.com", allow_agent=True)
        self.mock_exec_command.assert_called_once_with(ANY, "du -sk /path/to/data/*")
        assert self.cluster.topics['testTopic1'].partitions[0].size == 1001
        assert self.cluster.topics['testTopic1'].partitions[1].size == 1002
        assert self.cluster.topics['testTopic2'].partitions[0].size == 2001
        assert self.cluster.topics['testTopic2'].partitions[1].size == 2002

    def test_sizer_run_setsizes_badpartition(self):
        self.create_cluster_onehost()
        m_stdout = StringIO(u'1001\t/path/to/data/testTopic1-0\n'
                            u'1002\t/path/to/data/testTopic1-1\n'
                            u'5002\t/path/to/data/testTopic1-2\n'
                            u'2001\t/path/to/data/testTopic2-0\n'
                            u'2002\t/path/to/data/testTopic2-1\n')
        m_stderr = StringIO()
        m_stdin = StringIO()
        self.mock_exec_command.return_value = (m_stdin, m_stdout, m_stderr)

        sizer = SizerSSH(self.args, self.cluster)
        sizer.get_partition_sizes()

        self.mock_connect.assert_called_once_with(ANY, "brokerhost1.example.com", allow_agent=True)
        self.mock_exec_command.assert_called_once_with(ANY, "du -sk /path/to/data/*")
        assert self.cluster.topics['testTopic1'].partitions[0].size == 1001
        assert self.cluster.topics['testTopic1'].partitions[1].size == 1002
        assert self.cluster.topics['testTopic2'].partitions[0].size == 2001
        assert self.cluster.topics['testTopic2'].partitions[1].size == 2002
