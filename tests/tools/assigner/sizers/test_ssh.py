import unittest

from argparse import Namespace
from mock import call, patch, ANY
from subprocess import PIPE
from testfixtures import compare
from testfixtures.popen import MockPopen

from kafka.tools.exceptions import UnknownBrokerException
from kafka.tools.models.broker import Broker
from kafka.tools.models.cluster import Cluster
from kafka.tools.models.topic import Topic
from kafka.tools.assigner.sizers.ssh import SizerSSH


class SizerSSHTests(unittest.TestCase):
    def setUp(self):
        self.args = Namespace()
        self.args.property = ['datadir=/path/to/data']
        self.cluster = self.create_cluster_onehost()

    def create_cluster_onehost(self):
        cluster = Cluster()
        cluster.add_broker(Broker("brokerhost1.example.com", id=1))
        cluster.add_topic(Topic("testTopic1", 2))
        cluster.add_topic(Topic("testTopic2", 2))
        partition = cluster.topics['testTopic1'].partitions[0]
        partition.add_replica(cluster.brokers[1], 0)
        partition = cluster.topics['testTopic1'].partitions[1]
        partition.add_replica(cluster.brokers[1], 0)
        partition = cluster.topics['testTopic2'].partitions[0]
        partition.add_replica(cluster.brokers[1], 0)
        partition = cluster.topics['testTopic2'].partitions[1]
        partition.add_replica(cluster.brokers[1], 0)
        return cluster

    def test_sizer_create(self):
        sizer = SizerSSH(self.args, self.cluster)
        assert isinstance(sizer, SizerSSH)

    def test_sizer_run_missing_host(self):
        self.cluster.brokers[1].hostname = None
        sizer = SizerSSH(self.args, self.cluster)
        self.assertRaises(UnknownBrokerException, sizer.get_partition_sizes)

    def test_sizer_regex_bad(self):
        sizer = SizerSSH(self.args, self.cluster)
        assert sizer.size_re.match("foo") is None

    def test_sizer_regex_good(self):
        sizer = SizerSSH(self.args, self.cluster)
        match_obj = sizer.size_re.match("1001\t/path/to/data/testTopic1-0\n")
        assert match_obj is not None
        assert match_obj.group(1) == "1001"
        assert match_obj.group(2) == "testTopic1"
        assert match_obj.group(3) == "0"

    @patch('kafka.tools.assigner.models.reassignment.subprocess.Popen', new_callable=MockPopen)
    def test_sizer_run_setsizes_singlehost(self, mock_ssh):
        m_stdout = ("1001\t/path/to/data/testTopic1-0\n"
                    "1002\t/path/to/data/testTopic1-1\n"
                    "2001\t/path/to/data/testTopic2-0\n"
                    "2002\t/path/to/data/testTopic2-1\n")
        mock_ssh.set_default(stdout=m_stdout.encode('utf-8'))

        sizer = SizerSSH(self.args, self.cluster)
        sizer.get_partition_sizes()

        compare([call.Popen(['ssh', 'brokerhost1.example.com', 'du -sk /path/to/data/*'], stdout=PIPE, stderr=ANY)], mock_ssh.mock.method_calls)
        assert self.cluster.topics['testTopic1'].partitions[0].size == 1001
        assert self.cluster.topics['testTopic1'].partitions[1].size == 1002
        assert self.cluster.topics['testTopic2'].partitions[0].size == 2001
        assert self.cluster.topics['testTopic2'].partitions[1].size == 2002

    def test_process_df_match(self):
        sizer = SizerSSH(self.args, self.cluster)
        match_obj = sizer.size_re.match("5002\t/path/to/data/testTopic1-1\n")
        sizer.process_df_match(match_obj, 1)
        assert self.cluster.topics['testTopic1'].partitions[1].size == 5002

    def test_process_df_match_badtopic(self):
        sizer = SizerSSH(self.args, self.cluster)
        match_obj = sizer.size_re.match("5002\t/path/to/data/badTopic1-1\n")
        sizer.process_df_match(match_obj, 1)

    def test_process_df_match_badpartition(self):
        sizer = SizerSSH(self.args, self.cluster)
        match_obj = sizer.size_re.match("5002\t/path/to/data/testTopic1-2\n")
        sizer.process_df_match(match_obj, 1)
