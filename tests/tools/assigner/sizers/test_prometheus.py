import unittest

from argparse import Namespace
from mock import patch, Mock

from kafka.tools.exceptions import UnknownBrokerException, ConfigurationException
from kafka.tools.models.broker import Broker
from kafka.tools.models.cluster import Cluster
from kafka.tools.models.topic import Topic
from kafka.tools.assigner.sizers.prometheus import SizerPrometheus


class SizerPrometheusTests(unittest.TestCase):
    def setUp(self):
        self.args = Namespace()
        self.args.property = ['size_metric_name=kafka_log_size', 'metrics_port=1234']
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
        sizer = SizerPrometheus(self.args, self.cluster)
        assert isinstance(sizer, SizerPrometheus)

    def test_sizer_run_missing_host(self):
        self.cluster.brokers[1].hostname = None
        sizer = SizerPrometheus(self.args, self.cluster)
        self.assertRaises(UnknownBrokerException, sizer.get_partition_sizes)

    def test_sizer_missing_size_metric_name(self):
        args = Namespace()
        args.property = ['metrics_port=1234']
        sizer = SizerPrometheus(args, self.cluster)
        self.assertRaises(ConfigurationException, sizer.get_partition_sizes)

    def test_sizer_missing_metrics_port(self):
        args = Namespace()
        args.property = ['size_metric_name=kafka_log_size']
        sizer = SizerPrometheus(args, self.cluster)
        self.assertRaises(ConfigurationException, sizer.get_partition_sizes)

    def test_sizer_parse_prometheus_value_good(self):
        sizer = SizerPrometheus(self.args, self.cluster)
        assert sizer._parse_prometheus_value('1234.0') == 1234.0
        assert sizer._parse_prometheus_value('2.759052553E9') == 2759052553

    def test_sizer_parse_prometheus_value_bad(self):
        sizer = SizerPrometheus(self.args, self.cluster)
        self.assertRaises(UnknownBrokerException, sizer._parse_prometheus_value, 'foo')
        self.assertRaises(UnknownBrokerException, sizer._parse_prometheus_value, '123.0.0')
        self.assertRaises(UnknownBrokerException, sizer._parse_prometheus_value, 'E')

    def test_sizer_parse_prometheus_metric_good(self):
        sizer = SizerPrometheus(self.args, self.cluster)

        m = sizer._parse_prometheus_metric(
            'kafka_log_size{topic="testTopic1", partition="0"} 1234.0')
        assert m['name'] == 'kafka_log_size'
        assert m['labels']['topic'] == 'testTopic1'
        assert m['labels']['partition'] == '0'
        assert m['value'] == 1234

        m = sizer._parse_prometheus_metric(
            'kafka_log_size{topic="testTopic1", partition="0",} 1.0')
        assert m['name'] == 'kafka_log_size'
        assert m['labels']['topic'] == 'testTopic1'
        assert m['labels']['partition'] == '0'
        assert m['value'] == 1

        m = sizer._parse_prometheus_metric(
            'kafka_log_size{topic="testTopic222", partition="444", } 12.0')
        assert m['name'] == 'kafka_log_size'
        assert m['labels']['topic'] == 'testTopic222'
        assert m['labels']['partition'] == '444'
        assert m['value'] == 12

        m = sizer._parse_prometheus_metric(
            'kafka_log_size{topic="mytopic",} 2.759052553E9')
        self.assertIsInstance(m, dict)
        assert m['name'] == 'kafka_log_size'
        assert m['labels']['topic'] == 'mytopic'
        assert m['value'] == 2759052553

        m = sizer._parse_prometheus_metric(
            'kafka_log_size{partition="3",topic="mytopic",} 2.759052553E9')
        self.assertIsInstance(m, dict)
        assert m['name'] == 'kafka_log_size'
        assert m['labels']['topic'] == 'mytopic'
        assert m['labels']['partition'] == '3'
        assert m['value'] == 2759052553

        m = sizer._parse_prometheus_metric(
            'kafka_log_size{partition="11",topic="mytopic",extra="foo"} 0.0')
        self.assertIsInstance(m, dict)
        assert m['name'] == 'kafka_log_size'
        assert m['labels']['topic'] == 'mytopic'
        assert m['labels']['partition'] == '11'
        assert m['labels']['extra'] == 'foo'
        assert m['value'] == 0

    def test_sizer_parse_prometheus_metric_bad(self):
        sizer = SizerPrometheus(self.args, self.cluster)

        m = sizer._parse_prometheus_metric('kafka_log_size 2.0')
        assert m is None

        m = sizer._parse_prometheus_metric('kafka_log_size{} 0.0')
        assert m is None

        m = sizer._parse_prometheus_metric('kafka_log_size{topic="abc"} foo')
        assert m is None

        m = sizer._parse_prometheus_metric('kafka_log_size{topic="abc", partition="1"} foo')
        assert m is None

    @patch('kafka.tools.assigner.sizers.prometheus.urlopen')
    def test_sizer_query_prometheus_bad(self, mock_urlopen):
        m = Mock()
        m.getcode.return_value = 200
        m.read.return_value = 'kafka_log_size{topic="foo",partition="abc"} 0.0\n'
        mock_urlopen.return_value = m

        sizer = SizerPrometheus(self.args, self.cluster)
        self.assertRaises(UnknownBrokerException, sizer._query_prometheus, 'localhost')

    @patch('kafka.tools.assigner.sizers.prometheus.urlopen')
    def test_sizer_get_prometheus_metrics_200(self, mock_urlopen):
        m = Mock()
        m.getcode.return_value = 200
        m.read.side_effect = [
            '# HELP foo\nkafka_log_size{} 1.0\nlog_size{foo="bar"} 1.5\n',
            '404 not found\n',
        ]
        mock_urlopen.return_value = m

        sizer = SizerPrometheus(self.args, self.cluster)

        metrics = sizer._get_prometheus_metrics('localhost', 1234, '/metrics')
        self.assertSequenceEqual(
            metrics, [
                '# HELP foo',
                'kafka_log_size{} 1.0',
                'log_size{foo="bar"} 1.5'
            ])

        metrics = sizer._get_prometheus_metrics('localhost', 1234, '/metrics')
        self.assertSequenceEqual(metrics, ['404 not found'])

    @patch('kafka.tools.assigner.sizers.prometheus.urlopen')
    def test_sizer_get_prometheus_metrics_404(self, mock_urlopen):
        m = Mock()
        m.getcode.return_value = 404
        m.read.return_value = '404 not found'
        mock_urlopen.return_value = m

        sizer = SizerPrometheus(self.args, self.cluster)
        self.assertRaises(UnknownBrokerException, sizer._get_prometheus_metrics, 'localhost', 1234, '/metrics')

    @patch('kafka.tools.assigner.sizers.prometheus.urlopen')
    def test_sizer_get_partition_sizes(self, mock_urlopen):
        m = Mock()
        m.getcode.return_value = 200
        m.read.return_value = 'kafka_log_size{topic="testTopic1", partition="0"} 1234.0\n'
        mock_urlopen.return_value = m

        sizer = SizerPrometheus(self.args, self.cluster)
        sizer.get_partition_sizes()

        assert self.cluster.topics['testTopic1'].partitions[0].size == 1234
