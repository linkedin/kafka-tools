import unittest

from kafka.tools.assigner.exceptions import ConfigurationException
from kafka.tools.assigner.models.broker import Broker
from kafka.tools.assigner.models.topic import Topic


class BrokerTests(unittest.TestCase):
    def setUp(self):
        self.broker = Broker(1, 'brokerhost1.example.com')

    def add_partitions(self, pos, num):
        topic = Topic('testTopic', num)
        self.broker.partitions[pos] = []
        for i in range(num):
            self.broker.partitions[pos].append(topic.partitions[i])

    def test_broker_create(self):
        assert self.broker.id == 1
        assert self.broker.hostname == 'brokerhost1.example.com'
        assert self.broker.partitions == {}

    def test_broker_create_from_json_basic(self):
        jsonstr = '{"jmx_port":-1,"timestamp":"1466985807242","endpoints":["PLAINTEXT://10.0.0.10:9092"],"host":"10.0.0.10","version":3,"port":9092}'
        broker2 = Broker.create_from_json(1, jsonstr)
        assert broker2.id == 1
        assert broker2.hostname == '10.0.0.10'
        assert broker2.partitions == {}

    def test_broker_create_from_json_extended(self):
        jsonstr = '{"jmx_port":-1,"timestamp":"1466985807242","endpoints":["PLAINTEXT://10.0.0.10:9092"],"host":"10.0.0.10","version":3,"port":9092}'
        broker2 = Broker.create_from_json(1, jsonstr)
        assert broker2.jmx_port == -1
        assert broker2.timestamp == "1466985807242"
        assert broker2.endpoints == ["PLAINTEXT://10.0.0.10:9092"]
        assert broker2.version == 3
        assert broker2.port == 9092

    def test_broker_create_from_json_bad_host(self):
        jsonstr = '{"jmx_port":-1,"timestamp":"1466985807242","endpoints":["PLAINTEXT://10.0.0.10:9092"],"hostname":"10.0.0.10","version":3,"port":9092}'
        self.assertRaises(ConfigurationException, Broker.create_from_json, 1, jsonstr)

    def test_broker_create_from_json_bad_jmx_port(self):
        jsonstr = '{"timestamp":"1466985807242","endpoints":["PLAINTEXT://10.0.0.10:9092"],"host":"10.0.0.10","version":3,"port":9092}'
        broker2 = Broker.create_from_json(1, jsonstr)
        assert broker2.hostname == '10.0.0.10'

    def test_broker_copy_without_partitions(self):
        broker2 = self.broker.copy()
        assert broker2.id == 1
        assert broker2.hostname == 'brokerhost1.example.com'
        assert self.broker is not broker2

    def test_broker_copy_with_partitions(self):
        self.add_partitions(0, 1)
        broker2 = self.broker.copy()
        assert broker2.id == 1
        assert broker2.hostname == 'brokerhost1.example.com'
        assert broker2.partitions == {}
        assert self.broker is not broker2

    def test_broker_num_leaders(self):
        self.add_partitions(0, 2)
        assert self.broker.num_leaders() == 2

    def test_broker_num_partitions_at_position_zero(self):
        self.add_partitions(1, 2)
        assert self.broker.num_partitions_at_position(0) == 0

    def test_broker_num_partitions_single_position(self):
        self.add_partitions(0, 2)
        assert self.broker.num_partitions() == 2

    def test_broker_num_partitions_two_position(self):
        self.add_partitions(0, 2)
        self.add_partitions(1, 1)
        assert self.broker.num_partitions() == 3

    def test_broker_percent_leaders(self):
        self.add_partitions(0, 2)
        self.add_partitions(1, 2)
        assert self.broker.percent_leaders() == 50.0

    def test_broker_percent_leaders_zero(self):
        self.add_partitions(1, 1)
        assert self.broker.percent_leaders() == 0.0

    def test_broker_percent_leaders_no_partitions(self):
        assert self.broker.percent_leaders() == 0.0

    def test_broker_equality(self):
        broker2 = Broker(1, 'brokerhost1.example.com')
        assert self.broker == broker2

    def test_broker_inequality_hostname(self):
        broker2 = Broker(1, 'brokerhost2.example.com')
        assert self.broker != broker2

    def test_broker_inequality_id(self):
        broker2 = Broker(2, 'brokerhost1.example.com')
        assert self.broker != broker2

    def test_broker_equality_typeerror(self):
        self.assertRaises(TypeError, self.broker.__eq__, None)
