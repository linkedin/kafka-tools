import unittest
from mock import patch, MagicMock

from tests.tools.client.fixtures import group_coordinator, group_coordinator_error

from kafka.tools.client import Client
from kafka.tools.exceptions import GroupError, ConnectionError
from kafka.tools.models.broker import Broker


class SendHelperTests(unittest.TestCase):
    def setUp(self):
        # Dummy client for testing - we're not going to connect that bootstrap broker
        self.client = Client()
        self.group_coordinator = group_coordinator()
        self.coordinator_error = group_coordinator_error()

    def test_send_group_aware_request(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker1.rack = 'rack1'
        broker1.send = MagicMock()
        broker1.send.return_value = (1, 'fakeresponse')
        self.client.cluster.add_broker(broker1)

        self.client._send_any_broker = MagicMock()
        self.client._send_any_broker.return_value = self.group_coordinator
        self.client._send_group_aware_request('testgroup', 'fakerequest')

        assert 'testgroup' in self.client.cluster.groups
        assert self.client.cluster.groups['testgroup'].coordinator == broker1
        broker1.send.assert_called_once_with('fakerequest')

    @patch('kafka.tools.client.Broker')
    def test_send_group_aware_request_new_broker(self, mock_broker_class):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker1.rack = 'rack1'
        broker1.send = MagicMock()
        broker1.send.return_value = (1, 'fakeresponse')
        mock_broker_class.return_value = broker1

        self.client._send_any_broker = MagicMock()
        self.client._send_any_broker.return_value = self.group_coordinator
        self.client._send_group_aware_request('testgroup', 'fakerequest')

        mock_broker_class.assert_called_once_with('host1.example.com', id=1, port=8031, configuration=self.client.configuration)
        assert 1 in self.client.cluster.brokers
        assert self.client.cluster.brokers[1] == broker1
        assert self.client.cluster.groups['testgroup'].coordinator == broker1
        broker1.send.assert_called_once_with('fakerequest')

    def test_send_group_aware_request_error(self):
        self.client._send_any_broker = MagicMock()
        self.client._send_any_broker.return_value = self.coordinator_error
        self.assertRaises(GroupError, self.client._send_group_aware_request, 'testgroup', 'fakerequest')

    def test_send_all_brokers(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker1.rack = 'rack1'
        broker1.send = MagicMock()
        broker1.send.return_value = (1, 'fakeresponse')
        broker2 = Broker('host2.example.com', id=101, port=8032)
        broker2.rack = 'rack1'
        broker2.send = MagicMock()
        broker2.send.return_value = (2, 'otherresponse')
        self.client.cluster.add_broker(broker1)
        self.client.cluster.add_broker(broker2)

        val = self.client._send_all_brokers('fakerequest')
        broker1.send.assert_called_once_with('fakerequest')
        broker2.send.assert_called_once_with('fakerequest')
        assert val[1] == 'fakeresponse'
        assert val[101] == 'otherresponse'

    def test_send_all_brokers_error(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker1.rack = 'rack1'
        broker1.send = MagicMock()
        broker1.send.return_value = (1, 'fakeresponse')
        broker2 = Broker('host2.example.com', id=101, port=8032)
        broker2.rack = 'rack1'
        broker2.send = MagicMock()
        broker2.send.side_effect = ConnectionError
        self.client.cluster.add_broker(broker1)
        self.client.cluster.add_broker(broker2)

        val = self.client._send_all_brokers('fakerequest')
        broker1.send.assert_called_once_with('fakerequest')
        broker2.send.assert_called_once_with('fakerequest')
        assert val[1] == 'fakeresponse'
        assert val[101] is None

    @patch('kafka.tools.client.shuffle', lambda x: sorted(x))
    def test_send_any_broker(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker1.rack = 'rack1'
        broker1.send = MagicMock()
        broker1.send.return_value = (1, 'fakeresponse')
        broker2 = Broker('host2.example.com', id=101, port=8032)
        broker2.rack = 'rack1'
        broker2.send = MagicMock()
        broker2.send.return_value = (2, 'otherresponse')
        self.client.cluster.add_broker(broker1)
        self.client.cluster.add_broker(broker2)

        val = self.client._send_any_broker('fakerequest')
        broker2.send.assert_called_once_with('fakerequest')
        assert val == 'otherresponse'

    @patch('kafka.tools.client.shuffle', lambda x: sorted(x))
    def test_send_any_broker_one_error(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker1.rack = 'rack1'
        broker1.send = MagicMock()
        broker1.send.return_value = (1, 'fakeresponse')
        broker2 = Broker('host2.example.com', id=101, port=8032)
        broker2.rack = 'rack1'
        broker2.send = MagicMock()
        broker2.send.side_effect = ConnectionError
        self.client.cluster.add_broker(broker1)
        self.client.cluster.add_broker(broker2)

        val = self.client._send_any_broker('fakerequest')
        broker2.send.assert_called_once_with('fakerequest')
        broker1.send.assert_called_once_with('fakerequest')
        assert val == 'fakeresponse'

    @patch('kafka.tools.client.shuffle', lambda x: sorted(x))
    def test_send_all_error(self):
        broker1 = Broker('host1.example.com', id=1, port=8031)
        broker1.rack = 'rack1'
        broker1.send = MagicMock()
        broker1.send.side_effect = ConnectionError
        broker2 = Broker('host2.example.com', id=101, port=8032)
        broker2.rack = 'rack1'
        broker2.send = MagicMock()
        broker2.send.side_effect = ConnectionError
        self.client.cluster.add_broker(broker1)
        self.client.cluster.add_broker(broker2)

        self.assertRaises(ConnectionError, self.client._send_any_broker, 'fakerequest')
        broker1.send.assert_called_once_with('fakerequest')
        broker2.send.assert_called_once_with('fakerequest')

    def test_raise_if_not_connected(self):
        self.assertRaises(ConnectionError, self.client._raise_if_not_connected)
