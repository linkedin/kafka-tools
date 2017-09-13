import collections
import unittest
import socket
from mock import MagicMock, call, patch

from kafka.tools.configuration import ClientConfiguration
from kafka.tools.exceptions import ConfigurationException, ConnectionError
from kafka.tools.models.broker import Broker
from kafka.tools.models.topic import Topic
from kafka.tools.protocol.requests.api_versions_v0 import ApiVersionsV0Request
from kafka.tools.protocol.responses.api_versions_v0 import ApiVersionsV0Response
from kafka.tools.protocol.types.bytebuffer import ByteBuffer


class BrokerTests(unittest.TestCase):
    def setUp(self):
        self.configuration = ClientConfiguration(num_retries=3, retry_backoff=0.5)
        self.mock_sock = MagicMock()
        self.broker = Broker('brokerhost1.example.com', id=1, sock=self.mock_sock, configuration=self.configuration)

    def add_partitions(self, pos, num):
        topic = Topic('testTopic', num)
        self.broker.partitions[pos] = []
        for i in range(num):
            self.broker.partitions[pos].append(topic.partitions[i])

    def test_broker_create(self):
        assert self.broker.id == 1
        assert self.broker.hostname == 'brokerhost1.example.com'
        assert self.broker.partitions == {}
        assert isinstance(self.broker._sock, MagicMock)

    def test_broker_create_from_json_basic(self):
        jsonstr = '{"jmx_port":-1,"timestamp":"1466985807242","endpoints":["PLAINTEXT://10.0.0.10:9092"],"host":"10.0.0.10","version":3,"port":9092}'
        broker2 = Broker.create_from_json(1, jsonstr)
        assert broker2.id == 1
        assert broker2.hostname == '10.0.0.10'
        assert broker2.partitions == {}

    def test_broker_get_endpoint(self):
        jsonstr = ('{"jmx_port":-1,"timestamp":"1466985807242","endpoints":["PLAINTEXT://10.0.0.10:9092", "SSL://10.0.0.10:2834"],'
                   '"host":"10.0.0.10","version":3,"port":9092}')
        broker = Broker.create_from_json(1, jsonstr)
        endpoint = broker.get_endpoint("SSL")
        assert endpoint.protocol == "SSL"
        assert endpoint.hostname == "10.0.0.10"
        assert endpoint.port == 2834

    def test_broker_create_from_json_extended(self):
        jsonstr = '{"jmx_port":-1,"timestamp":"1466985807242","endpoints":["PLAINTEXT://10.0.0.10:9092"],"host":"10.0.0.10","version":3,"port":9092}'
        broker2 = Broker.create_from_json(1, jsonstr)
        assert broker2.jmx_port == -1
        assert broker2.timestamp == "1466985807242"
        assert len(broker2.endpoints) == 1
        assert broker2.endpoints[0].protocol == 'PLAINTEXT'
        assert broker2.endpoints[0].hostname == '10.0.0.10'
        assert broker2.endpoints[0].port == 9092
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
        broker2 = Broker('brokerhost1.example.com', id=1)
        assert self.broker == broker2

    def test_broker_inequality_hostname(self):
        broker2 = Broker('brokerhost2.example.com', id=1)
        assert self.broker != broker2

    def test_broker_inequality_id(self):
        broker2 = Broker('brokerhost1.example.com', id=2)
        assert self.broker != broker2

    def test_broker_equality_typeerror(self):
        self.assertRaises(TypeError, self.broker.__eq__, None)

    def test_broker_get_socket(self):
        sock = self.broker._get_socket(None)
        assert isinstance(sock, socket.socket)

    @patch('kafka.tools.models.broker.socket.socket')
    def test_broker_get_socket_wrapped(self, mock_socket):
        mock_context = MagicMock()
        mock_context.wrap_socket.return_value = 'fakewrappedsocket'
        mock_socket.return_value = 'fakesocket'

        sock = self.broker._get_socket(mock_context)
        mock_context.wrap_socket.assert_called_once_with('fakesocket', server_hostname='brokerhost1.example.com')
        print(sock)
        assert sock == 'fakewrappedsocket'

    def test_broker_connect(self):
        self.broker.connect()
        self.mock_sock.connect.assert_called_once_with(('brokerhost1.example.com', 9092))

    def test_broker_connect_error(self):
        self.mock_sock.connect.side_effect = socket.error
        self.assertRaises(ConnectionError, self.broker.connect)

    def test_broker_close(self):
        self.broker.close()
        self.mock_sock.shutdown.assert_called_once()
        self.mock_sock.close.assert_called_once()

    def test_broker_close_not_open(self):
        self.mock_sock.shutdown.side_effect = OSError()

        self.broker.close()
        self.mock_sock.close.assert_called_once()

    @patch.object(Broker, '_read_bytes')
    def test_broker_single_send(self, mock_read_bytes):
        # recv is called to get the response size (first 4 bytes)
        self.mock_sock.recv.return_value = b'\x00\x00\x00\x10'

        # read_bytes returns the response payload, minus the response size
        # correlation_id (4), error (2), array of (api_key (2), min_version (2), max_version (2))
        mock_read_bytes.return_value = b'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x01\x00\x01\x01\x01\x02\x02'

        request = ApiVersionsV0Request({})
        (correlation_id, response) = self.broker._single_send(request)

        # Check that the request was encoded properly
        self.mock_sock.sendall.assert_called_once_with(bytearray(b'\x00\x00\x00\x15\x00\x12\x00\x00\x00\x00\x00\x01\x00\x0bkafka-tools'))
        self.mock_sock.recv.assert_called_once_with(4)
        mock_read_bytes.assert_called_once_with(16)

        # Check that the response is what was expected to be decoded
        assert isinstance(response, ApiVersionsV0Response)
        assert response.correlation_id == 1
        assert response['error'] == 0
        assert isinstance(response['api_versions'], collections.Sequence)
        assert len(response['api_versions']) == 1
        assert len(response['api_versions'][0]) == 3
        assert response['api_versions'][0]['api_key'] == 1
        assert response['api_versions'][0]['min_version'] == 257
        assert response['api_versions'][0]['max_version'] == 514

        # Correlation ID must be incremented after each request
        assert self.broker._correlation_id == 2

    @patch.object(Broker, '_read_bytes')
    def test_broker_single_send_error(self, mock_read_bytes):
        self.mock_sock.recv.side_effect = socket.error
        mock_read_bytes.return_value = b'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x01\x00\x01\x01\x01\x02\x02'

        request = ApiVersionsV0Request({})
        self.assertRaises(ConnectionError, self.broker._single_send, request)

    @patch.object(ByteBuffer, 'getInt32')
    def test_broker_single_send_short(self, mock_getint):
        self.mock_sock.recv.return_value = b'\x00\x00\x00\x10'
        mock_getint.side_effect = EOFError

        request = ApiVersionsV0Request({})
        self.assertRaises(ConnectionError, self.broker._single_send, request)

    @patch.object(Broker, '_single_send')
    @patch.object(Broker, 'connect')
    @patch.object(Broker, 'close')
    def test_broker_send(self, mock_close, mock_connect, mock_send):
        mock_send.return_value = 'fakeresponse'
        self.broker.send('fakerequest')
        mock_connect.assert_not_called()
        mock_close.assert_not_called()
        mock_send.assert_called_once_with('fakerequest')

    @patch.object(Broker, '_single_send')
    @patch.object(Broker, 'connect')
    @patch.object(Broker, 'close')
    def test_broker_send_retry(self, mock_close, mock_connect, mock_send):
        def close_broker():
            self.broker._sock = None

        mock_send.side_effect = [ConnectionError, 'fakeresponse']
        mock_close.side_effect = close_broker
        self.broker.send('fakerequest')
        mock_close.assert_called_once_with()
        mock_connect.assert_called_once_with()
        mock_send.assert_has_calls([call('fakerequest'), call('fakerequest')])

    @patch.object(Broker, '_single_send')
    @patch.object(Broker, 'connect')
    @patch.object(Broker, 'close')
    def test_broker_send_exhaust_retry(self, mock_close, mock_connect, mock_send):
        def close_broker():
            self.broker._sock = None

        mock_send.side_effect = ConnectionError
        mock_close.side_effect = close_broker
        self.assertRaises(ConnectionError, self.broker.send, 'fakerequest')
        mock_close.assert_has_calls([call(), call()])
        mock_connect.assert_has_calls([call(), call()])
        mock_send.assert_has_calls([call('fakerequest'), call('fakerequest'), call('fakerequest')])

    def test_broker_read_bytes(self):
        self.mock_sock.recv.return_value = b'\x01\x02\x03\x04'
        response = self.broker._read_bytes(4)

        self.mock_sock.recv.assert_called_once_with(4)
        assert response == b'\x01\x02\x03\x04'

    def test_broker_read_bytes_concat(self):
        self.mock_sock.recv.side_effect = [b'\x01\x02', b'\x03\x04']
        response = self.broker._read_bytes(4)

        self.mock_sock.recv.assert_has_calls([call(4), call(2)])
        assert response == b'\x01\x02\x03\x04'

    def test_broker_read_bytes_nodata(self):
        self.mock_sock.recv.return_value = b''
        self.assertRaises(socket.error, self.broker._read_bytes, 4)

    def test_broker_read_bytes_error(self):
        self.mock_sock.recv.side_effect = socket.error
        self.assertRaises(socket.error, self.broker._read_bytes, 4)
