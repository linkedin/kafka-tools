import os
import ssl
import unittest
from mock import MagicMock, patch, call

from kafka.tools.configuration import ClientConfiguration, eval_boolean, check_file_access
from kafka.tools.exceptions import ConfigurationError


class ConfigurationTests(unittest.TestCase):
    def test_eval_boolean(self):
        assert eval_boolean(True)
        assert not eval_boolean(False)
        assert eval_boolean(1)
        assert not eval_boolean(0)
        assert eval_boolean('True')
        assert not eval_boolean('False')

    @patch('kafka.tools.configuration.os.access')
    def test_check_file_access(self, mock_access):
        mock_access.side_effect = [True, False]
        check_file_access('file1')
        self.assertRaises(ConfigurationError, check_file_access, 'file2')
        mock_access.assert_has_calls([call('file1', os.R_OK), call('file2', os.R_OK)])

    def test_create(self):
        config = ClientConfiguration()
        assert config.ssl_context is None

    def test_create_both_zk_and_hosts(self):
        self.assertRaises(ConfigurationError, ClientConfiguration, zkconnect='foo', broker_list='bar')

    def test_create_invalid_name(self):
        self.assertRaises(ConfigurationError, ClientConfiguration, invalidconfig='foo')

    def test_client_id(self):
        config = ClientConfiguration(client_id="testid")
        assert config.client_id == "testid"
        self.assertRaises(TypeError, ClientConfiguration, client_id=1)
        self.assertRaises(TypeError, ClientConfiguration, client_id=None)

    def test_metadata_refresh(self):
        config = ClientConfiguration(metadata_refresh=2345)
        assert config.metadata_refresh == 2345
        self.assertRaises(TypeError, ClientConfiguration, metadata_refresh='foo')
        self.assertRaises(TypeError, ClientConfiguration, metadata_refresh=-1)

    def test_max_request_size(self):
        config = ClientConfiguration(max_request_size=2345)
        assert config.max_request_size == 2345
        self.assertRaises(TypeError, ClientConfiguration, max_request_size='foo')
        self.assertRaises(TypeError, ClientConfiguration, max_request_size=-1)

    def test_num_retries(self):
        config = ClientConfiguration(num_retries=5)
        assert config.num_retries == 5
        self.assertRaises(TypeError, ClientConfiguration, num_retries='foo')
        self.assertRaises(TypeError, ClientConfiguration, num_retries=-1)

    def test_retry_backoff(self):
        config = ClientConfiguration(retry_backoff=5.4)
        assert config.retry_backoff == 5.4
        self.assertRaises(TypeError, ClientConfiguration, retry_backoff='foo')
        self.assertRaises(TypeError, ClientConfiguration, retry_backoff=-1)

    def test_broker_threads(self):
        config = ClientConfiguration(broker_threads=31)
        assert config.broker_threads == 31
        self.assertRaises(TypeError, ClientConfiguration, broker_threads='foo')
        self.assertRaises(TypeError, ClientConfiguration, broker_threads=-1)

    def test_broker_list(self):
        config = ClientConfiguration(broker_list='broker1.example.com:9091,broker2.example.com:9092')
        assert config.broker_list == [('broker1.example.com', 9091), ('broker2.example.com', 9092)]
        self.assertRaises(TypeError, ClientConfiguration, broker_list=1)

    def test_zkconnect(self):
        config = ClientConfiguration(zkconnect='zk.example.com:2181/kafka-cluster')
        assert config.zkconnect == 'zk.example.com:2181/kafka-cluster'
        self.assertRaises(TypeError, ClientConfiguration, zkconnect=1)

    def test_verify_certificates(self):
        config = ClientConfiguration(tls_verify_certificates=True)
        assert config.tls_verify_certificates
        config = ClientConfiguration(tls_verify_certificates=False)
        assert not config.tls_verify_certificates

    def test_verify_hostnames(self):
        config = ClientConfiguration(tls_verify_hostnames=True)
        assert config.tls_verify_hostnames
        config = ClientConfiguration(tls_verify_hostnames=False)
        assert not config.tls_verify_hostnames

    @patch('kafka.tools.configuration.check_file_access')
    def test_root_certificates(self, mock_access):
        mock_access.return_value = True
        config = ClientConfiguration(tls_root_certificates='filename')
        assert config.tls_root_certificates == 'filename'
        mock_access.assert_called_once_with('filename')

    @patch('kafka.tools.configuration.check_file_access')
    def test_client_certificate(self, mock_access):
        mock_access.return_value = True
        config = ClientConfiguration(tls_client_certificate='filename')
        assert config.tls_client_certificate == 'filename'
        mock_access.assert_called_once_with('filename')

    @patch('kafka.tools.configuration.check_file_access')
    def test_client_keyfile(self, mock_access):
        mock_access.return_value = True
        config = ClientConfiguration(tls_client_keyfile='filename')
        assert config.tls_client_keyfile == 'filename'
        mock_access.assert_called_once_with('filename')

    def test_client_key_password(self):
        def testfunc():
            return 'foo'

        config = ClientConfiguration(tls_client_key_password_callback=testfunc)
        assert config.tls_client_key_password_callback == testfunc
        self.assertRaises(TypeError, ClientConfiguration, tls_client_key_password_callback='notcallable')

    def test_verify_ssl_configuration(self):
        config = ClientConfiguration(tls_verify_certificates=False, tls_verify_hostnames=True)
        self.assertRaises(ConfigurationError, config._verify_ssl_configuration)
        config.tls_verify_certificates = True
        config._verify_ssl_configuration()

    def test_enable_tls_default(self):
        config = ClientConfiguration(enable_tls=True)
        assert isinstance(config.ssl_context, ssl.SSLContext)
        assert config.ssl_context.protocol == ssl.PROTOCOL_SSLv23
        assert config.ssl_context.verify_mode == ssl.CERT_REQUIRED
        assert config.ssl_context.check_hostname is True

    @patch('kafka.tools.configuration.ssl.SSLContext')
    @patch('kafka.tools.configuration.check_file_access')
    def test_enable_tls_custom_certs(self, mock_access, mock_ssl):
        def testfunc():
            return 'foo'

        config = ClientConfiguration(enable_tls=True,
                                     tls_verify_certificates=False,
                                     tls_verify_hostnames=False,
                                     tls_root_certificates='example_root_cert_file',
                                     tls_client_certificate='example_client_cert_file',
                                     tls_client_keyfile='example_client_key_file',
                                     tls_client_key_password_callback=testfunc)
        mock_ssl.assert_called_once_with(ssl.PROTOCOL_SSLv23)
        assert config.ssl_context.verify_mode == ssl.CERT_NONE
        assert config.ssl_context.check_hostname is False
        config.ssl_context.load_verify_locations.assert_called_once_with(cafile='example_root_cert_file')
        config.ssl_context.load_cert_chain.assert_called_once_with('example_client_cert_file',
                                                                   keyfile='example_client_key_file',
                                                                   password=testfunc)

    @patch('kafka.tools.configuration.ssl.SSLContext')
    @patch('kafka.tools.configuration.check_file_access')
    def test_enable_tls_error(self, mock_access, mock_ssl):
        def testfunc():
            return 'foo'

        ssl_instance = MagicMock()
        ssl_instance.load_cert_chain.side_effect = ssl.SSLError
        mock_ssl.return_value = ssl_instance

        self.assertRaises(ConfigurationError, ClientConfiguration, enable_tls=True, tls_verify_certificates=False,
                          tls_verify_hostnames=False, tls_root_certificates='example_root_cert_file',
                          tls_client_certificate='example_client_cert_file',
                          tls_client_keyfile='example_client_key_file', tls_client_key_password_callback=testfunc)
