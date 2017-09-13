# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import six
import ssl

from kafka.tools.exceptions import ConfigurationError


def eval_boolean(value):
    """Attempt to evaluate the argument as a boolean"""
    if isinstance(value, bool):
        return value
    elif isinstance(value, six.integer_types):
        return value != 0
    else:
        return value.lower() in ['true', 'yes', 'on']


def check_file_access(filename):
    if not os.access(filename, os.R_OK):
        raise ConfigurationError("filename specified ({0}) is not accessible for reading".format(filename))


class ClientConfiguration(object):
    #######################
    # CONNECTION PROPERTIES
    #
    # broker_list and zkconnect are the two possible ways of specifying the Kafka cluster to connect to. One of these
    # options must be provided, and only one can be provided. By default, the broker_list is set to point to localhost

    @property
    def broker_list(self):
        """The broker list to use for bootstrapping the client

        This broker list is only used for the initial client connect. The client will connect to one of the brokers
        specified and fetch the cluster information, including a list of brokers.

        The format of the broker list is a comma-separated list of hostname:port
            hostname:port[,hostname:port...]
        """
        return getattr(self, '_broker_list', 'localhost:9092')

    @broker_list.setter
    def broker_list(self, value):
        if not isinstance(value, six.string_types):
            raise TypeError("broker_list must be a string")

        # We're not going to validate much here - if the user does the wrong thing, they'll get an error on connect
        self._broker_list = []
        hostports = value.split(',')
        for hostport in hostports:
            (host, port) = hostport.rsplit(':', 1)
            self._broker_list.append((host, int(port)))

    @property
    def zkconnect(self):
        """The zookeeper connection string for the Kafka cluster

        This is the Zookeeper connection string that points to the Kafka cluster metadata. It is the same stirng that
        is used when configuring the Kafka brokers. The format is:
            host:port[,host:port...][/chroot/path]
        """
        return getattr(self, '_zkconnect', None)

    @zkconnect.setter
    def zkconnect(self, value):
        # We're not going to validate this past being a string. It's too much of a pain, and Kazoo will handle it
        if not isinstance(value, six.string_types):
            raise TypeError("zkconnect must be a string")
        self._zkconnect = value

    ################
    # SSL PROPERTIES
    #
    # All of these properties are using for setting up TLS connections to the Kafka brokers. The defaults provided are
    # reasonable for a secure connection, except that enable_tls is disabled by default.

    @property
    def enable_tls(self):
        """Enable TLS for Kafka broker connections"""
        return getattr(self, '_enable_tls', False)

    @enable_tls.setter
    def enable_tls(self, value):
        self._enable_tls = eval_boolean(value)

    @property
    def tls_verify_certificates(self):
        """Define whether or not to verify the server host certificate is valid and trusted when TLS is enabled"""
        return getattr(self, '_tls_verify_certificates', True)

    @tls_verify_certificates.setter
    def tls_verify_certificates(self, value):
        self._tls_verify_certificates = eval_boolean(value)

    @property
    def tls_verify_hostnames(self):
        """Define whether or not to verify the server hostname matches the host certificate provided"""
        return getattr(self, '_tls_verify_hostnames', True)

    @tls_verify_hostnames.setter
    def tls_verify_hostnames(self, value):
        self._tls_verify_hostnames = eval_boolean(value)

    @property
    def tls_root_certificates(self):
        """Path to the trusted root certificates. If not provided, the system default will be used"""
        return getattr(self, '_tls_root_certificates', None)

    @tls_root_certificates.setter
    def tls_root_certificates(self, value):
        check_file_access(value)
        self._tls_root_certificates = value

    @property
    def tls_client_certificate(self):
        """Path to the client certificate, optionally including a key. If not provided, no client certificate is used"""
        return getattr(self, '_tls_client_certificate', None)

    @tls_client_certificate.setter
    def tls_client_certificate(self, value):
        check_file_access(value)
        self._tls_client_certificate = value

    @property
    def tls_client_keyfile(self):
        """Path to the client certificate key file, if separate from the client certificate file."""
        return getattr(self, '_tls_client_keyfile', None)

    @tls_client_keyfile.setter
    def tls_client_keyfile(self, value):
        check_file_access(value)
        self._tls_client_keyfile = value

    @property
    def tls_client_key_password_callback(self):
        """A function that will be called to get the keyfile password.
        This is a function that will be called to get the password that protects the keyfile specified. This must be a
        Python callable that takes no arguments. It must return a string, byte, or bytearray

        If not specified, the keyfile is assumed to be unprotected
        """
        return getattr(self, '_tls_client_key_password_callback', None)

    @tls_client_key_password_callback.setter
    def tls_client_key_password_callback(self, value):
        if not callable(value):
            raise TypeError("tls_client_key_password_callback is not callable".format(value))
        self._tls_client_key_password_callback = value

    #######################
    # KAFKA CLIENT SETTINGS
    #
    # The rest of these configurations are used for controlling the behavior of the client

    @property
    def client_id(self):
        """The client ID string to use when talking to the brokers"""
        return getattr(self, '_client_id', "kafka-tools")

    @client_id.setter
    def client_id(self, value):
        raise_if_not_string("client_id", value)
        self._client_id = value

    @property
    def metadata_refresh(self):
        """How long topic and group metadata can be cached"""
        return getattr(self, '_metadata_refresh', 60000)

    @metadata_refresh.setter
    def metadata_refresh(self, value):
        raise_if_not_positive_integer("metadata_refresh", value)
        self._metadata_refresh = value

    @property
    def max_request_size(self):
        """The largest size for outgoing Kafka requests. Used to allocate the request buffer"""
        return getattr(self, '_max_request_size', 200000)

    @max_request_size.setter
    def max_request_size(self, value):
        raise_if_not_positive_integer("max_request_size", value)
        self._max_request_size = value

    @property
    def num_retries(self):
        """The number of times to retry a request when there is a failure"""
        return getattr(self, '_num_retries', 3)

    @num_retries.setter
    def num_retries(self, value):
        raise_if_not_positive_integer("_num_retries", value)
        self._num_retries = value

    @property
    def retry_backoff(self):
        """The number of seconds (float) to wait between request retries"""
        return getattr(self, '_retry_backoff', 0.5)

    @retry_backoff.setter
    def retry_backoff(self, value):
        raise_if_not_positive_float("_retry_backoff", value)
        self._retry_backoff = value

    @property
    def broker_threads(self):
        """How many threads to use in a pool for broker connections"""
        return getattr(self, '_broker_threads', 20)

    @broker_threads.setter
    def broker_threads(self, value):
        raise_if_not_positive_integer("broker_threads", value)
        self._broker_threads = value

    def _set_attributes(self, **kwargs):
        for key in kwargs:
            if not hasattr(self, key):
                raise ConfigurationError("Invalid configuration specified: {0}".format(key))
            setattr(self, key, kwargs[key])

    def __init__(self, **kwargs):
        """
        Create a configuration object, setting any provided options. Either broker_list or zkconnect (but not both)
        must be provided

        Raises:
            ConfigurationError: unless exactly one of broker_list or zkconnect is provided, or if any invalid option
                is specified.
        """
        if ('zkconnect' in kwargs) and ('broker_list' in kwargs):
            raise ConfigurationError("Only one of zkconnect and broker_list may be provided")
        self._set_attributes(**kwargs)

        # Create the SSL context if we are going to enable TLS
        self.ssl_context = self._create_ssl_context() if self.enable_tls else None

    def _verify_ssl_configuration(self):
        if self.tls_verify_hostnames and (not self.tls_verify_certificates):
            raise ConfigurationError("tls_verify_hostnames may not be specified if tls_verify_certificates is False")

    def _create_ssl_context(self):
        self._verify_ssl_configuration()

        try:
            context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            context.verify_mode = ssl.CERT_REQUIRED if self.tls_verify_certificates else ssl.CERT_NONE
            context.check_hostname = self.tls_verify_hostnames

            if self.tls_root_certificates is None:
                context.load_default_certs(purpose=ssl.Purpose.CLIENT_AUTH)
            else:
                context.load_verify_locations(cafile=self.tls_root_certificates)

            if self.tls_client_certificate is not None:
                context.load_cert_chain(self.tls_client_certificate,
                                        keyfile=self.tls_client_keyfile,
                                        password=self.tls_client_key_password_callback)
        except ssl.SSLError as e:
            raise ConfigurationError("Unable to configure SSL Context: {0}".format(e))

        return context


def raise_if_not_positive_integer(attr_name, value):
    if not (isinstance(value, six.integer_types) and (value > 0)):
        raise TypeError("{0} must be a positive integer".format(attr_name))


def raise_if_not_positive_float(attr_name, value):
    if not (isinstance(value, float) and (value > 0.0)):
        raise TypeError("{0} must be a positive float".format(attr_name))


def raise_if_not_string(attr_name, value):
    if not isinstance(value, six.string_types):
        raise TypeError("{0} must be a string".format(attr_name))
