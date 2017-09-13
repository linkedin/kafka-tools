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

from __future__ import division

import re
import socket
import struct
import time

from kafka.tools import log
from kafka.tools.configuration import ClientConfiguration
from kafka.tools.models import BaseModel
from kafka.tools.exceptions import ConfigurationException, ConnectionError
from kafka.tools.protocol.types.bytebuffer import ByteBuffer
from kafka.tools.utilities import json_loads


class Endpoint(BaseModel):
    equality_attrs = ['protocol', 'hostname', 'port']

    def __init__(self, protocol, hostname, port):
        self.protocol = protocol
        self.hostname = hostname
        self.port = port


class Broker(BaseModel):
    equality_attrs = ['hostname', 'id']

    @property
    def hostname(self):
        return self.endpoint.hostname

    @hostname.setter
    def hostname(self, value):
        self.endpoint.hostname = value

    @property
    def port(self):
        return self.endpoint.port

    @port.setter
    def port(self, value):
        self.endpoint.port = value

    def __init__(self, hostname, id=0, port=9092, sock=None, configuration=None):
        self.id = id
        self.endpoint = Endpoint('', hostname, port)
        self.jmx_port = -1
        self.rack = None
        self.version = None
        self.endpoints = None
        self.timestamp = None
        self.cluster = None
        self.partitions = {}
        self.endpoints = []
        self._sock = sock

        self._correlation_id = 1
        self._configuration = configuration or ClientConfiguration()

    @classmethod
    def create_from_json(cls, broker_id, jsondata):
        data = json_loads(jsondata)

        # These things are required, and we can't proceed if they're not there
        try:
            newbroker = cls(data['host'], id=broker_id, port=data['port'])
        except KeyError:
            raise ConfigurationException("Cannot parse broker data in zookeeper. This version of Kafka may not be supported.")

        # These things are optional, and are pulled in for convenience or extra features
        for attr in ['jmx_port', 'rack', 'version', 'timestamp']:
            try:
                setattr(newbroker, attr, data[attr])
            except KeyError:
                pass

        # if the broker defines multiple endpoints,
        newbroker._set_endpoints(data.get('endpoints', []))

        return newbroker

    def _set_endpoints(self, endpoints):
        endpoint_re = re.compile("(.*)://(.*):([0-9]+)", re.I)
        for endpoint in endpoints:
            m = endpoint_re.match(endpoint)
            if m is not None:
                self.endpoints.append(Endpoint(m.group(1), m.group(2), int(m.group(3))))

    # Shallow copy - do not copy partitions map over
    def copy(self):
        newbroker = Broker(self.hostname, id=self.id, port=self.port)
        newbroker.jmx_port = self.jmx_port
        newbroker.port = self.port
        newbroker.rack = self.rack
        newbroker.version = self.version
        newbroker.endpoints = self.endpoints
        newbroker.timestamp = self.timestamp
        newbroker.cluster = self.cluster
        return newbroker

    def num_leaders(self):
        return self.num_partitions_at_position(0)

    def num_partitions_at_position(self, pos=0):
        if pos in self.partitions:
            return len(self.partitions[pos])
        else:
            return pos

    def percent_leaders(self):
        if self.num_partitions() == 0:
            return 0.0
        return (self.num_leaders() / self.num_partitions()) * 100

    def total_size(self):
        return sum([p.size for pos in self.partitions for p in self.partitions[pos]], 0)

    def num_partitions(self):
        return sum([len(self.partitions[pos]) for pos in self.partitions], 0)

    def get_endpoint(self, protocol):
        for endpoint in self.endpoints:
            if endpoint.protocol == protocol:
                return endpoint
        return self.endpoint

    def _get_socket(self, sslcontext):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if sslcontext is not None:
            sock = sslcontext.wrap_socket(sock, server_hostname=self.hostname)
        return sock

    def connect(self):
        protocol = 'SSL' if self._configuration.ssl_context is not None else 'PLAINTEXT'
        endpoint = self.get_endpoint(protocol)

        log.info("Connecting to {0} on port {1} using {2}".format(self.hostname, self.port, protocol))
        try:
            self._sock = self._sock or self._get_socket(self._configuration.ssl_context)
            self._sock.connect((endpoint.hostname, endpoint.port))
        except socket.error as e:
            log.error("Cannot connect to broker {0}:{1}: {2}".format(endpoint.hostname, endpoint.port, e))
            raise ConnectionError("Cannot connect to broker {0}:{1}: {2}".format(endpoint.hostname, endpoint.port, e))

    def close(self):
        log.info("Disconnecting from {0}".format(self.hostname))

        # Shutdown throws an error if the socket is not connected, but that's OK
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        self._sock.close()
        self._sock = None

    def send(self, request):
        attempts = 0
        while attempts < self._configuration.num_retries:
            attempts += 1
            try:
                # Connect to the broker if not currently connected
                if self._sock is None:
                    self.connect()

                return self._single_send(request)
            except ConnectionError as e:
                if attempts >= self._configuration.num_retries:
                    log.error("Failed communicating with Kafka broker {0}. retries remaining = 0: {1}".format(self.id, e))
                    raise
                else:
                    log.warn("Failed communicating with Kafka broker {0}. retries remaining = {1}: {2}".format(self.id,
                                                                                                               self._configuration.num_retries - attempts,
                                                                                                               e))

            # Sleep for the backoff period before retrying the request, and force a reconnect
            self.close()
            time.sleep(self._configuration.retry_backoff)

    def _single_send(self, request):
        # Build the payload based on the request passed in. We'll fill in the size at the end
        buf = ByteBuffer(self._configuration.max_request_size)
        buf.putInt32(0)
        buf.putInt16(request.api_key)
        buf.putInt16(request.api_version)
        buf.putInt32(self._correlation_id)
        buf.putInt16(len(self._configuration.client_id))
        buf.put(struct.pack('{0}s'.format(len(self._configuration.client_id)), self._configuration.client_id.encode("utf-8")))
        request.encode(buf)

        # Close the payload and write the size (payload size without the size field itself)
        buf.limit = buf.position - 1
        payload_len = buf.capacity - 4
        buf.rewind()
        buf.putInt32(payload_len)
        buf.rewind()

        # Increment the correlation ID for the next request
        self._correlation_id += 1

        try:
            # Send the payload bytes to the broker
            self._sock.sendall(buf.get(buf.capacity))

            # Read the first 4 bytes so we know the size
            size = ByteBuffer(self._sock.recv(4)).getInt32()

            # Read the response that we're expecting
            response_data = self._read_bytes(size)
            response = ByteBuffer(response_data)

            # Parse off the correlation ID for the response
            correlation_id = response.getInt32()
        except EOFError:
            raise ConnectionError("Failed to read enough data from Kafka")
        except socket.error as e:
            raise ConnectionError("Failed communicating with Kafka: {0}".format(e))

        # Get the proper response class and parse the response
        return correlation_id, request.response.from_bytebuffer(correlation_id, response.slice())

    def _read_bytes(self, size):
        bytes_left = size
        responses = []

        while bytes_left:
            try:
                data = self._sock.recv(min(bytes_left, 4096))
            except socket.error:
                raise socket.error("Unable to receive data from Kafka")

            if data == b'':
                raise socket.error("Not enough data to read message -- did server kill socket?")

            bytes_left -= len(data)
            responses.append(data)
        return b''.join(responses)

    def to_dict(self):
        return {
            'id': self.id,
            'hostname': self.hostname,
            'jmx_port': self.jmx_port,
            'port': self.port,
            'rack': self.rack,
            'version': self.version
        }
