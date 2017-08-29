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

from kafka.tools import log
from kafka.tools.models import BaseModel
from kafka.tools.exceptions import ConfigurationException, ConnectionError
from kafka.tools.protocol.types.integers import Int16, Int32
from kafka.tools.protocol.types.string import String
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

    def __init__(self, hostname, id=0, port=9092, sock=None):
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

    def connect(self, sslcontext=None):
        protocol = 'SSL' if sslcontext is not None else 'PLAINTEXT'
        endpoint = self.get_endpoint(protocol)

        log.info("Connecting to {0} on port {1} using {2}".format(self.hostname, self.port, protocol))
        try:
            self._sock = self._sock or self._get_socket(sslcontext)
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

    def send(self, request):
        # Build the payload based on the request passed in
        payload = b''
        payload += Int16(request.api_key).encode()
        payload += Int16(request.api_version).encode()
        payload += Int32(self._correlation_id).encode()
        payload += String('kafka-tools').encode()
        payload += request.encode()

        # Add the size to the payload
        payload_len = len(payload)
        payload = Int32(payload_len).encode() + payload

        # Increment the correlation ID for the next request
        self._correlation_id += 1

        # Send the payload bytes to the broker
        self._sock.send(payload)

        # Read the first 4 bytes so we know the size
        resp = self._sock.recv(4)
        size, junk = Int32.decode(resp)

        # Read the response that we're expecting
        response_data = self._read_bytes(size.value())

        # Parse off the correlation ID for the response
        correlation_id, response_data = Int32.decode(response_data)

        # Get the proper response class and parse the response
        return correlation_id.value(), request.response.from_bytes(correlation_id, response_data)

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
