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

from kafka.tools.protocol.requests import BaseRequest, ArgumentError
from kafka.tools.protocol.responses.sasl_handshake_v0 import SaslHandshakeV0Response


class SaslHandshakeV0Request(BaseRequest):
    api_key = 17
    api_version = 0
    cmd = "SaslHandshake"
    response = SaslHandshakeV0Response

    help_string = ("Request:     {0}V{1}\n".format(cmd, api_version) +
                   "Format:      {0}V{1} mechanism\n".format(cmd, api_version) +
                   "Description: Handshake for SASL with the mechanism chosen by the client\n")

    schema = [
        {'name': 'mechanisms', 'type': 'string'},
    ]

    @classmethod
    def process_arguments(cls, cmd_args):
        if len(cmd_args) != 1:
            raise ArgumentError("SaslHandshakeV0 requires exactly one argument")
        return {'mechanism': cmd_args[0]}
