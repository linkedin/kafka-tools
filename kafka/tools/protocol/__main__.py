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

import sys

from six.moves import input

from kafka.tools import log
import kafka.tools.protocol.requests
from kafka.tools.modules import get_modules
from kafka.tools.protocol.arguments import set_up_arguments
from kafka.tools.protocol.errors import errors
from kafka.tools.protocol.help import show_help
from kafka.tools.models.broker import Broker


def _get_request_classes():
    request_classes = {}
    for cls in get_modules(kafka.tools.protocol.requests, kafka.tools.protocol.requests.BaseRequest):
        if cls.cmd.lower() not in request_classes:
            request_classes[cls.cmd.lower()] = {}
        request_classes[cls.cmd.lower()][cls.api_version] = cls
    return request_classes


def _get_request_commands(request_classes):
    request_cmds = {}
    for cmd in request_classes:
        request_cmds[cmd] = request_classes[cmd][max(request_classes[cmd].keys())]
        for ver in request_classes[cmd]:
            request_cmds[cmd + "v{0}".format(ver)] = request_classes[cmd][ver]
    return request_cmds


def _print_errors():
    for err_num in sorted(errors.keys()):
        print("{0}     {1} - {2}".format(err_num, errors[err_num]['short'], errors[err_num]['long']))


def _parse_command(broker, request_classes, request_cmds, cmd, cmd_args):
    if cmd in ('exit', 'quit', 'q'):
        raise EOFError
    elif cmd == 'help':
        show_help(request_classes, request_cmds, cmd_args)
    elif cmd == 'errors':
        _print_errors()
    elif cmd in request_cmds.keys():
        request_value = request_cmds[cmd].process_arguments(cmd_args)
        req = request_cmds[cmd](request_value)
        correlation_id, response = broker.send(req)
        print(response)
    else:
        log.error("Unknown command: {0}".format(cmd))


def _cli_loop(broker):
    # Get a list of all supported requests, as well as the command list
    request_classes = _get_request_classes()
    request_cmds = _get_request_commands(request_classes)

    try:
        while True:
            input_str = input("{0}:{1}> ".format(broker.hostname, broker.port))
            cmd_parts = input_str.split()
            if len(cmd_parts) == 0:
                continue
            cmd = cmd_parts.pop(0).lower()
            _parse_command(broker, request_classes, request_cmds, cmd, cmd_parts)
    except EOFError:
        print("")


def main():
    # Set up and parse all CLI arguments
    args = set_up_arguments()

    # Connect to the specified Kafka broker
    broker = Broker(args.broker, port=args.port)
    broker.connect()

    # Loop on reading a command
    _cli_loop(broker)

    # Disconnect
    broker.close()

    # Nothing to see here, move along
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
