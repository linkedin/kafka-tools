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


def main():
    # Set up and parse all CLI arguments
    args = set_up_arguments()

    # Get a list of all supported requests
    request_classes = {}
    for cls in get_modules(kafka.tools.protocol.requests, kafka.tools.protocol.requests.BaseRequest):
        if cls.cmd.lower() not in request_classes:
            request_classes[cls.cmd.lower()] = {}
        request_classes[cls.cmd.lower()][cls.api_version] = cls

    # Set up the request commands
    request_cmds = {}
    for cmd in request_classes:
        request_cmds[cmd] = request_classes[cmd][max(request_classes[cmd].keys())]
        for ver in request_classes[cmd]:
            request_cmds[cmd + "v{0}".format(ver)] = request_classes[cmd][ver]

    # Connect to the specified Kafka broker
    broker = Broker(args.broker, args.port)
    broker.connect()

    # Loop on reading a command
    while True:
        try:
            input_str = input("{0}:{1}> ".format(args.broker, args.port))
        except EOFError:
            print("")
            break

        cmd_parts = input_str.split()
        cmd = cmd_parts[0].lower()
        if len(cmd) == 0:
            continue

        if cmd in ('exit', 'quit', 'q'):
            break
        elif cmd == 'help':
            show_help(request_classes, request_cmds, cmd_parts)
        elif cmd == 'errors':
            for err_num in sorted(errors.keys()):
                print("{0}     {1} - {2}".format(err_num, errors[err_num]['short'], errors[err_num]['long']))
        elif cmd in request_cmds.keys():
            req = request_cmds[cmd](cmd_parts[1:])
            correlation_id, response = broker.send(req)
            print(response)
        else:
            log.error("Unknown command: {0}".format(cmd))

    # Disconnect
    broker.close()

    # Nothing to see here, move along
    return 0


if __name__ == "__main__":
    sys.exit(main())
