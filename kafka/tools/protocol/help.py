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


def show_help(request_classes, request_cmds, cmd_args):
    if len(cmd_args) == 0:
        print("Commands:")
        print("    quit        Exit kafka-protocol")
        print("    help        Display this command list")
        print("    errors      List known error codes\n")
        print("Requests:")
        for request in sorted(request_classes.keys()):
            print("    {0} (or {1})".format(request_classes[request][max(request_classes[request].keys())].cmd,
                                            ", ".join([request_classes[request][ver].cmd + "V{0}".format(ver)
                                                       for ver in sorted(request_classes[request].keys())])))
        print("\nFor more information on a request, type \"help <request>\"")
    elif cmd_args[0] in request_cmds:
        print(request_cmds[cmd_args[0]].help_string)
    else:
        print("{0} is not a valid request".format(cmd_args[0]))
