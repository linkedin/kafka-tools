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

import argparse
import pkg_resources


class CSVAction(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super(CSVAction, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        val_array = values.split(',')
        if hasattr(namespace, self.dest):
            old_array = getattr(namespace, self.dest)
            if old_array is not None:
                old_array.extend(val_array)
                val_array = old_array
        setattr(namespace, self.dest, val_array)


# action_map is a map of names to ActionModule children - the top level actions that can be called
# sizer_map is a map of names to SizerModule children - the modules to get partition sizes
def set_up_arguments(action_map, sizer_map, plugins):
    # Configure basic arguments
    aparser = argparse.ArgumentParser(prog='kafka-assigner', description='Rejigger Kafka cluster partitions')

    aparser.add_argument('-v', '--version', action='version', version=str(pkg_resources.get_distribution("kafka-tools").version))
    aparser.add_argument('-z', '--zookeeper', help='Zookeeper path to the cluster (i.e. zk-eat1-kafka.corp:12913/kafka-data-deployment)', required=True)
    aparser.add_argument('-l', '--leadership', help="Show cluster leadership balance", action='store_true')
    aparser.add_argument('-g', '--generate', help="Generate partition reassignment file", action='store_true')
    aparser.add_argument('-e', '--execute', help="Execute partition reassignment", action='store_true')
    aparser.add_argument('-m', '--moves', help="Max number of moves per step", required=False, default=10, type=int)
    aparser.add_argument('-x', '--exclude-topics', help="Comma-separated list of topics to skip when performing actions", action=CSVAction, default=[])
    aparser.add_argument('--sizer', help="Select module to use to get partition sizes", required=False, default='ssh', choices=sizer_map.keys())
    aparser.add_argument('-p', '--property', help="Property of the form 'key=value' to be passed to modules (i.e. sizer)", required=False, default=[],
                         action='append')
    aparser.add_argument('-s', '--size', help="Show partition sizes", action='store_true')
    aparser.add_argument('--skip-ple', help="Skip preferred replica election after finishing moves", action='store_true')
    aparser.add_argument('--ple-size', help="Max number of partitions in a single preferred leader election", required=False, default=2000, type=int)
    aparser.add_argument('--ple-wait', help="Time in seconds to wait between preferred leader elections", required=False, default=120, type=int)
    aparser.add_argument('--tools-path', help="Path to Kafka admin utilities, overriding PATH env var", required=False)
    aparser.add_argument('--output-json', help="Output JSON-formatted cluster information to stdout", default=False, action='store_true')

    # Call action module arg setup
    subparsers = aparser.add_subparsers(help='Select manipulation module to use')
    for action in action_map:
        action_map[action].configure_args(subparsers)

    # Call plugins for default arguments
    for plugin in plugins:
        plugin.set_default_arguments(aparser)

    # Parse the args that were passed
    args = aparser.parse_args()
    return args
