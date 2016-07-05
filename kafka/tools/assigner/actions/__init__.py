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

from kafka.tools.assigner.exceptions import ConfigurationException


# Base class that all kafka-assigner actions inherit from
class ActionModule(object):
    ##########################################################################################################
    # OVERRIDE ME - These are all the things you can/must override in your child class
    name = ""
    helpstr = ""
    needs_sizes = False

    def __init__(self, args, cluster):
        self.args = args
        self.cluster = cluster

    @classmethod
    def _add_args(cls, parser):
        pass

    def process_cluster(self):
        pass

    ##########################################################################################################
    # MODULE BASICS - You probably shouldn't modify past here unless you're changing the basic way all modules work

    @classmethod
    def configure_args(cls, subparser):
        if cls.name == "" or cls.helpstr == "":
            raise Exception("Cannot instantiate ActionModule directly")

        parser = subparser.add_parser(cls.name, help=cls.helpstr)
        cls._add_args(parser)
        parser.set_defaults(action=cls.name)

    def check_brokers(self, type_str="Source brokers"):
        if len(set(self.args.brokers) & set(self.cluster.brokers)) != len(self.args.brokers):
            raise ConfigurationException("{0} are not in the brokers list for this cluster".format(type_str))


# Special class for balance modules to inherit from that skips configs
class ActionBalanceModule(ActionModule):
    @classmethod
    def configure_args(cls, subparser):
        pass
