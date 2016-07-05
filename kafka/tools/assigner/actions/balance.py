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

import kafka.tools.assigner.actions.balancemodules
from kafka.tools.assigner.actions import ActionModule, ActionBalanceModule
from kafka.tools.assigner.modules import get_modules


class ActionBalance(ActionModule):
    name = "balance"
    helpstr = "Rebalance partitions across the cluster"
    needs_sizes = True

    def __init__(self, args, cluster):
        super(ActionBalance, self).__init__(args, cluster)

        self.balance_types = dict((cls.name, cls) for cls in get_modules(kafka.tools.assigner.actions.balancemodules, ActionBalanceModule))

        # Initialize all the modules to use
        self.modules = []
        for bmodule in args.types:
            self.modules.append(self.balance_types[bmodule](args, cluster))

    @classmethod
    def _add_args(cls, parser):
        # We'll need to build this list here as well as in the constructor
        balance_actions = get_modules(kafka.tools.assigner.actions.balancemodules, ActionBalanceModule)
        parser.add_argument('-t', '--types', help="Balance types to perform. Multiple may be specified and they will be run in order", required=True,
                            choices=[klass.name for klass in balance_actions], nargs='*')

    def process_cluster(self):
        for bmodule in self.modules:
            bmodule.process_cluster()
