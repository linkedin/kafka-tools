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

import random
from collections import deque
from kafka.tools.assigner.actions import ActionModule
from kafka.tools.assigner.exceptions import ConfigurationException


class ActionSetRF(ActionModule):
    name = "set-replication-factor"
    helpstr = "Increase the replication factor of the specified topics"

    def __init__(self, args, cluster):
        super(ActionSetRF, self).__init__(args, cluster)
        if args.replication_factor < 1:
            raise ConfigurationException("You cannot set replication-factor below 1")
        if args.replication_factor > self.cluster.num_brokers():
            raise ConfigurationException("You cannot set replication-factor greater than the number of brokers in the cluster")
        self.target_rf = self.args.replication_factor

    @classmethod
    def _add_args(cls, parser):
        parser.add_argument('-t', '--topics', help='List of topics to alter', required=True, nargs='*')
        parser.add_argument('-r', '--replication-factor', help='Target replication factor', required=True, type=int)

    def process_cluster(self):
        # Randomize a broker list to use for new replicas once. We'll round robin it from here
        idlist = list(self.cluster.brokers.keys())
        random.shuffle(idlist)
        brokers = deque(idlist)

        for partition in self.cluster.partitions():
            if partition.topic.name not in self.args.topics:
                continue

            while len(partition.replicas) < self.target_rf:
                broker_id = brokers.popleft()
                brokers.append(broker_id)
                broker = self.cluster.brokers[broker_id]
                if broker in partition.replicas:
                    continue
                partition.add_replica(broker)
            while len(partition.replicas) > self.target_rf:
                partition.remove_replica(partition.replicas[-1])
