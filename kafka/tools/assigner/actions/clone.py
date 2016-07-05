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

from kafka.tools.assigner import log
from kafka.tools.assigner.actions import ActionModule
from kafka.tools.assigner.exceptions import ConfigurationException


class ActionClone(ActionModule):
    name = "clone"
    helpstr = "Copy partitions from some brokers to a new broker (increasing RF)"

    def __init__(self, args, cluster):
        super(ActionClone, self).__init__(args, cluster)

        self.check_brokers()
        if args.to_broker not in self.cluster.brokers:
            raise ConfigurationException("Target broker is not in the brokers list for this cluster")

        self.sources = args.brokers
        self.to_broker = self.cluster.brokers[args.to_broker]

    @classmethod
    def _add_args(cls, parser):
        parser.add_argument('-b', '--brokers', help="List of source broker IDs", required=True, type=int, nargs='*')
        parser.add_argument('-t', '--to_broker', help="Broker ID to copy partitions to", required=True, type=int)

    def process_cluster(self):
        source_set = set(self.sources)
        for partition in self.cluster.partitions():
            if len(source_set & set([replica.id for replica in partition.replicas])) > 0:
                if self.to_broker in partition.replicas:
                    log.warn("Target broker (ID {0}) is already in the replica list for {1}:{2}".format(self.to_broker.id, partition.topic.name, partition.num))

                    # If the broker is already in the replica list, it ALWAYS becomes the leader
                    if self.to_broker != partition.replicas[0]:
                        partition.swap_replica_positions(self.to_broker, partition.replicas[0])
                else:
                    # If one of the source brokers is currently the leader, the target broker is the leader. Otherwise, the target leader is in second place
                    if partition.replicas[0].id in self.sources:
                        partition.add_replica(self.to_broker, 0)
                    else:
                        partition.add_replica(self.to_broker, 1)
