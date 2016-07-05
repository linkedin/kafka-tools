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

from collections import deque
from kafka.tools.assigner.actions import ActionModule
from kafka.tools.assigner.exceptions import ConfigurationException, NotEnoughReplicasException


class ActionRemove(ActionModule):
    name = "remove"
    helpstr = "Move partitions from one broker to one or more other brokers (maintaining RF)"

    def __init__(self, args, cluster):
        super(ActionRemove, self).__init__(args, cluster)

        self.check_brokers(type_str="Brokers to remove")
        if (args.to_brokers is not None) and (len(set(self.args.to_brokers) & set(self.args.brokers)) != 0):
            raise ConfigurationException("Brokers to remove were specified in the target broker list as well")

        self.brokers = args.brokers
        self.to_brokers = args.to_brokers
        if (self.to_brokers is None) or (len(self.to_brokers) == 0):
            self.to_brokers = list(set(self.cluster.brokers.keys()) - set(self.brokers))
        else:
            for broker in self.to_brokers:
                if broker not in self.cluster.brokers:
                    raise ConfigurationException("Target broker (ID {0}) is not in the brokers list for this cluster".format(broker))

    @classmethod
    def _add_args(cls, parser):
        parser.add_argument('-b', '--brokers', help="List of Broker IDs to remove", required=True, type=int, nargs='*')
        parser.add_argument('-t', '--to_brokers', help="List of Broker IDs to move partitions to (defaults to whole cluster)",
                            required=False, type=int, nargs='*')

    def process_cluster(self):
        # Make a deque for the target brokers so we can round-robin assignments
        todeque = deque(self.to_brokers)

        for broker_id in self.brokers:
            broker = self.cluster.brokers[broker_id]
            for position in broker.partitions:
                iterlist = list(broker.partitions[position])
                for partition in iterlist:
                    # Find a new replica for this partition
                    newreplica = None
                    attempts = 0
                    while attempts < len(todeque):
                        proposed = todeque.popleft()
                        todeque.append(proposed)
                        proposed_broker = self.cluster.brokers[proposed]
                        if proposed_broker not in partition.replicas:
                            newreplica = proposed_broker
                            break
                        attempts += 1

                    if newreplica is None:
                        raise NotEnoughReplicasException("Cannot find a new broker for {0}:{1} with replica list {2}".format(partition.topic.name,
                                                                                                                             partition.num,
                                                                                                                             partition.replicas))

                    # Replace the broker coming out with the new one
                    partition.swap_replicas(broker, newreplica)
