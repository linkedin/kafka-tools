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
from kafka.tools import log
from kafka.tools.assigner.actions import ActionModule
from kafka.tools.exceptions import ConfigurationException


class ActionClone(ActionModule):
    name = "clone"
    helpstr = "Copy partitions for specified topics from some brokers (list) to a new broker (single) and increasing RF"

    def __init__(self, args, cluster):
        super(ActionClone, self).__init__(args, cluster)

        self.check_brokers()
        # TODO Navneet - check how this was working in case of single target broker also
        # if args.to_broker not in self.cluster.brokers:
        #     raise ConfigurationException("Target broker is not in the brokers list for this cluster")

        self.topics = args.topics
        input_topics_not_present = []
        for topic in self.topics:
            if topic not in self.cluster.topics:
                input_topics_not_present.append(topic)

        if len(input_topics_not_present) > 0:
            raise ConfigurationException("Target topic is not in the topic list for this cluster" +
                                         str(input_topics_not_present))

        self.sources = args.brokers
        self.to_brokers = args.to_brokers

    @classmethod
    def _add_args(cls, parser):
        # in our case, we need to specify all LW brokers as we want to migrate leadership to AWS brokers,
        # the processing is constrained by topics specified in args however
        parser.add_argument('-b', '--brokers', help="List of source brokers where leadership needs to be migrated from", required=True,
                            type=int, nargs='*')
        # we want to do partition leadership migration for specified topics only
        parser.add_argument('-s', '--topics', help="List of topics's partition leaders to be migrated", required=True, type=str, nargs='*')
        # this is where cloning will happen, in our case this is expected to be broker in AWS
        parser.add_argument('-t', '--to_brokers', help="Broker ID to copy partitions to", required=True, type=int, nargs='*')

    def process_cluster(self):
        from_brokers = set(self.sources)
        to_brokers = deque(self.to_brokers)
        print(" source broker set is " + str(from_brokers))
        print(" target broker set is " + str(to_brokers))
        for partition in self.cluster.partitions_for(self.topics):
            if len(from_brokers & set([replica.id for replica in partition.replicas])) > 0:
                to_broker = to_brokers.popleft()
                print(" to_broker is " + str(to_broker))
                to_brokers.append(to_broker)
                targeted_broker = self.cluster.brokers[to_broker]
                print(" targeted_broker is " + str(targeted_broker) + repr(targeted_broker))
                if targeted_broker in partition.replicas:
                    log.warn("Targeted broker (ID {0}) is already in the replica list for {1}:{2}"
                             .format(targeted_broker.id, partition.topic.name, partition.num))

                    # If the broker is already in the replica list, it ALWAYS becomes the leader
                    if targeted_broker != partition.replicas[0]:
                        partition.swap_replica_positions(targeted_broker, partition.replicas[0])
                else:
                    # If one of the source brokers is currently the leader, the target broker is the leader.
                    # Otherwise, the target broker is in second place
                    if partition.replicas[0].id in self.sources:
                        partition.add_replica(targeted_broker, 0)
                    else:
                        partition.add_replica(targeted_broker, 1)
