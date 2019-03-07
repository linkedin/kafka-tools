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

from kafka.tools import log
from kafka.tools.assigner.actions import ActionModule
from kafka.tools.exceptions import ConfigurationException


class ActionClone(ActionModule):
    name = "clone"
    helpstr = "Copy partitions from some brokers to a new broker (increasing RF)"

    def __init__(self, args, cluster):
        super(ActionClone, self).__init__(args, cluster)

        self.check_brokers()
        if args.to_broker not in self.cluster.brokers:
            raise ConfigurationException("Target broker is not in the brokers list for this cluster")

        self.topics = args.topics
        input_topics_not_present = []
        for topic in self.topics:
            if topic not in self.cluster.topics:
                input_topics_not_present.append(topic)

        if len(input_topics_not_present) > 0:
            raise ConfigurationException("Target topic is not in the topic list for this cluster" + input_topics_not_present)

        self.sources = args.brokers
        self.to_broker = self.cluster.brokers[args.to_broker]

    @classmethod
    def _add_args(cls, parser):
        # in our case, we can specify all LW brokers as we want to migrate leadership to AWS brokers,
        # the processing is limited by topics specified
        parser.add_argument('-b', '--brokers', help="List of source brokers where leadership needs to be migrated from", required=True,
                            type=int, nargs='*')
        # we only want to do partition leadership migration topic by topic
        parser.add_argument('-s', '--topics', help="List of topics's partition leaders to be migrated", required=True, type=str, nargs='*')
        parser.add_argument('-t', '--to_broker', help="Broker ID to copy partitions to", required=True, type=int)

    def process_cluster(self):
        source_set = set(self.sources)
        print(" source_set is " + str(source_set))
        for partition in self.cluster.partitions_for(self.topics):
            print(" partition is " + str(partition.__dict__))
            print(" partition replicas are " + str(partition.replicas))
            print(" this should be printed")
            if len(source_set & set([replica.id for replica in partition.replicas])) > 0:
                print(" check if to_broker " + str(self.to_broker) + " already a replica")
                if self.to_broker in partition.replicas:
                    print(" yes, " + str(self.to_broker) + " already a replica")
                    log.warn("Target broker (ID {0}) is already in the replica list for {1}:{2}".format(self.to_broker.id, partition.topic.name, partition.num))
                    # If the broker is already in the replica list, it ALWAYS becomes the leader
                    if self.to_broker != partition.replicas[0]:
                        partition.swap_replica_positions(self.to_broker, partition.replicas[0])
                else:
                    print(" no, " + str(self.to_broker) + " not already a replica ")
                    # If one of the source brokers is currently the leader, the target broker is the leader.
                    # Otherwise, the target leader is in second place
                    print(" check if source broker a leader for partition ")
                    if partition.replicas[0].id in self.sources:
                        print(" source is leader, make to_broker  " + str(self.to_broker) + " leader of the partition")
                        partition.add_replica(self.to_broker, 0)
                    else:
                        print(" source isn't leader, make to_broker  " + str(self.to_broker) + " second in replica list")
                        partition.add_replica(self.to_broker, 1)
