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
from kafka.tools.assigner.actions import ActionBalanceModule


class ActionBalanceEven(ActionBalanceModule):
    name = "even"
    helpstr = "Evenly spread topics that are a multiple of the number of brokers across the cluster"

    def process_cluster(self):
        for topic in self.cluster.topics:
            if len(self.cluster.topics[topic].partitions) % len(self.cluster.brokers) != 0:
                log.warn("Skipping topic {0} as it has {1} partitions, which is not a multiple of the number of brokers ({2})".format(
                    topic, len(self.cluster.topics[topic].partitions), len(self.cluster.brokers)))
                continue
            rf = len(self.cluster.topics[topic].partitions[0].replicas)
            target = len(self.cluster.topics[topic].partitions) / len(self.cluster.brokers)

            different_rf = False
            for partition in self.cluster.topics[topic].partitions:
                if len(partition.replicas) != rf:
                    log.warn("Skipping topic {0} as not all partitions have the same replication factor".format(topic))
                    different_rf = True
            if different_rf:
                continue

            # Initialize broker map for this topic.
            pmap = [dict.fromkeys(self.cluster.brokers.keys(), 0) for pos in range(rf)]
            for partition in self.cluster.topics[topic].partitions:
                for i, replica in enumerate(partition.replicas):
                    pmap[i][replica.id] += 1

            for partition in self.cluster.topics[topic].partitions:
                for pos in range(rf):
                    # Current placement is fine. Leave the replica where it is
                    if pmap[pos][partition.replicas[pos].id] <= target:
                        continue

                    # Find a new replica for the partition at this position
                    for bid in pmap[pos]:
                        if pmap[pos][bid] >= target:
                            continue
                        broker = self.cluster.brokers[bid]
                        source = partition.replicas[pos]

                        if broker in partition.replicas:
                            other_pos = partition.replicas.index(broker)
                            partition.swap_replica_positions(source, broker)
                            pmap[other_pos][broker.id] -= 1
                            pmap[other_pos][source.id] += 1
                        else:
                            partition.swap_replicas(source, broker)

                        pmap[pos][broker.id] += 1
                        pmap[pos][source.id] -= 1
                        break
