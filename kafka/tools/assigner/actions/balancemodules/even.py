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
from kafka.tools.assigner.actions import ActionBalanceModule


def pmap_matches_target(pmap, target):
    for pos in pmap:
        for broker_id in pos:
            if pos[broker_id] != target:
                return False
    return True


class ActionBalanceEven(ActionBalanceModule):
    name = "even"
    helpstr = "Evenly spread topics across the cluster"

    def check_topic_ok(self, topic):
            # In even, we don't skip topics because of partition count

            if topic.name in self.args.exclude_topics:
                log.warn("Skipping topic {0} as it is explicitly excluded".format(topic.name))
                return False
            if any([len(partition.replicas) != len(topic.partitions[0].replicas) for partition in topic.partitions]):
                log.warn("Skipping topic {0} as not all partitions have the same replication factor".format(topic.name))
                return False
            return True

    def process_cluster(self):
        log.info("Starting even partition balance")

        # Initialize broker deques for each position for remainder assignment
        ordered_brokers = sorted(self.cluster.brokers.keys())
        max_rf = self.cluster.max_replication_factor()
        remainder_brokers = [deque(ordered_brokers) for pos in range(max_rf)]
        for pos in range(max_rf):
            # Advance the deque by max_rf places so that we don't collide replicas
            remainder_brokers[pos].rotate(-pos)

        for topic_name in sorted(self.cluster.topics):
            topic = self.cluster.topics[topic_name]
            if not self.check_topic_ok(topic):
                continue

            # How many partitions per broker, and what's the last one that can be evenly balanced
            target = len(topic.partitions) // len(self.cluster.brokers)
            last_even_partition = len(topic.partitions) - (len(topic.partitions) % len(self.cluster.brokers)) - 1

            # Initialize broker map for this topic.
            pmap = [dict.fromkeys(self.cluster.brokers.keys(), 0) for pos in range(len(topic.partitions[0].replicas))]
            for pnum in range(0, last_even_partition + 1):
                partition = topic.partitions[pnum]
                for i, replica in enumerate(partition.replicas):
                    pmap[i][replica.id] += 1

            # Balance all but the last remainder partitions
            while not pmap_matches_target(pmap, target):
                for pnum in range(0, last_even_partition + 1):
                    partition = topic.partitions[pnum]

                    for pos in range(len(partition.replicas)):
                        # Current placement is fine (or low). Leave the replica where it is
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

            # Distribute the remainder partitions evenly among the brokers
            # This is a pretty dumb round robin distribution, but it will be stable
            for pnum in range(last_even_partition + 1, len(topic.partitions)):
                partition = topic.partitions[pnum]

                for pos in range(len(partition.replicas)):
                    # Find a new replica for this partition
                    proposed = remainder_brokers[pos].popleft()
                    remainder_brokers[pos].append(proposed)

                    partition.swap_replicas(partition.replicas[pos], self.cluster.brokers[proposed])
