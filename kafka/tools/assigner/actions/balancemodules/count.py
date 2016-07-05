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

from operator import attrgetter

from kafka.tools.assigner import log
from kafka.tools.assigner.actions import ActionBalanceModule


class ActionBalanceCount(ActionBalanceModule):
    name = "count"
    helpstr = "Move the smallest partitions in the cluster to even the partition count per-broker for each replica position"

    def process_cluster(self):
        log.info("Starting partition balance by count")

        # Figure out the max RF for the cluster and sort all partition lists by size (ascending)
        max_pos = self.cluster.max_replication_factor()
        for broker in self.cluster.brokers:
            for pos in self.cluster.brokers[broker].partitions:
                self.cluster.brokers[broker].partitions[pos].sort(key=attrgetter('size'))

        # Calculate partition counts for each position first
        max_count = {}
        for pos in range(max_pos):
            # Calculate the maximum number of partitions each broker should have (floor(average))
            # We'll also track a remainder and make sure they only go 1 per broker
            pcount = 0
            for broker in self.cluster.brokers:
                if pos in self.cluster.brokers[broker].partitions:
                    pcount += self.cluster.brokers[broker].num_partitions_at_position(pos)
            max_count[pos] = [pcount / len(self.cluster.brokers), pcount % len(self.cluster.brokers)]
            log.info("Calculating ideal state for replica position {0} - max {1} partitions".format(pos, max_count[pos][0] + 1))

        # Balance partition counts for each replica position separately
        for pos in range(max_pos):
            for broker_id in self.cluster.brokers:
                broker = self.cluster.brokers[broker_id]
                # Figure out how many more partitions this broker needs
                diff = max_count[pos][0]
                if max_count[pos][1]:
                    diff += 1
                    max_count[pos][1] -= 1
                if pos in broker.partitions:
                    diff -= broker.num_partitions_at_position(pos)

                if diff > 0:
                    log.debug("Moving {0} partitions to broker {1}".format(diff, broker_id))

                    # Iterate through the largest brokers to find diff partitions to move to this broker
                    for source_id in self.cluster.brokers:
                        source = self.cluster.brokers[source_id]
                        if diff == 0:
                            break
                        if pos not in source.partitions:
                            continue

                        iterlist = list(source.partitions[pos])
                        for partition in iterlist:
                            # If we have moved enough partitions from this broker, exit out of the inner loop
                            if (source.num_partitions_at_position(pos) < max_count[pos][0]) or (diff == 0):
                                break
                            # If the partition is already on the target, swap positions only if it makes the balance better
                            if broker in partition.replicas:
                                other_pos = partition.replicas.index(broker)
                                if (other_pos in source.partitions) and (source.num_partitions_at_position(other_pos) < max_count[other_pos][0]):
                                    partition.swap_replica_positions(source, broker)
                            else:
                                partition.swap_replicas(source, broker)
                                diff -= 1

                    log.debug("Finish broker {0} with {1} partitions".format(broker_id, broker.num_partitions_at_position(pos)))
                elif diff < 0:
                    log.debug("Moving {0} partitions off broker {1}".format(-diff, broker_id))

                    # Iterate through the smallest brokers to find diff partitions to move off this broker
                    for target_id in self.cluster.brokers:
                        target = self.cluster.brokers[target_id]
                        if diff == 0:
                            break
                        if (pos in target.partitions) and (target.num_partitions_at_position(pos) > (max_count[pos][0] + 1)):
                            continue

                        iterlist = list(broker.partitions[pos])
                        for partition in iterlist:
                            # If we have moved enough partitions to this broker, exit out of the inner loop
                            if ((pos in target.partitions) and (target.num_partitions_at_position(pos) >= max_count[pos][0])) or (diff == 0):
                                break
                            # Skip partitions that are already on the target broker
                            if target in partition.replicas:
                                continue

                            partition.swap_replicas(broker, target)
                            diff += 1

                    log.debug("Finish broker {0} with {1} partitions".format(broker, broker.num_partitions_at_position(pos)))
                else:
                    log.debug("Skipping broker {0} which has {1} partitions".format(broker, broker.num_partitions_at_position(pos)))
                    continue
