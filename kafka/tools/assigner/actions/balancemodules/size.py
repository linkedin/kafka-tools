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

from kafka.tools import log
from kafka.tools.assigner.actions import ActionBalanceModule


class ActionBalanceSize(ActionBalanceModule):
    name = "size"
    helpstr = "Move the largest partitions in the cluster to even the total size on disk per-broker for each replica position"

    # We override this so we can select either the size or the scaled_size attributes to sort on
    def __init__(self, args, cluster, size_attr='size'):
        super(ActionBalanceSize, self).__init__(args, cluster)
        self._size_attr = size_attr

    def process_cluster(self):
        log.info("Starting partition balance by {0}".format(self._size_attr))

        # Figure out the max RF for the cluster
        max_rf = self.cluster.max_replication_factor()

        # Calculate cluster information and sorted partition lists first
        partitions = {}
        sizes = {}
        targets = {}
        margins = {}
        for pos in range(max_rf):
            sizes[pos] = {}
            targets[pos] = {}
            margins[pos] = {}

            # Create a sorted list of partitions to use at this position (descending size)
            # Throw out partitions that are 4K or less in size, as they are effectively empty
            partitions[pos] = [p for p in self.cluster.partitions(self.args.exclude_topics) if (len(p.replicas) > pos) and (getattr(p, self._size_attr) > 4)]
            if len(partitions[pos]) == 0:
                continue
            partitions[pos].sort(key=attrgetter(self._size_attr), reverse=True)

            # Calculate broker size at this position
            for broker in self.cluster.brokers:
                if pos in self.cluster.brokers[broker].partitions:
                    sizes[pos][broker] = sum([getattr(p, self._size_attr) for p in self.cluster.brokers[broker].partitions[pos]], 0)
                else:
                    sizes[pos][broker] = 0

            # Calculate the median size of partitions (margin is median/2) and the average size per broker to target
            # Yes, I know the median calculation is slightly broken (it keeps integers). This is OK
            targets[pos] = sum([getattr(p, self._size_attr) for p in partitions[pos]], 0) // len(self.cluster.brokers)
            sizelen = len(partitions[pos])
            if not sizelen % 2:
                margins[pos] = (getattr(partitions[pos][sizelen // 2], self._size_attr) + getattr(partitions[pos][sizelen // 2 - 1], self._size_attr)) // 4
            else:
                margins[pos] = getattr(partitions[pos][sizelen // 2], self._size_attr) // 2

        # Balance partitions for each replica position separately
        for pos in range(max_rf):
            if len(sizes[pos]) == 0:
                continue

            log.info("Calculating ideal state for replica position {0}".format(pos))
            log.debug("Target average size per-broker is {0} kibibytes (+/- {1})".format(targets[pos], margins[pos]))

            for broker_id in self.cluster.brokers:
                broker = self.cluster.brokers[broker_id]

                # Skip brokers that are larger than our minimum target size
                min_move = targets[pos] - margins[pos] - sizes[pos][broker_id]
                max_move = min_move + (margins[pos] * 2)
                if min_move <= 0:
                    continue
                log.debug("Moving between {0} and {1} kibibytes to broker {2}".format(min_move, max_move, broker_id))

                # Find partitions to move to this broker
                for partition in partitions[pos]:
                    partition_size = getattr(partition, self._size_attr)

                    # We can use this partition if all of the following are true: the partition has a replica at this position,
                    # it's size is less than or equal to the max move size, the broker at this replica position would not go out
                    # of range, and it doesn't already exist on this broker at this position
                    if ((len(partition.replicas) <= pos) or (partition_size > max_move) or
                       ((sizes[pos][partition.replicas[pos].id] - partition_size) < (targets[pos] - margins[pos])) or
                       (partition.replicas[pos] == broker)):
                        continue

                    # We can only use a partition that this replica exists on if swapping positions wouldn't hurt balance of the other position or broker
                    source = partition.replicas[pos]
                    if broker in partition.replicas:
                        other_pos = partition.replicas.index(broker)
                        if ((sizes[other_pos][broker_id] - partition_size < targets[other_pos] - margins[other_pos]) or
                           (sizes[other_pos][source.id] + partition_size > targets[pos] + margins[pos]) or
                           (sizes[pos][broker_id] + partition_size > targets[pos] + margins[pos]) or
                           (sizes[pos][source.id] - partition_size < targets[pos] - margins[pos])):
                            continue

                        partition.swap_replica_positions(source, broker)
                        sizes[other_pos][broker_id] -= partition_size
                        sizes[other_pos][source.id] += partition_size
                    else:
                        # Move the partition and adjust sizes
                        partition.swap_replicas(source, broker)
                    sizes[pos][broker_id] += partition_size
                    sizes[pos][source.id] -= partition_size
                    min_move -= partition_size
                    max_move -= partition_size

                    # If we have moved enough partitions, stop for this broker
                    if min_move <= 0:
                        break
