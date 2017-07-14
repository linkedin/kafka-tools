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
from operator import attrgetter

from kafka.tools import log
from kafka.tools.exceptions import BalanceException
from kafka.tools.assigner.actions import ActionBalanceModule


class ActionBalanceRackAware(ActionBalanceModule):
    name = "rackaware"
    helpstr = "Reassign partition replicas to assure they are in different racks, attempting to maintain counts and size per-broker"

    def __init__(self, args, cluster):
        super(ActionBalanceRackAware, self).__init__(args, cluster)

        # Set up a random ordered deque of brokers to use when we can't swap
        broker_list = list(self.cluster.brokers.values())
        random.shuffle(broker_list)
        self._random_brokers = deque(broker_list)

    def process_cluster(self):
        log.info("Starting partition balance by rack")

        # Check if rack information is set for the cluster
        broker_racks = [broker.rack for broker in self.cluster.brokers.values()]
        if len(set(broker_racks)) == 1:
            raise BalanceException("Cannot balance cluster by rack as it has no rack information")

        # Figure out the max RF for the cluster
        max_rf = self.cluster.max_replication_factor()

        # Balance partitions at each position separately
        for pos in range(max_rf):
            self._process_partitions_at_pos(pos)

    def _get_sorted_partition_list_at_pos(self, pos):
        # Create a list of partitions to use at this position, sorted by size
        partitions = [p for p in self.cluster.partitions(self.args.exclude_topics) if (len(p.replicas) > (pos + 1))]
        partitions.sort(key=attrgetter('size'))
        return partitions

    def _process_partitions_at_pos(self, pos):
        # Create a sorted list of partitions to use at this position (descending size)
        partitions = self._get_sorted_partition_list_at_pos(pos)

        for i, partition in enumerate(partitions):
            # Check if the partition is already on different racks
            partition_racks = racks_for_replica_list(partition.replicas)
            if len(partition_racks) == len(set(partition_racks)):
                continue

            # Find the partition that is closest in size that has a replica that can be swapped
            # We slice the partitions list here, leaving off the partition we're working on. This way we can just pop off the lists
            large_partitions = partitions[min(i+1, len(partitions)):]
            large_partitions.reverse()
            swap_partition = self._try_pick_swap_partition(partition, pos, partitions[0:i], large_partitions)

            if swap_partition is not None:
                # Swap the replicas at the current position on both partitions
                orig_replica = partition.replicas[pos]
                partition.swap_replicas(orig_replica, swap_partition.replicas[pos])
                swap_partition.swap_replicas(swap_partition.replicas[pos], orig_replica)
            else:
                # If we don't have a swap, pick the next broker in a different rack
                swap_broker = self._try_pick_new_broker(partition, pos)
                partition.swap_replicas(partition.replicas[pos], swap_broker)

    def _try_pick_swap_partition(self, partition, pos, small_partitions, large_partitions):
        """
        Given a partition and a replica position, try and find a partition that has a replica in the same position
        that can be swapped to improve rack separation. The partition selected should be the closest in size possible.

        :params partiton: the Partition with the replica that needs to be changed
        :params pos: the position of the replica to be changed
        :params small_partitions: a list of Partition objects, sorted in order by size, smaller than the Partion to change
        :params large_partitions: a list of Partition objects, sorted in order by size, larger than the Partion to change
        :returns: a Partition object that is OK to swap, or None
        """
        while len(small_partitions) + len(large_partitions) > 0:
            smaller_size_diff = difference_in_size_to_last_partition(partition, small_partitions)
            larger_size_diff = difference_in_size_to_last_partition(partition, large_partitions)

            # Check the partition with the smallest size difference
            target = None
            if larger_size_diff < smaller_size_diff:
                target = large_partitions.pop()
            else:
                target = small_partitions.pop()
            if check_partition_swappable(partition.replicas, target.replicas, pos):
                return target

        return None

    def _try_pick_new_broker(self, partition, pos):
        """
        Given a partition and a replica position, pick a new broker for that position that is in a different rack than other replicas

        :params partition: the Partition to modify
        :params pos: the replica position to change
        :returns: the Broker to swap in at the specified position
        :raises: BalanceException if a broker cannot be found that is in a different rack
        """
        replica_racks = set(racks_for_replica_list(partition.replicas, pos))
        for bidx in range(len(self._random_brokers)):
            broker = self._random_brokers.popleft()
            self._random_brokers.append(broker)
            if broker in partition.replicas:
                continue
            if broker.rack not in replica_racks:
                return broker

        raise BalanceException("Cannot find a swap for broker {0} on partition {1}:{2} at position {3}".format(partition.replicas[pos].id,
                                                                                                               partition.topic.name,
                                                                                                               partition.num,
                                                                                                               pos))


def difference_in_size_to_last_partition(partition, partitions):
    """
    Return the difference in size between the specified Partition and the last Partition in the provided list. If the list is
    empty, return infinity.

    :params partition: a Partition object to use for calculating the difference
    :params partitions: a list of Partition objects
    :returns: The difference in size between partition and the last Partition in the partitions list, or infinity
    """
    if len(partitions) == 0:
        return float("inf")
    return abs(partition.size - partitions[-1].size)


def racks_for_replica_list(replicas, pos=None):
    """
    Returns a set of racks for each of the given replicas in the list
    Skip the replica at position pos, if specified

    :params replicas: a list of Broker objects
    :params pos: a replica position to skip, or None to not skip a replica
    :returns: a list of racks
    """
    return [replica.rack for i, replica in enumerate(replicas) if (pos is None) or (i != pos)]


def check_partition_swappable(replicas_a, replicas_b, pos):
    """
    Check if the broker at position pos in the first replica list can be swapped with the replica at position pos in the second list
    1. replicas_a[pos] must not be in the replicas_b list
    2. replicas_b[pos] must not be in the replicas_a list
    3. replicas_a[pos] must have a different rack than the replicas in replicas_b (except for replicas_b[pos])
    4. replicas_b[pos] must have a different rack than the replicas in replicas_a (except for replicas_a[pos])

    :params replicas_a: the first replica list
    :params replicas_b: the second replica list
    :params pos: the position in the replica list to be replaced
    :returns: True if the broker can be swapped into this replica list, False otherwise
    """
    if replicas_a[pos] in replicas_b or replicas_b[pos] in replicas_a:
        return False
    return replicas_a[pos].rack not in racks_for_replica_list(replicas_b, pos) and replicas_b[pos].rack not in racks_for_replica_list(replicas_a, pos)
