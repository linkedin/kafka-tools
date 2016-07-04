from __future__ import division

from kafka.tools.assigner.actions import ActionModule


class ActionReorder(ActionModule):
    name = "reorder"
    helpstr = "Reelect partition leaders using replica reordering"

    def process_cluster(self):
        # Start all the leader counts at zero
        leaders = {}
        for broker in self.cluster.brokers:
            leaders[broker] = 0

        for partition in self.cluster.partitions():
            # The best leader is either:
            #  1) The first replica that has 0 leaders so far
            #  2) The replica with the lowest leader ratio
            new_leader = None
            min_ratio = None
            for replica in partition.replicas:
                if leaders[replica.id] == 0:
                    new_leader = replica
                    break
            if new_leader is None:
                for replica in partition.replicas:
                    leader_ratio = leaders[replica.id] / replica.num_partitions()
                    if (min_ratio is None) or (leader_ratio < min_ratio):
                        min_ratio = leader_ratio
                        new_leader = replica

            # If the leader changed, swap the new leader with the current leader
            if new_leader != partition.replicas[0]:
                partition.swap_replica_positions(new_leader, partition.replicas[0])

            # Update the brokers hash
            leaders[new_leader.id] += 1
