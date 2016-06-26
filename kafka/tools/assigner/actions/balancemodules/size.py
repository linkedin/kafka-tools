from kafka.tools.assigner.actions.balancemodules import ActionBalanceModule


class ActionBalanceSize(ActionBalanceModule):
  name = "size"
  helpstr = "Move the largest partitions in the cluster to even the total size on disk per-broker for each replica position"

  def process_cluster(self):
    self.log.info("Starting partition balance by size")

    # Figure out the max RF for the cluster
    max_pos = self.cluster.max_replication_factor()

    # Balance partitions for each replica position separately
    for pos in range(max_pos):
      self.log.info("Calculating ideal state for replica position {0}".format(pos))

      # Calculate starting broker sizes at this position
      sizes = {}
      for broker in self.cluster.brokers:
        if pos in self.cluster.brokers[broker].partitions:
          sizes[broker] = sum([p.size for p in self.cluster.brokers[broker].partitions[pos]], 0)
        else:
          sizes[broker] = 0

      # Create a sorted list of partitions to use at this position (descending size)
      # Throw out partitions that are 4K or less in size, as they are effectively empty
      partitions = [p for t in self.cluster.topics for p in self.cluster.topics[t].partitions if (len(p.replicas) > pos) and (p.size > 4)]
      partitions.sort(key=attrgetter('size'), reverse=True)

      # Calculate the median size of partitions (margin is median/2) and the average size per broker to target
      # Yes, I know the median calculation is slightly broken (it keeps integers). This is OK
      avg_size = sum([p.size for p in partitions], 0) / len(self.cluster.brokers)
      sizelen = len(partitions)
      if not sizelen % 2:
        margin = (partitions[sizelen / 2].size + partitions[sizelen / 2 - 1].size) / 4
      else:
        margin = partitions[sizelen / 2].size / 2
      self.log.debug("Target average size per-broker is {0} kibibytes (+/- {1})".format(avg_size, margin))

      for broker in self.cluster.brokers:
        # Skip brokers that are larger than our minimum target size
        min_move = avg_size - margin - sizes[broker]
        max_move = min_move + (margin * 2)
        if min_move <= 0:
          continue
        self.log.debug("Moving between {0} and {1} kibibytes to broker {2}".format(min_move, max_move, broker))

        # Find partitions to move to this broker
        for partition in partitions:
          # We can use this partition if all of the following are true: the partition has a replica at this position, it's size is less than or equal to the
          # max move size, the broker at this replica position would not go out of range, and it doesn't already exist on this broker
          if ((len(partition.replicas) <= pos) or (partition.size > max_move) or ((sizes[partition.replicas[pos]] - partition.size) < (avg_size - margin)) or
             (broker in partition.replicas)):
            continue

          # Move the partition and adjust sizes
          source = partition.replicas[pos]
          partition.swap_replicas(self.cluster.brokers[source], self.cluster.brokers[broker])
          min_move -= partition.size
          max_move -= partition.size
          sizes[broker] += partition.size
          sizes[source] -= partition.size

          # If we have moved enough partitions, stop for this broker
          if min_move <= 0:
            break
