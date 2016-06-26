from kafka.tools.assigner.actions.balancemodules import ActionBalanceModule


class ActionBalanceEven(ActionBalanceModule):
  name = "even"
  helpstr = "Evenly spread topics that are a multiple of the number of brokers across the cluster"

  def process_cluster(self):
    for topic in self.cluster.topics:
      if len(self.cluster.topics[topic].partitions) % len(self.cluster.brokers) != 0:
        self.log.warn("Skipping topic {0} as it has {1} partitions, which is not a multiple of the number of brokers ({2})".format(
            topic, len(self.cluster.topics[topic].partitions), len(self.cluster.brokers)))
        continue
      rf = len(self.cluster.topics[topic].partitions[0].replicas)
      target = len(self.cluster.topics[topic].partitions) / len(self.cluster.brokers)
      for partition in self.cluster.topics[topic].partitions:
        if len(partition.replicas) != rf:
          self.log.warn("Skipping topic {0} as not all partitions have the same replication factor".format(topic))
          continue

      # Initialize broker map for this topic.
      pmap = [dict.fromkeys(self.cluster.brokers.keys(), 0) for pos in range(rf)]

      for partition in self.cluster.topics[topic].partitions:
        for pos in range(rf):
          # Current placement is fine. Leave the replica where it is
          if pmap[pos][partition.replicas[pos]] < target:
            pmap[pos][partition.replicas[pos]] += 1
            continue

          # Find a new replica for this partition at this position
          for bid in pmap[pos]:
            # Skip this broker if it's already in the replica list, or if it's already at capacity
            if (bid in partition.replicas) or (pmap[pos][bid] >= target):
              continue

            source = partition.replicas[pos]
            partition.swap_replicas(self.cluster.brokers[source], self.cluster.brokers[bid])
            pmap[pos][bid] += 1
            break
