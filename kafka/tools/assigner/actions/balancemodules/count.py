from kafka.tools.assigner.actions.balancemodules import ActionBalanceModule


class ActionBalanceCount(ActionBalanceModule):
  name = "count"
  helpstr = "Move the smallest partitions in the cluster to even the partition count per-broker for each replica position"

  def process_cluster(self):
    self.log.info("Starting partition balance by count")

    # Figure out the max RF for the cluster and sort all partition lists by size (ascending)
    max_pos = self.cluster.max_replication_factor()
    for broker in self.cluster.brokers:
      for pos in self.cluster.brokers[broker].partitions:
        self.cluster.brokers[broker].partitions[pos].sort(key=attrgetter('size'))

    # Balance partition counts for each replica position separately
    for pos in range(max_pos):
      # Calculate the maximum number of partitions each broker should have (floor(average) + 1)
      pcount = 0
      for broker in self.cluster.brokers:
        if pos in self.cluster.brokers[broker].partitions:
          pcount += len(self.cluster.brokers[broker].partitions[pos])
      pmax = (pcount / len(self.cluster.brokers)) + 1
      remainder = pcount % len(self.cluster.brokers)
      self.log.info("Calculating ideal state for replica position {0} - max {1} partitions".format(pos, pmax))

      for broker in self.cluster.brokers:
        # Figure out how many more partitions this broker needs
        if remainder > 0:
          # Get rid of the "extra" partitions from our average
          diff = pmax
          remainder -= 1
        else:
          diff = pmax - 1
        if pos in self.cluster.brokers[broker].partitions:
          diff -= len(self.cluster.brokers[broker].partitions[pos])

        if diff > 0:
          self.log.debug("Moving {0} partitions to broker {1}".format(diff, broker))

          # Iterate through the largest brokers to find diff partitions to move to this broker
          for source in self.cluster.brokers:
            if diff == 0:
              break
            if pos not in self.cluster.brokers[source].partitions:
              continue

            iterlist = list(self.cluster.brokers[source].partitions[pos])
            for partition in iterlist:
              # If we have moved enough partitions from this broker, exit out of the inner loop
              if (len(self.cluster.brokers[source].partitions[pos]) < pmax) or (diff == 0):
                break
              # Skip partitions that are already on the target broker
              if broker in partition.replicas:
                continue

              partition.swap_replicas(self.cluster.brokers[source], self.cluster.brokers[broker])
              diff -= 1

          self.log.debug("Finish broker {0} with {1} partitions".format(broker, len(self.cluster.brokers[broker].partitions[pos])))
        elif diff < 0:
          self.log.debug("Moving {0} partitions off broker {1}".format(-diff, broker))

          # Iterate through the smallest brokers to find diff partitions to move off this broker
          for target in self.cluster.brokers:
            if diff == 0:
              break
            if (pos in self.cluster.brokers[target].partitions) and (len(self.cluster.brokers[target].partitions[pos]) > (pmax + 1)):
              continue

            iterlist = list(self.cluster.brokers[broker].partitions[pos])
            for partition in iterlist:
              # If we have moved enough partitions to this broker, exit out of the inner loop
              if ((pos in self.cluster.brokers[target].partitions) and (len(self.cluster.brokers[target].partitions[pos]) >= pmax)) or (diff == 0):
                break
              # Skip partitions that are already on the target broker
              if target in partition.replicas:
                continue

              partition.add_replica(self.cluster.brokers[target], pos)
              partition.remove_replica(self.cluster.brokers[broker])
              diff += 1

          self.log.debug("Finish broker {0} with {1} partitions".format(broker, len(self.cluster.brokers[broker].partitions[pos])))
        else:
          self.log.debug("Skipping broker {0} which has {1} partitions".format(broker, len(self.cluster.brokers[broker].partitions[pos])))
          continue
