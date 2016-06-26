from kafka.tools.assigner import log
from kafka.tools.assigner.models.reassignment import Reassignment


def select_partitions_to_move(cluster, newcluster):
  log.info("Generating moves to reach the ideal state")
  moves = []
  for partition in newcluster.partitions:
    if partition.replicas != cluster.topics[partition.topic.name].partitions[partition.num].replicas:
      moves.append(partition)
  return moves


def split_partitions_into_batches(partitions, batch_size=10):
  # Currently, this is a very simplistic implementation that just breaks the list of partitions down
  # into even sized chunks. While it could be implemented as a generator, it's not so that it can
  # split the list into more efficient batches.
  batches = [Reassignment(partitions[i:i + batch_size]) for i in xrange(0, len(partitions), batch_size)]
  return batches
