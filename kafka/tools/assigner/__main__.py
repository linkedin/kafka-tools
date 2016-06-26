import json
import logging
import os
import sys

import kafka.tools.assigner.actions
import kafka.tools.assigner.plugins
import kafka.tools.assigner.sizers
from kafka.tools.assigner import log
from kafka.tools.assigner.arguments import set_up_arguments
from kafka.tools.assigner.batcher import select_partitions_to_move, split_partitions_into_batches
from kafka.tools.assigner.modules import get_modules
from kafka.tools.assigner.tools import get_tools_path, check_java_home, execute_preferred_replica_election
from kafka.tools.assigner.models.cluster import Cluster


def main():
  # Start by loading all the modules
  action_map = {cls.name: cls for cls in get_modules(kafka.tools.assigner.actions, kafka.tools.assigner.actions.ActionModule)}
  sizer_map = {cls.name: cls for cls in get_modules(kafka.tools.assigner.sizers, kafka.tools.assigner.sizers.SizerModule)}
  plugins_list = get_modules(kafka.tools.assigner.plugins, kafka.tools.assigner.plugins.PluginModule)

  # Instantiate all plugins
  plugins = [plugin() for plugin in plugins_list]

  # Set up and parse all CLI arguments
  args = set_up_arguments(action_map, sizer_map, plugins)

  for plugin in plugins:
    plugin.set_arguments(args)

  tools_path = get_tools_path(args.tools_path)
  check_java_home()
  cluster = Cluster.create_from_zookeeper(args.zookeeper)

  for plugin in plugins:
    plugin.set_cluster(cluster)

  # If the module needs the partition sizes, call a size module to get the information
  if action_map[args.action].needs_sizes:
    sizer_to_run = sizer_map[args.sizer](args, cluster)
    sizer_to_run.get_partition_sizes()

    if args.size:
      log.info("Partition Sizes:")
      for topic in cluster.topics:
        for partition in cluster.topics[topic].partitions:
          log.info("{0} {1}:{2}".format(partition.size, topic, partition.num))

  for plugin in plugins:
    plugin.after_sizes()

  if args.leadership:
    log.info("Cluster Leadership Balance (before):")
    cluster.log_broker_summary()

  # Clone the cluster, and run the action to generate a new cluster state
  newcluster = cluster.clone()
  action_to_run = action_map[args.action](args, newcluster)
  action_to_run.process_cluster()

  for plugin in plugins:
    plugin.set_new_cluster(action_to_run.cluster)

  if args.leadership:
    log.info("Cluster Leadership Balance (after):")
    newcluster.log_broker_summary()

  move_partitions = select_partitions_to_move(cluster, action_to_run.cluster)
  batches = split_partitions_into_batches(move_partitions, args.moves)

  for plugin in plugins:
    plugin.set_batches(batches)

  log.info("Partition moves required: {0}".format(len(move_partitions)))
  log.info("Number of batches: {0}".format(len(batches)))

  dry_run = args.generate or not args.execute
  if dry_run:
    log.info("--execute flag NOT specified. DRY RUN ONLY")

  for i, batch in enumerate(batches):
    log.info("Executing partition reassignment {0}/{1}: {2}".format(i + 1, len(batches), repr(batch)))
    batch.execute(i + 1, len(batches), args.zookeeper, tools_path, dry_run, plugins, dry_run)

  for plugin in plugins:
    plugin.before_ple()

  if not args.skip_ple:
    if dry_run:
      log.info("Dry run - skipping preferred replica election")
    else:
      execute_preferred_replica_election(cluster, args.zookeeper, tools_path, args.ple_size, args.ple_wait)

  for plugin in plugins:
    plugin.finished()

  sys.exit(0)

if __name__ == "__main__":
  main()
