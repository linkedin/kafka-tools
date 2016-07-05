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

import sys
import time

import kafka.tools.assigner.actions
import kafka.tools.assigner.plugins
import kafka.tools.assigner.sizers
from kafka.tools.assigner import log
from kafka.tools.assigner.arguments import set_up_arguments
from kafka.tools.assigner.batcher import split_partitions_into_batches
from kafka.tools.assigner.modules import get_modules
from kafka.tools.assigner.tools import get_tools_path, check_java_home
from kafka.tools.assigner.models.cluster import Cluster
from kafka.tools.assigner.models.reassignment import Reassignment
from kafka.tools.assigner.models.replica_election import ReplicaElection


def get_module_map(base_module_name, base_module_class):
    return dict((cls.name, cls) for cls in get_modules(base_module_name, base_module_class))


def get_plugins_list():
    return get_modules(kafka.tools.assigner.plugins, kafka.tools.assigner.plugins.PluginModule)


def check_and_get_sizes(action_cls, args, cluster, sizer_map):
    if action_cls.needs_sizes:
        sizer_to_run = sizer_map[args.sizer](args, cluster)
        sizer_to_run.get_partition_sizes()

        if args.size:
            log.info("Partition Sizes:")
            for topic in cluster.topics:
                for partition in cluster.topics[topic].partitions:
                    log.info("{0} {1}:{2}".format(partition.size, topic, partition.num))


def run_preferred_replica_elections(batches, args, tools_path, plugins, dry_run):
    for i, batch in enumerate(batches):
        # Sleep between PLEs
        if i > 0 and not dry_run:
            log.info("Waiting {0} seconds for replica election to complete".format(args.ple_wait))
            time.sleep(args.ple_wait)

        log.info("Executing preferred replica election {0}/{1}".format(i + 1, len(batches)))
        batch.execute(i + 1, len(batches), args.zookeeper, tools_path, plugins, dry_run)


def main():
    # Start by loading all the modules
    action_map = get_module_map(kafka.tools.assigner.actions, kafka.tools.assigner.actions.ActionModule)
    sizer_map = get_module_map(kafka.tools.assigner.sizers, kafka.tools.assigner.sizers.SizerModule)
    plugins_list = get_plugins_list()

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
    check_and_get_sizes(action_map[args.action], args, cluster, sizer_map)

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

    move_partitions = cluster.changed_partitions(action_to_run.cluster)
    batches = split_partitions_into_batches(move_partitions, batch_size=args.moves, use_class=Reassignment)

    for plugin in plugins:
        plugin.set_batches(batches)

    log.info("Partition moves required: {0}".format(len(move_partitions)))
    log.info("Number of batches: {0}".format(len(batches)))

    dry_run = args.generate or not args.execute
    if dry_run:
        log.info("--execute flag NOT specified. DRY RUN ONLY")

    for i, batch in enumerate(batches):
        log.info("Executing partition reassignment {0}/{1}: {2}".format(i + 1, len(batches), repr(batch)))
        batch.execute(i + 1, len(batches), args.zookeeper, tools_path, plugins, dry_run)

    for plugin in plugins:
        plugin.before_ple()

    if not args.skip_ple:
        all_cluster_partitions = [p for p in action_to_run.cluster.partitions()]
        batches = split_partitions_into_batches(all_cluster_partitions, batch_size=args.ple_size, use_class=ReplicaElection)
        log.info("Number of replica elections: {0}".format(len(batches)))
        run_preferred_replica_elections(batches, args, tools_path, plugins, dry_run)

    for plugin in plugins:
        plugin.finished()

    return 0

if __name__ == "__main__":
    sys.exit(main())
