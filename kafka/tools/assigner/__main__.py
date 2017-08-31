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

import os
import sys
import time
import json

import kafka.tools.assigner.actions
import kafka.tools.assigner.plugins
import kafka.tools.assigner.sizers
from kafka.tools import log
from kafka.tools.utilities import get_tools_path, check_java_home
from kafka.tools.models.cluster import Cluster

from kafka.tools.assigner.arguments import set_up_arguments
from kafka.tools.assigner.batcher import split_partitions_into_batches
from kafka.tools.exceptions import ProgrammingException
from kafka.tools.modules import get_modules
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


def get_all_plugins():
    return [plugin() for plugin in get_plugins_list()]


def run_plugins_at_step(plugins, step_name, *args):
    for plugin in plugins:
        try:
            func = getattr(plugin, step_name)
        except AttributeError:
            raise ProgrammingException("Attempt to call plugins with an unknown step")
        if len(args) > 0:
            func(args)
        else:
            func()


def print_leadership(type_str, cluster, dont_skip):
    if dont_skip:
        log.info("Cluster Leadership Balance ({0}):".format(type_str))
        cluster.log_broker_summary()


def is_dry_run(args):
    if args.generate or not args.execute:
        log.info("--execute flag NOT specified. DRY RUN ONLY")
        return True
    return False


def main():
    # Start by loading all the modules
    action_map = get_module_map(kafka.tools.assigner.actions, kafka.tools.assigner.actions.ActionModule)
    sizer_map = get_module_map(kafka.tools.assigner.sizers, kafka.tools.assigner.sizers.SizerModule)
    plugins = get_all_plugins()

    # Set up and parse all CLI arguments
    args = set_up_arguments(action_map, sizer_map, plugins)
    run_plugins_at_step(plugins, 'set_arguments', args)

    tools_path = get_tools_path(args.tools_path)
    check_java_home()

    cluster = Cluster.create_from_zookeeper(args.zookeeper, getattr(args, 'default_retention', 1))
    run_plugins_at_step(plugins, 'set_cluster', cluster)

    # If the module needs the partition sizes, call a size module to get the information
    check_and_get_sizes(action_map[args.action], args, cluster, sizer_map)
    run_plugins_at_step(plugins, 'after_sizes')
    print_leadership("before", cluster, args.leadership)

    # Clone the cluster, and run the action to generate a new cluster state
    newcluster = cluster.clone()
    action_to_run = action_map[args.action](args, newcluster)
    action_to_run.process_cluster()
    run_plugins_at_step(plugins, 'set_new_cluster', action_to_run.cluster)
    print_leadership("after", newcluster, args.leadership)

    move_partitions = cluster.changed_partitions(action_to_run.cluster)
    batches = split_partitions_into_batches(move_partitions, batch_size=args.moves, use_class=Reassignment)
    run_plugins_at_step(plugins, 'set_batches', batches)

    log.info("Partition moves required: {0}".format(len(move_partitions)))
    log.info("Number of batches: {0}".format(len(batches)))
    dry_run = is_dry_run(args)

    for i, batch in enumerate(batches):
        log.info("Executing partition reassignment {0}/{1}: {2}".format(i + 1, len(batches), repr(batch)))
        batch.execute(i + 1, len(batches), args.zookeeper, tools_path, plugins, dry_run)

    run_plugins_at_step(plugins, 'before_ple')

    if not args.skip_ple:
        all_cluster_partitions = [p for p in action_to_run.cluster.partitions(args.exclude_topics)]
        batches = split_partitions_into_batches(all_cluster_partitions, batch_size=args.ple_size, use_class=ReplicaElection)
        log.info("Number of replica elections: {0}".format(len(batches)))
        run_preferred_replica_elections(batches, args, tools_path, plugins, dry_run)

    run_plugins_at_step(plugins, 'finished')

    if args.output_json:
        data = {
            'before': cluster.to_dict(),
            'after': action_to_run.cluster.to_dict()
        }
        sys.stdout.write(json.dumps(data, indent=4, sort_keys=True))

    return os.EX_OK


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
