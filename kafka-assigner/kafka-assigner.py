#!/usr/bin/env python

# Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
# 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

from __future__ import division

import argparse
import commands
import json
import logging
import math
import os
import paramiko
import re
import sys
import time
from collections import deque
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException
from operator import attrgetter
from tempfile import NamedTemporaryFile

# Set up logging before anything else
log = logging.getLogger('kafka-assigner')
log.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('[%(levelname)s] %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

########################################################################################################################
# Helper functions for main and modules


class Cluster:
  def __init__(self):
    self.brokers = {}
    self.topics = {}

  def clone(self):
    newcluster = Cluster()

    for broker in self.brokers:
      newcluster.brokers[broker] = Broker(broker, self.brokers[broker].hostname)

    for tname in self.topics:
      topic = self.topics[tname]
      newtopic = Topic(tname, len(topic.partitions))
      for i, partition in enumerate(newtopic.partitions):
        partition.replicas = list(topic.partitions[i].replicas)
        partition.size = topic.partitions[i].size
        for pos, replica in enumerate(partition.replicas):
          newcluster.brokers[replica].add_partition(pos, partition)
      newcluster.topics[tname] = newtopic

    return newcluster


class Broker:
  def __init__(self, broker_id, hostname):
    self.broker_id = broker_id
    self.hostname = hostname
    self.partitions = {}

  def add_partition(self, pos, partition):
    if pos not in self.partitions:
      self.partitions[pos] = [partition]
    else:
      self.partitions[pos].append(partition)

  def remove_partition(self, partition):
    pos = partition.replicas.index(self.broker_id)
    self.partitions[pos].remove(partition)

  def num_leaders(self):
    if 0 in self.partitions:
      return len(self.partitions[0])
    else:
      return 0

  def percent_leaders(self):
    if self.num_partitions() == 0:
      return 0.0
    return (self.num_leaders() / self.num_partitions()) * 100

  def total_size(self):
    return sum([p.size for pos in self.partitions for p in self.partitions[pos]], 0)

  def num_partitions(self):
    return sum([len(self.partitions[pos]) for pos in self.partitions], 0)


class Partition:
  def __init__(self, topic, num):
    self.topic = topic
    self.num = num
    self.replicas = []
    self.size = 0

  def __eq__(self, other):
    return (self.topic == other.topic) and (self.num == other.num)


class Topic:
  def __init__(self, name, partitions):
    self.name = name
    self.partitions = []
    for i in range(partitions):
      self.partitions.append(Partition(self, i))

  def __eq__(self, other):
    return self.name == other.name


def batch(iterable, n=1):
   l = len(iterable)
   for ndx in range(0, l, n):
       yield iterable[ndx:min(ndx+n, l)]


def get_partition_sizes(cluster, args):
  # Set up an SSH client for connecting to the brokers, and silence the logs
  client = paramiko.SSHClient()
  plogger = paramiko.util.logging.getLogger()
  plogger.setLevel(logging.WARNING)
  client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  client.load_system_host_keys()

  # Get broker partition sizes
  sizes = {}
  size_re = re.compile("^([0-9]+)\s+.*?\/([a-z0-9_-]+)-([0-9]+)\s*$", re.I)
  for broker in cluster.brokers:
    log.info("Getting partition sizes for {0}".format(cluster.brokers[broker].hostname))
    client.connect(cluster.brokers[broker].hostname, allow_agent=True)
    stdin, stdout, stderr = client.exec_command('du -sk {0}/*'.format(args.datadir))
    for ln in stdout.readlines():
      m = size_re.match(ln)
      if m:
        size = int(m.group(1))
        topic = m.group(2)
        pnum = int(m.group(3))

        if topic not in cluster.topics:
          log.warn("Unknown topic found on disk on broker {0}: {1}".format(broker, topic))
        elif pnum >= len(cluster.topics[topic].partitions):
          log.warn("Unknown partition found on disk on broker {0}: {1}:{2}".format(broker, topic, pnum))
        elif size > cluster.topics[topic].partitions[pnum].size:
          cluster.topics[topic].partitions[pnum].size = int(m.group(1))

  if args.size:
    log.info("Partition Sizes:")
    for topic in cluster.topics:
      for partition in cluster.topics[topic].partitions:
        log.info("{0} {1}:{2}".format(partition.size, topic, partition.num))

  return sizes


def is_exec_file(fname):
  return os.path.isfile(fname) and os.access(fname, os.X_OK)

########################################################################################################################
# MODULE CODE BELOW


# The elect module is a dummy module that does no partition moves, just triggers the PLE
def module_elect(cluster, args):
  return cluster


# Given a topic, assure that all its partitions have the specified replication factor by adding or removing replicas
def module_set_replication_factor(cluster, args):
  if args.replication_factor < 1:
    log.error('You cannot set replication-factor below 1')
    sys.exit(1)

  for partition in cluster.topics[args.topic].partitions:
    replica_set = set(partition.replicas)
    if len(partition.replicas) < args.replication_factor:
      broker_id_list = [broker_id for broker_id in cluster.brokers.keys() if broker_id not in replica_set]
      random.shuffle(broker_id_list)
      for broker_id in broker_id_list:
        partition.replicas.append(broker_id)
        cluster.brokers[broker_id].add_partition(len(partition.replicas) - 1, partition)
        if len(partition.replicas) == args.replication_factor:
          break
    elif len(partition.replicas) > args.replication_factor:
      for broker_id in replica_set:
        replicas = partition.replicas[:]
        random.shuffle(replicas)
        if broker_id in replicas:
          cluster.brokers[broker_id].remove_partition(partition)
          partition.replicas.remove(broker_id)
          if len(partition.replicas) == args.replication_factor:
            break

    if len(partition.replicas) != args.replication_factor:
      log.error('Not able to set replication-factor to %d', args.replication_factor)
      sys.exit(1)
  return cluster


# Given a layout and a list of brokers with partition counts per broker, reorder replicas to produce balanced leadership
# NOTE - This can be very hard on wildcard consumers
def module_reorder(cluster, args):
  # Start all the leader counts at zero
  leaders = {}
  for broker in cluster.brokers:
    leaders[broker] = 0

  for topic in sorted(cluster.topics.keys()):
    for partition in cluster.topics[topic].partitions:
      # The best leader is either:
      #  1) The first replica that has 0 leaders so far
      #  2) The replica with the lowest leader ratio
      new_leader = None
      min_ratio = None
      for replica in partition.replicas:
        if leaders[replica] == 0:
          new_leader = replica
          break
      if new_leader is None:
        for replica in partition.replicas:
          leader_ratio = leaders[replica] / cluster.brokers[replica].num_partitions()
          if (min_ratio is None) or (leader_ratio < min_ratio):
            min_ratio = leader_ratio
            new_leader = replica

      # If the leader changed, add it to our partition move list
      if partition.replicas.index(new_leader) != 0:
        # Reorder the replica list and add it to the list of moves
        cluster.brokers[new_leader].remove_partition(partition)
        cluster.brokers[new_leader].add_partition(0, partition)
        cluster.brokers[partition.replicas[0]].remove_partition(partition)
        cluster.brokers[partition.replicas[0]].add_partition(1, partition)
        partition.replicas.remove(new_leader)
        partition.replicas.insert(0, new_leader)

      # Update the brokers hash
      leaders[new_leader] += 1

  return cluster


# Increase the RF for all partitions on the from broker, adding them to the to_broker and setting the to_broker to leader
def module_clone(cluster, args):
  for b in args.brokers:
    if b not in cluster.brokers:
      log.error("Source broker (ID {0}) is not in the brokers list for this cluster".format(b))
      sys.exit(1)
  if args.to_broker not in cluster.brokers:
    log.error("Target broker (ID {0}) is not in the brokers list for this cluster".format(args.to_broker))
    sys.exit(1)

  source_set = set(args.brokers)
  for topic in cluster.topics:
    for partition in cluster.topics[topic].partitions:
      if len(source_set & set(partition.replicas)) > 0:
        if args.to_broker in partition.replicas:
          log.warn("Target broker (ID {0}) is already in the replica list for {1}:{2}".format(args.to_broker, partition.topic.name, partition.num))

          # If the broker is already in the replica list, it ALWAYS becomes the leader
          if partition.replicas.index(args.to_broker) != 0:
            newcluster.brokers[args.to_broker].remove_partition(partition)
            newcluster.brokers[args.to_broker].add_partition(0, partition)
            partition.replicas.remove(args.to_broker)
            partition.replicas.insert(0, args.to_broker)
        else:
          # If one of the source brokers is currently the leader, the target broker is the leader. Otherwise, the target leader is in second place
          if partition.replicas[0] in args.brokers:
            cluster.brokers[args.to_broker].add_partition(0, partition)
            partition.replicas.insert(0, args.to_broker)
          else:
            cluster.brokers[args.to_broker].add_partition(1, partition)
            partition.replicas.insert(1, args.to_broker)

  return cluster


# Decrease the RF for all partitions, removing the from brokers. This will fail if the RF goes under 1 for any partition
def module_trim(cluster, args):
  for b in args.brokers:
    if b not in cluster.brokers:
      log.error("Broker (ID {0}) is not in the brokers list for this cluster".format(b))
      sys.exit(1)

  # For each broker specified, remove it from the replica list for all its partitions
  for b in args.brokers:
    for pos in cluster.brokers[b].partitions:
      for partition in cluster.brokers[b].partitions[pos]:
        partition.replicas.remove(b)
        if len(partition.replicas) < 1:
          log.error("Cannot trim {0}:{1} as it would result in an empty replica list".format(partition.topic.name, partition.num))
          sys.exit(1)

      cluster.brokers[b].partitions[pos] = []

  return cluster


# Move partitions from one broker to one or more other brokers (maintaining RF)
def module_remove(cluster, args):
  if args.broker not in cluster.brokers:
    log.warn("Broker to remove (ID {0}) is not in the brokers list for this cluster. This may just be because it's offline".format(args.broker))
  if (args.to_brokers is None) or (len(args.to_brokers) == 0):
    args.to_brokers = list(set(cluster.brokers.keys()) - set([args.broker]))
  else:
    for b in args.to_brokers:
      if b not in cluster.brokers:
        log.error("Target broker (ID {0}) is not in the brokers list for this cluster".format(b))
        sys.exit(1)
    if args.broker in args.to_brokers:
      log.error("Broker to remove (ID {0}) was specified in the target broker list as well".format(b))
      sys.exit(1)

  # Make a deque for the target brokers so we can round-robin assignments
  todeque = deque(args.to_brokers)

  for pos in cluster.brokers[args.broker].partitions:
    iterlist = list(cluster.brokers[args.broker].partitions[pos])
    for partition in iterlist:
      # Find a new replica for this partition
      newreplica = None
      attempts = 0
      while attempts < len(todeque):
        proposed = todeque.popleft()
        todeque.append(proposed)
        if proposed not in partition.replicas:
          newreplica = proposed
          break
        attempts += 1

      if newreplica is None:
        log.error("Cannot find a new broker for {0}:{1} with replica list {2}".format(partition.topic.name, partition.num, partition.replicas))
        sys.exit(1)

      # Replace the broker coming out with the new one
      cluster.brokers[args.broker].remove_partition(partition)
      cluster.brokers[newreplica].add_partition(pos, partition)
      partition.replicas[pos] = newreplica

  return cluster


# Rebalance partitions across the cluster
def module_balance(cluster, args):
  # Start with adding the partition sizes to the cluster state (get via SSH)
  get_partition_sizes(cluster, args)

  for bmodule in args.types:
    if bmodule == 'count':
      cluster = module_balance_count(cluster, args)
    elif bmodule == 'size':
      cluster = module_balance_size(cluster, args)
    elif bmodule == 'even':
      cluster = module_balance_even(cluster, args)
    elif bmodule == 'leader':
      cluster = module_reorder(cluster, args)

  return cluster


# Rebalance partitions in the cluster based on the count, moving the smallest partitions first
def module_balance_count(cluster, args):
  log.info("Starting partition balance by count")

  # Figure out the max RF for the cluster and sort all partition lists by size (ascending)
  max_pos = 0
  for broker in cluster.brokers:
    for pos in cluster.brokers[broker].partitions:
      cluster.brokers[broker].partitions[pos].sort(key=attrgetter('size'))
      if pos > max_pos:
        max_pos = pos

  # Balance partition counts for each replica position separately
  for pos in range(max_pos):
    # Calculate the maximum number of partitions each broker should have (floor(average) + 1)
    pcount = 0
    for broker in cluster.brokers:
      if pos in cluster.brokers[broker].partitions:
        pcount += len(cluster.brokers[broker].partitions[pos])
    pmax = (pcount // len(cluster.brokers)) + 1
    remainder = pcount % len(cluster.brokers)
    log.info("Calculating ideal state for replica position {0} - max {1} partitions".format(pos, pmax))

    for broker in cluster.brokers:
      # Figure out how many more partitions this broker needs
      if remainder > 0:
        # Get rid of the "extra" partitions from our average
        diff = pmax
        remainder -= 1
      else:
        diff = pmax - 1
      if pos in cluster.brokers[broker].partitions:
        diff -= len(cluster.brokers[broker].partitions[pos])

      if diff > 0:
        log.debug("Moving {0} partitions to broker {1}".format(diff, broker))

        # Iterate through the largest brokers to find diff partitions to move to this broker
        for source in cluster.brokers:
          if diff == 0:
            break
          if pos not in cluster.brokers[source].partitions:
            continue

          iterlist = list(cluster.brokers[source].partitions[pos])
          for partition in iterlist:
            # If we have moved enough partitions from this broker, exit out of the inner loop
            if (len(cluster.brokers[source].partitions[pos]) < pmax) or (diff == 0):
              break
            # Skip partitions that are already on the target broker
            if broker in partition.replicas:
              continue

            cluster.brokers[source].remove_partition(partition)
            cluster.brokers[broker].add_partition(pos, partition)
            partition.replicas[pos] = broker
            diff -= 1

        log.debug("Finish broker {0} with {1} partitions".format(broker, len(cluster.brokers[broker].partitions[pos])))
      elif diff < 0:
        log.debug("Moving {0} partitions off broker {1}".format(-diff, broker))

        # Iterate through the smallest brokers to find diff partitions to move off this broker
        for target in cluster.brokers:
          if diff == 0:
            break
          if (pos in cluster.brokers[target].partitions) and (len(cluster.brokers[target].partitions[pos]) > (pmax + 1)):
            continue

          iterlist = list(cluster.brokers[broker].partitions[pos])
          for partition in iterlist:
            # If we have moved enough partitions to this broker, exit out of the inner loop
            if ((pos in cluster.brokers[target].partitions) and (len(cluster.brokers[target].partitions[pos]) >= pmax)) or (diff == 0):
              break
            # Skip partitions that are already on the target broker
            if target in partition.replicas:
              continue

            cluster.brokers[broker].remove_partition(partition)
            cluster.brokers[target].add_partition(pos, partition)
            partition.replicas[pos] = target
            diff += 1

        log.debug("Finish broker {0} with {1} partitions".format(broker, len(cluster.brokers[broker].partitions[pos])))
      else:
        log.debug("Skipping broker {0} which has {1} partitions".format(broker, len(cluster.brokers[broker].partitions[pos])))
        continue

  return cluster


# Rebalance partitions in the cluster based on the size on disk, moving the largest partitions first
def module_balance_size(cluster, args):
  log.info("Starting partition balance by size")

  # Figure out the max RF for the cluster and sort all partition lists by size (descending)
  max_pos = 0
  for broker in cluster.brokers:
    for pos in cluster.brokers[broker].partitions:
      cluster.brokers[broker].partitions[pos].sort(key=attrgetter('size'))
      if pos > max_pos:
        max_pos = pos

  # Balance partitions for each replica position separately
  for pos in range(max_pos):
    log.info("Calculating ideal state for replica position {0}".format(pos))

    # Calculate starting broker sizes at this position
    sizes = {}
    for broker in cluster.brokers:
      if pos in cluster.brokers[broker].partitions:
        sizes[broker] = sum([p.size for p in cluster.brokers[broker].partitions[pos]], 0)
      else:
        sizes[broker] = 0

    # Create a sorted list of partitions to use at this position (descending size)
    # Throw out partitions that are 4K or less in size, as they are effectively empty
    partitions = [p for t in cluster.topics for p in cluster.topics[t].partitions if (len(p.replicas) > pos) and (p.size > 4)]
    partitions.sort(key=attrgetter('size'), reverse=True)

    # Calculate the median size of partitions (margin is median/2) and the average size per broker to target
    # Yes, I know the median calculation is slightly broken (it keeps integers). This is OK
    avg_size = sum([p.size for p in partitions], 0) // len(cluster.brokers)
    sizelen = len(partitions)
    if not sizelen % 2:
      margin = (partitions[sizelen // 2].size + partitions[sizelen // 2 - 1].size) // 4
    else:
      margin = partitions[sizelen // 2].size // 2
    log.debug("Target average size per-broker is {0} kibibytes (+/- {1})".format(avg_size, margin))

    for broker in cluster.brokers:
      # Skip brokers that are larger than our minimum target size
      min_move = avg_size - margin - sizes[broker]
      max_move = min_move + (margin * 2)
      if min_move <= 0:
        continue
      log.debug("Moving between {0} and {1} kibibytes to broker {2}".format(min_move, max_move, broker))

      # Find partitions to move to this broker
      for partition in partitions:
        # We can use this partition if all of the following are true: the partition has a replica at this position, it's size is less than or equal to the
        # max move size, the broker at this replica position would not go out of range, and it doesn't already exist on this broker
        if ((len(partition.replicas) <= pos) or (partition.size > max_move) or ((sizes[partition.replicas[pos]] - partition.size) < (avg_size - margin)) or
           (broker in partition.replicas)):
          continue

        # Move the partition and adjust sizes
        source = partition.replicas[pos]
        cluster.brokers[source].remove_partition(partition)
        cluster.brokers[broker].add_partition(pos, partition)
        partition.replicas[pos] = broker
        min_move -= partition.size
        max_move -= partition.size
        sizes[broker] += partition.size
        sizes[source] -= partition.size

        # If we have moved enough partitions, stop for this broker
        if min_move <= 0:
          break

  return cluster


# Evenly spread topics that are a multiple of the number of brokers across the cluster
def module_balance_even(cluster, args):
  for topic in cluster.topics:
    if len(cluster.topics[topic].partitions) % len(cluster.brokers) != 0:
      log.warn("Skipping topic {0} as it has {1} partitions, which is not a multiple of the number of brokers ({2})".format(
        topic, len(cluster.topics[topic].partitions), len(cluster.brokers)))
      continue
    rf = len(cluster.topics[topic].partitions[0].replicas)
    target = len(cluster.topics[topic].partitions) // len(cluster.brokers)
    for partition in cluster.topics[topic].partitions:
      if len(partition.replicas) != rf:
        log.warn("Skipping topic {0} as not all partitions have the same replication factor".format(topic))
        continue

    # Initialize broker map for this topic.
    pmap = [dict.fromkeys(cluster.brokers.keys(), 0) for pos in range(rf)]

    for partition in cluster.topics[topic].partitions:
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
          cluster.brokers[source].remove_partition(partition)
          cluster.brokers[bid].add_partition(pos, partition)
          partition.replicas[pos] = bid
          pmap[pos][bid] += 1
          break

  return cluster

########################################################################################################################
# MAIN

aparser = argparse.ArgumentParser(description='Rejigger Kafka cluster partitions')
aparser.add_argument('-z', '--zookeeper', help='Zookeeper path to the cluster (i.e. zoo1.example.com:2181/kafka-cluster)', required=True)
aparser.add_argument('-l', '--leadership', help="Show cluster leadership balance", action='store_true')
aparser.add_argument('-g', '--generate', help="Generate partition reassignment file", action='store_true')
aparser.add_argument('-e', '--execute', help="Execute partition reassignment", action='store_true')
aparser.add_argument('-m', '--moves', help="Max number of moves per step", required=False, default=10, type=int)
aparser.add_argument('--ple-size', help="Max size in bytes for a preferred leader election string", required=False, default=900000, type=int)
aparser.add_argument('--ple-wait', help="Time in seconds to wait between preferred leader elections", required=False, default=300, type=int)
aparser.add_argument('--tools-path', help="Path to Kafka admin utilities, overriding PATH env var", required=False)

subparsers = aparser.add_subparsers(help='Select manipulation module to use')
s_clone = subparsers.add_parser('clone', help='Copy partitions from some brokers to a new broker (increasing RF)')
s_clone.add_argument('-b', '--brokers', help="List of source broker IDs", required=True, type=int, nargs='*')
s_clone.add_argument('-t', '--to_broker', help="Broker ID to copy partitions to", required=True, type=int)
s_clone.set_defaults(func=module_clone)

s_trim = subparsers.add_parser('trim', help='Remove partitions from some brokers (reducing RF)')
s_trim.add_argument('-b', '--brokers', help="List of broker IDs to remove", required=True, type=int, nargs='*')
s_trim.set_defaults(func=module_trim)

s_remove = subparsers.add_parser('remove', help='Move partitions from one broker to one or more other brokers (maintaining RF)')
s_remove.add_argument('-b', '--broker', help="Broker ID to remove", required=True, type=int)
s_remove.add_argument('-t', '--to_brokers', help="List of Broker IDs to move partitions to (defaults to whole cluster)", required=False, type=int, nargs='*')
s_remove.set_defaults(func=module_remove)

s_elect = subparsers.add_parser('elect', help='Reelect partition leaders using preferred replica election')
s_elect.set_defaults(func=module_elect)

s_set_replication_factor = subparsers.add_parser('set-replication-factor', help='Increase the replication factor of the specified topic')
s_set_replication_factor.add_argument('-t', '--topic', help='Topic to alter', required=True)
s_set_replication_factor.add_argument('-r', '--replication-factor', help='Target replication factor', required=True, type=int)
s_set_replication_factor.set_defaults(func=module_set_replication_factor)

s_reorder = subparsers.add_parser('reorder', help='Reelect partition leaders using replica reordering')
s_reorder.set_defaults(func=module_reorder)

s_balance = subparsers.add_parser('balance', help='Rebalance partitions across the cluster')
s_balance.add_argument('-t', '--types', help="Balance types to perform. Multiple may be specified and they will be run in order", required=True, choices=['count', 'size', 'even', 'leader'], nargs='*')
s_balance.add_argument('-s', '--size', help="Show partition sizes", action='store_true')
s_balance.add_argument('-d', '--datadir', help="Path to the data directory on the broker", required=False, default="/tmp/kafka-logs")
s_balance.set_defaults(func=module_balance)

args = aparser.parse_args()

# Find the kafka admin utilities
if args.tools_path is None:
  if 'PATH' in os.environ:
    for path in os.environ['PATH'].split(os.pathsep):
      path = path.strip('"')
      script_file = os.path.join(path, 'kafka-reassign-partitions.sh')
      if is_exec_file(script_file):
        args.tools_path = path
        break
  if args.tools_path is None:
    log.error("Cannot find the Kafka admin utilities using PATH. Try using the --tools-path option")
    sys.exit(1)
else:
  script_file = os.path.join(args.tools_path, 'kafka-reassign-partitions.sh')
  if not is_exec_file(script_file):
    log.error("--tools-path does not lead to the Kafka admin utilities ({0} is not an executable)".format(script_file))
    sys.exit(1)

# Make sure that JAVA_HOME is specified and is valid
if 'JAVA_HOME' in os.environ:
  java_bin = os.path.join(os.environ['JAVA_HOME'], 'bin', 'java')
  if not is_exec_file(java_bin):
    log.error("The JAVA_HOME environment variable doesn't seem to work ({0} is not an executable)".format(java_bin))
    sys.exit(1)
else:
  log.error("The JAVA_HOME environment variable must be set")
  sys.exit(1)

# Validate we got a good ZK connect string
zkparts = args.zookeeper.split('/')
if (len(zkparts) != 2):
  log.error('You must specify a full Zookeeper path (i.e. zoo1.example.com:2181/kafka-cluster)')
  sys.exit(1)

log.info("Connecting to zookeeper {0}".format(zkparts[0]))
try:
  zk = KazooClient(zkparts[0])
  zk.start()
except KazooException as e:
  log.error("Cannot connect to Zookeeper: {0}".format(e))
  sys.exit(1)

# Get broker list
cluster = Cluster()
for b in zk.get_children("/{0}/brokers/ids".format(zkparts[1])):
  bdata, bstat = zk.get("/{0}/brokers/ids/{1}".format(zkparts[1], b))
  bj = json.loads(bdata)
  cluster.brokers[int(b)] = Broker(int(b), bj['host'])

# Get current partition state
log.info("Getting partition list from Zookeeper")
for topic in zk.get_children("/{0}/brokers/topics".format(zkparts[1])):
  zdata, zstat = zk.get("/{0}/brokers/topics/{1}".format(zkparts[1], topic))
  zj = json.loads(zdata)

  t = Topic(topic, len(zj['partitions']))
  for partition in zj['partitions']:
    t.partitions[int(partition)].replicas = zj['partitions'][partition]

    # Also assign partitions to brokers
    for i, replica in enumerate(zj['partitions'][partition]):
      if replica not in cluster.brokers:
        # Hit a replica that's not in the ID list (which means it's dead)
        # We'll add it, but trying to get sizes will fail as we don't have a hostname
        cluster.brokers[replica] = Broker(replica, None)
      cluster.brokers[replica].add_partition(i, t.partitions[int(partition)])

  cluster.topics[topic] = t

log.info("Closing connection to zookeeper")
zk.stop()
zk.close()

if args.leadership:
  log.info("Cluster Leadership Balance (before):")
  for broker in sorted(cluster.brokers.keys()):
    log.info("Broker {0}: partitions={1}/{2} ({3:.2f}%), size={4}".format(broker, cluster.brokers[broker].num_leaders(),
                                                                          cluster.brokers[broker].num_partitions(),
                                                                          cluster.brokers[broker].percent_leaders(),
                                                                          cluster.brokers[broker].total_size()))

# Call the appropriate module to create ideal state
newcluster = cluster.clone()
idealstate = args.func(newcluster, args)

if args.leadership:
  log.info("Cluster Leadership Balance (after):")
  for broker in sorted(newcluster.brokers.keys()):
    log.info("Broker {0}: partitions={1}/{2} ({3:.2f}%), size={4}".format(broker, newcluster.brokers[broker].num_leaders(),
                                                                          newcluster.brokers[broker].num_partitions(),
                                                                          newcluster.brokers[broker].percent_leaders(),
                                                                          newcluster.brokers[broker].total_size()))

# Generate a list of moves required to get from the current state to the ideal state
log.info("Generating moves to reach the ideal state")
moves = []
for topic in newcluster.topics:
  for i, partition in enumerate(newcluster.topics[topic].partitions):
    if partition.replicas != cluster.topics[topic].partitions[i].replicas:
      moves.append({"topic": topic, "partition": i, "replicas": partition.replicas})

log.info("Partition moves required: {0}".format(len(moves)))
if (len(moves) == 0) and (args.func != module_elect):
  log.info("No partition reassignment needed")
  sys.exit(0)

if args.generate:
  for i, move in enumerate(batch(moves, args.moves)):
    reassignment = {'partitions': move, 'version': 1}
    log.info("Partition Reassignment File {0}: {1}".format(i+1, json.dumps(reassignment)))
  sys.exit(0)

if (args.execute):
  if len(moves) > 0:
    total_batches = (len(moves) // args.moves) + 1
    for i, move in enumerate(batch(moves, args.moves)):
      reassignment = {'partitions': move, 'version': 1}
      log.info("Executing partition reassignment {0}/{1}: {2}".format(i+1, total_batches, json.dumps(reassignment)))
      with NamedTemporaryFile() as assignfile:
        assignfile.write(json.dumps(reassignment))
        assignfile.flush()
        commands.getoutput('{0}/kafka-reassign-partitions.sh --zookeeper {1} --execute --reassignment-json-file {2}'.format(args.tools_path, args.zookeeper, assignfile.name))

        failed_re = re.compile('.*Reassignment of partition.*?\s+failed', re.DOTALL)
        progress_re = re.compile('.*Reassignment of partition.*?\s+still\s+in\s+progress', re.DOTALL)

        # Loop waiting for reassignment to finish
        finished = False
        while (not finished):
          verify_out = commands.getoutput('{0}/kafka-reassign-partitions.sh --zookeeper {1} --verify --reassignment-json-file {2}'.format(args.tools_path, args.zookeeper, assignfile.name))
          if (failed_re.match(verify_out)):
            log.error('Failed reassignment file: {0}'.format(json.dumps(move)))
            log.error('Reassignment tool status: {0}'.format(verify_out))
            sys.exit(1)
          elif (progress_re.match(verify_out)):
            completed_partitions=verify_out.count('completed successfully')
            total_partitions=verify_out.count('Reassignment of partition')
            remaining_partitions=total_partitions - completed_partitions
            log.info('Partition reassignment {0}/{1} in progress [ {2}/{3} partitions remain ]. Sleeping 10 seconds'.format(
              i+1, total_batches, remaining_partitions, total_partitions))
            time.sleep(10)
          else:
            finished = True

  # Split PLE so a single request isn't longer than max ZK data size
  log.info("Generating preferred replica election")
  ples = []
  for topic in newcluster.topics:
    for i, partition in enumerate(newcluster.topics[topic].partitions):
      ples.append({"topic": topic, "partition": i})

  ple_str = json.dumps({"partitions": ples})
  batch_size = int(math.ceil(len(ples) / int(math.ceil(len(ple_str) / args.ple_size))))
  for i, ple in enumerate(batch(ples, batch_size)):
    # Sleep between PLEs
    if i > 0:
      log.info("Waiting {0} seconds for replica election to complete".format(args.ple_wait))
      time.sleep(args.ple_wait)

    reassignment = {'partitions': ple}
    log.info("Executing preferred replica election {0}".format(i))
    with NamedTemporaryFile() as assignfile:
      assignfile.write(json.dumps(reassignment))
      assignfile.flush()
      commands.getoutput('{0}/kafka-preferred-replica-election.sh --zookeeper {1} --path-to-json-file {2}'.format(args.tools_path, args.zookeeper, assignfile.name))

sys.exit(0)
