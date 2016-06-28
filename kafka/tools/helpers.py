import os
import logging
import re

import paramiko

log = logging.getLogger('kafka-assigner')


class Cluster(object):
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


class Broker(object):
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


class Partition(object):
    def __init__(self, topic, num):
        self.topic = topic
        self.num = num
        self.replicas = []
        self.size = 0

    def __eq__(self, other):
        return (self.topic == other.topic) and (self.num == other.num)


class Topic(object):
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
        yield iterable[ndx:min(ndx + n, l)]


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
