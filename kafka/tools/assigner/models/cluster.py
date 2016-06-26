import sys
from kazoo.client import KazooClient

from kafka.tools.assigner import log
from kafka.tools.assigner.exceptions import ZookeeperException
from kafka.tools.assigner.models.broker import Broker
from kafka.tools.assigner.models.topic import Topic


class Cluster:
  def __init__(self):
    self.brokers = {}
    self.topics = {}

  @classmethod
  def create_from_zookeeper(cls, zkconnect):
    log.info("Connecting to zookeeper {0}".format(zkconnect))
    try:
      zk = KazooClient(zkconnect)
      zk.start()
    except Exception as e:
      raise ZookeeperException("Cannot connect to Zookeeper: {0}".format(e))

    # Get broker list
    cluster = cls()
    for b in zk.get_children("/brokers/ids"):
      broker_data, bstat = zk.get("/brokers/ids/{1}".format(b))
      cluster.add_broker(Broker.create_from_json(int(broker_id), broker_data))

    # Get current partition state
    log.info("Getting partition list from Zookeeper")
    for topic in zk.get_children("/brokers/topics"):
      zdata, zstat = zk.get("/brokers/topics/{1}".format(topic))
      zj = json.loads(zdata)

      newtopic = Topic(topic, len(zj['partitions']))
      for partition in zj['partitions']:
        for i, replica in enumerate(zj['partitions'][partition]):
          if replica not in cluster.brokers:
            # Hit a replica that's not in the ID list (which means it's dead)
            # We'll add it, but trying to get sizes will fail as we don't have a hostname
            cluster.add_broker(Broker(replica))
          newtopic.partitions[int(partition)].add_replica(cluster.brokers[replica], i)
      cluster.add_topic(newtopic)

    log.info("Closing connection to zookeeper")
    zk.stop()
    zk.close()

    return cluster

  def clone(self):
    newcluster = Cluster()

    # We're not going to clone in the subclasses because we need to map partitions between topics and brokers
    # So we do shallow copies and populate the partition information ourselves
    for broker in self.brokers:
      newcluster.add_broker(self.brokers[broker].copy())

    for topic_name in self.topics:
      topic = self.topics[topic_name]
      newtopic = topic.copy()

      for i, partition in enumerate(topic.partitions):
        newpartition = partition.copy()
        newtopic.add_partition(newpartition)
        for pos, broker in enumerate(partition.replicas):
          newpartition.add_replica(newcluster.brokers[broker.id])
          newcluster.brokers[broker.id].add_partition(newpartition.replication_factor() - 1, partition)

      newcluster.add_topic(newtopic)

    return newcluster

  def add_broker(self, broker):
    broker.cluster = self
    self.brokers[broker.id] = broker

  def add_topic(self, topic):
    topic.cluster = self
    self.topics[topic.name] = topic

  # Iterate over all the partitions in this cluster
  # Order is undefined
  def partitions(self):
    for topic in self.topics:
      for partition in self.topics[topic].partitions:
        yield partition

  def max_replication_factor(self):
    max_pos = 0
    for broker in self.brokers:
      if len(self.brokers[broker].partitions) > max_pos:
        max_pos = len(self.brokers[broker].partitions)
    return max_pos

  def log_broker_summary(self):
    for broker_id in sorted(self.brokers.keys()):
      broker = self.brokers[broker_id]
      log.info("Broker {0}: partitions={1}/{2} ({3:.2f}%), size={4}".format(broker_id,
                                                                            broker.num_leaders(),
                                                                            broker.num_partitions(),
                                                                            broker.percent_leaders(),
                                                                            broker.total_size()))
