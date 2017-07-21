import argparse

from kafka.tools.models.broker import Broker
from kafka.tools.models.cluster import Cluster
from kafka.tools.models.topic import Topic


def set_up_cluster():
    cluster = Cluster()
    cluster.retention = 100000
    cluster.add_broker(Broker("brokerhost1.example.com", id=1))
    cluster.add_broker(Broker("brokerhost2.example.com", id=2))
    cluster.brokers[1].rack = "a"
    cluster.brokers[2].rack = "b"
    cluster.add_topic(Topic("testTopic1", 2))
    cluster.add_topic(Topic("testTopic2", 2))
    partition = cluster.topics['testTopic1'].partitions[0]
    partition.add_replica(cluster.brokers[1], 0)
    partition.add_replica(cluster.brokers[2], 1)
    partition = cluster.topics['testTopic1'].partitions[1]
    partition.add_replica(cluster.brokers[2], 0)
    partition.add_replica(cluster.brokers[1], 1)
    partition = cluster.topics['testTopic2'].partitions[0]
    partition.add_replica(cluster.brokers[2], 0)
    partition.add_replica(cluster.brokers[1], 1)
    partition = cluster.topics['testTopic2'].partitions[1]
    partition.add_replica(cluster.brokers[1], 0)
    partition.add_replica(cluster.brokers[2], 1)
    return cluster


def set_up_cluster_4broker():
    cluster = Cluster()
    cluster.add_broker(Broker("brokerhost1.example.com", id=1))
    cluster.add_broker(Broker("brokerhost2.example.com", id=2))
    cluster.add_broker(Broker("brokerhost3.example.com", id=3))
    cluster.add_broker(Broker("brokerhost4.example.com", id=4))
    cluster.brokers[1].rack = "a"
    cluster.brokers[2].rack = "a"
    cluster.brokers[3].rack = "b"
    cluster.brokers[4].rack = "b"
    cluster.add_topic(Topic("testTopic1", 4))
    cluster.add_topic(Topic("testTopic2", 4))
    cluster.add_topic(Topic("testTopic3", 4))
    partition = cluster.topics['testTopic1'].partitions[0]
    partition.add_replica(cluster.brokers[1], 0)
    partition.add_replica(cluster.brokers[2], 1)
    partition = cluster.topics['testTopic1'].partitions[1]
    partition.add_replica(cluster.brokers[2], 0)
    partition.add_replica(cluster.brokers[3], 1)
    partition = cluster.topics['testTopic1'].partitions[2]
    partition.add_replica(cluster.brokers[2], 0)
    partition.add_replica(cluster.brokers[3], 1)
    partition = cluster.topics['testTopic1'].partitions[3]
    partition.add_replica(cluster.brokers[4], 0)
    partition.add_replica(cluster.brokers[1], 1)
    partition = cluster.topics['testTopic2'].partitions[0]
    partition.add_replica(cluster.brokers[4], 0)
    partition.add_replica(cluster.brokers[3], 1)
    partition = cluster.topics['testTopic2'].partitions[1]
    partition.add_replica(cluster.brokers[2], 0)
    partition.add_replica(cluster.brokers[4], 1)
    partition = cluster.topics['testTopic2'].partitions[2]
    partition.add_replica(cluster.brokers[2], 0)
    partition.add_replica(cluster.brokers[1], 1)
    partition = cluster.topics['testTopic2'].partitions[3]
    partition.add_replica(cluster.brokers[3], 0)
    partition.add_replica(cluster.brokers[1], 1)
    partition = cluster.topics['testTopic3'].partitions[0]
    partition.add_replica(cluster.brokers[3], 0)
    partition.add_replica(cluster.brokers[2], 1)
    partition = cluster.topics['testTopic3'].partitions[1]
    partition.add_replica(cluster.brokers[4], 0)
    partition.add_replica(cluster.brokers[2], 1)
    partition = cluster.topics['testTopic3'].partitions[2]
    partition.add_replica(cluster.brokers[1], 0)
    partition.add_replica(cluster.brokers[2], 1)
    partition = cluster.topics['testTopic3'].partitions[3]
    partition.add_replica(cluster.brokers[3], 0)
    partition.add_replica(cluster.brokers[4], 1)
    return cluster


def set_up_subparser():
    aparser = argparse.ArgumentParser(prog='kafka-assigner', description='Rejigger Kafka cluster partitions')
    subparsers = aparser.add_subparsers(help='Select manipulation module to use')
    return (aparser, subparsers)
