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


def set_up_cluster_big():
    cluster = Cluster()
    cluster.retention = 100000

    cluster.add_broker(Broker("broker100.example.com", id=10100))
    cluster.add_broker(Broker("broker101.example.com", id=10101))
    cluster.add_broker(Broker("broker102.example.com", id=10102))
    cluster.add_broker(Broker("broker113.example.com", id=10113))
    cluster.add_broker(Broker("broker114.example.com", id=10114))
    cluster.add_broker(Broker("broker115.example.com", id=10115))
    cluster.add_broker(Broker("broker128.example.com", id=10128))
    cluster.add_broker(Broker("broker129.example.com", id=10129))
    cluster.add_broker(Broker("broker130.example.com", id=10130))

    cluster.brokers[10100].rack = "115"
    cluster.brokers[10101].rack = "115"
    cluster.brokers[10102].rack = "115"
    cluster.brokers[10113].rack = "113"
    cluster.brokers[10114].rack = "113"
    cluster.brokers[10115].rack = "113"
    cluster.brokers[10128].rack = "114"
    cluster.brokers[10129].rack = "114"
    cluster.brokers[10130].rack = "114"

    cluster.add_topic(Topic("myTopic1", 9))
    partition = cluster.topics["myTopic1"].partitions[0]
    partition.add_replica(cluster.brokers[10130], 0)
    partition.add_replica(cluster.brokers[10115], 1)
    partition.add_replica(cluster.brokers[10100], 2)
    partition = cluster.topics["myTopic1"].partitions[1]
    partition.add_replica(cluster.brokers[10128], 0)
    partition.add_replica(cluster.brokers[10101], 1)
    partition.add_replica(cluster.brokers[10114], 2)
    partition = cluster.topics["myTopic1"].partitions[2]
    partition.add_replica(cluster.brokers[10100], 0)
    partition.add_replica(cluster.brokers[10130], 1)
    partition.add_replica(cluster.brokers[10115], 2)
    partition = cluster.topics["myTopic1"].partitions[3]
    partition.add_replica(cluster.brokers[10130], 0)
    partition.add_replica(cluster.brokers[10102], 1)
    partition.add_replica(cluster.brokers[10113], 2)
    partition = cluster.topics["myTopic1"].partitions[4]
    partition.add_replica(cluster.brokers[10101], 0)
    partition.add_replica(cluster.brokers[10113], 1)
    partition.add_replica(cluster.brokers[10128], 2)
    partition = cluster.topics["myTopic1"].partitions[5]
    partition.add_replica(cluster.brokers[10113], 0)
    partition.add_replica(cluster.brokers[10128], 1)
    partition.add_replica(cluster.brokers[10102], 2)
    partition = cluster.topics["myTopic1"].partitions[6]
    partition.add_replica(cluster.brokers[10129], 0)
    partition.add_replica(cluster.brokers[10115], 1)
    partition.add_replica(cluster.brokers[10101], 2)
    partition = cluster.topics["myTopic1"].partitions[7]
    partition.add_replica(cluster.brokers[10102], 0)
    partition.add_replica(cluster.brokers[10114], 1)
    partition.add_replica(cluster.brokers[10129], 2)
    partition = cluster.topics["myTopic1"].partitions[8]
    partition.add_replica(cluster.brokers[10100], 0)
    partition.add_replica(cluster.brokers[10115], 1)
    partition.add_replica(cluster.brokers[10130], 2)

    return cluster


def set_up_subparser():
    aparser = argparse.ArgumentParser(prog='kafka-assigner', description='Rejigger Kafka cluster partitions')
    subparsers = aparser.add_subparsers(help='Select manipulation module to use')
    return (aparser, subparsers)
