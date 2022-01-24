import argparse
import json
from kafka.tools.models.broker import Broker
from kafka.tools.models.cluster import Cluster, add_topic_with_replicas
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


def set_up_cluster_9broker():
    cluster_dict = {
        "brokers": [
            {
                "1": {
                    "id": 1,
                    "rack": "1",
                    "jmx_port": 9999,
                    "host": "localhost1",
                    "timestamp": "1631715232017",
                    "port": 9092,
                    "version": 4
                }
            },
            {
                "2": {
                    "id": 2,
                    "rack": "1",
                    "jmx_port": 9999,
                    "host": "localhost2",
                    "timestamp": "1631715232017",
                    "port": 9092,
                    "version": 4
                }
            },
            {
                "3": {
                    "id": 3,
                    "rack": "1",
                    "jmx_port": 9999,
                    "host": "localhost3",
                    "timestamp": "1631715232017",
                    "port": 9092,
                    "version": 4
                }

            },
            {
                "4": {
                    "id": 4,
                    "rack": "2",
                    "jmx_port": 9999,
                    "host": "localhost4",
                    "timestamp": "1631715232017",
                    "port": 9092,
                    "version": 4
                }
            },
            {
                "5": {
                    "id": 5,
                    "rack": "2",
                    "jmx_port": 9999,
                    "host": "localhost5",
                    "timestamp": "1631715232017",
                    "port": 9092,
                    "version": 4
                }
            },
            {
                "6": {
                    "id": 6,
                    "rack": "2",
                    "jmx_port": 9999,
                    "host": "localhost6",
                    "timestamp": "1631715232017",
                    "port": 9092,
                    "version": 4
                }
            },
            {
                "7": {
                    "id": 7,
                    "rack": "3",
                    "jmx_port": 9999,
                    "host": "localhost7",
                    "timestamp": "1631715232017",
                    "port": 9092,
                    "version": 4
                }

            },
            {
                "8": {
                    "id": 8,
                    "rack": "3",
                    "jmx_port": 9999,
                    "host": "localhost8",
                    "timestamp": "1631715232017",
                    "port": 9092,
                    "version": 4
                }
            },
            {
                "9": {
                    "id": 9,
                    "rack": "3",
                    "jmx_port": 9999,
                    "host": "localhost9",
                    "timestamp": "1631715232017",
                    "port": 9092,
                    "version": 4
                }
            }
        ],
        "topics": {
            "topic1": {
                "partitions": {
                    "0": [1, 2, 4],
                    "1": [1, 5, 3],
                    "2": [2, 6, 8],
                    "3": [1, 9, 7]
                }
            },
            "topic2": {
                "partitions": {
                    "0": [7, 8, 1],
                    "1": [7, 3, 6],
                    "2": [7, 9, 5],
                    "3": [5, 2, 1],
                    "4": [5, 1, 8]
                }
            }
        }
    }


    cluster = Cluster(retention=3)
    for broker in cluster_dict["brokers"]:
        cluster.add_broker(
            Broker.create_from_json(
                int(list(broker.keys())[0]),
                json.dumps(list(broker.values())[0])
            )
        )

    for topic, topic_data in cluster_dict["topics"].items():
        add_topic_with_replicas(cluster, topic, topic_data)
    return cluster
