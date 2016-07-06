Getting Started
===============

This document describes how to install and configure kafka-tools.

Prerequisites
-------------

These tools are generally written in Python, and besides a basic
installation, you will need the following additional modules:

- Kazoo

In addition, you will need to run it on a host that has the following:

- A copy of the Kafka admin tools (including kafka-reassign-partitions.sh).
- Access to the Zookeeper ensemble for the cluster.
- SSH access to the Kafka brokers (with credentials preferably loaded into
  ssh-agent).
