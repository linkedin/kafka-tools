Getting Started
===============

This document describes how to install and configure kafka-tools.

Prerequisites
-------------

Software that must be installed:

- Python (https://www.python.org/)
- Kazoo (https://kazoo.readthedocs.io/en/latest/)
- Kafka (https://kafka.apache.org/)

In addition, you will need to run it on a host that has:

- Access to the Zookeeper ensemble for the Kafka cluster.
- SSH access to the Kafka brokers (with credentials preferably loaded into
  ssh-agent).
  
Quick Install
-------------

1) Download the Kafka binaries from https://kafka.apache.org/downloads.html.
2) Use pip to install the kafka-tools packge from pypi.

Source Install
--------------

1) Download the Kafka binaries from https://kafka.apache.org/downloads.html.
2) Clone the kafka-tools repository from https://github.com/linkedin/kafka-tools
3) Run the tests using tox.
4) Install kafka-tools using setup.py.
