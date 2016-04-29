# Kafka Tools - kafka-assigner

kafka-assigner is a script we use for performing changes to partition assignments in a Kafka cluster. It works as a wrapper around the admin CLI utilities that are provided with Kafka, making it much easier to perform common tasks, such as:
* Removing a broker from the cluster
* Rebalancing partitions in a cluster using one or more algorithms
* Fixing the replication factor for a topic
* Performing preferred replica elections

## Prerequisites
In order to run kafka-assigner, you will need to have the following Python modules installed:
* Paramiko
* Kazoo

In addition, you will need to run it on a host that has the following:
* A copy of the Kafka admin tools (including kafka-reassign-partitions.sh)
* Access to the Zookeeper ensemble for the cluster
* SSH access to the Kafka brokers (with credentials preferably loaded into ssh-agent)

## Usage
For details about how to use kafka-assigner, please refer to the wiki page (to be added)

## Contributing
We're always open to fixes and new features! Please open a PR for any changes that you have and someone will review and merge it. If you're not up for writing the code, open an issue for any problems or requests.

## License
Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied.

