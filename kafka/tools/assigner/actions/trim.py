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

from kafka.tools.assigner.actions import ActionModule
from kafka.tools.assigner.exceptions import NotEnoughReplicasException


class ActionTrim(ActionModule):
    name = "trim"
    helpstr = "Remove partitions from some brokers (reducing RF)"

    def __init__(self, args, cluster):
        super(ActionTrim, self).__init__(args, cluster)

        self.check_brokers(type_str="Brokers to remove")
        self.brokers = args.brokers

    @classmethod
    def _add_args(cls, parser):
        parser.add_argument('-b', '--brokers', help="List of broker IDs to remove", required=True, type=int, nargs='*')

    def process_cluster(self):
        # For each broker specified, remove it from the replica list for all its partitions
        for broker_id in self.brokers:
            broker = self.cluster.brokers[broker_id]
            for position in broker.partitions:
                partition_list = broker.partitions[position][:]
                for partition in partition_list:
                    partition.remove_replica(broker)
                    if len(partition.replicas) < 1:
                        raise NotEnoughReplicasException("Cannot trim {0}:{1} as it would result in an empty replica list".format(
                            partition.topic.name, partition.num))
