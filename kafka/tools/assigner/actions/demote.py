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
from kafka.tools.exceptions import NotEnoughReplicasException


class ActionDemote(ActionModule):
    name = "demote"
    helpstr = "Force a broker to relinquish leadership wherever it is safe to do so. This is done by reordering " \
        "replica lists and then performing a PLE."

    def __init__(self, args, cluster):
        super(ActionDemote, self).__init__(args, cluster)

    @classmethod
    def _add_args(cls, parser):
        parser.add_argument(
            '-b', '--brokers', type=int, help='List of broker ids to demote', required=True, nargs='*')
        parser.add_argument('-t', '--topics', required=False, nargs='*', help=\
            'Optional list of topics to use. If not specified, leadership of all topics lead by the brokers to ' \
            'demote will be changed.')

    def process_cluster(self):
        # Randomize a broker list to use for new replicas once. We'll round robin it from here
        brokers_to_demote = set(self.args.brokers)
        topics_to_consider = self.args.topics and set(self.args.topics) or None

        for partition in self.cluster.partitions(self.args.exclude_topics):
            if topics_to_consider and partition.topic.name not in topics_to_consider:
                continue

            if all(b.id in brokers_to_demote for b in partition.replicas):
                msg = 'Topic {0} partition {1} only has replicas in the list of brokers to be demoted: {2}'.format(
                    partition.topic.name,
                    str(partition.num),
                    ', '.join(str(b.id) for b in partition.replicas))
                raise NotEnoughReplicasException(msg)

            while partition.replicas[0].id in brokers_to_demote:
                broker = partition.replicas[0]
                partition.remove_replica(broker)
                partition.add_replica(broker)
