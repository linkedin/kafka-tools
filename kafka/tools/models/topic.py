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

import time

from kafka.tools.exceptions import OffsetError
from kafka.tools.models import BaseModel
from kafka.tools.models.partition import Partition
from kafka.tools.utilities import raise_if_error


class Topic(BaseModel):
    equality_attrs = ['name']

    def __init__(self, name, partitions):
        self.name = name
        self.partitions = []
        self.cluster = None
        self.internal = False
        self.retention = 1
        self._last_updated = time.time()

        for i in range(partitions):
            self.add_partition(Partition(self, i))

    # Shallow copy - do not copy partitions (zero partitions)
    def copy(self):
        newtopic = Topic(self.name, 0)
        newtopic.cluster = self.cluster
        return newtopic

    def add_partition(self, partition):
        self.partitions.append(partition)

    def to_dict(self):
        data = {'partitions': {}, 'retention': self.retention}
        for partition in self.partitions:
            data['partitions'][partition.num] = partition.to_dict()
        return data

    def updated_since(self, check_time):
        return check_time <= self._last_updated

    def assure_has_partitions(self, target_count):
        """
        Assure that the topic has only the specified number of partitions, adding or deleting as needed

        Args:
            target_count (int): the number of partitions to have
        """
        while len(self.partitions) < target_count:
            partition = Partition(self.name, len(self.partitions))
            self.add_partition(partition)

        # While Kafka doesn't support partition deletion (only topics), it's possible for a topic
        # to be deleted and recreated with a smaller partition count before we see that it's been
        # deleted. This would look like partition deletion, so we should support it.
        while len(self.partitions) > target_count:
            partition = self.partitions.pop()
            partition.delete_replicas(0)


class TopicOffsets:
    def __init__(self, topic):
        self.topic = topic
        self.partitions = [-1 for i in range(len(topic.partitions))]

    def set_offsets_from_list(self, partitions):
        """
        Given a partition_responses object from a ListOffsets response, update the offsets
        with the values in the response

        Args:
            partitions (Array): the partition_response object from a ListOffsets response

        Raises:
            OffsetError: If there was a failure retrieving any of the offsets
        """
        for partition in partitions:
            raise_if_error(OffsetError, partition['error'])
            self.partitions[partition['partition']] = partition['offsets'][0]

    def set_offsets_from_fetch(self, partitions):
        """
        Given a partition_responses object from a OffsetFetch response, update the offsets
        with the values in the response

        Args:
            partitions (Array): the partition_response object from a OffsetFetch response

        Raises:
            OffsetError: If there was a failure retrieving any of the offsets
        """
        for partition in partitions:
            raise_if_error(OffsetError, partition['error'])
            self.partitions[partition['partition']] = partition['offset']
