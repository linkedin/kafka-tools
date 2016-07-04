import unittest

from kafka.tools.assigner.exceptions import ProgrammingException
from kafka.tools.assigner.batcher import split_partitions_into_batches
from kafka.tools.assigner.models.broker import Broker
from kafka.tools.assigner.models.topic import Topic
from kafka.tools.assigner.models.reassignment import Reassignment
from kafka.tools.assigner.models.replica_election import ReplicaElection


class BatcherTests(unittest.TestCase):
    def setUp(self):
        self.topic = Topic('testTopic', 10)
        self.broker = Broker(1, 'brokerhost1.example.com')
        for i in range(10):
            self.topic.partitions[i].replicas = [self.broker]

    def test_split_batches_empty(self):
        partitions = []
        batches = split_partitions_into_batches(partitions, batch_size=1, use_class=Reassignment)
        assert len(batches) == 0

    def test_split_batches_no_class(self):
        partitions = []
        self.assertRaises(ProgrammingException, split_partitions_into_batches, partitions, batch_size=1)

    def test_split_batches_proper_class(self):
        batches = split_partitions_into_batches(self.topic.partitions, batch_size=100, use_class=Reassignment)
        assert isinstance(batches[0], Reassignment)

        batches = split_partitions_into_batches(self.topic.partitions, batch_size=100, use_class=ReplicaElection)
        assert isinstance(batches[0], ReplicaElection)

    def test_split_batches_singles(self):
        batches = split_partitions_into_batches(self.topic.partitions, batch_size=1, use_class=Reassignment)
        partition_count = sum([len(batch.partitions) for batch in batches])
        assert len(batches) == 10
        assert partition_count == 10

    def test_split_batches_doubles(self):
        batches = split_partitions_into_batches(self.topic.partitions, batch_size=2, use_class=Reassignment)
        partition_count = sum([len(batch.partitions) for batch in batches])
        assert len(batches) == 5
        assert partition_count == 10

    def test_split_batches_notenough(self):
        batches = split_partitions_into_batches(self.topic.partitions, batch_size=20, use_class=Reassignment)
        partition_count = sum([len(batch.partitions) for batch in batches])
        assert len(batches) == 1
        assert partition_count == 10
