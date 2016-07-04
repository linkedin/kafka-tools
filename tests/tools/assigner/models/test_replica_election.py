import json
import unittest

from mock import patch, ANY

from kafka.tools.assigner.models.broker import Broker
from kafka.tools.assigner.models.topic import Topic
from kafka.tools.assigner.models.replica_election import ReplicaElection


class ReplicaElectionTests(unittest.TestCase):
    def setUp(self):
        self.topic = Topic('testTopic', 10)
        self.broker = Broker(1, 'brokerhost1.example.com')
        for i in range(10):
            self.topic.partitions[i].replicas = [self.broker]
        self.replica_election = ReplicaElection(self.topic.partitions, pause_time=0)

    def test_replica_election_create(self):
        assert self.replica_election is not None

    def test_replica_election_repr(self):
        t_repr = json.loads(repr(self.replica_election))
        expect_repr = {'partitions': []}
        for i in range(10):
            expect_repr['partitions'].append({'topic': 'testTopic', 'partition': i})
        assert t_repr == expect_repr

    def test_replica_election_dict(self):
        t_repr = self.replica_election.dict_for_replica_election()
        expect_repr = {'partitions': []}
        for i in range(10):
            expect_repr['partitions'].append({'topic': 'testTopic', 'partition': i})
        assert t_repr == expect_repr

    @patch('kafka.tools.assigner.models.replica_election.subprocess.call')
    def test_replica_election_execute(self, mock_call):
        self.replica_election.execute(1, 1, 'zk_connect_string', '/path/to/tools', plugins=[], dry_run=False)
        mock_call.assert_called_once_with(['/path/to/tools/kafka-preferred-replica-election.sh',
                                           '--zookeeper', 'zk_connect_string',
                                           '--path-to-json-file', ANY])
