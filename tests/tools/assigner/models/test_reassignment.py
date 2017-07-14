import json
import unittest

from mock import call, patch, ANY
from subprocess import PIPE
from testfixtures import compare
from testfixtures.popen import MockPopen

from kafka.tools.exceptions import ReassignmentFailedException
from kafka.tools.models.broker import Broker
from kafka.tools.models.topic import Topic
from kafka.tools.assigner.models.reassignment import Reassignment
from kafka.tools.assigner.plugins import PluginModule


class ReassignmentTests(unittest.TestCase):
    def setUp(self):
        self.topic = Topic('testTopic', 10)
        self.broker = Broker('brokerhost1.example.com', id=1)
        for i in range(10):
            self.topic.partitions[i].replicas = [self.broker]
        self.reassignment = Reassignment(self.topic.partitions, pause_time=0)
        self.null_plugin = PluginModule()

    def test_reassignment_create(self):
        assert self.reassignment is not None

    def test_reassignment_dict(self):
        t_repr = self.reassignment.dict_for_reassignment()
        expect_repr = {'version': 1, 'partitions': []}
        for i in range(10):
            expect_repr['partitions'].append({'topic': 'testTopic', 'partition': i, 'replicas': [1]})
        assert t_repr == expect_repr

    def test_reassignment_repr(self):
        t_repr = json.loads(repr(self.reassignment))
        expect_repr = {'version': 1, 'partitions': []}
        for i in range(10):
            expect_repr['partitions'].append({'topic': 'testTopic', 'partition': i, 'replicas': [1]})
        assert t_repr == expect_repr

    @patch.object(Reassignment, '_execute')
    def test_reassignment_execute_real(self, mock_exec):
        self.reassignment.execute(1, 1, 'zkconnect', '/path/to/tools', plugins=[self.null_plugin], dry_run=False)
        mock_exec.assert_called_once_with(1, 1, 'zkconnect', '/path/to/tools')

    @patch.object(Reassignment, '_execute')
    def test_reassignment_execute_dryrun(self, mock_exec):
        self.reassignment.execute(1, 1, 'zkconnect', '/path/to/tools', plugins=[self.null_plugin], dry_run=True)
        mock_exec.assert_not_called()

    @patch('kafka.tools.assigner.models.reassignment.subprocess.Popen', new_callable=MockPopen)
    @patch.object(Reassignment, 'check_completion')
    def test_reassignment_internal_execute(self, mock_check, mock_popen):
        mock_popen.set_default()
        mock_check.side_effect = [10, 5, 0]

        self.reassignment._execute(1, 1, 'zkconnect', '/path/to/tools')

        compare([call.Popen(['/path/to/tools/kafka-reassign-partitions.sh', '--execute', '--zookeeper', 'zkconnect', '--reassignment-json-file', ANY],
                            stderr=ANY, stdout=ANY),
                 call.Popen_instance.wait()], mock_popen.mock.method_calls)
        assert len(mock_check.mock_calls) == 3

    @patch('kafka.tools.assigner.models.reassignment.subprocess.Popen', new_callable=MockPopen)
    def test_check_completion_calls_tool(self, mock_popen):
        cmd_stdout = ("Status of partition reassignment:\n"
                      "Reassignment of partition [testTopic,0] completed successfully\n"
                      "Reassignment of partition [testTopic,1] completed successfully\n"
                      "Reassignment of partition [testTopic,2] completed successfully\n"
                      "Reassignment of partition [testTopic,3] completed successfully\n")
        mock_popen.set_default(stdout=cmd_stdout.encode('utf-8'))
        self.reassignment.check_completion('zkconnect', '/path/to/tools', 'assignfilename')
        compare([call.Popen(['/path/to/tools/kafka-reassign-partitions.sh', '--verify', '--zookeeper', 'zkconnect', '--reassignment-json-file',
                             'assignfilename'], stderr=ANY, stdout=PIPE)],
                mock_popen.mock.method_calls)

    def test_verify_regex_bad(self):
        assert self.reassignment.status_re.match("Status of partition reassignment:\n") is None

    def test_verify_regex_failed(self):
        match_obj = self.reassignment.status_re.match("Reassignment of partition [testTopic,1] failed\n")
        assert match_obj is not None
        assert match_obj.group(1) == "failed"

    def test_verify_regex_progress(self):
        match_obj = self.reassignment.status_re.match("Reassignment of partition [testTopic,1] still in progress\n")
        assert match_obj is not None
        assert match_obj.group(1) == "still in progress"

    def test_verify_regex_success(self):
        match_obj = self.reassignment.status_re.match("Reassignment of partition [testTopic,1] completed successfully\n")
        assert match_obj is not None
        assert match_obj.group(1) == "completed successfully"

    def test_process_verify_match_failed(self):
        count = self.reassignment.process_verify_match("Reassignment of partition [testTopic,1] failed\n")
        assert count == -1

    def test_process_verify_match_progress(self):
        count = self.reassignment.process_verify_match("Reassignment of partition [testTopic,1] still in progress\n")
        assert count == 1

    def test_process_verify_match_other(self):
        count = self.reassignment.process_verify_match("Reassignment of partition [testTopic,1] completed successfully\n")
        assert count == 0

    @patch('kafka.tools.assigner.models.reassignment.subprocess.Popen', new_callable=MockPopen)
    def test_check_completion_failed(self, mock_popen):
        cmd_stdout = ("Status of partition reassignment:\n"
                      "ERROR: Assigned replicas (1,2) don't match the list of replicas for reassignment (1,2,3) for partition [testTopic,1]\n"
                      "Reassignment of partition [testTopic,0] completed successfully\n"
                      "Reassignment of partition [testTopic,1] failed\n"
                      "Reassignment of partition [testTopic,2] still in progress\n"
                      "Reassignment of partition [testTopic,3] completed successfully\n")
        mock_popen.set_default(stdout=cmd_stdout.encode('utf-8'))
        self.assertRaises(ReassignmentFailedException, self.reassignment.check_completion, 'zkconnect', '/path/to/tools', 'assignfilename')

    @patch('kafka.tools.assigner.models.reassignment.subprocess.Popen', new_callable=MockPopen)
    def test_check_completion_success(self, mock_popen):
        cmd_stdout = ("Status of partition reassignment:\n"
                      "Reassignment of partition [testTopic,0] completed successfully\n"
                      "Reassignment of partition [testTopic,1] completed successfully\n"
                      "Reassignment of partition [testTopic,2] completed successfully\n"
                      "Reassignment of partition [testTopic,3] completed successfully\n")
        mock_popen.set_default(stdout=cmd_stdout.encode('utf-8'))
        assert self.reassignment.check_completion('zkconnect', '/path/to/tools', 'assignfilename') == 0

    @patch('kafka.tools.assigner.models.reassignment.subprocess.Popen', new_callable=MockPopen)
    def test_check_completion_progress(self, mock_popen):
        cmd_stdout = ("Status of partition reassignment:\n"
                      "Reassignment of partition [testTopic,0] completed successfully\n"
                      "Reassignment of partition [testTopic,1] still in progress\n"
                      "Reassignment of partition [testTopic,2] completed successfully\n"
                      "Reassignment of partition [testTopic,3] completed successfully\n")
        mock_popen.set_default(stdout=cmd_stdout.encode('utf-8'))
        assert self.reassignment.check_completion('zkconnect', '/path/to/tools', 'assignfilename') == 1
