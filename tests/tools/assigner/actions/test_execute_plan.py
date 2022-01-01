import sys
import json
import unittest
from mock import patch
from argparse import Namespace
from tempfile import NamedTemporaryFile
from .fixtures import set_up_cluster_9broker, set_up_subparser
from kafka.tools.assigner.actions.execute_plan import ActionExecutePlan
from kafka.tools.exceptions import InvalidPlanFormatException


class ActionExecutePlanTests(unittest.TestCase):
    def setUp(self):
        self.cluster = set_up_cluster_9broker()
        self.parser, self.subparser = set_up_subparser()
        self.args = Namespace()
        self.wrong_plan_file = NamedTemporaryFile(mode="w")
        self.correct_plan_file = NamedTemporaryFile(mode="w")

        self.__add_wrong_plan()
        self.__add_correct_plan()

    def __add_wrong_plan(self):
        self.wrong_plan_file.write(
            json.dumps({"topic": "topic1", "partition": 0, "replicas": [1,2,8]})
        )
        self.wrong_plan_file.flush()

    def __add_correct_plan(self):
        self.correct_plan_file.write(
            json.dumps(
                [
                    {"topic": "topic1", "partition": 0,"replicas": [1,2,8]},
                    {"topic": "topic1", "partition": 1, "replicas": [2,7,8]}
                ]
            )
        )
        self.correct_plan_file.flush()

    def test_create_class(self):
        action = ActionExecutePlan(self.args, self.cluster)
        assert isinstance(action, ActionExecutePlan)

    @patch("os.path.isfile")
    def test_configure_args(self, mock_isfile):
        ActionExecutePlan.configure_args(self.subparser)
        sys.argv = ["kafka-assigner", "execute_plan", "--plan-file-path", "/tmp/path"]
        args = self.parser.parse_args()
        mock_isfile.assert_called_once_with("/tmp/path")
        assert args.action == "execute_plan"

    def test_wrong_plan_format(self):
        self.args.plan_file_path = self.wrong_plan_file.name
        action = ActionExecutePlan(self.args, self.cluster)
        self.assertRaises(InvalidPlanFormatException, action.process_cluster)

    def test_correct_plan_format(self):
        self.args.plan_file_path = self.correct_plan_file.name
        action = ActionExecutePlan(self.args, self.cluster)
        action.process_cluster()
        partition = list(filter(lambda x: int(x.num) == 0, self.cluster.topics["topic1"].partitions))
        assert len(partition) == 1
        self.assertListEqual(
            [b.id for b in partition[0].replicas],
            [1,2,8]
        )

    def tearDown(self):
        self.wrong_plan_file.close()
        self.correct_plan_file.close()

