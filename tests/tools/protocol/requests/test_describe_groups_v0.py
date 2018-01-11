import unittest

from kafka.tools.protocol.requests.describe_groups_v0 import DescribeGroupsV0Request


class DescribeGroupsV0RequestTest(unittest.TestCase):
    def test_process_arguments(self):
        val = DescribeGroupsV0Request.process_arguments(['groupname', 'anothergroup'])
        assert val == {'group_ids': ['groupname', 'anothergroup']}

    def test_process_arguments_all(self):
        val = DescribeGroupsV0Request.process_arguments([])
        assert val == {'group_ids': None}
