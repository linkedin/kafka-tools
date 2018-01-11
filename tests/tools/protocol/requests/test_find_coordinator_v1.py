import unittest

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.find_coordinator_v1 import FindCoordinatorV1Request


class FindCoordinatorV1RequestTests(unittest.TestCase):
    def test_process_arguments(self):
        val = FindCoordinatorV1Request.process_arguments(['group', 'groupname'])
        assert val == {'coordinator_key': 'groupname', 'coordinator_type': 0}

    def test_process_arguments_missing(self):
        self.assertRaises(ArgumentError, FindCoordinatorV1Request.process_arguments, ['groupname'])

    def test_process_arguments_badtype(self):
        self.assertRaises(ArgumentError, FindCoordinatorV1Request.process_arguments, ['topic', 'groupname'])

    def test_process_arguments_toomany(self):
        self.assertRaises(ArgumentError, FindCoordinatorV1Request.process_arguments, ['group', 'groupname', 'anothergroup'])
