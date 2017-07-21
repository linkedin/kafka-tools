import unittest

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.api_versions_v0 import ApiVersionsV0Request


class ApiVersionsV0RequestTests(unittest.TestCase):
    def test_process_arguments(self):
        assert ApiVersionsV0Request.process_arguments([]) == {}

    def test_process_arguments_extra(self):
        self.assertRaises(ArgumentError, ApiVersionsV0Request.process_arguments, ['foo'])
