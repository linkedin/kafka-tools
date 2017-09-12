import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.controlled_shutdown_v1 import ControlledShutdownV1Request


class ControlledShutdownV1RequestTests(unittest.TestCase):
    def test_process_arguments(self):
        assert ControlledShutdownV1Request.process_arguments([3]) == {'broker_id': 3}

    def test_process_arguments_missing(self):
        self.assertRaises(ArgumentError, ControlledShutdownV1Request.process_arguments, [])

    def test_process_arguments_extra(self):
        self.assertRaises(ArgumentError, ControlledShutdownV1Request.process_arguments, [3, 4])

    def test_process_arguments_nonnumeric(self):
        self.assertRaises(ArgumentError, ControlledShutdownV1Request.process_arguments, ['foo'])

    def test_schema(self):
        validate_schema(ControlledShutdownV1Request.schema)
