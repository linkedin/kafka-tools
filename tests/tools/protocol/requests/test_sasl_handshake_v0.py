import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests import ArgumentError
from kafka.tools.protocol.requests.sasl_handshake_v0 import SaslHandshakeV0Request


class SaslHandshakeV0RequestTests(unittest.TestCase):
    def test_process_arguments(self):
        val = SaslHandshakeV0Request.process_arguments(['sasl_mechanism'])
        assert val == {'mechanism': 'sasl_mechanism'}

    def test_process_arguments_missing(self):
        self.assertRaises(ArgumentError, SaslHandshakeV0Request.process_arguments, [])

    def test_process_arguments_two(self):
        self.assertRaises(ArgumentError, SaslHandshakeV0Request.process_arguments, ['sasl_mechanism', 'extraarg'])

    def test_schema(self):
        validate_schema(SaslHandshakeV0Request.schema)
