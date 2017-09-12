import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests.offset_fetch_v1 import OffsetFetchV1Request


class OffsetFetchV1RequestTests(unittest.TestCase):
    def test_schema(self):
        validate_schema(OffsetFetchV1Request.schema)
