import six
import unittest

from kafka.tools.protocol.errors import error_short, error_long


class ErrorTests(unittest.TestCase):
    def test_error_short(self):
        assert error_short(3) == 'UNKNOWN_TOPIC_OR_PARTITION'

    def test_error_short_unknown(self):
        assert error_short(99) == 'NOSUCHERROR'

    def test_error_long(self):
        assert isinstance(error_long(3), six.string_types)

    def test_error_long_unknown(self):
        assert isinstance(error_long(99), six.string_types)
