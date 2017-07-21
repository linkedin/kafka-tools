import sys
import unittest

from kafka.tools.protocol.arguments import set_up_arguments


class ArgumentTests(unittest.TestCase):
    def test_get_arguments_none(self):
        sys.argv = ['kafka-protocol']
        args = set_up_arguments()

        # These are the current defaults, but they're not important as to the specific values
        # We're more concerned that they just exist
        assert args.broker == 'localhost'
        assert args.port == 9092
        assert not args.tls
