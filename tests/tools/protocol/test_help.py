import unittest

from kafka.tools.protocol.help import show_help


class FakeRequest:
    cmd = "FakeRequest"
    help_string = "Show help for FakeRequest"


class HelpTests(unittest.TestCase):
    def setUp(self):
        self.request_classes = {
            'fakerequest': {
                0: FakeRequest
            }
        }
        self.request_cmds = {
            'fakerequest': FakeRequest,
            'fakerequestv0': FakeRequest,
        }

    def test_help_general(self):
        # As this is only designed to output, we just want to make sure it doesn't fail
        show_help(self.request_classes, self.request_cmds, [])
        assert True

    def test_help_show_cmd(self):
        show_help(self.request_classes, self.request_cmds, ['fakerequest'])
        assert True

    def test_help_bad_cmd(self):
        show_help(self.request_classes, self.request_cmds, ['badrequest'])
        assert True
