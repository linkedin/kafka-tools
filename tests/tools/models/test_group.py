import unittest

from kafka.tools.models.group import Group


class GroupTests(unittest.TestCase):
    def test_group_create(self):
        group = Group('testgroup')
        assert group.name == 'testgroup'

    def test_updated_since(self):
        group = Group('testgroup')
        group._last_updated = 100
        assert group.updated_since(99)
