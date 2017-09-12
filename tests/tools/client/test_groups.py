import time
import unittest
from mock import MagicMock

from tests.tools.client.fixtures import describe_groups, list_groups, list_groups_error

from kafka.tools.client import Client
from kafka.tools.models.broker import Broker
from kafka.tools.models.group import Group, GroupMember
from kafka.tools.protocol.requests.list_groups_v0 import ListGroupsV0Request


def assert_cluster_has_groups(cluster, dg):
    for dgroup in dg['groups']:
        assert dgroup['group_id'] in cluster.groups
        group = cluster.groups[dgroup['group_id']]
        assert group.name == dgroup['group_id']
        assert group.protocol == dgroup['protocol']
        assert group.protocol_type == dgroup['protocol_type']
        assert len(group.members) == len(dgroup['members'])

        for i, gmember in enumerate(dgroup['members']):
            member = group.members[i]
            assert member.group == group
            assert member.name == gmember['member_id']
            assert member.client_id == gmember['client_id']
            assert member.client_host == gmember['client_host']
            assert member.metadata == gmember['member_metadata']
            assert member.assignment_data == gmember['member_assignment']


class GroupsTests(unittest.TestCase):
    def setUp(self):
        # Dummy client for testing - we're not going to connect that bootstrap broker
        self.client = Client()
        self.describe_groups = describe_groups()

    def test_update_groups_from_describe_create(self):
        self.client._update_groups_from_describe(self.describe_groups)
        assert_cluster_has_groups(self.client.cluster, self.describe_groups)

    def test_update_groups_from_describe_assignment(self):
        self.client._update_groups_from_describe(self.describe_groups)
        assert_cluster_has_groups(self.client.cluster, self.describe_groups)

        group = self.client.cluster.groups['testgroup']
        assert group.members[0].topics == {'topic1': [0]}
        assert group.members[1].topics == {'topic1': [1]}

    def test_update_groups_from_describe_update(self):
        self.client.cluster.add_group(Group('testgroup'))
        self.client._update_groups_from_describe(self.describe_groups)

        assert_cluster_has_groups(self.client.cluster, self.describe_groups)

    def test_update_groups_from_describe_clear_members(self):
        self.client.cluster.add_group(Group('testgroup'))
        self.client.cluster.groups['testgroup'].members = [GroupMember('badmember')]
        self.client._update_groups_from_describe(self.describe_groups)

        assert_cluster_has_groups(self.client.cluster, self.describe_groups)

    def test_maybe_update_groups_list_expired(self):
        self.client._send_all_brokers = MagicMock()
        self.client._send_all_brokers.return_value = ['metadata_response']
        self.client._update_groups_from_lists = MagicMock()

        fake_last_time = time.time() - (self.client.configuration.metadata_refresh * 2)
        self.client._last_group_list = fake_last_time
        self.client._maybe_update_groups_list()

        assert self.client._last_group_list > fake_last_time
        self.client._send_all_brokers.assert_called_once()
        arg = self.client._send_all_brokers.call_args[0][0]
        assert isinstance(arg, ListGroupsV0Request)
        self.client._update_groups_from_lists.assert_called_once_with(['metadata_response'])

    def test_maybe_update_groups_list_nocache(self):
        self.client._send_all_brokers = MagicMock()
        self.client._update_groups_from_lists = MagicMock()

        fake_last_time = time.time() - 1000
        self.client._last_group_list = fake_last_time
        self.client._maybe_update_groups_list(cache=False)

        assert self.client._last_group_list > fake_last_time

    def test_maybe_update_groups_list_usecache(self):
        self.client._send_all_brokers = MagicMock()
        self.client._update_groups_from_lists = MagicMock()

        fake_last_time = time.time() - 1000
        self.client._last_group_list = fake_last_time
        self.client._maybe_update_groups_list(cache=True)

    def test_update_groups_from_lists(self):
        self.client._add_or_update_group = MagicMock()
        list_group = list_groups()
        val = self.client._update_groups_from_lists({1: list_group})

        assert val == 0
        self.client._add_or_update_group.assert_called_once_with(list_group['groups'][0], 1)

    def test_update_groups_from_lists_error(self):
        self.client._add_or_update_group = MagicMock()
        val = self.client._update_groups_from_lists({1: list_groups_error()})

        assert val == 1
        self.client._add_or_update_group.assert_not_called()

    def test_update_groups_from_lists_none(self):
        self.client._add_or_update_group = MagicMock()
        val = self.client._update_groups_from_lists({1: None})

        assert val == 1
        self.client._add_or_update_group.assert_not_called()

    def test_add_or_update_group(self):
        broker = Broker('host1.example.com', id=1, port=8031)
        self.client.cluster.add_broker(broker)
        list_group = list_groups()

        self.client._add_or_update_group(list_group['groups'][0], 1)
        assert 'group1' in self.client.cluster.groups
        assert self.client.cluster.groups['group1'].coordinator == broker
        assert self.client.cluster.groups['group1'].protocol_type == 'protocol1'

    def test_add_or_update_group_update(self):
        broker = Broker('host1.example.com', id=1, port=8031)
        self.client.cluster.add_broker(broker)
        group = Group('group1')
        self.client.cluster.add_group(group)
        list_group = list_groups()

        self.client._add_or_update_group(list_group['groups'][0], 1)
        assert 'group1' in self.client.cluster.groups
        assert self.client.cluster.groups['group1'].coordinator == broker
        assert self.client.cluster.groups['group1'].protocol_type == 'protocol1'
