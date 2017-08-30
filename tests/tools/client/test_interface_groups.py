import time
import unittest
from mock import MagicMock

from tests.tools.client.fixtures import describe_groups, describe_groups_error

from kafka.tools.client import Client
from kafka.tools.exceptions import GroupError
from kafka.tools.models.broker import Broker
from kafka.tools.models.group import Group
from kafka.tools.protocol.requests.describe_groups_v0 import DescribeGroupsV0Request


class InterfaceGroupsTests(unittest.TestCase):
    def setUp(self):
        # Dummy client for testing - we're not going to connect that bootstrap broker
        self.client = Client()
        self.client._connected = True

        # Two brokers for the client
        broker = Broker('host1.example.com', id=1, port=8031)
        broker.rack = 'rack1'
        self.client.cluster.add_broker(broker)
        broker = Broker('host2.example.com', id=101, port=8032)
        broker.rack = 'rack1'
        self.client.cluster.add_broker(broker)

        self.describe_groups = describe_groups()
        self.describe_groups_error = describe_groups_error()

    def test_list_groups(self):
        self.client.cluster.add_group(Group('group2'))

        self.client._maybe_update_groups_list = MagicMock()
        self.client._maybe_update_groups_list.return_value = 312

        groups, errs = self.client.list_groups()
        self.client._maybe_update_groups_list.assert_called_once_with(True)
        assert errs == 312
        assert groups == ['group2']

    def test_list_groups_cache(self):
        self.client._maybe_update_groups_list = MagicMock()

        self.client.list_groups(cache=False)
        self.client._maybe_update_groups_list.assert_called_once_with(False)

    def test_get_group(self):
        def add_group_to_cluster(group_name, request):
            # _send_group_aware_request has a side effect of creating the group and setting the coordinator
            self.client.cluster.add_group(Group(group_name))
            self.client.cluster.groups[group_name].coordinator = self.client.cluster.brokers[1]
            return self.describe_groups

        self.client._send_group_aware_request = MagicMock()
        self.client._send_group_aware_request.side_effect = add_group_to_cluster

        val = self.client.get_group('testgroup')

        self.client._send_group_aware_request.assert_called_once()
        assert self.client._send_group_aware_request.call_args[0][0] == 'testgroup'
        assert isinstance(self.client._send_group_aware_request.call_args[0][1], DescribeGroupsV0Request)

        assert isinstance(val, Group)
        assert val.name == 'testgroup'
        print(val.coordinator)
        print(self.client.cluster.brokers[1])
        assert val.coordinator == self.client.cluster.brokers[1]

    def test_get_group_existing_cached(self):
        group = Group('testgroup')
        group.coordinator = self.client.cluster.brokers[1]
        group._last_updated = time.time()
        self.client.cluster.add_group(group)

        self.client._send_group_aware_request = MagicMock()
        self.client._send_group_aware_request.return_value = self.describe_groups
        self.client._update_groups_from_describe = MagicMock()
        val = self.client.get_group('testgroup')

        self.client._send_group_aware_request.assert_not_called()
        self.client._update_groups_from_describe.assert_not_called()

        assert isinstance(val, Group)
        assert val.name == 'testgroup'
        assert val.coordinator == self.client.cluster.brokers[1]

    def test_get_group_error(self):
        group = Group('badgroup')
        group.coordinator = self.client.cluster.brokers[1]
        self.client.cluster.add_group(group)

        self.client._send_group_aware_request = MagicMock()
        self.client._send_group_aware_request.return_value = self.describe_groups_error

        self.assertRaises(GroupError, self.client.get_group, 'badgroup', cache=False)
        self.client._send_group_aware_request.assert_called_once()

    def test_get_group_nonexistent(self):
        self.client._send_group_aware_request = MagicMock()
        self.client._send_group_aware_request.side_effect = GroupError

        self.assertRaises(GroupError, self.client.get_group, 'nonexistentgroup')
        self.client._send_group_aware_request.assert_called_once()
