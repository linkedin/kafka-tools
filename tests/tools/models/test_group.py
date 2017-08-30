import unittest

from kafka.tools.models.group import Group, GroupMember


class GroupTests(unittest.TestCase):
    def test_group_create(self):
        group = Group('testgroup')
        assert group.name == 'testgroup'

    def test_updated_since(self):
        group = Group('testgroup')
        group._last_updated = 100
        assert group.updated_since(99)

    def test_add_member(self):
        group = Group('testgroup')
        group.add_member('membername', client_id='clientid', client_host='host1', metadata=b'\x00\x32', assignment=b'\x01\x34')
        assert len(group.members) == 1
        assert group.members[0].name == 'membername'
        assert group.members[0].client_id == 'clientid'
        assert group.members[0].client_host == 'host1'
        assert group.members[0].metadata == b'\x00\x32'
        assert group.members[0].assignment_data == b'\x01\x34'

    def test_clear_members(self):
        group = Group('testgroup')
        group.members = 'baddata'
        group.clear_members()
        assert group.members == []

    def test_set_assignment(self):
        group = Group('testgroup')
        group.protocol_type = 'consumer'
        member = GroupMember('membername', assignment=b'\x00\x00\x00\x00\x00\x01\x00\x06topic1\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x65\xbd')
        member.group = group

        member.set_assignment()
        assert member.assignment_version == 0
        assert member.assignment_data == b'\x00\x00\x00\x00\x00\x01\x00\x06topic1\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x65\xbd'
        assert member.user_data == b'\x65\xbd'
        assert member.topics == {'topic1': [0]}

    def test_subscribed_topics(self):
        group = Group('testgroup')
        group.protocol_type = 'consumer'
        member = GroupMember('member1')
        member.group = group
        member.topics = {'topic1': [0]}
        group.members.append(member)
        member = GroupMember('member2')
        member.group = group
        member.topics = {'topic2': [0], 'topic1': [1]}
        group.members.append(member)

        topics = group.subscribed_topics()
        print(topics)
        assert set(['topic1', 'topic2']) == set(topics)
