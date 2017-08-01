from kafka.tools.protocol.responses.list_groups_v0 import ListGroupsV0Response
from kafka.tools.protocol.responses.describe_groups_v0 import DescribeGroupsV0Response
from kafka.tools.protocol.responses.metadata_v1 import MetadataV1Response
from kafka.tools.protocol.responses.group_coordinator_v0 import GroupCoordinatorV0Response


def describe_groups():
    return DescribeGroupsV0Response.from_dict({'groups': [{'group_id': 'testgroup',
                                                           'error': 0,
                                                           'state': 'Stable',
                                                           'protocol': 'testprotocol',
                                                           'protocol_type': 'testprotocoltype',
                                                           'members': [{'member_id': 'testmember1',
                                                                        'client_id': 'testclientid1',
                                                                        'client_host': 'host.example.com',
                                                                        'member_metadata': b'\x90\x83\x24\xbc',
                                                                        'member_assignment': b'\x89\x89\xac\xda'},
                                                                       {'member_id': 'testmember2',
                                                                        'client_id': 'testclientid2',
                                                                        'client_host': 'otherhost.example.com',
                                                                        'member_metadata': b'\x89\x34\x78\xad',
                                                                        'member_assignment': b'\x34\x79\x8f\xbc'}]}]})


def topic_metadata():
    return MetadataV1Response.from_dict({'brokers': [{'node_id': 1,
                                                      'host': 'host1.example.com',
                                                      'port': 8031,
                                                      'rack': 'rack1'},
                                                     {'node_id': 101,
                                                      'host': 'host2.example.com',
                                                      'port': 8032,
                                                      'rack': 'rack2'}],
                                         'controller_id': 1,
                                         'topics': [{'error': 0,
                                                     'name': 'topic1',
                                                     'internal': False,
                                                     'partitions': [{'error': 0,
                                                                     'id': 0,
                                                                     'leader': 1,
                                                                     'replicas': [101, 1],
                                                                     'isrs': [101, 1]},
                                                                    {'error': 0,
                                                                     'id': 1,
                                                                     'leader': 101,
                                                                     'replicas': [101, 1],
                                                                     'isrs': [1, 101]}]}]})


def topic_metadata_error():
    return MetadataV1Response.from_dict({'brokers': [{'node_id': 1,
                                                      'host': 'host1.example.com',
                                                      'port': 8031,
                                                      'rack': 'rack1'},
                                                     {'node_id': 101,
                                                      'host': 'host2.example.com',
                                                      'port': 8032,
                                                      'rack': 'rack2'}],
                                         'controller_id': 1,
                                         'topics': [{'error': 3,
                                                     'name': 'topic1',
                                                     'internal': False,
                                                     'partitions': None}]})


def group_coordinator():
    return GroupCoordinatorV0Response.from_dict({'error': 0,
                                                 'node_id': 1,
                                                 'host': 'host1.example.com',
                                                 'port': 8031})


def group_coordinator_error():
    return GroupCoordinatorV0Response.from_dict({'error': 15,
                                                 'node_id': -1,
                                                 'host': None,
                                                 'port': -1})


def list_groups():
    return ListGroupsV0Response.from_dict({'error': 0, 'groups': [{'group_id': 'group1',
                                                                   'protocol_type': 'protocol1'}]})


def list_groups_error():
    return ListGroupsV0Response.from_dict({'error': 1, 'groups': None})
