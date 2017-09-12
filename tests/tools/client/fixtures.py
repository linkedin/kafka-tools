from kafka.tools.protocol.responses.list_groups_v0 import ListGroupsV0Response
from kafka.tools.protocol.responses.list_offset_v0 import ListOffsetV0Response
from kafka.tools.protocol.responses.offset_commit_v2 import OffsetCommitV2Response
from kafka.tools.protocol.responses.offset_fetch_v1 import OffsetFetchV1Response
from kafka.tools.protocol.responses.describe_groups_v0 import DescribeGroupsV0Response
from kafka.tools.protocol.responses.metadata_v1 import MetadataV1Response
from kafka.tools.protocol.responses.metadata_v0 import MetadataV0Response
from kafka.tools.protocol.responses.group_coordinator_v0 import GroupCoordinatorV0Response


def describe_groups():
    ma_1 = b'\x00\x00\x00\x00\x00\x01\x00\x06topic1\x00\x00\x00\x01\x00\x00\x00\x00\xff\xff\xff\xff'
    ma_2 = b'\x00\x00\x00\x00\x00\x01\x00\x06topic1\x00\x00\x00\x01\x00\x00\x00\x01\xff\xff\xff\xff'
    return DescribeGroupsV0Response({'groups': [{'group_id': 'testgroup',
                                                 'error': 0,
                                                 'state': 'Stable',
                                                 'protocol': 'roundrobin',
                                                 'protocol_type': 'consumer',
                                                 'members': [{'member_id': 'testmember1',
                                                              'client_id': 'testclientid1',
                                                              'client_host': 'host.example.com',
                                                              'member_metadata': b'\x90\x83\x24\xbc',
                                                              'member_assignment': ma_1},
                                                             {'member_id': 'testmember2',
                                                              'client_id': 'testclientid2',
                                                              'client_host': 'otherhost.example.com',
                                                              'member_metadata': b'\x89\x34\x78\xad',
                                                              'member_assignment': ma_2}
                                                             ]
                                                 }]
                                     })


def describe_groups_error():
    return DescribeGroupsV0Response({'groups': [{'group_id': 'testgroup',
                                                 'error': 16,
                                                 'state': None,
                                                 'protocol': None,
                                                 'protocol_type': None,
                                                 'members': None}]})


def topic_metadata():
    return MetadataV1Response({'brokers': [{'node_id': 1,
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


def topic_metadata_v0():
    return MetadataV0Response({'brokers': [{'node_id': 1,
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
    return MetadataV1Response({'brokers': [{'node_id': 1,
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
    return GroupCoordinatorV0Response({'error': 0,
                                       'node_id': 1,
                                       'host': 'host1.example.com',
                                       'port': 8031})


def group_coordinator_error():
    return GroupCoordinatorV0Response({'error': 15,
                                       'node_id': -1,
                                       'host': None,
                                       'port': -1})


def list_groups():
    return ListGroupsV0Response({'error': 0, 'groups': [{'group_id': 'group1',
                                                         'protocol_type': 'protocol1'}]})


def list_groups_error():
    return ListGroupsV0Response({'error': 1, 'groups': None})


def list_offset():
    return ListOffsetV0Response({'responses': [{'topic': 'topic1',
                                                'partition_responses': [{'partition': 0,
                                                                         'error': 0,
                                                                         'offsets': [4829]},
                                                                        {'partition': 1,
                                                                         'error': 0,
                                                                         'offsets': [8904]}]}]})


def list_offset_error():
    return ListOffsetV0Response({'responses': [{'topic': 'topic1',
                                                'partition_responses': [{'partition': 0,
                                                                         'error': 6,
                                                                         'offsets': None},
                                                                        {'partition': 1,
                                                                         'error': 0,
                                                                         'offsets': [8904]}]}]})


def offset_fetch():
    return OffsetFetchV1Response({'responses': [{'topic': 'topic1',
                                                 'partition_responses': [{'partition': 0,
                                                                          'metadata': None,
                                                                          'error': 0,
                                                                          'offset': 4829},
                                                                         {'partition': 1,
                                                                          'metadata': None,
                                                                          'error': 0,
                                                                          'offset': 8904}]}]})


def offset_fetch_error():
    return OffsetFetchV1Response({'responses': [{'topic': 'topic1',
                                                 'partition_responses': [{'partition': 0,
                                                                          'error': 6,
                                                                          'metadata': None,
                                                                          'offset': -1},
                                                                         {'partition': 1,
                                                                          'metadata': None,
                                                                          'error': 0,
                                                                          'offset': 8904}]}]})


def offset_commit_response():
    return OffsetCommitV2Response({'responses': [{'topic': 'topic1',
                                                  'partition_responses': [{'partition': 0,
                                                                           'error': 0},
                                                                          {'partition': 1,
                                                                           'error': 16}]}]})
