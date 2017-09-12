import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.responses.api_versions_v0 import ApiVersionsV0Response
from kafka.tools.protocol.responses.controlled_shutdown_v1 import ControlledShutdownV1Response
from kafka.tools.protocol.responses.create_topics_v0 import CreateTopicsV0Response
from kafka.tools.protocol.responses.delete_topics_v0 import DeleteTopicsV0Response
from kafka.tools.protocol.responses.describe_groups_v0 import DescribeGroupsV0Response
from kafka.tools.protocol.responses.fetch_v0 import FetchV0Response
from kafka.tools.protocol.responses.fetch_v1 import FetchV1Response
from kafka.tools.protocol.responses.fetch_v2 import FetchV2Response
from kafka.tools.protocol.responses.group_coordinator_v0 import GroupCoordinatorV0Response
from kafka.tools.protocol.responses.heartbeat_v0 import HeartbeatV0Response
from kafka.tools.protocol.responses.join_group_v0 import JoinGroupV0Response
from kafka.tools.protocol.responses.leader_and_isr_v0 import LeaderAndIsrV0Response
from kafka.tools.protocol.responses.leave_group_v0 import LeaveGroupV0Response
from kafka.tools.protocol.responses.list_groups_v0 import ListGroupsV0Response
from kafka.tools.protocol.responses.list_offset_v0 import ListOffsetV0Response
from kafka.tools.protocol.responses.member_assignment_v0 import MemberAssignmentV0
from kafka.tools.protocol.responses.metadata_v0 import MetadataV0Response
from kafka.tools.protocol.responses.metadata_v1 import MetadataV1Response
from kafka.tools.protocol.responses.offset_commit_v0 import OffsetCommitV0Response
from kafka.tools.protocol.responses.offset_commit_v1 import OffsetCommitV1Response
from kafka.tools.protocol.responses.offset_commit_v2 import OffsetCommitV2Response
from kafka.tools.protocol.responses.offset_fetch_v0 import OffsetFetchV0Response
from kafka.tools.protocol.responses.offset_fetch_v1 import OffsetFetchV1Response
from kafka.tools.protocol.responses.produce_v0 import ProduceV0Response
from kafka.tools.protocol.responses.produce_v1 import ProduceV1Response
from kafka.tools.protocol.responses.produce_v2 import ProduceV2Response
from kafka.tools.protocol.responses.sasl_handshake_v0 import SaslHandshakeV0Response
from kafka.tools.protocol.responses.stop_replica_v0 import StopReplicaV0Response
from kafka.tools.protocol.responses.sync_group_v0 import SyncGroupV0Response
from kafka.tools.protocol.responses.update_metadata_v0 import UpdateMetadataV0Response
from kafka.tools.protocol.responses.update_metadata_v1 import UpdateMetadataV1Response
from kafka.tools.protocol.responses.update_metadata_v2 import UpdateMetadataV2Response


class ValidateResponseSchemaTests(unittest.TestCase):
    def test_api_versions_v0(self):
        obj = ApiVersionsV0Response({})
        validate_schema(obj.schema)

    def test_controlled_shutdown_v1(self):
        obj = ControlledShutdownV1Response({})
        validate_schema(obj.schema)

    def test_create_topics_v0(self):
        obj = CreateTopicsV0Response({})
        validate_schema(obj.schema)

    def test_delete_topics_v0(self):
        obj = DeleteTopicsV0Response({})
        validate_schema(obj.schema)

    def test_describe_groups_v0(self):
        obj = DescribeGroupsV0Response({})
        validate_schema(obj.schema)

    def test_fetch_v0(self):
        obj = FetchV0Response({})
        validate_schema(obj.schema)

    def test_fetch_v1(self):
        obj = FetchV1Response({})
        validate_schema(obj.schema)

    def test_fetch_v2(self):
        obj = FetchV2Response({})
        validate_schema(obj.schema)

    def test_group_coordinator_v0(self):
        obj = GroupCoordinatorV0Response({})
        validate_schema(obj.schema)

    def test_heartbeat_v0(self):
        obj = HeartbeatV0Response({})
        validate_schema(obj.schema)

    def test_join_group_v0(self):
        obj = JoinGroupV0Response({})
        validate_schema(obj.schema)

    def test_leader_and_isr_v0(self):
        obj = LeaderAndIsrV0Response({})
        validate_schema(obj.schema)

    def test_leave_group_v0(self):
        obj = LeaveGroupV0Response({})
        validate_schema(obj.schema)

    def test_list_groups_v0(self):
        obj = ListGroupsV0Response({})
        validate_schema(obj.schema)

    def test_list_offset_v0(self):
        obj = ListOffsetV0Response({})
        validate_schema(obj.schema)

    def test_member_assignment_v0(self):
        obj = MemberAssignmentV0({})
        validate_schema(obj.schema)

    def test_metadata_v0(self):
        obj = MetadataV0Response({})
        validate_schema(obj.schema)

    def test_metadata_v1(self):
        obj = MetadataV1Response({})
        validate_schema(obj.schema)

    def test_offset_commit_v0(self):
        obj = OffsetCommitV0Response({})
        validate_schema(obj.schema)

    def test_offset_commit_v1(self):
        obj = OffsetCommitV1Response({})
        validate_schema(obj.schema)

    def test_offset_commit_v2(self):
        obj = OffsetCommitV2Response({})
        validate_schema(obj.schema)

    def test_offset_fetch_v0(self):
        obj = OffsetFetchV0Response({})
        validate_schema(obj.schema)

    def test_offset_fetch_v1(self):
        obj = OffsetFetchV1Response({})
        validate_schema(obj.schema)

    def test_produce_v0(self):
        obj = ProduceV0Response({})
        validate_schema(obj.schema)

    def test_produce_v1(self):
        obj = ProduceV1Response({})
        validate_schema(obj.schema)

    def test_produce_v2(self):
        obj = ProduceV2Response({})
        validate_schema(obj.schema)

    def test_sasl_handshake_v0(self):
        obj = SaslHandshakeV0Response({})
        validate_schema(obj.schema)

    def test_stop_replica_v0(self):
        obj = StopReplicaV0Response({})
        validate_schema(obj.schema)

    def test_sync_group_v0(self):
        obj = SyncGroupV0Response({})
        validate_schema(obj.schema)

    def test_update_metadata_v0(self):
        obj = UpdateMetadataV0Response({})
        validate_schema(obj.schema)

    def test_update_metadata_v1(self):
        obj = UpdateMetadataV1Response({})
        validate_schema(obj.schema)

    def test_update_metadata_v2(self):
        obj = UpdateMetadataV2Response({})
        validate_schema(obj.schema)
