import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.responses.alter_configs_v0 import AlterConfigsV0Response
from kafka.tools.protocol.responses.alter_replica_log_dirs_v0 import AlterReplicaLogDirsV0Response
from kafka.tools.protocol.responses.api_versions_v0 import ApiVersionsV0Response
from kafka.tools.protocol.responses.api_versions_v1 import ApiVersionsV1Response
from kafka.tools.protocol.responses.controlled_shutdown_v0 import ControlledShutdownV0Response
from kafka.tools.protocol.responses.controlled_shutdown_v1 import ControlledShutdownV1Response
from kafka.tools.protocol.responses.create_acls_v0 import CreateAclsV0Response
from kafka.tools.protocol.responses.create_partitions_v0 import CreatePartitionsV0Response
from kafka.tools.protocol.responses.create_topics_v0 import CreateTopicsV0Response
from kafka.tools.protocol.responses.create_topics_v1 import CreateTopicsV1Response
from kafka.tools.protocol.responses.create_topics_v2 import CreateTopicsV2Response
from kafka.tools.protocol.responses.delete_acls_v0 import DeleteAclsV0Response
from kafka.tools.protocol.responses.delete_topics_v0 import DeleteTopicsV0Response
from kafka.tools.protocol.responses.delete_topics_v1 import DeleteTopicsV1Response
from kafka.tools.protocol.responses.delete_records_v0 import DeleteRecordsV0Response
from kafka.tools.protocol.responses.describe_acls_v0 import DescribeAclsV0Response
from kafka.tools.protocol.responses.describe_configs_v0 import DescribeConfigsV0Response
from kafka.tools.protocol.responses.describe_groups_v0 import DescribeGroupsV0Response
from kafka.tools.protocol.responses.describe_groups_v1 import DescribeGroupsV1Response
from kafka.tools.protocol.responses.describe_log_dirs_v0 import DescribeLogDirsV0Response
from kafka.tools.protocol.responses.fetch_v0 import FetchV0Response
from kafka.tools.protocol.responses.fetch_v1 import FetchV1Response
from kafka.tools.protocol.responses.fetch_v2 import FetchV2Response
from kafka.tools.protocol.responses.group_coordinator_v0 import GroupCoordinatorV0Response
from kafka.tools.protocol.responses.find_coordinator_v1 import FindCoordinatorV1Response
from kafka.tools.protocol.responses.heartbeat_v0 import HeartbeatV0Response
from kafka.tools.protocol.responses.heartbeat_v1 import HeartbeatV1Response
from kafka.tools.protocol.responses.join_group_v0 import JoinGroupV0Response
from kafka.tools.protocol.responses.join_group_v1 import JoinGroupV1Response
from kafka.tools.protocol.responses.join_group_v2 import JoinGroupV2Response
from kafka.tools.protocol.responses.leader_and_isr_v0 import LeaderAndIsrV0Response
from kafka.tools.protocol.responses.leader_and_isr_v1 import LeaderAndIsrV1Response
from kafka.tools.protocol.responses.leave_group_v0 import LeaveGroupV0Response
from kafka.tools.protocol.responses.leave_group_v1 import LeaveGroupV1Response
from kafka.tools.protocol.responses.list_groups_v0 import ListGroupsV0Response
from kafka.tools.protocol.responses.list_groups_v1 import ListGroupsV1Response
from kafka.tools.protocol.responses.list_offset_v0 import ListOffsetV0Response
from kafka.tools.protocol.responses.list_offset_v1 import ListOffsetV1Response
from kafka.tools.protocol.responses.list_offset_v2 import ListOffsetV2Response
from kafka.tools.protocol.responses.member_assignment_v0 import MemberAssignmentV0
from kafka.tools.protocol.responses.metadata_v0 import MetadataV0Response
from kafka.tools.protocol.responses.metadata_v1 import MetadataV1Response
from kafka.tools.protocol.responses.metadata_v2 import MetadataV2Response
from kafka.tools.protocol.responses.metadata_v3 import MetadataV3Response
from kafka.tools.protocol.responses.metadata_v4 import MetadataV4Response
from kafka.tools.protocol.responses.metadata_v5 import MetadataV5Response
from kafka.tools.protocol.responses.offset_commit_v0 import OffsetCommitV0Response
from kafka.tools.protocol.responses.offset_commit_v1 import OffsetCommitV1Response
from kafka.tools.protocol.responses.offset_commit_v2 import OffsetCommitV2Response
from kafka.tools.protocol.responses.offset_commit_v3 import OffsetCommitV3Response
from kafka.tools.protocol.responses.offset_fetch_v0 import OffsetFetchV0Response
from kafka.tools.protocol.responses.offset_fetch_v1 import OffsetFetchV1Response
from kafka.tools.protocol.responses.offset_fetch_v2 import OffsetFetchV2Response
from kafka.tools.protocol.responses.offset_fetch_v3 import OffsetFetchV3Response
from kafka.tools.protocol.responses.offsets_for_leader_epoch_v0 import OffsetForLeaderEpochV0Response
from kafka.tools.protocol.responses.produce_v0 import ProduceV0Response
from kafka.tools.protocol.responses.produce_v1 import ProduceV1Response
from kafka.tools.protocol.responses.produce_v2 import ProduceV2Response
from kafka.tools.protocol.responses.sasl_authenticate_v0 import SaslAuthenticateV0Response
from kafka.tools.protocol.responses.sasl_handshake_v0 import SaslHandshakeV0Response
from kafka.tools.protocol.responses.sasl_handshake_v1 import SaslHandshakeV1Response
from kafka.tools.protocol.responses.stop_replica_v0 import StopReplicaV0Response
from kafka.tools.protocol.responses.sync_group_v0 import SyncGroupV0Response
from kafka.tools.protocol.responses.sync_group_v1 import SyncGroupV1Response
from kafka.tools.protocol.responses.update_metadata_v0 import UpdateMetadataV0Response
from kafka.tools.protocol.responses.update_metadata_v1 import UpdateMetadataV1Response
from kafka.tools.protocol.responses.update_metadata_v2 import UpdateMetadataV2Response
from kafka.tools.protocol.responses.update_metadata_v3 import UpdateMetadataV3Response
from kafka.tools.protocol.responses.update_metadata_v4 import UpdateMetadataV4Response


class ValidateResponseSchemaTests(unittest.TestCase):
    def test_schema_validation(self):
        for klass in (
                AlterConfigsV0Response,
                AlterReplicaLogDirsV0Response,
                ApiVersionsV0Response,
                ApiVersionsV1Response,
                ControlledShutdownV0Response,
                ControlledShutdownV1Response,
                CreateAclsV0Response,
                CreatePartitionsV0Response,
                CreateTopicsV0Response,
                CreateTopicsV1Response,
                CreateTopicsV2Response,
                DeleteAclsV0Response,
                DeleteTopicsV0Response,
                DeleteTopicsV1Response,
                DeleteRecordsV0Response,
                DescribeAclsV0Response,
                DescribeConfigsV0Response,
                DescribeGroupsV0Response,
                DescribeGroupsV1Response,
                DescribeLogDirsV0Response,
                FetchV0Response,
                FetchV1Response,
                FetchV2Response,
                GroupCoordinatorV0Response,
                FindCoordinatorV1Response,
                HeartbeatV0Response,
                HeartbeatV1Response,
                JoinGroupV0Response,
                JoinGroupV1Response,
                JoinGroupV2Response,
                LeaderAndIsrV0Response,
                LeaderAndIsrV1Response,
                LeaveGroupV0Response,
                LeaveGroupV1Response,
                ListGroupsV0Response,
                ListGroupsV1Response,
                ListOffsetV0Response,
                ListOffsetV1Response,
                ListOffsetV2Response,
                MemberAssignmentV0,
                MetadataV0Response,
                MetadataV1Response,
                MetadataV2Response,
                MetadataV3Response,
                MetadataV4Response,
                MetadataV5Response,
                OffsetCommitV0Response,
                OffsetCommitV1Response,
                OffsetCommitV2Response,
                OffsetCommitV3Response,
                OffsetFetchV0Response,
                OffsetFetchV1Response,
                OffsetFetchV2Response,
                OffsetFetchV3Response,
                OffsetForLeaderEpochV0Response,
                ProduceV0Response,
                ProduceV1Response,
                ProduceV2Response,
                SaslAuthenticateV0Response,
                SaslHandshakeV0Response,
                SaslHandshakeV1Response,
                StopReplicaV0Response,
                SyncGroupV0Response,
                SyncGroupV1Response,
                UpdateMetadataV0Response,
                UpdateMetadataV1Response,
                UpdateMetadataV2Response,
                UpdateMetadataV3Response,
                UpdateMetadataV4Response,
        ):
            obj = klass({})
            try:
                validate_schema(obj.schema)
            except (KeyError, TypeError) as e:
                self.fail("Failed to validate schema for {0}: {1}".format(klass.__name__, e))
