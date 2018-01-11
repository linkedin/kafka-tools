import unittest
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests.alter_configs_v0 import AlterConfigsV0Request
from kafka.tools.protocol.requests.alter_replica_log_dirs_v0 import AlterReplicaLogDirsV0Request
from kafka.tools.protocol.requests.api_versions_v0 import ApiVersionsV0Request
from kafka.tools.protocol.requests.api_versions_v1 import ApiVersionsV1Request
from kafka.tools.protocol.requests.controlled_shutdown_v0 import ControlledShutdownV0Request
from kafka.tools.protocol.requests.controlled_shutdown_v1 import ControlledShutdownV1Request
from kafka.tools.protocol.requests.create_acls_v0 import CreateAclsV0Request
from kafka.tools.protocol.requests.create_partitions_v0 import CreatePartitionsV0Request
from kafka.tools.protocol.requests.create_topics_v0 import CreateTopicsV0Request
from kafka.tools.protocol.requests.create_topics_v1 import CreateTopicsV1Request
from kafka.tools.protocol.requests.create_topics_v2 import CreateTopicsV2Request
from kafka.tools.protocol.requests.delete_acls_v0 import DeleteAclsV0Request
from kafka.tools.protocol.requests.delete_records_v0 import DeleteRecordsV0Request
from kafka.tools.protocol.requests.delete_topics_v0 import DeleteTopicsV0Request
from kafka.tools.protocol.requests.delete_topics_v1 import DeleteTopicsV1Request
from kafka.tools.protocol.requests.describe_acls_v0 import DescribeAclsV0Request
from kafka.tools.protocol.requests.describe_configs_v0 import DescribeConfigsV0Request
from kafka.tools.protocol.requests.describe_groups_v0 import DescribeGroupsV0Request
from kafka.tools.protocol.requests.describe_groups_v1 import DescribeGroupsV1Request
from kafka.tools.protocol.requests.describe_log_dirs_v0 import DescribeLogDirsV0Request
from kafka.tools.protocol.requests.find_coordinator_v1 import FindCoordinatorV1Request
from kafka.tools.protocol.requests.group_coordinator_v0 import GroupCoordinatorV0Request
from kafka.tools.protocol.requests.heartbeat_v0 import HeartbeatV0Request
from kafka.tools.protocol.requests.join_group_v0 import JoinGroupV0Request
from kafka.tools.protocol.requests.join_group_v1 import JoinGroupV1Request
from kafka.tools.protocol.requests.join_group_v2 import JoinGroupV2Request
from kafka.tools.protocol.requests.leader_and_isr_v0 import LeaderAndIsrV0Request
from kafka.tools.protocol.requests.leader_and_isr_v1 import LeaderAndIsrV1Request
from kafka.tools.protocol.requests.leave_group_v0 import LeaveGroupV0Request
from kafka.tools.protocol.requests.leave_group_v1 import LeaveGroupV1Request
from kafka.tools.protocol.requests.list_groups_v0 import ListGroupsV0Request
from kafka.tools.protocol.requests.list_groups_v1 import ListGroupsV1Request
from kafka.tools.protocol.requests.list_offset_v0 import ListOffsetV0Request
from kafka.tools.protocol.requests.list_offset_v1 import ListOffsetV1Request
from kafka.tools.protocol.requests.list_offset_v2 import ListOffsetV2Request
from kafka.tools.protocol.requests.offset_commit_v0 import OffsetCommitV0Request
from kafka.tools.protocol.requests.offset_commit_v1 import OffsetCommitV1Request
from kafka.tools.protocol.requests.offset_commit_v2 import OffsetCommitV2Request
from kafka.tools.protocol.requests.offset_commit_v3 import OffsetCommitV3Request
from kafka.tools.protocol.requests.offset_fetch_v0 import OffsetFetchV0Request
from kafka.tools.protocol.requests.offset_fetch_v1 import OffsetFetchV1Request
from kafka.tools.protocol.requests.offset_fetch_v2 import OffsetFetchV2Request
from kafka.tools.protocol.requests.offset_fetch_v3 import OffsetFetchV3Request
from kafka.tools.protocol.requests.offsets_for_leader_epoch_v0 import OffsetForLeaderEpochV0Request
from kafka.tools.protocol.requests.sasl_handshake_v0 import SaslHandshakeV0Request
from kafka.tools.protocol.requests.sasl_handshake_v1 import SaslHandshakeV1Request
from kafka.tools.protocol.requests.sasl_authenticate_v0 import SaslAuthenticateV0Request
from kafka.tools.protocol.requests.stop_replica_v0 import StopReplicaV0Request
from kafka.tools.protocol.requests.sync_group_v0 import SyncGroupV0Request
from kafka.tools.protocol.requests.sync_group_v1 import SyncGroupV1Request
from kafka.tools.protocol.requests.topic_metadata_v0 import TopicMetadataV0Request
from kafka.tools.protocol.requests.topic_metadata_v1 import TopicMetadataV1Request
from kafka.tools.protocol.requests.topic_metadata_v2 import TopicMetadataV2Request
from kafka.tools.protocol.requests.topic_metadata_v3 import TopicMetadataV3Request
from kafka.tools.protocol.requests.topic_metadata_v4 import TopicMetadataV4Request
from kafka.tools.protocol.requests.topic_metadata_v5 import TopicMetadataV5Request
from kafka.tools.protocol.requests.update_metadata_v0 import UpdateMetadataV0Request
from kafka.tools.protocol.requests.update_metadata_v1 import UpdateMetadataV1Request
from kafka.tools.protocol.requests.update_metadata_v2 import UpdateMetadataV2Request
from kafka.tools.protocol.requests.update_metadata_v3 import UpdateMetadataV3Request
from kafka.tools.protocol.requests.update_metadata_v4 import UpdateMetadataV4Request


class ValidateResponseSchemaTests(unittest.TestCase):
    def test_schema_validation(self):
        for klass in (
            AlterConfigsV0Request,
            AlterReplicaLogDirsV0Request,
            ApiVersionsV0Request,
            ApiVersionsV1Request,
            ControlledShutdownV0Request,
            ControlledShutdownV1Request,
            CreateAclsV0Request,
            CreatePartitionsV0Request,
            CreateTopicsV0Request,
            CreateTopicsV1Request,
            CreateTopicsV2Request,
            DeleteAclsV0Request,
            DeleteRecordsV0Request,
            DeleteTopicsV0Request,
            DeleteTopicsV1Request,
            DescribeAclsV0Request,
            DescribeConfigsV0Request,
            DescribeGroupsV0Request,
            DescribeGroupsV1Request,
            DescribeLogDirsV0Request,
            FindCoordinatorV1Request,
            GroupCoordinatorV0Request,
            HeartbeatV0Request,
            JoinGroupV0Request,
            JoinGroupV1Request,
            JoinGroupV2Request,
            LeaderAndIsrV0Request,
            LeaderAndIsrV1Request,
            LeaveGroupV0Request,
            LeaveGroupV1Request,
            ListGroupsV0Request,
            ListGroupsV1Request,
            ListOffsetV0Request,
            ListOffsetV1Request,
            ListOffsetV2Request,
            OffsetCommitV0Request,
            OffsetCommitV1Request,
            OffsetCommitV2Request,
            OffsetCommitV3Request,
            OffsetFetchV0Request,
            OffsetFetchV1Request,
            OffsetFetchV2Request,
            OffsetFetchV3Request,
            OffsetForLeaderEpochV0Request,
            SaslHandshakeV0Request,
            SaslHandshakeV1Request,
            SaslAuthenticateV0Request,
            StopReplicaV0Request,
            SyncGroupV0Request,
            SyncGroupV1Request,
            TopicMetadataV0Request,
            TopicMetadataV1Request,
            TopicMetadataV2Request,
            TopicMetadataV3Request,
            TopicMetadataV4Request,
            TopicMetadataV5Request,
            UpdateMetadataV0Request,
            UpdateMetadataV1Request,
            UpdateMetadataV2Request,
            UpdateMetadataV3Request,
            UpdateMetadataV4Request,
        ):
            try:
                validate_schema(klass.schema)
            except (KeyError, TypeError) as e:
                self.fail("Failed to validate schema for {0}: {1}".format(klass.__name__, e))
