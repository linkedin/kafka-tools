# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


errors = {
    -1: {'short': 'UNKNOWN',
         'long': "The server experienced an unexpected error when processing the request"},
    0: {'short': 'NONE',
        'long': ""},
    1: {'short': 'OFFSET_OUT_OF_RANGE',
        'long': "The requested offset is not within the range of offsets maintained by the server."},
    2: {'short': 'CORRUPT_MESSAGE',
        'long': "This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt."},
    3: {'short': 'UNKNOWN_TOPIC_OR_PARTITION',
        'long': "This server does not host this topic-partition."},
    4: {'short': 'INVALID_FETCH_SIZE',
        'long': "The requested fetch size is invalid."},
    5: {'short': 'LEADER_NOT_AVAILABLE',
        'long': "There is no leader for this topic-partition as we are in the middle of a leadership election."},
    6: {'short': 'NOT_LEADER_FOR_PARTITION',
        'long': "This server is not the leader for that topic-partition."},
    7: {'short': 'REQUEST_TIMED_OUT',
        'long': "The request timed out."},
    8: {'short': 'BROKER_NOT_AVAILABLE',
        'long': "The broker is not available."},
    9: {'short': 'REPLICA_NOT_AVAILABLE',
        'long': "The replica is not available for the requested topic-partition"},
    10: {'short': 'MESSAGE_TOO_LARGE',
         'long': "The request included a message larger than the max message size the server will accept."},
    11: {'short': 'STALE_CONTROLLER_EPOCH',
         'long': "The controller moved to another broker."},
    12: {'short': 'OFFSET_METADATA_TOO_LARGE',
         'long': "The metadata field of the offset request was too large."},
    13: {'short': 'NETWORK_EXCEPTION',
         'long': "The server disconnected before a response was received."},
    14: {'short': 'GROUP_LOAD_IN_PROGRESS',
         'long': "The coordinator is loading and hence can't process requests for this group."},
    15: {'short': 'GROUP_COORDINATOR_NOT_AVAILABLE',
         'long': "The group coordinator is not available."},
    16: {'short': 'NOT_COORDINATOR_FOR_GROUP',
         'long': "This is not the correct coordinator for this group."},
    17: {'short': 'INVALID_TOPIC_EXCEPTION',
         'long': "The request attempted to perform an operation on an invalid topic."},
    18: {'short': 'RECORD_LIST_TOO_LARGE',
         'long': "The request included message batch larger than the configured segment size on the server."},
    19: {'short': 'NOT_ENOUGH_REPLICAS',
         'long': "Messages are rejected since there are fewer in-sync replicas than required."},
    20: {'short': 'NOT_ENOUGH_REPLICAS_AFTER_APPEND',
         'long': "Messages are written to the log, but to fewer in-sync replicas than required."},
    21: {'short': 'INVALID_REQUIRED_ACKS',
         'long': "Produce request specified an invalid value for required acks."},
    22: {'short': 'ILLEGAL_GENERATION',
         'long': "Specified group generation id is not valid."},
    23: {'short': 'INCONSISTENT_GROUP_PROTOCOL',
         'long': "The group member's supported protocols are incompatible with those of existing members."},
    24: {'short': 'INVALID_GROUP_ID',
         'long': "The configured groupId is invalid"},
    25: {'short': 'UNKNOWN_MEMBER_ID',
         'long': "The coordinator is not aware of this member."},
    26: {'short': 'INVALID_SESSION_TIMEOUT',
         'long': "The session timeout is not within the range allowed by the broker " +
                 "(as configured by group.min.session.timeout.ms and group.max.session.timeout.ms)."},
    27: {'short': 'REBALANCE_IN_PROGRESS',
         'long': "The group is rebalancing, so a rejoin is needed."},
    28: {'short': 'INVALID_COMMIT_OFFSET_SIZE',
         'long': "The committing offset data size is not valid"},
    29: {'short': 'TOPIC_AUTHORIZATION_FAILED',
         'long': "Topic authorization failed."},
    30: {'short': 'GROUP_AUTHORIZATION_FAILED',
         'long': "Group authorization failed."},
    31: {'short': 'CLUSTER_AUTHORIZATION_FAILED',
         'long': "Cluster authorization failed."},
    32: {'short': 'INVALID_TIMESTAMP',
         'long': "The timestamp of the message is out of acceptable range."},
    33: {'short': 'UNSUPPORTED_SASL_MECHANISM',
         'long': "The broker does not support the requested SASL mechanism."},
    34: {'short': 'ILLEGAL_SASL_STATE',
         'long': "Request is not valid given the current SASL state."},
    35: {'short': 'UNSUPPORTED_VERSION',
         'long': "The version of API is not supported."},
    36: {'short': 'TOPIC_ALREADY_EXISTS',
         'long': "Topic with this name already exists."},
    37: {'short': 'INVALID_PARTITIONS',
         'long': "Number of partitions is invalid."},
    38: {'short': 'INVALID_REPLICATION_FACTOR',
         'long': "Replication-factor is invalid."},
    39: {'short': 'INVALID_REPLICA_ASSIGNMENT',
         'long': "Replica assignment is invalid."},
    40: {'short': 'INVALID_CONFIG',
         'long': "Configuration is invalid."},
    41: {'short': 'NOT_CONTROLLER',
         'long': "This is not the correct controller for this cluster."},
    42: {'short': 'INVALID_REQUEST',
         'long': "This most likely occurs because of a request being malformed by the client library or" +
                 " the message was sent to an incompatible broker. See the broker logs for more details."},
    43: {'short': 'UNSUPPORTED_FOR_MESSAGE_FORMAT',
         'long': "The message format version on the broker does not support the request."},
    44: {'short': 'POLICY_VIOLATION',
         'long': "Request parameters do not satisfy the configured policy."},
    45: {'short': 'OUT_OF_ORDER_SEQUENCE_NUMBER',
         'long': "The broker received an out of order sequence number"},
    46: {'short': 'DUPLICATE_SEQUENCE_NUMBER',
         'long': "The broker received a duplicate sequence number"},
    47: {'short': 'INVALID_PRODUCER_EPOCH',
         'long': "Producer attempted an operation with an old epoch. Either there is a newer producer " +
                 "with the same transactionalId, or the producer's transaction has been expired by the broker."},
    48: {'short': 'INVALID_TXN_STATE',
         'long': "The producer attempted a transactional operation in an invalid state"},
    49: {'short': 'INVALID_PRODUCER_ID_MAPPING',
         'long': "The producer attempted to use a producer id which is not currently assigned to its transactional id"},
    50: {'short': 'INVALID_TRANSACTION_TIMEOUT',
         'long': "The transaction timeout is larger than the maximum value allowed by " +
                 "the broker (as configured by max.transaction.timeout.ms)."},
    51: {'short': 'CONCURRENT_TRANSACTIONS',
         'long': "The producer attempted to update a transaction " +
                 "while another concurrent operation on the same transaction was ongoing"},
    52: {'short': 'TRANSACTION_COORDINATOR_FENCED',
         'long': "Indicates that the transaction coordinator sending a WriteTxnMarker " +
                 "is no longer the current coordinator for a given producer"},
    53: {'short': 'TRANSACTIONAL_ID_AUTHORIZATION_FAILED',
         'long': "Transactional Id authorization failed"},
    54: {'short': 'SECURITY_DISABLED',
         'long': "Security features are disabled."},
    55: {'short': 'OPERATION_NOT_ATTEMPTED',
         'long': "The broker did not attempt to execute this operation. This may happen for batched RPCs " +
                 "where some operations in the batch failed, causing the broker to respond without trying the rest."},
    56: {'short': 'KAFKA_STORAGE_ERROR',
         'long': "Disk error when trying to access log file on the disk."},
    57: {'short': 'LOG_DIR_NOT_FOUND',
         'long': "The user-specified log directory is not found in the broker config."},
    58: {'short': 'SASL_AUTHENTICATION_FAILED',
         'long': "SASL Authentication failed."},
    59: {'short': 'UNKNOWN_PRODUCER_ID',
         'long': "This exception is raised by the broker if it could not locate the producer metadata " +
                 "associated with the producerId in question. This could happen if, for instance, the producer's records " +
                 "were deleted because their retention time had elapsed. Once the last records of the producerId are " +
                 "removed, the producer's metadata is removed from the broker, and future appends by the producer will " +
                 "return this exception."},
    60: {'short': 'REASSIGNMENT_IN_PROGRESS',
         'long': "A partition reassignment is in progress"},
}


def error_short(err_num):
    if err_num in errors:
        return errors[err_num]['short']
    else:
        return "NOSUCHERROR"


def error_long(err_num):
    if err_num in errors:
        return errors[err_num]['long']
    else:
        return "This is an unknown error code"
