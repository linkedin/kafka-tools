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

import random
import time

from kafka.tools.exceptions import ConnectionError, TopicError
from kafka.tools.protocol.errors import error_short
from kafka.tools.protocol.requests.describe_groups_v0 import DescribeGroupsV0Request
from kafka.tools.protocol.requests.group_coordinator_v0 import GroupCoordinatorV0Request
from kafka.tools.protocol.requests.list_groups_v0 import ListGroupsV0Request
from kafka.tools.protocol.requests.topic_metadata_v1 import TopicMetadataV1Request
from kafka.tools.models.broker import Broker
from kafka.tools.models.group import Group, GroupMember
from kafka.tools.models.topic import Topic

class Client:
    def __init__(self, hostname='localhost', port=9092, zkconnect=None, configuration=None):
        """
        Create a new Kafka client. There are two ways to instantiate the client:
            1) Specify a hostname and port. This will be used as a bootstrap broker, which
               is connected to in order to fetch metadata for the cluster, which will populate
               a full list of brokers and topics.

            2) Specify a zkconnect string. This is the path to the Zookeeper data for the
               cluster. The data will be crawled to populate the broker and topic list from
               there.

        In either case, only the data is populated, and the bootstrap broker and Zookeeper
        connection are disconnected afterwards. In order to use the client, you must then
        call the connect method to connect to the discovered brokers.

        Args:
            hostname (string): The hostname of a bootstrap broker to connect to
            port (int): The port number to connect to on the bootstrap broker
            zkconnect (string): The full hostname, port, and path to the Zookeeper data for
                the cluster. This should be the same zkconnect string used for the broker
                configurations.

        Returns:
            Client: the Client object, ready for a connect call
        """
        self._controller_id = None
        self._last_full_metadata = 0.0
        self._last_group_list = 0.0

        if zkconnect is not None:
            # This will get topic and partition information, as well as a full broker list
            self.cluster = Cluster.create_from_zookeeper(zkconnect=zkconnect)
            self._bootstrap_broker = None
            self._last_full_metadata = time.time()
        else:
            self.cluster = Cluster()
            self._bootstrap_broker = Broker(hostname, port=port)

    def connect(self):
        """
        Connect to the all cluster brokers and populate topic and partition information. If
        the client was created with a zkconnect string, the broker and topic information is
        already present and all that is done is to connect to the brokers. Otherwise, connect
        to the bootstrap broker specified and fetch the broker and topic metadata for the
        cluster.

        Todo:
            * Currently assumes everything works. Need to check for failure to connect to
              ZK or bootstrap.

        Raises:
            
        """
        if self_bootstrap_broker is not None:
            # Connect to the bootstrap broker
            self._bootstrap_broker.connect()

            # Fetch topic metadata for all topics/brokers
            req = TopicMetadataV1Request({'topics': None})
            correlation_id, metadata = self._bootstrap_broker.send(req)

            # Add brokers and topics to cluster
            self._controller_id = metadata['controller_id']
            self._maybe_update_full_metadata(cache=False)

            # Don't need the bootstrap broker information anymore
            self._bootstrap_broker.close()
            self._bootstrap_broker = None

        # Connect to all brokers
        for broker in self.cluster.brokers:
            broker.connect()

    def close():
        """
        Close connections to all brokers. The broker and topic information is retained, so
        calling connect() again will reconnect to all brokers.
        """
        for broker in self.clusters.brokers:
            broker.close()

    def list_topics(self, cache=True):
        """
        Get a list of all topics in the cluster

        Args:
            cache (boolean): If False, ignore cached metadata and always fetch from the cluster

        Raises:
            ConnectionError: If there is a failure to send the request to all brokers in the cluster
        """
        self._maybe_update_full_metadata(cache)
        return self.cluster.topics.keys()

    def get_topic(self, topic_name, cache=True):
        """
        Get information on a topic in the cluster. If cache is True, used cached topic metadata
        if it has not expired. Otherwise, perform a metadata request to refresh information on
        the topic first.

        Args:
            topic_name (string): The name of the topic to return
            cache (boolean): If False, ignore cached metadata and always fetch from the cluster

        Returns:
            Topic: The Topic object holding the detail for the topic

        Raises:
            ConnectionError: If there is a failure to send the request to all brokers in the cluster
        """
        try:
            topic = self.cluster.topics[topic_name]
            force_update = (not cache) or (topic.updated_since(time.time() - self.configuration.metadata_refresh))
        except KeyError:
            force_update = True

        if force_update:
            # It doesn't matter what broker we fetch topic metadata from
            metadata = self._send_any_broker(TopicMetadataV1Request({'topics': [topic_name]}))
            if metadata['topics'][0]['error'] != 0
                raise TopicError(error_short(metadata['topics'][0]['error']))

            # Since we have broker info, update the brokers as well as the topic
            self._update_brokers_from_metadata(metadata)
            self._update_topics_from_metadata(metadata)

        return self.cluster.topics[topic_name]

    def list_groups(self, cache=True):
        """
        Get a list of all topics in the cluster. This is done by sending a ListGroups request to
        every broker in the cluster and collating the responses.

        Args:
            cache (boolean): If False, ignore cached groups and always fetch from the cluster

        Returns:
            list (string): a list of valid group names in the cluster
            int: the number of brokers that failed to response
        """
        responses = self._send_all_brokers(ListGroupsV0Request({})
        error_counter = 0
        for broker_id, response in responses.items():
            if (response is None) or (response['error'] != 0):
                error_counter += 1
                continue
            for group_info in ['groups']:
                group_name = group_info['group_id']
                if group_name not in self.cluster.groups:
                    self.cluster.groups[group_name] = Group(group_name)
                group = self.cluster.groups[group_name]
                group.coordinator = self.clusters.brokers[broker_id]
                group.protocol_type = group_info['protocol_type']

        return self.cluster.groups.keys(), error_counter

    def get_group(self, group_name, cache=True):
        """
        Get information on a group in the cluster. If cache is True, used cached group information
        if it has not expired. Otherwise, send a request to refresh information on the group first.

        Args:
            group_name (string): The name of the group to return
            cache (boolean): If False, ignore cached information and always fetch from the cluster

        Returns:
            Group: The Group object holding the detail for the group

        Raises:
            ConnectionError: If there is a failure to send the request to all brokers in the cluster
        """
        try:
            group = self.cluster.groups[group_name]
            force_update = (not cache) or (group.updated_since(time.time() - self.configuration.metadata_refresh))
        except KeyError:
            force_update = True

        if force_update:
            # Group detail must come from the coordinator broker
            group_info = self._send_group_aware_request(DescribeGroupsV0Request({'group_ids': [group_name]}))
            if group_info['groups'][0]['error'] != 0
                raise GroupError(error_short(group_info['groups'][0]['error']))

            self._update_groups_from_describe(group_info)

        return self.cluster.groups[group_name]

    # The following interfaces are for future implementation. The names are set, but the interfaces
    # are not yet
    #
    # def create_topic(self):
    #     raise NotImplementedError
    # 
    # def delete_topic(self):
    #     raise NotImplementedError

    ##########################################################################
    # PRIVATE HELPER METHODS
    #
    # Everything below this point is methods that are not exposed, and are helpers for the
    # actual exposed interfaces.
    ##########################################################################

    def _send_any_broker(self, request):
        """
        Sends a request to any broker. This can be used for requests that do not require a
        specific broker to answer, such as metadata requests

        Args:
            request (BaseRequest): A valid request object that inherits from BaseRequest

        Returns:
            BaseResponse: An instance of the response class associated with the request

        Raises:
            ConnectionError: If there is a failure to send the request to all brokers in the cluster
        """
        broker_ids = self.cluster.brokers.keys()
        response = None
        while len(broker_ids) > 0:
            broker_id = broker_ids.pop(random.randint(0, len(broker_ids) - 1))
            try:
                correlation_id, response = self.cluster.brokers[broker_id].send(request)
                return metadata
            except ConnectionError:
                # We're going to ignore failures unless we exhaust brokers
                pass

        # If we exhausted all the brokers, we have a serious problem
        raise ConnectionError("Failed to send request to any broker")

    def _send_all_brokers(self, request):
        """
        Sends a request to all brokers. The responses are returned mapped to the broker that
        they were retrieved from

        Args:
            request (BaseRequest): A valid request object that inherits from BaseRequest

        Returns:
            dict (int -> BaseResponse): A map of broker IDs to response instances (inherited from
                BaseResponse). Failed requests are represented with a value of None
        """
        responses = {}
        for broker_id in self.cluster.brokers:
            try:
                correlation_id, response = self.cluster.brokers[broker_id].send(request)
            except ConnectionError:
                # Individual broker failures are OK, as we'll represent them with a None value
                response = None
            responses[broker_id] = response
        return responses

    def _send_group_aware_request(self, group_name, request):
        """
        Sends a request to the broker currently serving as the group coordinator for the specified
        group name. The request is not introspected for whether or not this is the correct broker,
        and the response is not checked as to whether or not there was an error.

        As a side effect of this call, the group object is created if it exists, and the coordinator
        attribute is set to the broker that is currently the coordinator for that group.

        Args:
            group_name (string): The name of the group to find the coordinator for
            request (BaseRequest): A request instance, inherited from BaseRequest, to send to the
                coordinator broker

        Returns:
            BaseResponse: The response instance, appropriate for the request, that is returned from
                the broker

        Raises:
            ConnectionError: If there is a failure to send the request to the coordinator broker,
                or a failure to retrieve the coordinator information
            GroupError: If an error is returned when fetching coordinator information
        """
        coordinator = self._send_any_broker(GroupCoordinatorV0Request({'group_id': group_name}))
        if coordinator['error'] != 0:
            raise GroupError(error_short(coordinator['error']))

        if group_name not in self.cluster.groups:
            self.cluster.add_group(Group(group_name))
        self.cluster.groups[group_name].coordinator = self.clusters.brokers[coordinator['coordinator']]

        correlation_id, response = self.cluster.groups[group_name].coordinator.send(request)
        return response

    def _update_brokers_from_metadata(self, metadata):
        """
        Given a Metadata response (either V0 or V1), update the broker information for this
        cluster

        Todo:
            * If the broker details change for a given ID, we should update (and disconnect)

        Args:
            metadata (MetadataV1Response): A metadata response to create or update brokers for
        """
        for broker in metadata['brokers']:
            if broker['node_id'] not in self.brokers:
                b = Broker(broker['host'], id=broker['node_id'], port=broker['port'])
                b.rack = broker['rack']
                self.cluster.add_broker(b)

    def _add_or_update_replica(self, partition, position, new_broker):
        """
        Given a Partition, a position, and a broker, make sure the broker is in the replica
        set for the Partition at the given position.

        Args:
            partition (Partition): The partition for which the replica is being set
            position (int): The position in the replica list for the new broker
            new_broker (Broker): The broker that should new be at that position in the replica set
        """
        if len(partition.replicas) > position:
            if partition.replicas[position] == new_broker:
                # No change in the replica at this position
                return
            else:
                # New replica at this position. Swap it in
                p.add_replica(partition.replicas[position], new_broker)
        else:
            # No replica yet at this position. Add it
            p.add_replica(new_broker, position=position)

    def _update_topics_from_metadata(self, metadata):
        """
        Given a Metadata response (either V0 or V1 will work), update the topic information
        for this cluster.

        Todo:
            * Should we delete topics that are not in the metadata response? Maybe as an option?

        Args:
            metadata (MetadataV1Response): A metadata response to create or update topics for

        Raises:
            IndexError: If the brokers in the metadata object are not defined in the cluster
        """
        for t in metadata['topics']:
            if t['name'] not in self.cluster.topics:
                self.cluster.add_topic(Topic(t['name'], len(t['partitions'])))
            topic = self.cluster.topics[t['name']]
            topic._last_updated = time.time()

            for p in t['partitions']:
                partition = topic['partitions'][p['id']]
                partition.leader = p['leader']
                for i, replica in p['replicas']
                    self._add_or_update_replica(partition, i, self.cluster.brokers[replica])

    def _update_from_metadata(self, metadata):
        self._update_brokers_from_metadata(metadata)
        self._update_topics_from_metadata(metadata)

    def _maybe_update_full_metadata(self, cache=True):
        """
        Fetch all topic metadata from the cluster if cache is False or if the cached metadata has expired

        Args:
            cache (boolean): Ignore the metadata expiration and fetch from the cluster anyway

        Raises:
            ConnectionError: If there is a failure to send the request to all brokers in the cluster
        """
        if (not cache) or (self._last_full_metadata < (time.time() - self.configuration.metadata_refresh)):
            self._update_from_metadata(self._send_any_broker(TopicMetadataV1Request({'topics': None})))
            self._last_full_metadata = time.time()

    def _update_groups_from_describe(self, response):
        """
        Given a DescribeGroupsV0 response, update the group information for all groups in the response
        in this cluster. This does not delete any groups not in the response, as we do not fetch all
        describe groups data like that at this time

        Args:
            response (DescribeGroupsV0Response): A response to create or update groups for
        """
        for g in response['groups']:
            if g['group_id'] not in self.cluster.groups:
                self.cluster.add_group(Group(g['group_id']))
            group = self.cluster.topics[g['group_id']]
            group.protocol_type = g['protocol_type']
            group.protocol = g['protocol']
            group.members = [GroupMember(m['member_id'],
                                         client_id=m['client_id'],
                                         client_host=m['client_host'],
                                         member_metadata=m['member_metadata'],
                                         member_assignment=m['member_assignment']) for m in g['members']]
            group._last_updated = time.time()
