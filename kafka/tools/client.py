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

from multiprocessing.pool import ThreadPool
from random import shuffle
from threading import RLock
import collections
import six
import time

from kafka.tools.configuration import ClientConfiguration
from kafka.tools.exceptions import ConnectionError, GroupError, TopicError, ConfigurationError
from kafka.tools.protocol.requests.describe_groups_v0 import DescribeGroupsV0Request
from kafka.tools.protocol.requests.group_coordinator_v0 import GroupCoordinatorV0Request
from kafka.tools.protocol.requests.list_groups_v0 import ListGroupsV0Request
from kafka.tools.protocol.requests.list_offset_v0 import ListOffsetV0Request
from kafka.tools.protocol.requests.offset_commit_v2 import OffsetCommitV2Request
from kafka.tools.protocol.requests.offset_fetch_v1 import OffsetFetchV1Request
from kafka.tools.protocol.requests.topic_metadata_v1 import TopicMetadataV1Request
from kafka.tools.models.broker import Broker
from kafka.tools.models.cluster import Cluster
from kafka.tools.models.group import Group
from kafka.tools.models.topic import Topic, TopicOffsets
from kafka.tools.utilities import synchronized, raise_if_error


class Client:
    # Special timestamps for offset requests
    OFFSET_EARLIEST = -2
    OFFSET_LATEST = -1

    def __init__(self, **kwargs):
        """
        Create a new Kafka client. There are two ways to instantiate the client:
            1) Provide a ClientConfiguration object with configuration=obj. This specifies all the configuration options
               for the client, or uses the defaults in ClientConfiguration. If the configuration keyword argument is
               present, all other arguments are ignored

            2) Provide keyword arguments that will be passed to create a new ClientConfiguration object. If the
               configuration keyword argument is not provided, this is the option that is chosen.

        The client will not be connected, either using the broker_list or the zkconnect option, until the connect()
        method is called

        Args:
            configuration (ClientConfiguration): a ClientConfiguration object to specify client settings
            kwargs: keyword arguments that are passed to create a new ClientConfiguration object

        Returns:
            Client: the Client object, ready for a connect call
        """
        if 'configuration' in kwargs:
            if not isinstance(kwargs['configuration'], ClientConfiguration):
                raise ConfigurationError("configuration object is not an instance of ClientConfiguration")
            self.configuration = kwargs['configuration']
        else:
            self.configuration = ClientConfiguration(**kwargs)

        self._controller_id = None
        self._last_full_metadata = 0.0
        self._last_group_list = 0.0
        self.cluster = Cluster()
        self._connected = False
        self._lock = RLock()

    @synchronized
    def connect(self):
        """
        Connect to the all cluster brokers and populate topic and partition information. If the client was created with
        a zkconnect string, first we connect to Zookeeper to bootstrap the broker and topic information from there,
        and then the client connects to all brokers in the cluster. Otherwise, connect to the bootstrap broker
        specified and fetch the broker and topic metadata for the cluster.

        Todo:
            * Currently assumes everything works. Need to check for failure to connect to
              ZK or bootstrap.
        """
        if self.configuration.zkconnect is not None:
            self.cluster = Cluster.create_from_zookeeper(zkconnect=self.configuration.zkconnect, fetch_topics=False)
            self._connected = True
        else:
            # Connect to bootstrap brokers until we succeed or exhaust the list
            try_brokers = list(self.configuration.broker_list)
            self._connected = False
            while (len(try_brokers) > 0) and not self._connected:
                self._connected = self._maybe_bootstrap_cluster(try_brokers.pop())

            if not self._connected:
                raise ConnectionError("Unable to bootstrap cluster information")

        # Connect to all brokers
        self._connect_all_brokers()

    @synchronized
    def close(self):
        """
        Close connections to all brokers. The configuration information is retained, so calling connect() again will
        reconnect to all brokers.
        """
        for broker_id in self.cluster.brokers:
            self.cluster.brokers[broker_id].close()
        self._connected = False

    @synchronized
    def list_topics(self, cache=True):
        """
        Get a list of all topics in the cluster

        Args:
            cache (boolean): If False, ignore cached metadata and always fetch from the cluster

        Raises:
            ConnectionError: If there is a failure to send the request to all brokers in the cluster
        """
        self._raise_if_not_connected()
        self._maybe_update_full_metadata(cache)
        return list(self.cluster.topics.keys())

    @synchronized
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
            TopicError: If the topic does not exist, or there is a problem getting metadata for it
        """
        self._raise_if_not_connected()
        try:
            topic = self.cluster.topics[topic_name]
            force_update = (not cache) or (not topic.updated_since(time.time() - self.configuration.metadata_refresh))
        except KeyError:
            force_update = True

        if force_update:
            # It doesn't matter what broker we fetch topic metadata from
            metadata = self._send_any_broker(TopicMetadataV1Request({'topics': [topic_name]}))
            raise_if_error(TopicError, metadata['topics'][0]['error'])

            self._update_from_metadata(metadata)

        return self.cluster.topics[topic_name]

    @synchronized
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
        self._raise_if_not_connected()
        error_counter = self._maybe_update_groups_list(cache)
        return list(self.cluster.groups.keys()), error_counter

    @synchronized
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
            GroupError: If the group does not exist or there is a problem fetching information for it
        """
        self._raise_if_not_connected()
        try:
            group = self.cluster.groups[group_name]
            force_update = (not cache) or (not group.updated_since(time.time() - self.configuration.metadata_refresh))
        except KeyError:
            force_update = True

        if force_update:
            # Group detail must come from the coordinator broker
            group_info = self._send_group_aware_request(group_name, DescribeGroupsV0Request({'group_ids': [group_name]}))
            raise_if_error(GroupError, group_info['groups'][0]['error'])

            self._update_groups_from_describe(group_info)

        return self.cluster.groups[group_name]

    @synchronized
    def get_offsets_for_topic(self, topic_name, timestamp=OFFSET_LATEST):
        """
        Get the offsets for all the partitions in the specified topic. The offsets are requested at the specified
        timestamp, which defaults to the latest offsets available (defined as the offset of the next message to be
        produced).

        Args:
            topic_name (string or list): The name of the topic to fetch offsets for. For multiple topics, a list of
                topics can be provided
            timestamp (int): The timestamp (in millis) to fetch offsets at. If not specified, the default is the special
                timestamp OFFSET_LATEST, which requests the current end of the partitions (tail). You can also specify
                the special offset OFFSET_EARLIEST, which requests the oldest offset for each partition (head)

        Return:
            TopicOffsets: A TopicOffsets instance that contain offsets for all the partitions in the topic.

        Raises:
            ConnectionError: If there is a failure to send the request to a broker
            TopicError: If the topic does not exist or there is a problem getting information for it
            OffsetError: If there is a failure retrieving offsets for the specified topic or timestamp
            TypeError: If the timestamp is not an integer
        """
        return self.get_offsets_for_topics([topic_name], timestamp)[topic_name]

    @synchronized
    def get_offsets_for_topics(self, topic_list, timestamp=OFFSET_LATEST):
        """
        Get the offsets for all the partitions in the specified topics. The offsets are requested at the specified
        timestamp, which defaults to the latest offsets available (defined as the offset of the next message to be
        produced).

        Args:
            topic_list (list): a list of topic name strings to fetch offsets for.
            timestamp (int): The timestamp (in millis) to fetch offsets at. If not specified, the default is the special
                timestamp OFFSET_LATEST, which requests the current end of the partitions (tail). You can also specify
                the special offset OFFSET_EARLIEST, which requests the oldest offset for each partition (head)

        Return:
            dict (string -> TopicOffsets): A dictionary mapping topic names to TopicOffsets instances that contain
                offsets for all the partitions in the topic.

        Raises:
            ConnectionError: If there is a failure to send the request to a broker
            TopicError: If a topic does not exist or there is a problem getting information for it
            OffsetError: If there is a failure retrieving offsets for the specified topic or timestamp
            TypeError: If the timestamp is not an integer
        """
        self._raise_if_not_connected()
        if not isinstance(timestamp, six.integer_types):
            raise TypeError("timestamp must be a valid integer")

        # Get the topic information, making sure all the leadership info is current
        self._maybe_update_metadata_for_topics(topic_list)

        # Get a broker to topic-partition mapping
        broker_to_tp = self._map_topic_partitions_to_brokers(topic_list)

        request_values = {}
        for broker_id in broker_to_tp:
            request_values[broker_id] = {'replica_id': -1, 'topics': []}
            for topic_name in broker_to_tp[broker_id]:
                request_values[broker_id]['topics'].append({'topic': topic_name,
                                                            'partitions': [{'partition': i,
                                                                            'timestamp': timestamp,
                                                                            'max_num_offsets': 1} for i in broker_to_tp[broker_id][topic_name]]})

        return self._send_list_offsets_to_brokers(request_values)

    @synchronized
    def get_offsets_for_group(self, group_name, topic_list=None):
        """
        Get the latest offsets committed by the specified group. If a topic_name is specified, only the offsets for that
        topic are returned. Otherwise, offsets for all topics that the group is subscribed to are returned.

        Args:
            group_name (string): The name of the group to fetch offsets for
            topic_list (list): A list of string topic names to fetch offsets for. Defaults to None, which specifies all
                topics that are subscribed to by the group.

        Return:
            dict (string -> TopicOffsets): A dictionary mapping topic names to TopicOffsets instances that contain
                offsets for all the partitions in the topic. The dictionary will contain a single key if the topic_name
                provided is not None.

        Raises:
            ConnectionError: If there is a failure to send requests to brokers
            TopicError: If the topic does not exist or there is a problem getting information for it
            GroupError: If there is a failure to get information for the specified group
            OffsetError: If there is a failure retrieving offsets for the topic(s)
        """
        self._raise_if_not_connected()

        # Get the group we're fetching offsets for (potentially updating the group information)
        group = self.get_group(group_name)
        fetch_topics = self._get_topics_for_group(group, topic_list)

        # Get the topic information, making sure all the leadership info is current
        self._maybe_update_metadata_for_topics(fetch_topics)

        request_values = {'group_id': group_name,
                          'topics': [{'topic': topic, 'partitions': list(range(len(self.cluster.topics[topic].partitions)))} for topic in fetch_topics]}
        response = self._send_group_aware_request(group_name, OffsetFetchV1Request(request_values))

        rv = {}
        for topic in response['responses']:
            topic_name = topic['topic']
            rv[topic_name] = TopicOffsets(self.cluster.topics[topic_name])
            rv[topic_name].set_offsets_from_fetch(topic['partition_responses'])

        return rv

    @synchronized
    def set_offsets_for_group(self, group_name, topic_offsets):
        """
        Given a group name and a list of topics and offsets, write the offsets as the latest for the group. This can only
        be done if the consumer is an old consumer (committing to Kafka), or if it is a new consumer in the "Empty"
        state (i.e. not running)

        Args:
            group_name (string): the name of the consumer group to set offsets for
            topic_offsets (list): a list of TopicOffsets objects which describe the topics and offsets to set

        Returns:
            dict (string -> list): a map of topic names to a list of error code responses for each partition. Error code
                0 indicates no error.

        Raises:
            ConnectionError: If there is a failure to send requests to brokers
            TypeError: if the topic_offsets argument is not properly formatted
            GroupError: If the group is not in the "Empty" state (if it is a new consumer), or if the group coordinator
                is unavailable
        """
        self._raise_if_not_connected()
        if isinstance(topic_offsets, six.string_types) or (not isinstance(topic_offsets, collections.Sequence)):
            raise TypeError("topic_offsets argument is not a list")

        # Get the group we're setting offsets for (potentially updating the group information)
        group = self.get_group(group_name)
        if group.state not in (None, 'Empty', 'Dead'):
            raise GroupError("The consumer group must be in the 'Empty' or 'Dead' state to set offsets, not '{0}'".format(group.state))

        # Get the topic information, making sure all the leadership info is current
        fetch_topics = [offsets.topic.name for offsets in topic_offsets]
        self._maybe_update_metadata_for_topics(fetch_topics)

        response = self._send_set_offset_request(group_name, topic_offsets)
        return self._parse_set_offset_response(response)

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

    def _raise_if_not_connected(self):
        if not self._connected:
            raise ConnectionError("The client is not yet connected")

    def _maybe_bootstrap_cluster(self, broker_port):
        """Attempt to bootstrap the cluster information using the given broker"""
        broker = Broker(broker_port[0], port=broker_port[1], configuration=self.configuration)

        try:
            broker.connect()
        except ConnectionError:
            # Just skip to the next bootstrap broker
            return False

        # Fetch topic metadata for all topics/brokers
        req = TopicMetadataV1Request({'topics': None})
        correlation_id, metadata = broker.send(req)
        broker.close()

        # Add brokers and topics to cluster
        self._controller_id = metadata['controller_id']
        self._update_from_metadata(metadata)
        self._last_full_metadata = time.time()
        return True

    def _connect_all_brokers(self):
        pool = ThreadPool(processes=self.configuration.broker_threads)
        for broker_id in self.cluster.brokers:
            pool.apply_async(self.cluster.brokers[broker_id].connect)
        pool.close()
        pool.join()

    def _send_to_broker(self, broker_id, request):
        """
        Given a broker ID and a request, send the request to that broker and return the response

        Args:
            broker_id (int): The ID of a broker in the cluster
            request (BaseRequest): A valid request object that inherits from BaseRequest

        Returns:
            BaseResponse: An instance of the response class associated with the request

        Raises:
            ConnectionError: If there is a failure to send the request to all brokers in the cluster
        """
        correlation_id, response = self.cluster.brokers[broker_id].send(request)
        return response

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
        broker_ids = list(self.cluster.brokers.keys())
        shuffle(broker_ids)
        while len(broker_ids) > 0:
            broker_id = broker_ids.pop()
            try:
                if self.cluster.brokers[broker_id].hostname is not None:
                    return self._send_to_broker(broker_id, request)
            except ConnectionError:
                # We're going to ignore failures unless we exhaust brokers
                pass

        # If we exhausted all the brokers, we have a serious problem
        raise ConnectionError("Failed to send request to any broker")

    def _send_some_brokers(self, requests, ignore_errors=True):
        """
        Sends a request to one or more brokers. The responses are returned mapped to the broker that
        they were retrieved from. This method uses a thread pool to parallelize sends.

        Args:
            request (int -> BaseRequest): A dictionary, where keys are integer broker IDs and the values are valid
                request objects that inherit from BaseRequest.

        Returns:
            dict (int -> BaseResponse): A map of broker IDs to response instances (inherited from
                BaseResponse). Failed requests are represented with a value of None
        """
        results = {}
        pool = ThreadPool(processes=self.configuration.broker_threads)
        for broker_id in requests:
            results[broker_id] = pool.apply_async(self._send_to_broker, (broker_id, requests[broker_id]))
        pool.close()
        pool.join()

        responses = {}
        for broker_id in results:
            try:
                responses[broker_id] = results[broker_id].get()
            except ConnectionError:
                if ignore_errors:
                    # Individual broker failures are OK, as we'll represent them with a None value
                    responses[broker_id] = None
                else:
                    raise
        return responses

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
        requests = {}
        for broker_id in self.cluster.brokers:
            if self.cluster.brokers[broker_id].hostname is not None:
                requests[broker_id] = request
        return self._send_some_brokers(requests)

    def _make_broker(self, broker_dict):
        return Broker(broker_dict['host'], id=broker_dict['node_id'], port=broker_dict['port'], configuration=self.configuration)

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
        response = self._send_any_broker(GroupCoordinatorV0Request({'group_id': group_name}))
        raise_if_error(GroupError, response['error'])

        if group_name not in self.cluster.groups:
            self.cluster.add_group(Group(group_name))
        try:
            self.cluster.groups[group_name].coordinator = self.cluster.brokers[response['node_id']]
        except KeyError:
            broker = self._make_broker(response)
            self.cluster.add_broker(broker)
            self.cluster.groups[group_name].coordinator = broker

        response = self._send_to_broker(self.cluster.groups[group_name].coordinator.id, request)
        return response

    def _send_list_offsets_to_brokers(self, request_values):
        """
        Given a mapping of broker IDs to values for ListOffset requests, send the requests to all the brokers and
        collate the responses into a mapping of topic names to TopicOffsets instances

        Args:
            request_values (dict): a mapping of broker ID (int) to value dictionaries for ListOffsets requests

        Returns:
            dict (string -> TopicOffsets): A dictionary mapping topic names to TopicOffsets instances that contain
                offsets for all the partitions requested.

        Raises:
            ConnectionError: If there is a failure to send the request to a broker
            OffsetError: If there is a failure retrieving offsets
        """
        requests = {}
        for broker_id in request_values:
            requests[broker_id] = ListOffsetV0Request(request_values[broker_id])
        responses = self._send_some_brokers(requests, ignore_errors=False)

        rv = {}
        for broker_id in responses:
            response = responses[broker_id]
            for topic in response['responses']:
                topic_name = topic['topic']
                if topic_name not in rv:
                    rv[topic_name] = TopicOffsets(self.cluster.topics[topic_name])
                rv[topic_name].set_offsets_from_list(topic['partition_responses'])

        return rv

    def _send_set_offset_request(self, group_name, topic_offsets):
        request = {'group_id': group_name,
                   'group_generation_id': -1,
                   'member_id': '',
                   'retention_time': -1,
                   'topics': []}
        for offset in topic_offsets:
            if not (isinstance(offset, TopicOffsets) and isinstance(offset.topic, Topic)):
                raise TypeError("TopicOffsets objects are not properly formed")

            request['topics'].append({'topic': offset.topic.name,
                                      'partitions': [{'partition': p_num,
                                                      'offset': p_offset,
                                                      'metadata': None} for p_num, p_offset in enumerate(offset.partitions)]})

        return self._send_group_aware_request(group_name, OffsetCommitV2Request(request))

    def _parse_set_offset_response(self, response):
        rv = {}
        for topic in response['responses']:
            topic_name = topic['topic']
            rv[topic_name] = [-1] * len(topic['partition_responses'])
            for partition in topic['partition_responses']:
                rv[topic_name][partition['partition']] = partition['error']

        return rv

    def _update_brokers_from_metadata(self, metadata):
        """
        Given a Metadata response (either V0 or V1), update the broker information for this
        cluster. We don't delete brokers because we don't know if the brokers is gone temporarily
        (crashed or maintenance) or permanently.

        Args:
            metadata (MetadataV1Response): A metadata response to create or update brokers for
        """
        for b in metadata['brokers']:
            try:
                broker = self.cluster.brokers[b['node_id']]
                if (broker.hostname != b['host']) or (broker.port != b['port']):
                    # if the hostname or port changes, close the existing connection
                    broker.close()
                    broker.hostname = b['host']
                    broker.port = b['port']
            except KeyError:
                broker = self._make_broker(b)
                self.cluster.add_broker(broker)
            broker.rack = b['rack']

    def _maybe_delete_topics_not_in_metadata(self, metadata, delete):
        """
        If delete is True, check each topic in the cluster and delete it if it is not also in the metadata
        response provided. This should only be used when the metadata response has a full list of all topics
        in the cluster.

        Args:
            metadata (MetadataV1Response): a metadata response that contains the full list of topics in the
                cluster
            delete (boolean): If False, no action is taken
        """
        if not delete:
            return

        topic_list = metadata.topic_names()
        topics_for_deletion = []
        for topic_name in self.cluster.topics:
            if topic_name in topic_list:
                continue

            self.cluster.topics[topic_name].assure_has_partitions(0)
            topics_for_deletion.append(topic_name)

        for topic_name in topics_for_deletion:
            del self.cluster.topics[topic_name]

    def _update_or_add_partition(self, partition_metadata, partition):
        partition.leader = self.cluster.brokers[partition_metadata['leader']]
        for i, replica in enumerate(partition_metadata['replicas']):
            if replica not in self.cluster.brokers:
                # We have a replica ID that is not a known broker. This can happen if a broker is offline, or
                # if the partition is otherwise assigned to a non-existent broker ID. In this case, we need to
                # create a broker object for this with no endpoint information as a placeholder.
                self.cluster.add_broker(Broker(hostname=None, id=replica))
            partition.add_or_update_replica(i, self.cluster.brokers[replica])
        partition.delete_replicas(len(partition_metadata['replicas']))

    def _update_topics_from_metadata(self, metadata, delete=False):
        """
        Given a Metadata response (either V0 or V1 will work), update the topic information
        for this cluster.

        Args:
            metadata (MetadataV1Response): A metadata response to create or update topics for
            delete (boolean): If True, delete topics from the cluster that are not present in the
                metadata response

        Raises:
            IndexError: If the brokers in the metadata object are not defined in the cluster
        """
        for t in metadata['topics']:
            if t['name'] not in self.cluster.topics:
                self.cluster.add_topic(Topic(t['name'], len(t['partitions'])))
            topic = self.cluster.topics[t['name']]
            topic._last_updated = time.time()

            topic.assure_has_partitions(len(t['partitions']))
            for p in t['partitions']:
                self._update_or_add_partition(p, topic.partitions[p['id']])

        self._maybe_delete_topics_not_in_metadata(metadata, delete)

    def _update_from_metadata(self, metadata, delete=False):
        """
        Given a metadata response, update both the brokers and topics from it. If specified, delete the topics
        that are not present in the provided metadata

        Args:
            metadata (MetadataV1Response): A metadata response to create or update brokers and topics for
            delete (boolean): If True, delete topics from the cluster that are not present in the metadata response
        """
        self._update_brokers_from_metadata(metadata)
        self._update_topics_from_metadata(metadata, delete=delete)

    def _maybe_update_metadata_for_topics(self, topics, cache=True):
        """
        Fetch metadata for the topics specified from the cluster if cache is False or if the cached metadata has expired.
        The cluster brokers and topics are updated with the metadata information.

        Args:
            cache (boolean): Ignore the metadata expiration and fetch from the cluster anyway

        Raises:
            ConnectionError: If there is a failure to send the request to all brokers in the cluster
        """
        force_update = not cache
        for topic in topics:
            try:
                if not self.cluster.topics[topic].updated_since(time.time() - self.configuration.metadata_refresh):
                    force_update = True
            except KeyError:
                force_update = True

        if force_update:
            self._update_from_metadata(self._send_any_broker(TopicMetadataV1Request({'topics': topics})), delete=False)

    def _maybe_update_full_metadata(self, cache=True):
        """
        Fetch all topic metadata from the cluster if cache is False or if the cached metadata has expired. The cluster
        brokers and topics are updated with the metadata information, potentially deleting ones that no longer exist

        Args:
            cache (boolean): Ignore the metadata expiration and fetch from the cluster anyway

        Raises:
            ConnectionError: If there is a failure to send the request to all brokers in the cluster
        """
        if (not cache) or (self._last_full_metadata < (time.time() - self.configuration.metadata_refresh)):
            self._update_from_metadata(self._send_any_broker(TopicMetadataV1Request({'topics': None})), delete=True)
            self._last_full_metadata = time.time()

    def _add_or_update_group(self, group_info, coordinator):
        """
        Given group information from a ListGroups response, assure that the group exists in the cluster as specified

        Args:
            group_info (dict): A group from a ListGroups response, which contains group_id and protocol_type keys
            coordinator (int): The ID of the group coordinator broker
        """
        group_name = group_info['group_id']
        try:
            group = self.cluster.groups[group_name]
        except KeyError:
            group = Group(group_name)
            self.cluster.add_group(group)
        group.coordinator = self.cluster.brokers[coordinator]
        group.protocol_type = group_info['protocol_type']

    def _update_groups_from_lists(self, responses):
        """
        Given a list of ListGroups responses, make sure that all the groups are in the cluster correctly

        Args:
            responses (dict): a mapping of broker IDs to ListGroupsV0Response instances from each broker

        Returns:
            int: a count of the number of responses that were not present or in error
        """
        error_counter = 0
        for broker_id, response in responses.items():
            if (response is None) or (response['error'] != 0):
                error_counter += 1
                continue

            for group_info in response['groups']:
                self._add_or_update_group(group_info, broker_id)

        return error_counter

    def _maybe_update_groups_list(self, cache=True):
        """
        Fetch lists of groups from all brokers if cache is False or if the cached group list has expired.

        Args:
            cache (boolean): Ignore the metadata expiration and fetch from the cluster anyway

        Returns:
            int: a count of the number of brokers that returned no response or error responses
        """
        if (not cache) or (self._last_group_list < (time.time() - self.configuration.metadata_refresh)):
            error_counter = self._update_groups_from_lists(self._send_all_brokers(ListGroupsV0Request({})))
            self._last_group_list = time.time()
            return error_counter
        return 0

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
            group = self.cluster.groups[g['group_id']]
            group.state = g['state']
            group.protocol_type = g['protocol_type']
            group.protocol = g['protocol']
            group.clear_members()
            for m in g['members']:
                group.add_member(m['member_id'],
                                 client_id=m['client_id'],
                                 client_host=m['client_host'],
                                 metadata=m['member_metadata'],
                                 assignment=m['member_assignment'])
            group._last_updated = time.time()

    def _map_topic_partitions_to_brokers(self, topic_list):
        """
        Given a list of topics, map the topic-partitions to the leader brokers

        Args:
            topic_list (list): a list of the topic name strings to map

        Returns:
            dict: a multi-dimensional dictionary where the keys are broker IDs and the values are a dictionary where the
                keys are topic names and the values are an array of partition IDs

        Raises:
            TopicError: if any of the topics do not exist in the cluster
        """
        broker_to_tp = {}
        for topic_name in topic_list:
            try:
                topic = self.cluster.topics[topic_name]
            except KeyError:
                raise TopicError("Topic {0} does not exist in the cluster".format(topic_name))

            for i, partition in enumerate(topic.partitions):
                broker_to_tp.setdefault(partition.leader.id, {}).setdefault(topic_name, []).append(i)

        return broker_to_tp

    def _get_topics_for_group(self, group, topic_list):
        """
        Given a group and a topic_list, return a list of topics that is either the topic list provided or the list of
        all topics consumed by the group

        Args:
            group (Group): the group for which the topic list is being created
            topic_list (list): A list of string topic names to fetch offsets for. Defaults to None, which specifies all
                topics that are subscribed to by the group.

        Returns:
             list: a list of topics to be fetched for the group

        Raises:
            GroupError: if the topic list is empty
        """
        # We'll be nice. If the topic_list is a string, convert it to a list
        if isinstance(topic_list, six.string_types):
            topic_list = [topic_list]

        # Create a list of topics to fetch
        fetch_topics = topic_list or group.subscribed_topics()
        if len(fetch_topics) == 0:
            raise GroupError("No topic specified is consumed by the group")
        return fetch_topics
