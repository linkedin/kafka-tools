import math
from kafka.tools import log
from prettytable import PrettyTable
from kafka.tools.exceptions import BalanceException
from kafka.tools.assigner.actions import ActionBalanceModule


class ActionBalanceTopicPartitionInRack(ActionBalanceModule):
    name = "topic_partition_in_rack"
    helpstr = "Reassign topic partition replicas to assure they are almost equal distributed among brokers in a racks"

    def __init__(self, args, cluster):
        super(ActionBalanceTopicPartitionInRack, self).__init__(args, cluster)

    def __calculate_max_possible_count(self, broker_count):
        total_count = sum(map(lambda x: x[1], broker_count))
        count = total_count * 1.0 / len(broker_count)

        if (total_count % len(broker_count)) == 0:
            count = int(count)
        else:
            count = int(math.floor(count)) + 1
        return count

    def _unskew_topic_partition_in_rack(self, topic, partitions):
        """
        For given partitions of a topic as params applying following algorithm

        1. Grouping partitions based on rack id (getting rack id from broker).
        2. In a rack, sorting (reverse) brokers based on number of partitions present.
        3. Balancing partitions on brokers by moving partitions from left to right
        """

        # Creating a map of rack id, broker and count of partition on broker
        rack_broker_count, broker_partition = {}, {}
        for p in partitions:
            for broker in p.replicas:
                if broker.rack not in rack_broker_count:
                    rack_broker_count[broker.rack] = {}

                if broker.id not in rack_broker_count[broker.rack]:
                    rack_broker_count[broker.rack][broker.id] = 0

                if broker.id not in broker_partition:
                    broker_partition[broker.id] = set()

                broker_partition[broker.id].add(p)
                rack_broker_count[broker.rack][broker.id] += 1
        # For a rack, balancing partitions
        for rack_id in rack_broker_count:
            broker_count = sorted(
                map(
                    lambda x: list(x),
                    rack_broker_count[rack_id].items()
                ),
                key=lambda x: x[1],
                reverse=True
            )

            # calculating max possible number of partitions in a broker
            count = self.__calculate_max_possible_count(broker_count)

            # On sorted broker_count list,
            # balancing number of partitions from left to right
            left, right, adjusted_count = 0, len(broker_count) - 1, 0
            while left < right:
                bleft = broker_count[left]
                bid, bcount = bleft[0], bleft[1]
                adjusted_count = bcount - count
                if adjusted_count <= 0:
                    left += 1
                    continue

                for p in broker_partition[bid]:
                    bright = broker_count[right]
                    if bright[1] >= count:
                        right -= 1

                    if left >= right or adjusted_count == 0:
                        break

                    replicas = [b.id for b in p.replicas]
                    if bid in replicas and bright[0] not in replicas:
                        index = replicas.index(bid)
                        p.swap_replicas(p.replicas[index], self.cluster.brokers[bright[0]])
                        adjusted_count -= 1
                        bleft[1] -= 1
                        bright[1] += 1
                left += 1


    def _unskew_topic_partition_leader_in_rack(self, topic, partitions):

        # Creating a map of rack id, broker and count of partition leader on broker
        rack_broker_leader_count_map, broker_partition = {}, {}
        for p in partitions:
            for broker in p.replicas:
                if broker.rack not in rack_broker_leader_count_map:
                    rack_broker_leader_count_map[broker.rack] = {}

                if broker.id not in rack_broker_leader_count_map[broker.rack]:
                    rack_broker_leader_count_map[broker.rack][broker.id] = 0

                if p.replicas.index(broker) == 0:
                    rack_broker_leader_count_map[broker.rack][broker.id] += 1

                if broker.id not in broker_partition:
                    broker_partition[broker.id] = set()

                broker_partition[broker.id].add(p)

        # For a rack, balancing partitions leader 
        for rack_id in rack_broker_leader_count_map:
            broker_count = sorted(
                map(
                    lambda x: list(x),
                    rack_broker_leader_count_map[rack_id].items()
                ),
                key=lambda x:x[1],
                reverse=True
            )

            count = self.__calculate_max_possible_count(broker_count)
            left, right, adjusted_count = 0, len(broker_count) - 1, 0
            while  left < right:
                bleft = broker_count[left]
                bid, bcount = bleft[0], bleft[1]
                adjusted_count = bcount - count
                if adjusted_count <= 0:
                    left += 1
                    continue                

                for p in broker_partition[bid]:
                    bright = broker_count[right]
                    if bright[1] >= count:
                        right -= 1

                    if left >= right or adjusted_count == 0:
                        break

                    replicas = [b.id for b in p.replicas]
                    if not(bid in replicas and bright[0] not in replicas and replicas.index(bid) == 0):
                        continue

                    # Swaping leader from left side broker and non leader from right side broker
                    p.swap_replicas(p.replicas[0], self.cluster.brokers[bright[0]])
                    adjusted_count -= 1
                    bleft[1] -= 1
                    bright[1] += 1

                    for p2 in broker_partition[bright[0]]:
                        replicas = [b.id for b in p2.replicas]
                        if bright[0] in replicas and replicas.index(bright[0]) != 0 and bid not in replicas:
                            index2 = replicas.index(bright[0])
                            p2.swap_replicas(p2.replicas[index2], self.cluster.brokers[bid])
                            break
                left += 1

    def process_cluster(self):
        log.info("Starting {} module".format(self.name))

        # Check if rack information is set for the cluster
        for broker in self.cluster.brokers.values():
            if broker.rack:
                continue
            raise BalanceException("Cannot balance cluster by rack as it has no rack information")  

        for topic in self.cluster.topics:
            if topic in self.args.exclude_topics:
                log.debug("Skipping topic {0} as it is explicitly excluded".format(topic))
                continue

            before_rearrange = self.__create_count_map(
                self.cluster.topics[topic].partitions
            )
            self._unskew_topic_partition_in_rack(
                topic, self.cluster.topics[topic].partitions)

            after_rearrange = self.__create_count_map(
                self.cluster.topics[topic].partitions
            )
            self._unskew_topic_partition_leader_in_rack(
                topic, self.cluster.topics[topic].partitions)

            after_leader_rearrange = self.__create_count_map(
                self.cluster.topics[topic].partitions
            )

            self.__stats(
                topic,
                before_rearrange,
                after_rearrange,
                after_leader_rearrange
            )

    def __stats(self, topic, before, after, after_leader):
        table = PrettyTable()
        table.field_names = [
            "broker_id", "rack_id", "before(leader + follower = total)",
            "after(leader + follower = total)", "after_leader(leader + follower = total)"
        ]

        fmt = "{} + {} = {}"

        for _id in before:
            table.add_row([
                _id, self.cluster.brokers[_id].rack,
                fmt.format(
                    before[_id]["leader"], before[_id]["follower"],
                    before[_id]["leader"] + before[_id]["follower"]
                ),
                fmt.format(
                    after[_id]["leader"], after[_id]["follower"],
                    after[_id]["leader"] + after[_id]["follower"]
                ),
                fmt.format(
                    after_leader[_id]["leader"], after_leader[_id]["follower"],
                    after_leader[_id]["leader"] + after_leader[_id]["follower"]
                )
            ])
        log.info("\n" + table.get_string(sortby="rack_id"))


    def __create_count_map(self, partitions):
        """Create broker, leader & follower count dict."""
        count_map = {}
        for p in partitions:
            for i in range(len(p.replicas)):
                broker_id = p.replicas[i].id
                if broker_id not in count_map:
                    count_map[broker_id] = {"leader": 0, "follower": 0}
                if i == 0:
                    count_map[broker_id]["leader"] += 1
                else:
                    count_map[broker_id]["follower"] += 1
        return count_map
