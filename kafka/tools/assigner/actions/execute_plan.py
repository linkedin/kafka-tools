import json
from kafka.tools.assigner.actions import ActionModule
from kafka.tools.assigner.arguments import file_path_checker
from kafka.tools.models.partition import Partition
from kafka.tools.exceptions import InvalidPlanFormatException, UnknownBrokerException


class ActionExecutePlan(ActionModule):
    name = "execute_plan"
    helpstr = "Execute plan from given path"
    needs_sizes = False

    def __init__(self, args, cluster):
        super(ActionExecutePlan, self).__init__(args, cluster)

    @classmethod
    def _add_args(cls, parser):
        parser.add_argument(
            '--plan-file-path',
            required=True, 
            help="File path where kafka re-assign plan is stored in json format",
            type=file_path_checker
        )

    def __check_plan_format(self, plan):
        """Expecting plan will of type list of dict"""
        if not (isinstance(plan, list) and len(plan) > 0 and isinstance(plan[0], dict)):
            raise InvalidPlanFormatException()

        # Check all broker id in plan must be present in cluster
        brokers_in_plan, brokers_in_cluster = set(), set(self.cluster.brokers.keys())
        
        for partition_plan in plan:
            brokers_in_plan.update(partition_plan["replicas"])

        missing = list(brokers_in_plan - brokers_in_cluster)
        if missing:
            raise UnknownBrokerException(
                "Broker ids = {} in plan not present in cluster".format(str(missing))
            )

    def process_cluster(self):
        plan = json.load(open(self.args.plan_file_path))
        self.__check_plan_format(plan)

        # check broker exist in cluster or not in plan

        topic_track = {}

        for partition_plan in plan:
            if partition_plan['topic'] not in topic_track:
                self.cluster.topics[partition_plan["topic"]].partitions = []
                topic_track[partition_plan['topic']] = True

            topic = self.cluster.topics[partition_plan["topic"]]
            partition = Partition(topic, partition_plan["partition"])
            topic.partitions.append(partition)

            for replica in partition_plan["replicas"]:
                partition.add_replica(self.cluster.brokers[replica])
