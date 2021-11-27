import json
from kafka.tools.assigner.actions import ActionModule
from kafka.tools.assigner import file_path_checker
from kafka.tools.models.partition import Partition
from kafka.tools.exceptions import InvalidPlanFormatException


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
		"""
		Expecting plan will of type list of dict
		"""
		if not (isinstance(list, plan) and len(plan) > 0 and isinstance(dict, plan[0])):
			raise InvalidPlanFormatException()

    def process_cluster(self):
    	plan = json.load(open(args.plan_file_path))
    	self.__check_plan_format(plan)

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
