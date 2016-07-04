from kafka.tools.assigner.actions.reorder import ActionReorder
from kafka.tools.assigner.actions import ActionBalanceModule


class ActionBalanceLeader(ActionBalanceModule):
    name = "leader"
    helpstr = "Balance the cluster leadership by reordering the partition replica lists"

    def __init__(self, args, cluster):
        super(ActionBalanceLeader, self).__init__(args, cluster)

        # This module merely calls the main "reorder" module, so we're going to instantiate a copy of that
        self._reorder = ActionReorder(args, cluster)

    def process_cluster(self):
        # Call the reorder module
        self._reorder.process_cluster()
