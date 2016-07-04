import kafka.tools.assigner.actions.balancemodules
from kafka.tools.assigner.actions import ActionModule, ActionBalanceModule
from kafka.tools.assigner.modules import get_modules


class ActionBalance(ActionModule):
    name = "balance"
    helpstr = "Rebalance partitions across the cluster"
    needs_sizes = True

    def __init__(self, args, cluster):
        super(ActionBalance, self).__init__(args, cluster)

        self.balance_types = dict((cls.name, cls) for cls in get_modules(kafka.tools.assigner.actions.balancemodules, ActionBalanceModule))

        # Initialize all the modules to use
        self.modules = []
        for bmodule in args.types:
            self.modules.append(self.balance_types[bmodule](args, cluster))

    @classmethod
    def _add_args(cls, parser):
        # We'll need to build this list here as well as in the constructor
        balance_actions = get_modules(kafka.tools.assigner.actions.balancemodules, ActionBalanceModule)
        parser.add_argument('-t', '--types', help="Balance types to perform. Multiple may be specified and they will be run in order", required=True,
                            choices=[klass.name for klass in balance_actions], nargs='*')

    def process_cluster(self):
        for bmodule in self.modules:
            bmodule.process_cluster()
