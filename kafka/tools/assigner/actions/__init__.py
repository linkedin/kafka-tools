# Base class that all kafka-assigner actions inherit from
class ActionModule(object):
    ##########################################################################################################
    # OVERRIDE ME - These are all the things you can/must override in your child class
    name = ""
    helpstr = ""
    needs_sizes = False

    def __init__(self, args, cluster):
        self.args = args
        self.cluster = cluster

    @classmethod
    def _add_args(cls, parser):
        pass

    def process_cluster(self):
        pass

    ##########################################################################################################
    # MODULE BASICS - You probably shouldn't modify past here unless you're changing the basic way all modules work

    @classmethod
    def configure_args(cls, subparser):
        if cls.name == "" or cls.helpstr == "":
            raise Exception("Cannot instantiate ActionModule directly")

        parser = subparser.add_parser(cls.name, help=cls.helpstr)
        cls._add_args(parser)
        parser.set_defaults(action=cls.name)


# Special class for balance modules to inherit from that skips configs
class ActionBalanceModule(ActionModule):
    @classmethod
    def configure_args(cls, subparser):
        pass
