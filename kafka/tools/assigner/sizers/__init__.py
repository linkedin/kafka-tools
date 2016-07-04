# Base class that all sizers inherit from
class SizerModule(object):
    name = ""
    helpstr = ""

    ##########################################################################################################
    # OVERRIDE ME - These are all the things you can/must override in your child class
    def __init__(self, args, cluster):
        self.args = args
        self.cluster = cluster

    def get_partition_sizes(self):
        pass

    ##########################################################################################################
    # MODULE BASICS - You probably shouldn't modify past here unless you're changing the basic way all modules work
