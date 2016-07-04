class PluginModule:
    def __init__(self):
        pass

    def set_default_arguments(self, argument_parser):
        pass

    def set_arguments(self, arguments):
        pass

    def set_cluster(self, cluster):
        pass

    def after_sizes(self):
        pass

    def set_new_cluster(self, cluster):
        pass

    def set_batches(self, batches):
        pass

    def before_execute_batch(self, num):
        pass

    def after_execute_batch(self, num):
        pass

    def before_ple(self):
        pass

    def finished(self):
        pass
