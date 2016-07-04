import argparse


# action_map is a map of names to ActionModule children - the top level actions that can be called
# sizer_map is a map of names to SizerModule children - the modules to get partition sizes
def set_up_arguments(action_map, sizer_map, plugins):
    # Configure basic arguments
    aparser = argparse.ArgumentParser(prog='kafka-assigner', description='Rejigger Kafka cluster partitions')
    aparser.add_argument('-z', '--zookeeper', help='Zookeeper path to the cluster (i.e. zk-eat1-kafka.corp:12913/kafka-data-deployment)', required=True)
    aparser.add_argument('-l', '--leadership', help="Show cluster leadership balance", action='store_true')
    aparser.add_argument('-g', '--generate', help="Generate partition reassignment file", action='store_true')
    aparser.add_argument('-e', '--execute', help="Execute partition reassignment", action='store_true')
    aparser.add_argument('-m', '--moves', help="Max number of moves per step", required=False, default=10, type=int)
    aparser.add_argument('-s', '--sizer', help="Select module to use to get partition sizes", required=False, default='ssh', choices=sizer_map.keys())
    aparser.add_argument('-d', '--datadir', help="Path to the data directory on the broker", required=False, default="/mnt/u001/kafka/i001_caches")
    aparser.add_argument('--skip-ple', help="Skip preferred replica election after finishing moves", action='store_true')
    aparser.add_argument('--ple-size', help="Max number of partitions in a single preferred leader election", required=False, default=2000, type=int)
    aparser.add_argument('--ple-wait', help="Time in seconds to wait between preferred leader elections", required=False, default=120, type=int)
    aparser.add_argument('--tools-path', help="Path to Kafka admin utilities, overriding PATH env var", required=False)

    # Call action module arg setup
    subparsers = aparser.add_subparsers(help='Select manipulation module to use')
    for action in action_map:
        action_map[action].configure_args(subparsers)

    # Call plugins for default arguments
    for plugin in plugins:
        plugin.set_default_arguments(aparser)

    # Parse the args that were passed
    args = aparser.parse_args()
    return args
