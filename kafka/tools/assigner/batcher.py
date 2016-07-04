from kafka.tools.assigner.exceptions import ProgrammingException


def split_partitions_into_batches(partitions, batch_size=10, use_class=None):
    # Currently, this is a very simplistic implementation that just breaks the list of partitions down
    # into even sized chunks. While it could be implemented as a generator, it's not so that it can
    # split the list into more efficient batches.
    if use_class is None:
        raise ProgrammingException("split_partitions_into_batches called with no use_class")

    batches = [use_class(partitions[i:i + batch_size]) for i in range(0, len(partitions), batch_size)]
    return batches
