import json
import time
from tempfile import NamedTemporaryFile

from kafka.tools.assigner import log


class Reassignment:
  def __init__(self, partitions):
    self.partitions = partitions

  def __repr__(self):
    reassignment = {'partitions': [], 'version': 1}
    for partition in self.partitions:
      reassignment['partitions'].append(partition.dict_for_move())
    return json.dumps(reassignment)

  def execute(self, num, total, zookeeper, tools_path, plugins=[], dry_run=True):
    for plugin in plugins:
      plugin.before_execute_batch(num)

    if not dry_run:
      with NamedTemporaryFile() as assignfile:
        assignment_json = repr(self)
        assignfile.write(assignment_json)
        assignfile.flush()
        commands.getoutput('{0}/kafka-reassign-partitions.sh --zookeeper {1} --execute --reassignment-json-file {2}'.format(tools_path,
                                                                                                                            zookeeper,
                                                                                                                            assignfile.name))

        failed_re = re.compile('.*Reassignment of partition.*?\s+failed', re.DOTALL)
        progress_re = re.compile('.*Reassignment of partition.*?\s+still\s+in\s+progress', re.DOTALL)

        # Loop waiting for reassignment to finish
        finished = False
        while not finished:
          verify_out = commands.getoutput('{0}/kafka-reassign-partitions.sh --zookeeper {1} --verify --reassignment-json-file {2}'.format(tools_path,
                                                                                                                                          zookeeper,
                                                                                                                                          assignfile.name))
          if (failed_re.match(verify_out)):
            log.error('Failed reassignment file: {0}'.format(json.dumps(assignment_json)))
            log.error('Reassignment tool status: {0}'.format(verify_out))
            sys.exit(1)
          elif (progress_re.match(verify_out)):
            completed_partitions = verify_out.count('completed successfully')
            total_partitions = verify_out.count('Reassignment of partition')
            remaining_partitions = total_partitions - completed_partitions
            log.info('Partition reassignment {0}/{1} in progress [ {2}/{3} partitions remain ]. Sleeping 10 seconds'.format(num,
                                                                                                                            total,
                                                                                                                            remaining_partitions,
                                                                                                                            total_partitions))
            time.sleep(10)
          else:
            finished = True

    for plugin in plugins:
      plugin.after_execute_batch(num)
