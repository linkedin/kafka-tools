import logging
import sys

########################################################################################################
# Everything below here is run on load. Keep it light and bulletproof

# Set up logging, since it is pulled in by everything
log = logging.getLogger('kafka-assigner')
log.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('[%(levelname)s] %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)
