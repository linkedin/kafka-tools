# Copyright 2016 LinkedIn Corp.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import sys

"""
This file is run before ever load. We need to keep it lightweight and
bulletproof. Currently, we only setup logging, since it's used by everything.
"""

formatter = logging.Formatter('[%(levelname)s] %(message)s')

ch = logging.StreamHandler(sys.stderr)
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

log = logging.getLogger('kafka-tools')
log.setLevel(logging.INFO)
log.addHandler(ch)
