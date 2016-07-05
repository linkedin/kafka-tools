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

from kafka.tools.assigner.exceptions import ProgrammingException


def split_partitions_into_batches(partitions, batch_size=10, use_class=None):
    # Currently, this is a very simplistic implementation that just breaks the list of partitions down
    # into even sized chunks. While it could be implemented as a generator, it's not so that it can
    # split the list into more efficient batches.
    if use_class is None:
        raise ProgrammingException("split_partitions_into_batches called with no use_class")

    batches = [use_class(partitions[i:i + batch_size]) for i in range(0, len(partitions), batch_size)]
    return batches
