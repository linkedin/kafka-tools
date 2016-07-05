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
