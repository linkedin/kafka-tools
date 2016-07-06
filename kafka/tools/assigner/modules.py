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

import glob
import importlib
import inspect
import os


def is_class(klass):
    return inspect.isclass(klass) and (not inspect.isabstract(klass))


def check_class(klass, base_class):
    if not is_class(klass):
        return None
    base_classes = inspect.getmro(klass)
    if len(base_classes) == 1:
        return None
    if base_classes[1] == base_class:
        return klass


def check_classes_in_module(module, base_class):
    module_list = []
    for item in dir(module):
        value = getattr(module, item)
        if check_class(value, base_class) is not None:
            module_list.append(value)

    return module_list


def get_modules(base_package, base_class):
    module_list = []
    module_file_paths = glob.glob(os.path.join(base_package.__path__[0], "*.py"))
    for module_file_path in module_file_paths:
        module_filename = os.path.basename(module_file_path)
        module_name = os.path.splitext(module_filename)[0]
        if module_name.startswith("__"):
            continue

        # Import the module
        module = importlib.import_module("." + module_name, package=base_package.__name__)

        # Add all children of the base_class in the module
        module_list.extend(check_classes_in_module(module, base_class))

    return list(set(module_list))
