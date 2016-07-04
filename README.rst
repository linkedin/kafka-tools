Kafka Tools
===========

This repository is a collection of tools and scripts for working with
`Apache Kafka <http://kafka.apache.org>`. The Site Reliability team
for Kafka at LinkedIn has built these over time in order to make
managing Kafka a little bit easier. Our intention is to add to this
repository as more tools are developed, and we welcome additions and
modifications that make things better for all!

.. image:: https://travis-ci.org/linkedin/kafka-tools.svg
   :target: https://travis-ci.org/linkedin/kafka-tools.svg
   :alt: Build Status
.. image:: https://codeclimate.com/github/linkedin/kafka-tools/badges/gpa.svg
   :target: https://codeclimate.com/github/linkedin/kafka-tools
   :alt: Code Climate
.. image:: https://codeclimate.com/github/linkedin/kafka-tools/badges/coverage.svg
   :target: https://codeclimate.com/github/linkedin/kafka-tools/coverage
   :alt: Code Climate Test Coverage
.. image:: https://coveralls.io/repos/github/linkedin/kafka-tools/badge.svg?branch=master
   :target: https://coveralls.io/github/linkedin/kafka-tools?branch=master
   :alt: Coveralls Test Coverage
.. image:: https://codeclimate.com/github/linkedin/kafka-tools/badges/issue_count.svg
   :target: https://codeclimate.com/github/linkedin/kafka-tools
   :alt: Issue Count

Current Scripts
---------------

-  kafka-assigner - This script is used for performing partition
   reassignments and preferred replica elections. It uses the admin CLI
   utilities provided with Kafka and layers on additional logic to
   perform tasks like removing a broker from a cluster, and rebalancing
   partitions in a cluster according to several criteria.

Getting Started
---------------

Prerequisites
~~~~~~~~~~~~~

These tools are generally written in Python, and besides a basic
installation, you will need the following additional modules:

- Paramiko
- Kazoo

Usage
-----

For details about how to use these scripts effectively, please refer to
the linked wiki pages for each one

Other Projects
--------------

In addition to these tools, LinkedIn has also open-sourced
`Burrow <https://github.com/linkedin/Burrow>`, a robust system for
monitoring Kafka consumer clients.

License
-------

Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version
2.0 (the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
