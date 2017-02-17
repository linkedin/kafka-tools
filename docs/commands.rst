Command List
============

kafka-assigner
--------------

kafka-assigner is used for performing partition reassignments and preferred
replica elections. It uses the admin CLI utilities provided with Kafka and
layers on additional logic to perform tasks like removing a broker,
rebalancing partitions, fixing partition replication factors, and performing
preferred replica elections.
