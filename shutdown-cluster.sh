docker stop zookeeper-server kafka-server cassandra-server
docker rm -f zookeeper-server kafka-server cassandra-server
docker network rm project-network