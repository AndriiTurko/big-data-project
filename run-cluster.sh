docker network create project-network

docker run -d --name zookeeper-server --network project-network -e ALLOW_ANONYMOUS_LOGIN=yes bitnami/zookeeper:latest
docker run -d --name kafka-server --network project-network -e ALLOW_PLAINTEXT_LISTENER=yes -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 bitnami/kafka:latest

docker run -it --rm --network project-network -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 bitnami/kafka:latest kafka-topics.sh --create --bootstrap-server kafka-server:9092 --replication-factor 1 --partitions 3 --topic wikipedia-stream

docker run --name cassandra-node --network project-network -p 9042:9042 -d cassandra:latest
sleep 80
docker cp ./ddl.cql cassandra-node:/ddl.cql
docker exec -it cassandra-node cqlsh -f ./ddl.cql