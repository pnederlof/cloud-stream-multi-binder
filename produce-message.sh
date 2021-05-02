docker exec -it docker_kafka_1 /opt/kafka/bin/kafka-console-producer.sh \
--bootstrap-server localhost:9092 --topic process-in --property "parse.key=true" --property "key.separator=:"