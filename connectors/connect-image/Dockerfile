# Debezium Connect + MongoDB Kafka Connector. Wire format is JsonConverter;
# Avro + Schema Registry is a Connect-config flip when needed.

FROM debezium/connect:3.0.0.Final

ARG MONGO_CONNECTOR_VERSION=1.13.0

USER root

RUN mkdir -p /kafka/connect/mongodb-kafka-connect \
 && curl -fsSL \
      "https://repo1.maven.org/maven2/org/mongodb/kafka/mongo-kafka-connect/${MONGO_CONNECTOR_VERSION}/mongo-kafka-connect-${MONGO_CONNECTOR_VERSION}-all.jar" \
      -o /kafka/connect/mongodb-kafka-connect/mongo-kafka-connect.jar \
 && chown -R kafka:kafka /kafka/connect/mongodb-kafka-connect

USER kafka
