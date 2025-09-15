#!/bin/sh
set -e

TOPIC=${KAFKA_TOPIC:-user-events}
BOOTSTRAP=${KAFKA_HOST:-kafka:9092}
PARTITIONS=${KAFKA_PARTITIONS:-12}
REPLICATION=${KAFKA_REPLICATION:-1}

echo "Waiting for Kafka..."

# Ждём пока Kafka станет доступна
while ! kafka-topics --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1; do
  sleep 1
done

# Проверяем, есть ли топик
if ! kafka-topics --bootstrap-server "$BOOTSTRAP" --list | grep -q "^$TOPIC$"; then
  echo "Creating Kafka topic: $TOPIC"
  kafka-topics --bootstrap-server "$BOOTSTRAP" \
               --create \
               --topic "$TOPIC" \
               --partitions "$PARTITIONS" \
               --replication-factor "$REPLICATION"
else
  echo "Kafka topic $TOPIC already exists"
fi

echo "Kafka ready, starting API..."
exec "$@"
